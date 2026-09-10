"""Regression tests for connection handling on single-connection pools.

Three related defects are covered here:

1. ``AsyncTable.find_one`` used to ``return`` from inside ``async for``, leaving
   the ``find()`` generator suspended on its yield. The event loop closed it
   later, in a separate task, and that close emitted a ROLLBACK.

2. ``find()``, ``query()``, ``distinct()`` and ``find_similar()`` held a
   connection across their yields, so an abandoned generator kept it checked out
   until finalization. Rows are now materialized before the first yield.

3. ``StaticPool`` - the pool SQLAlchemy picks for sqlite ":memory:" - hands the
   same DBAPI connection to every concurrent checkout, so a stray ROLLBACK, or
   any read landing inside another transaction, discarded uncommitted work.
   Checkouts on single-connection pools are now serialized.
"""
from __future__ import annotations

import asyncio
import gc
import threading
import time

import pytest
from sqlalchemy import text
from sqlalchemy.pool import StaticPool

from dbset import async_connect, connect
from dbset.exceptions import ConnectionError as DbsetConnectionError, SchemaError
import dbset.connection as connection_module
from dbset.connection import (
    _ambient_state,
    _AsyncSingleConnectionScope,
    _engine_scope,
    _needs_serialization,
)

MEMORY_URL = "sqlite+aiosqlite:///:memory:"
SYNC_MEMORY_URL = "sqlite:///:memory:"


def suspended_generators() -> int:
    """Count async generators that are alive and paused on a yield."""
    return len(
        [o for o in gc.get_objects() if type(o).__name__ == "async_generator" and o.ag_frame]
    )


@pytest.mark.asyncio
async def test_find_one_closes_generator():
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    before = suspended_generators()
    row = await table.find_one(job_id="j1")

    assert row["job_id"] == "j1"
    assert suspended_generators() == before


@pytest.mark.asyncio
async def test_find_releases_connection_before_yielding():
    """An abandoned find() must not keep a connection checked out."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    for i in range(5):
        await table.insert({"job_id": f"j{i}", "state": "open"})

    rows = table.find()
    await rows.__anext__()  # generator now suspended mid-iteration

    # The scope holds no connection while the generator is suspended.
    scope = _engine_scope(db._engine, _AsyncSingleConnectionScope)
    assert _ambient_state(db._engine.sync_engine) is None
    assert not scope._lock.locked()

    # ... and an unrelated write in another task goes through and survives.
    await asyncio.wait_for(
        asyncio.create_task(table.update({"state": "done"}, job_id="j0")),
        timeout=5,
    )
    del rows
    gc.collect()
    await asyncio.sleep(0)

    assert (await table.find_one(job_id="j0"))["state"] == "done"


@pytest.mark.asyncio
async def test_read_during_open_transaction_does_not_lose_the_write():
    """A read from another task must not discard an uncommitted UPDATE."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    in_transaction = asyncio.Event()

    async def writer() -> None:
        async with db._pool.acquire() as conn:
            await conn.execute(text("UPDATE jobs SET state='done' WHERE job_id='j1'"))
            in_transaction.set()  # signalled from inside the transaction
            await asyncio.sleep(0.05)  # window for the reader to interleave

    async def reader() -> None:
        await in_transaction.wait()
        await table.find_one(job_id="j1")

    await asyncio.gather(writer(), reader())

    assert (await table.find_one(job_id="j1"))["state"] == "done"


@pytest.mark.asyncio
async def test_concurrent_writes_are_not_lost_on_memory_db():
    """Interleaved read-modify-write tasks must all survive on StaticPool."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    for i in range(10):
        await table.insert({"job_id": f"j{i}", "state": "open", "n": 0})

    async def worker(i: int) -> None:
        for _ in range(5):
            row = await table.find_one(job_id=f"j{i}")
            await table.update({"state": "done", "n": row["n"] + 1}, job_id=f"j{i}")
            await asyncio.sleep(0)

    await asyncio.gather(*(worker(i) for i in range(10)))

    rows = [row async for row in table.find()]
    assert len(rows) == 10
    assert all(row["state"] == "done" and row["n"] == 5 for row in rows)


@pytest.mark.asyncio
async def test_child_task_inside_open_transaction_does_not_deadlock():
    """Tasks spawned inside a checkout inherit ownership of the guard."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    async def inside_transaction() -> None:
        async with db._pool.acquire() as conn:
            await conn.execute(text("UPDATE jobs SET state='done' WHERE job_id='j1'"))
            # A child task is a different task, but shares the context.
            await asyncio.gather(table.find_one(job_id="j1"))

    await asyncio.wait_for(inside_transaction(), timeout=10)

    assert (await table.find_one(job_id="j1"))["state"] == "done"


@pytest.mark.asyncio
async def test_write_inside_find_loop_does_not_deadlock():
    """find() holds no connection across yields, so writing in the loop works."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    for i in range(5):
        await table.insert({"job_id": f"j{i}", "state": "open"})

    async def iterate() -> int:
        seen = 0
        async for row in table.find():
            await table.update({"state": "seen"}, job_id=row["job_id"])
            seen += 1
        return seen

    assert await asyncio.wait_for(iterate(), timeout=10) == 5
    assert {row["state"] async for row in table.find()} == {"seen"}


def test_guard_survives_a_new_event_loop():
    """Engines outlive loops (pytest-asyncio, repeated asyncio.run())."""

    async def contended_round(db):
        table = db["jobs"]

        async def worker(i: int) -> None:
            row = await table.find_one(job_id=f"j{i}")
            await table.update({"n": row["n"] + 1}, job_id=f"j{i}")

        await asyncio.gather(*(worker(i) for i in range(5)))

    async def seed(db):
        for i in range(5):
            await db["jobs"].insert({"job_id": f"j{i}", "n": 0})

    loop = asyncio.new_event_loop()
    try:
        db = loop.run_until_complete(async_connect(MEMORY_URL, skip_null_columns=False))
        loop.run_until_complete(seed(db))
        loop.run_until_complete(contended_round(db))
    finally:
        loop.close()

    loop2 = asyncio.new_event_loop()
    try:
        loop2.run_until_complete(contended_round(db))
        rows = loop2.run_until_complete(_collect(db))
    finally:
        loop2.close()

    assert len(rows) == 5
    assert all(row["n"] == 2 for row in rows)


async def _collect(db):
    return [row async for row in db["jobs"].find()]


def test_sync_static_pool_writes_are_not_lost_across_threads():
    """StaticPool + check_same_thread=False is the documented sqlite recipe."""
    db = connect(
        SYNC_MEMORY_URL,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
        skip_null_columns=False,
    )
    table = db["jobs"]
    for i in range(5):
        table.insert({"job_id": f"j{i}", "state": "open", "n": 0})

    errors: list[BaseException] = []
    barrier = threading.Barrier(5)

    def worker(i: int) -> None:
        try:
            barrier.wait(timeout=10)
            for _ in range(5):
                row = table.find_one(job_id=f"j{i}")
                table.update({"state": "done", "n": row["n"] + 1}, job_id=f"j{i}")
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors
    rows = list(table.find())
    assert len(rows) == 5
    assert all(row["state"] == "done" and row["n"] == 5 for row in rows)


def test_sync_read_during_open_transaction_does_not_lose_the_write():
    """Same defect, sync API: a read from another thread must not roll back."""
    db = connect(
        SYNC_MEMORY_URL,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
        skip_null_columns=False,
    )
    table = db["jobs"]
    table.insert({"job_id": "j1", "state": "open"})

    in_transaction = threading.Event()

    def writer() -> None:
        with db._pool.acquire() as conn:
            conn.execute(text("UPDATE jobs SET state='done' WHERE job_id='j1'"))
            in_transaction.set()
            time.sleep(0.2)

    def reader() -> None:
        in_transaction.wait(timeout=10)
        table.find_one(job_id="j1")

    threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert table.find_one(job_id="j1")["state"] == "done"


@pytest.mark.asyncio
async def test_serialization_applies_only_to_single_connection_pools(tmp_path):
    """File-backed sqlite gets a real pool, so nothing is serialized there."""
    file_db = await async_connect(f"sqlite+aiosqlite:///{tmp_path}/db.sqlite")
    assert not _needs_serialization(file_db._engine)

    memory_db = await async_connect(MEMORY_URL)
    assert _needs_serialization(memory_db._engine)


@pytest.mark.asyncio
async def test_schema_ddl_shares_the_connection_scope():
    """Schema DDL opens connections directly, bypassing the pool wrapper."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    in_transaction = asyncio.Event()

    async def writer() -> None:
        async with db._pool.acquire() as conn:
            await conn.execute(text("UPDATE jobs SET state='done' WHERE job_id='j1'"))
            in_transaction.set()
            await asyncio.sleep(0.05)

    async def ddl() -> None:
        await in_transaction.wait()
        # reflect() opens a connection straight on the engine
        await db._schema.reflect()

    await asyncio.gather(writer(), ddl())

    assert (await table.find_one(job_id="j1"))["state"] == "done"


@pytest.mark.asyncio
async def test_abandoned_scope_generator_leaves_the_engine_usable():
    """Finalization runs in a foreign context; it must not wedge the engine."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    async def wrapper():
        async with db._pool.connect() as conn:
            yield conn  # abandoned below, finalized by the event loop

    gen = wrapper()
    await gen.__anext__()
    del gen
    gc.collect()
    await asyncio.sleep(0.05)

    # The engine still works, and from a task that did not inherit the context.
    row = await asyncio.wait_for(
        asyncio.create_task(table.find_one(job_id="j1")), timeout=5
    )
    assert row["state"] == "open"
    await asyncio.wait_for(
        asyncio.create_task(table.update({"state": "done"}, job_id="j1")), timeout=5
    )
    assert (await table.find_one(job_id="j1"))["state"] == "done"


def test_abandoned_sync_scope_generator_leaves_the_engine_usable():
    """Sync cleanup can run on a foreign thread; it must not wedge the engine."""
    db = connect(
        SYNC_MEMORY_URL,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
        skip_null_columns=False,
    )
    table = db["jobs"]
    table.insert({"job_id": "j1", "state": "open"})

    holder = {}

    def wrapper():
        with db._pool.connect() as conn:
            yield conn

    def open_scope():
        gen = wrapper()
        next(gen)
        holder["gen"] = gen  # keep it alive past this thread's death

    opener = threading.Thread(target=open_scope)
    opener.start()
    opener.join(timeout=10)

    holder.clear()  # finalized here, on the main thread
    gc.collect()

    result = {}

    def reader():
        try:
            result["row"] = table.find_one(job_id="j1")
        except BaseException as exc:  # noqa: BLE001 - asserted below
            result["error"] = exc

    thread = threading.Thread(target=reader)
    thread.start()
    thread.join(timeout=10)

    assert "error" not in result, result.get("error")
    assert result["row"]["state"] == "open"


@pytest.mark.asyncio
async def test_transaction_commits_and_rolls_back():
    """transaction() yields a connection that commits or rolls back as a unit.

    Only statements run on the yielded connection are pinned by this test.
    Whether table methods called inside the block join the transaction is
    pool-dependent (they do on a single-connection pool, they do not on a real
    one); that gap predates this change.
    """
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "seed", "state": "open"})

    async with db.transaction() as conn:
        await conn.execute(text("UPDATE jobs SET state='kept' WHERE job_id='seed'"))

    assert (await table.find_one(job_id="seed"))["state"] == "kept"

    with pytest.raises(RuntimeError):
        async with db.transaction() as conn:
            await conn.execute(text("UPDATE jobs SET state='dropped' WHERE job_id='seed'"))
            raise RuntimeError("boom")

    assert (await table.find_one(job_id="seed"))["state"] == "kept"


def test_sync_transaction_commits_and_rolls_back(tmp_path):
    db = connect(f"sqlite:///{tmp_path}/db.sqlite", skip_null_columns=False)
    table = db["jobs"]
    table.insert({"job_id": "seed", "state": "open"})

    with db.transaction() as conn:
        conn.execute(text("UPDATE jobs SET state='kept' WHERE job_id='seed'"))

    assert table.find_one(job_id="seed")["state"] == "kept"

    with pytest.raises(RuntimeError):
        with db.transaction() as conn:
            conn.execute(text("UPDATE jobs SET state='dropped' WHERE job_id='seed'"))
            raise RuntimeError("boom")

    assert table.find_one(job_id="seed")["state"] == "kept"


@pytest.mark.asyncio
async def test_caller_exceptions_are_not_rewritten():
    """An error raised inside a checkout must reach the caller unchanged."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    await db["jobs"].insert({"job_id": "j1", "state": "open"})

    with pytest.raises(RuntimeError, match="boom"):
        async with db._pool.acquire():
            raise RuntimeError("boom")


def test_singleton_thread_pool_is_not_serialized_across_threads():
    """Each thread gets its own connection there - only nesting needs handling."""
    db = connect(SYNC_MEMORY_URL, skip_null_columns=False)
    db["jobs"].insert({"job_id": "j1", "state": "open"})  # creates the scope

    assert type(db._engine.pool).__name__ == "SingletonThreadPool"
    scope = _engine_scope(
        db._engine, lambda: pytest.fail("scope was not created by the pool path")
    )
    assert scope._lock is None


def test_static_pool_scope_is_serialized_across_threads():
    db = connect(
        SYNC_MEMORY_URL,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    db["jobs"].insert({"job_id": "j1", "state": "open"})

    scope = _engine_scope(
        db._engine, lambda: pytest.fail("scope was not created by the pool path")
    )
    assert scope._lock is not None


@pytest.mark.asyncio
async def test_reuse_right_after_abandoning_a_scope():
    """The next call must not nest onto a connection about to be finalized."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    await table.insert({"job_id": "j1", "state": "open"})

    async def wrapper():
        async with db._pool.connect() as conn:
            yield conn

    gen = wrapper()
    await gen.__anext__()
    del gen  # finalizer scheduled, not run yet - same context keeps the state

    # No gc.collect(), no sleep: this is the window the nesting counter covers.
    row = await table.find_one(job_id="j1")
    assert row["state"] == "open"

    await asyncio.sleep(0.05)
    await table.update({"state": "done"}, job_id="j1")
    assert (await table.find_one(job_id="j1"))["state"] == "done"


def test_loop_closed_with_a_scope_still_open_reports_the_lost_database():
    """The ":memory:" database dies with the connection - say so, once, loudly.

    StaticPool terminates the abandoned connection and opens a fresh, empty
    database in its place. The first use after that raises; the engine is then
    usable again against the new (empty) database rather than wedged.
    """

    async def open_and_abandon(db):
        async def wrapper():
            async with db._pool.connect() as conn:
                yield conn

        gen = wrapper()
        await gen.__anext__()
        return gen  # deliberately left suspended when the loop closes

    loop = asyncio.new_event_loop()
    try:
        db = loop.run_until_complete(async_connect(MEMORY_URL, skip_null_columns=False))
        loop.run_until_complete(db["jobs"].insert({"job_id": "j1", "state": "open"}))
        held = loop.run_until_complete(open_and_abandon(db))
    finally:
        loop.close()  # no shutdown_asyncgens()

    del held
    gc.collect()

    async def use(db):
        await db["jobs"].insert({"job_id": "j2", "state": "open"})
        return await db["jobs"].find_one(job_id="j2")

    loop2 = asyncio.new_event_loop()
    try:
        # The schema layer wraps it, so match on the message rather than the type.
        with pytest.raises((DbsetConnectionError, SchemaError), match="was lost"):
            loop2.run_until_complete(use(db))

        row = loop2.run_until_complete(use(db))
    finally:
        loop2.close()

    assert row["state"] == "open"


def test_sync_write_inside_a_read_scope_is_durable():
    """A nested write must not be rolled back by the enclosing read scope."""
    db = connect(SYNC_MEMORY_URL, skip_null_columns=False)
    table = db["jobs"]
    table.insert({"job_id": "j1", "state": "open"})

    with db._pool.connect():  # non-transactional outer scope
        table.update({"state": "done"}, job_id="j1")

    assert table.find_one(job_id="j1")["state"] == "done"


@pytest.mark.asyncio
async def test_checkout_retries_when_another_loop_swaps_the_lock(monkeypatch):
    """A lock swapped while we waited is handed back, not held alongside the new one."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    await db["jobs"].insert({"job_id": "j1", "state": "open"})

    scope = _engine_scope(db._engine, _AsyncSingleConnectionScope)
    original_acquire = connection_module._acquire_within
    swapped = []

    async def swap_once(lock, timeout):
        await original_acquire(lock, timeout)
        if not swapped:
            # Stand in for another thread's loop rebinding the engine lock while
            # this caller was waiting on the old one.
            swapped.append(lock)
            scope._lock = asyncio.Lock()

    monkeypatch.setattr(connection_module, "_acquire_within", swap_once)

    row = await asyncio.wait_for(db["jobs"].find_one(job_id="j1"), timeout=10)

    assert row["state"] == "open"
    assert swapped, "the test did not exercise a swap"
    assert not swapped[0].locked()  # the stale lock was handed back
    assert not scope._lock.locked()


@pytest.mark.asyncio
async def test_checkout_gives_up_when_the_lock_keeps_being_swapped(monkeypatch):
    """The retry is bounded: a pathological swap loop ends in ConnectionError."""
    db = await async_connect(MEMORY_URL, skip_null_columns=False)
    await db["jobs"].insert({"job_id": "j1", "state": "open"})

    scope = _engine_scope(db._engine, _AsyncSingleConnectionScope)
    original_acquire = connection_module._acquire_within
    attempts = []

    async def swap_always(lock, timeout):
        await original_acquire(lock, timeout)
        attempts.append(lock)
        scope._lock = asyncio.Lock()

    monkeypatch.setattr(connection_module, "_acquire_within", swap_always)

    with pytest.raises((DbsetConnectionError, SchemaError), match="Timed out waiting"):
        await asyncio.wait_for(db["jobs"].find_one(job_id="j1"), timeout=10)

    assert len(attempts) == connection_module._LOCK_REBIND_ATTEMPTS
    assert not any(lock.locked() for lock in attempts)
