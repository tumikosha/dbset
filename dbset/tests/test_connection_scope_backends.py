"""Connection-scope behaviour checked against every backend, not just sqlite.

The lost-write defect these tests came from is specific to single-connection
pools, but the fixes touch transaction handling for every pool. PostgreSQL is
where a real pool, real savepoints and a real driver disagree with sqlite, so it
gets the same scenarios.

Set DBSET_PG_URL (or DB_URL / DATABASE_URL) to a PostgreSQL instance to run the
PostgreSQL half, e.g.:

    docker run -d --name dbset-pg -e POSTGRES_PASSWORD=dbset -e POSTGRES_USER=dbset \\
        -e POSTGRES_DB=dbset -p 55432:5432 pgvector/pgvector:pg16
    DBSET_PG_URL=postgresql://dbset:dbset@127.0.0.1:55432/dbset pytest
"""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from dbset import async_connect, connect

_PG_URL = (
    os.environ.get("DBSET_PG_URL")
    or os.environ.get("DB_URL")
    or os.environ.get("DATABASE_URL")
    or ""
)

#: MySQL is checked the same way when DBSET_MYSQL_URL points at one, e.g.
#: docker run -d -e MYSQL_ROOT_PASSWORD=dbset -e MYSQL_DATABASE=dbset \
#:     -e MYSQL_USER=dbset -e MYSQL_PASSWORD=dbset -p 33306:3306 mysql:8
_MYSQL_URL = os.environ.get("DBSET_MYSQL_URL", "")


def _mysql(driver: str) -> str:
    """Return the MySQL URL with the given driver, or "" if not configured."""
    if not _MYSQL_URL or "mysql" not in _MYSQL_URL:
        return ""
    _, _, rest = _MYSQL_URL.partition("://")
    return f"mysql+{driver}://{rest}"


def _pg(driver: str) -> str:
    """Return the PostgreSQL URL with the given driver, or "" if not configured."""
    if not _PG_URL or "postgres" not in _PG_URL:
        return ""
    _, _, rest = _PG_URL.partition("://")
    return f"postgresql+{driver}://{rest}"


ASYNC_URLS = [
    pytest.param("sqlite+aiosqlite:///:memory:", id="sqlite-memory"),
    pytest.param("sqlite-file", id="sqlite-file"),
    pytest.param(
        _pg("asyncpg") or "",
        id="postgres",
        marks=pytest.mark.skipif(not _pg("asyncpg"), reason="DBSET_PG_URL not set"),
    ),
    pytest.param(
        _mysql("aiomysql") or "",
        id="mysql",
        marks=pytest.mark.skipif(not _mysql("aiomysql"), reason="DBSET_MYSQL_URL not set"),
    ),
]

SYNC_URLS = [
    pytest.param("sqlite:///:memory:", id="sqlite-memory"),
    pytest.param("sqlite-file", id="sqlite-file"),
    pytest.param(
        _pg("psycopg2") or "",
        id="postgres",
        marks=pytest.mark.skipif(not _pg("psycopg2"), reason="DBSET_PG_URL not set"),
    ),
    pytest.param(
        _mysql("pymysql") or "",
        id="mysql",
        marks=pytest.mark.skipif(not _mysql("pymysql"), reason="DBSET_MYSQL_URL not set"),
    ),
]


def _resolve(url: str, tmp_path, sync: bool) -> str:
    if url != "sqlite-file":
        return url
    prefix = "sqlite" if sync else "sqlite+aiosqlite"
    return f"{prefix}:///{tmp_path}/db.sqlite"


def _table_name() -> str:
    """PostgreSQL keeps its tables between runs, so use a fresh name each time."""
    return f"scope_{uuid.uuid4().hex[:12]}"


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_transaction_is_atomic_for_table_operations(url, tmp_path):
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "seed"})

    async with db.transaction():
        await table.insert({"k": "kept"})

    with pytest.raises(RuntimeError):
        async with db.transaction():
            await table.insert({"k": "dropped"})
            raise RuntimeError("boom")

    assert sorted([row["k"] async for row in table.find()]) == ["kept", "seed"]
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_write_inside_a_read_scope_is_durable(url, tmp_path):
    """A read scope autocommits, so a write nested in it stands on its own."""
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "j1", "state": "open"})

    async with db._pool.connect() as conn:  # non-transactional outer scope
        await conn.execute(await _select_one(table))
        await table.update({"state": "done"}, k="j1")

    assert (await table.find_one(k="j1"))["state"] == "done"
    await db.close()


async def _select_one(table):
    from sqlalchemy import select

    return select(await table._get_table()).limit(1)


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_streaming_reads_the_whole_result(url, tmp_path):
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert_many([{"n": i} for i in range(50)])

    streamed = [row["n"] async for row in table.find(_stream=True, _order_by="n")]
    buffered = [row["n"] async for row in table.find(_order_by="n")]
    streamed_all = sorted([row["n"] async for row in table.all(_stream=True)])

    assert streamed == buffered == streamed_all == list(range(50))
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_caller_exceptions_are_not_rewritten(url, tmp_path):
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    await db[_table_name()].insert({"k": "j1"})

    with pytest.raises(RuntimeError, match="boom"):
        async with db._pool.acquire():
            raise RuntimeError("boom")

    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_concurrent_read_modify_write_keeps_every_update(url, tmp_path):
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    for i in range(10):
        await table.insert({"k": f"j{i}", "n": 0})

    async def worker(i: int) -> None:
        for _ in range(5):
            row = await table.find_one(k=f"j{i}")
            await table.update({"n": row["n"] + 1}, k=f"j{i}")
            await asyncio.sleep(0)

    await asyncio.gather(*(worker(i) for i in range(10)))

    rows = [row async for row in table.find()]
    assert len(rows) == 10
    assert all(row["n"] == 5 for row in rows)
    await db.close()


@pytest.mark.parametrize("url", SYNC_URLS)
def test_sync_transaction_is_atomic_for_table_operations(url, tmp_path):
    db = connect(_resolve(url, tmp_path, sync=True), skip_null_columns=False)
    table = db[_table_name()]
    table.insert({"k": "seed"})

    with db.transaction():
        table.insert({"k": "kept"})

    with pytest.raises(RuntimeError):
        with db.transaction():
            table.insert({"k": "dropped"})
            raise RuntimeError("boom")

    assert sorted(row["k"] for row in table.find()) == ["kept", "seed"]
    db.close()


@pytest.mark.parametrize("url", SYNC_URLS)
def test_sync_streaming_reads_the_whole_result(url, tmp_path):
    db = connect(_resolve(url, tmp_path, sync=True), skip_null_columns=False)
    table = db[_table_name()]
    table.insert_many([{"n": i} for i in range(50)])

    streamed = [row["n"] for row in table.find(_stream=True, _order_by="n")]
    assert streamed == list(range(50))
    db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_a_failed_operation_does_not_poison_the_transaction(url, tmp_path):
    """A failed nested operation undoes itself and leaves the block usable.

    On backends with real savepoints the nested block gets its own rollback
    boundary, so a caught error does not poison the transaction; on sqlite,
    where pysqlite releases a savepoint as a commit, nested work joins the
    transaction and the driver keeps going after the error. Same outcome
    either way, which is the point.
    """
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "seed"})

    block_raised = False
    try:
        async with db.transaction():
            await table.insert({"k": "a"})
            try:
                await table.insert({"k": "a", "id": 1})  # duplicate primary key
            except Exception:
                pass  # swallowed on purpose - the transaction is still doomed
            await table.insert({"k": "b"})
    except Exception:
        block_raised = True

    committed = sorted([row["k"] async for row in table.find()])
    assert not block_raised
    assert committed == ["a", "b", "seed"], committed
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_nested_write_joins_the_ambient_transaction(url, tmp_path):
    """A nested write is part of the ambient transaction, so it rolls back with it."""
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "j1", "state": "open"})

    with pytest.raises(RuntimeError):
        async with db._pool.acquire(ambient=True):
            await table.update({"state": "done"}, k="j1")
            assert (await table.find_one(k="j1"))["state"] == "done"
            raise RuntimeError("boom")

    assert (await table.find_one(k="j1"))["state"] == "open"

    async with db._pool.acquire(ambient=True):
        await table.update({"state": "done"}, k="j1")

    assert (await table.find_one(k="j1"))["state"] == "done"
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_ambient_connection_is_returned_to_the_pool(url, tmp_path):
    """Leaving the block must check the connection back in, not leak it."""
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "seed"})

    async with db.transaction():
        await table.insert({"k": "kept"})

    pool = db._engine.pool
    if hasattr(pool, "checkedout"):  # StaticPool has no such notion
        assert pool.checkedout() == 0

    assert sorted([row["k"] async for row in table.find()]) == ["kept", "seed"]
    await db.close()


def test_memory_database_detection():
    """The lost-database check has to recognise every in-memory spelling."""
    from sqlalchemy import create_engine

    from dbset.connection import _is_memory_database

    memory = [
        "sqlite://",
        "sqlite:///:memory:",
        "sqlite:///file:memdb1?mode=memory&cache=shared&uri=true",
        "sqlite:///file:memdb2?mode=memory&mode=rw&uri=true",  # repeated key -> tuple
    ]
    for url in memory:
        engine = create_engine(url)
        try:
            assert _is_memory_database(engine), url
        finally:
            engine.dispose()

    engine = create_engine("sqlite:////tmp/dbset-not-memory.sqlite")
    try:
        assert not _is_memory_database(engine)
    finally:
        engine.dispose()


@pytest.mark.parametrize("url", SYNC_URLS)
def test_sync_streaming_does_not_leak_execution_options(url, tmp_path):
    """execution_options() on a Connection is sticky - it must not be used."""
    db = connect(_resolve(url, tmp_path, sync=True), skip_null_columns=False)
    table = db[_table_name()]
    table.insert_many([{"n": i} for i in range(5)])

    with db._pool.acquire(ambient=True) as conn:
        list(table.find(_stream=True))
        assert "stream_results" not in conn.get_execution_options()

    db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ASYNC_URLS)
async def test_closing_the_transaction_connection_is_reported(url, tmp_path):
    """Reopening under an open transaction would lose it silently - say so."""
    db = await async_connect(_resolve(url, tmp_path, sync=False), skip_null_columns=False)
    table = db[_table_name()]
    await table.insert({"k": "seed"})

    pool = db._engine.pool
    before = pool.checkedout() if hasattr(pool, "checkedout") else 0

    with pytest.raises(Exception, match="no longer exists"):
        async with db._pool.acquire(ambient=True) as conn:
            await conn.close()
            await table.insert({"k": "reopened"})

    if hasattr(pool, "checkedout"):
        assert pool.checkedout() == before  # nothing leaked

    assert [row["k"] async for row in table.find()] == ["seed"]
    await db.close()
