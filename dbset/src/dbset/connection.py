"""Connection pooling and management for sync and async engines."""
from __future__ import annotations

import asyncio
import contextvars
import threading
import weakref
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator

from sqlalchemy import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.pool import SingletonThreadPool, StaticPool

from .exceptions import ConnectionError

#: How long a caller waits for the shared connection of a single-connection pool
#: before giving up with a ConnectionError instead of hanging forever.
CHECKOUT_LOCK_TIMEOUT = 30.0

#: Pools that hand out one connection per checkout scope instead of a real pool:
#: StaticPool (one connection for the whole engine, chosen for sqlite ":memory:")
#: and SingletonThreadPool (one connection per thread).
_SINGLE_CONNECTION_POOLS = (StaticPool, SingletonThreadPool)



class _ScopeState:
    """The connection one scope currently owns, plus how it is being used."""

    __slots__ = ("conn", "active", "nested", "transactional", "engine_key")

    def __init__(self, conn, transactional: bool, engine_key=None) -> None:
        self.conn = conn
        #: The engine this state belongs to. The ambient map is keyed by id(),
        #: and an address freed by a collected engine can be handed to a new
        #: one, so readers confirm identity against this.
        self.engine_key = engine_key
        self.active = True
        #: Whether the outermost block opened a transaction of its own. A nested
        #: transactional block inside a non-transactional scope has to commit
        #: explicitly, or the scope's close would roll it back.
        self.transactional = transactional
        #: Nested blocks currently running on this connection. The outermost
        #: block must not close the connection out from under them, which can
        #: happen when it is abandoned and finalized while a nested block is
        #: still live.
        self.nested = 0


#: The connection each engine has open in the current scope, as
#: {engine key id: _ScopeState}. One module-level ContextVar rather than one per
#: engine: contexts hold strong references to their variables, so a per-engine
#: variable would outlive the engine it belongs to.
_AMBIENT: contextvars.ContextVar = contextvars.ContextVar(
    "dbset_ambient_connections", default=None
)


def _engine_key(engine: AsyncEngine | Engine):
    """Return the object that identifies this engine across sync/async wrappers."""
    return engine.sync_engine if isinstance(engine, AsyncEngine) else engine


def _ambient_state(key) -> _ScopeState | None:
    """Return the live connection state published for this engine, if any."""
    ambient = _AMBIENT.get()
    if not ambient:
        return None
    state = ambient.get(id(key))
    if state is None or not state.active or state.engine_key is not key:
        return None
    return state


def _publish(key, state: _ScopeState):
    """Publish a connection for nested blocks in this context to reuse."""
    ambient = _AMBIENT.get()
    updated = dict(ambient) if ambient else {}
    updated[id(key)] = state
    return _AMBIENT.set(updated)


def _unpublish(token) -> None:
    """Undo _publish(), tolerating a cleanup that runs in a foreign context."""
    try:
        _AMBIENT.reset(token)
    except ValueError:
        # Finalization can run in a different context than the one that
        # published. The state's active flag is what readers check, so a stale
        # mapping is harmless.
        pass


#: How many times a checkout retries when another event loop swaps the engine
#: lock underneath it. The wait as a whole is still bounded by
#: CHECKOUT_LOCK_TIMEOUT; this only stops a pathological swap loop from
#: spinning.
_LOCK_REBIND_ATTEMPTS = 8


async def _acquire_within(lock: asyncio.Lock, timeout: float) -> None:
    """
    Acquire the lock within `timeout` seconds, raising asyncio.TimeoutError.

    asyncio.timeout() is used where available: on Python 3.11 asyncio.wait_for()
    swallows a cancellation that arrives just as the lock is granted, which
    would leave a cancelled caller looping instead of unwinding.
    """
    if hasattr(asyncio, "timeout"):
        async with asyncio.timeout(timeout):
            await lock.acquire()
        return
    await asyncio.wait_for(lock.acquire(), timeout)


def _dbapi_connection_of(conn):
    """Return the DBAPI connection behind a pooled connection, or None."""
    raw = getattr(conn, "sync_connection", conn)
    pooled = getattr(raw, "connection", None)
    return getattr(pooled, "dbapi_connection", None)


def _identity_ref(obj):
    """
    Return a callable that yields obj while it is worth comparing against.

    A weak reference where the object allows one - some DBAPI wrappers use
    __slots__ and cannot be referenced weakly, and those get a strong reference
    instead. Either way the point is the same: comparing identity against a
    stored id() would alias, because a freed address gets reused.
    """
    try:
        return weakref.ref(obj)
    except TypeError:
        return lambda: obj


class _AsyncSingleConnectionScope:
    """
    One connection per engine, shared by everything running inside one scope.

    On a single-connection pool a second checkout is not a second connection: it
    is the same DBAPI connection handed out twice. Closing either copy emits a
    ROLLBACK that discards whatever the other one had not committed yet. So
    instead of opening a second connection, a nested checkout reuses the one the
    scope already holds, and only the outermost one closes it.

    "Scope" is a ContextVar, not the task: asyncio copies the current context
    into every task at creation, so a task spawned inside a checkout keeps
    working on the same connection instead of deadlocking against it, while
    unrelated tasks wait for the lock.

    The lock is rebuilt when the scope is used from a different event loop while
    idle, because asyncio.Lock binds to the loop it first blocks on and engines
    routinely outlive loops (pytest-asyncio, repeated asyncio.run()).
    """

    def __init__(self, memory_database: bool = False) -> None:
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._bind_guard = threading.Lock()
        #: sqlite ":memory:" lives inside its DBAPI connection. If the pool ever
        #: replaces that connection, the database is gone and a fresh empty one
        #: takes its place - worth reporting rather than silently continuing.
        self._memory_database = memory_database
        #: A reference, not id(): a recycled address would alias as "same".
        self._dbapi_ref = None
        #: Whether a connection was ever seen, so a reference that came back
        #: empty reads as "replaced" rather than "first time".
        self._dbapi_seen = False

    def _bind_loop(self) -> None:
        """Rebind to the running loop if this scope is idle and was used elsewhere."""
        loop = asyncio.get_running_loop()
        if self._loop is loop:
            return
        # A closed previous loop can still hold the lock: a scope suspended when
        # the loop was closed never ran its cleanup. Nothing can be waiting on
        # that lock any more, so take a fresh one.
        if not self._lock.locked() or (self._loop is not None and self._loop.is_closed()):
            self._lock = asyncio.Lock()
            self._loop = loop
            return
        raise ConnectionError(
            "The database engine is in use by another event loop that is still "
            "running. A single-connection engine (sqlite ':memory:') cannot be "
            "shared between concurrently running event loops."
        )

    async def _acquire_lock(self) -> asyncio.Lock:
        """Take the engine lock, returning the exact lock object that was taken."""
        deadline = asyncio.get_running_loop().time() + CHECKOUT_LOCK_TIMEOUT
        for _ in range(_LOCK_REBIND_ATTEMPTS):
            # Bind and read under one mutex, then check after acquiring that the
            # lock is still the engine's: another thread's loop may have swapped
            # it in between, and two callers holding different locks would run
            # on the same connection at once.
            with self._bind_guard:
                self._bind_loop()
                lock = self._lock

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                await _acquire_within(lock, remaining)
            except asyncio.TimeoutError:
                break

            with self._bind_guard:
                if self._lock is lock:
                    return lock
            # Another loop swapped the lock while we waited: hand this one back
            # and try again on whatever the engine holds now.
            lock.release()

        raise ConnectionError(
            "Timed out waiting for the single shared connection "
            f"(waited {CHECKOUT_LOCK_TIMEOUT}s). This engine uses a "
            "single-connection pool, so all database work is serialized: "
            "one scope holds the connection while the others wait for it. "
            "Work that awaits an unrelated task from inside an open "
            "transaction cannot make progress here - finish the database "
            "work before handing off, or use a file-backed database "
            "instead of sqlite ':memory:'."
        )

    def _check_database_survived(self, conn: AsyncConnection) -> None:
        """Report an in-memory database that the pool replaced under us."""
        if not self._memory_database:
            return
        dbapi_connection = _dbapi_connection_of(conn)
        if dbapi_connection is None:
            return
        previous = self._dbapi_ref() if self._dbapi_ref is not None else None
        seen = self._dbapi_seen
        self._dbapi_ref = _identity_ref(dbapi_connection)
        self._dbapi_seen = True
        if seen and previous is not dbapi_connection:
            raise ConnectionError(
                "The sqlite ':memory:' database was lost: its connection was "
                "invalidated (a cancelled operation or a closed event loop will "
                "do it) and the pool opened a fresh, empty database in its "
                "place. Everything written so far is gone. Use a file-backed "
                "database if the data has to survive."
            )

    @asynccontextmanager
    async def connection(
        self, engine: AsyncEngine, transactional: bool
    ) -> AsyncIterator[AsyncConnection]:
        """
        Yield the scope's connection, opening it if this is the outermost use.

        Args:
            engine: Engine to open the connection on
            transactional: Whether the block needs a transaction of its own

        Yields:
            AsyncConnection to run statements on
        """
        key = _engine_key(engine)
        state = _ambient_state(key)
        if state is not None:
            async with _nested_async(state, engine, transactional) as conn:
                yield conn
            return

        lock = await self._acquire_lock()
        try:
            conn = await _open_async(engine, transactional)
        except BaseException:
            lock.release()
            raise
        try:
            self._check_database_survived(conn)
        except BaseException:
            await conn.close()
            lock.release()
            raise

        state = _ScopeState(conn, transactional, key)
        token = _publish(key, state)
        try:
            async with _async_block(conn, transactional) as conn:
                yield conn
        finally:
            state.active = False
            _unpublish(token)
            try:
                if state.nested == 0:
                    await state.conn.close()
            finally:
                lock.release()


class _SyncSingleConnectionScope:
    """
    Sync counterpart of _AsyncSingleConnectionScope, scoped to the thread.

    The sync API reaches a single-connection pool through the documented sqlite
    recipe (``poolclass=StaticPool, check_same_thread=False``) and through
    SingletonThreadPool. StaticPool aliases one connection across all threads
    and needs the cross-thread lock; SingletonThreadPool gives each thread its
    own connection, so only the per-thread nesting reuse applies there.

    The lock is a semaphore, not an RLock: generator finalization can run the
    cleanup on a thread other than the one that opened the scope, and releasing
    an RLock from a foreign thread raises.
    """

    def __init__(self, cross_thread: bool = True, memory_database: bool = False) -> None:
        self._lock = threading.BoundedSemaphore(1) if cross_thread else None
        self._memory_database = memory_database
        self._local = threading.local()

    def _check_database_survived(self, conn: Connection) -> None:
        """Report an in-memory database that the pool replaced under us.

        Tracked per thread: SingletonThreadPool gives each thread its own
        connection, and with it its own ":memory:" database.
        """
        if not self._memory_database:
            return
        dbapi_connection = _dbapi_connection_of(conn)
        if dbapi_connection is None:
            return
        ref = getattr(self._local, "dbapi_ref", None)
        seen = getattr(self._local, "dbapi_seen", False)
        previous = ref() if ref is not None else None
        self._local.dbapi_ref = _identity_ref(dbapi_connection)
        self._local.dbapi_seen = True
        if seen and previous is not dbapi_connection:
            raise ConnectionError(
                "The sqlite ':memory:' database was lost: its connection was "
                "invalidated and the pool opened a fresh, empty database in its "
                "place. Everything written so far is gone. Use a file-backed "
                "database if the data has to survive."
            )

    def _acquire_lock(self):
        if self._lock is None:
            return None
        if not self._lock.acquire(timeout=CHECKOUT_LOCK_TIMEOUT):
            raise ConnectionError(
                "Timed out waiting for the single shared connection "
                f"(waited {CHECKOUT_LOCK_TIMEOUT}s). This engine uses a "
                "single-connection pool, so all database work is serialized "
                "across threads."
            )
        return self._lock

    @contextmanager
    def connection(self, engine: Engine, transactional: bool) -> Iterator[Connection]:
        """
        Yield the thread's connection, opening it if this is the outermost use.

        Args:
            engine: Engine to open the connection on
            transactional: Whether the block needs a transaction of its own

        Yields:
            Connection to run statements on
        """
        key = _engine_key(engine)
        state = _ambient_state(key)
        if state is not None:
            with _nested_sync(state, engine, transactional) as conn:
                yield conn
            return

        lock = self._acquire_lock()
        try:
            conn = _open_sync(engine, transactional)
        except BaseException:
            if lock is not None:
                lock.release()
            raise
        try:
            self._check_database_survived(conn)
        except BaseException:
            conn.close()
            if lock is not None:
                lock.release()
            raise

        state = _ScopeState(conn, transactional, key)
        token = _publish(key, state)
        try:
            with _sync_block(conn, transactional) as conn:
                yield conn
        finally:
            state.active = False
            _unpublish(token)
            try:
                if state.nested == 0:
                    state.conn.close()
            finally:
                if lock is not None:
                    lock.release()


def _reject_closed_transaction(state: _ScopeState) -> None:
    """Refuse to silently reopen a connection whose transaction is gone."""
    if not state.transactional:
        return
    raise ConnectionError(
        "The connection this transaction was running on has been closed, so "
        "the transaction no longer exists. Work done from here on could not be "
        "committed with the rest of the block. Do not close the connection "
        "transaction() yields."
    )


@asynccontextmanager
async def _nested_async(
    state: _ScopeState, engine: AsyncEngine, transactional: bool
) -> AsyncIterator[AsyncConnection]:
    """Run a nested block on the connection the scope already holds."""
    if state.conn.closed:
        _reject_closed_transaction(state)
        state.conn = await _open_async(engine, state.transactional)
    state.nested += 1
    try:
        async with _async_block(state.conn, transactional) as conn:
            yield conn
        if transactional and not state.transactional and state.conn.in_transaction():
            # Reached only where the dialect has no AUTOCOMMIT level, so the
            # enclosing scope opened an implicit transaction after all. Nothing
            # else will commit this write, and the scope's close would roll it
            # back. The commit is connection-wide, which is why a scope that
            # needs a boundary uses acquire() instead.
            await state.conn.commit()
    finally:
        state.nested -= 1
        # The outer block may have been finalized while this one ran; it leaves
        # the connection to the last nested block to close.
        if not state.active and state.nested == 0:
            await state.conn.close()


@contextmanager
def _nested_sync(
    state: _ScopeState, engine: Engine, transactional: bool
) -> Iterator[Connection]:
    """Sync counterpart of _nested_async()."""
    if state.conn.closed:
        _reject_closed_transaction(state)
        state.conn = _open_sync(engine, state.transactional)
    state.nested += 1
    try:
        with _sync_block(state.conn, transactional) as conn:
            yield conn
        if transactional and not state.transactional and state.conn.in_transaction():
            # Connection-wide, as in _nested_async().
            state.conn.commit()
    finally:
        state.nested -= 1
        if not state.active and state.nested == 0:
            state.conn.close()


async def _open_async(engine: AsyncEngine, transactional: bool = True) -> AsyncConnection:
    """
    Open a connection, reporting failures as ConnectionError.

    A non-transactional scope opens in AUTOCOMMIT. Otherwise SQLAlchemy starts
    an implicit transaction on the first statement, and a write nested inside
    the scope has to commit the whole connection to become durable - which
    would sweep up whatever the enclosing scope had pending. The pool resets the
    isolation level when the connection goes back.
    """
    try:
        conn = await engine.connect()
    except Exception as e:
        raise ConnectionError(f"Failed to connect: {e}") from e
    if not transactional:
        try:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
        except Exception:
            # Not every dialect has an AUTOCOMMIT level; the implicit
            # transaction is then the old behaviour, which still works.
            pass
    return conn


def _open_sync(engine: Engine, transactional: bool = True) -> Connection:
    """Sync counterpart of _open_async()."""
    try:
        conn = engine.connect()
    except Exception as e:
        raise ConnectionError(f"Failed to connect: {e}") from e
    if not transactional:
        try:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        except Exception:
            pass
    return conn


def _savepoints_work(conn) -> bool:
    """
    Report whether a nested block can safely take a SAVEPOINT here.

    pysqlite releases a savepoint as a commit, so on sqlite nested work would
    survive a rollback of the block containing it - the opposite of what
    transaction() promises. There a nested block joins the open transaction
    instead and the outermost block decides. Everywhere else the savepoint is
    real, and it buys the nested block a rollback boundary of its own: a failed
    statement undoes only that block rather than poisoning the whole
    transaction, which is what PostgreSQL would otherwise do.
    """
    return conn.dialect.name != "sqlite"


@asynccontextmanager
async def _async_block(
    conn: AsyncConnection, transactional: bool
) -> AsyncIterator[AsyncConnection]:
    """Run the block in its own transaction, a savepoint, or the open one."""
    if not transactional:
        yield conn
    elif not conn.in_transaction():
        async with conn.begin():
            yield conn
    elif _savepoints_work(conn):
        async with conn.begin_nested():  # SAVEPOINT
            yield conn
    else:
        yield conn


@contextmanager
def _sync_block(conn: Connection, transactional: bool) -> Iterator[Connection]:
    """Sync counterpart of _async_block()."""
    if not transactional:
        yield conn
    elif not conn.in_transaction():
        with conn.begin():
            yield conn
    elif _savepoints_work(conn):
        with conn.begin_nested():  # SAVEPOINT
            yield conn
    else:
        yield conn


#: One connection scope per engine, shared by every component that opens a
#: connection on it (connection pool, schema manager).
_ENGINE_SCOPES: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_ENGINE_SCOPES_LOCK = threading.Lock()


def _needs_serialization(engine: AsyncEngine | Engine) -> bool:
    """Report whether this engine's pool hands the same connection to everyone."""
    return isinstance(engine.pool, _SINGLE_CONNECTION_POOLS)


def _is_memory_database(engine: AsyncEngine | Engine) -> bool:
    """Report whether this engine's data lives inside its DBAPI connection."""
    url = engine.url
    if url.get_backend_name() != "sqlite":
        return False
    if url.database in (None, "", ":memory:"):
        return True
    # file:...?mode=memory&cache=shared - SQLAlchemy parses the query string, and
    # a repeated key arrives as a tuple.
    mode = (url.query or {}).get("mode")
    if isinstance(mode, (tuple, list)):
        return "memory" in mode
    return mode == "memory"


def _engine_scope(engine: AsyncEngine | Engine, factory):
    """Return the process-wide connection scope for this engine, creating it once."""
    key = _engine_key(engine)
    with _ENGINE_SCOPES_LOCK:
        scope = _ENGINE_SCOPES.get(key)
        if scope is None:
            scope = factory()
            _ENGINE_SCOPES[key] = scope
    return scope


@asynccontextmanager
async def scoped_connection(
    engine: AsyncEngine, transactional: bool = False, ambient: bool = False
) -> AsyncIterator[AsyncConnection]:
    """
    Open a connection, serialized and shared if the pool has only one.

    StaticPool - the pool SQLAlchemy selects for sqlite ":memory:" - hands the
    same DBAPI connection to every concurrent checkout. Two coroutines that each
    open a "connection" therefore share one transaction, and the first one to
    close emits a ROLLBACK that discards the other's uncommitted work. On such a
    pool checkouts are serialized and nested ones reuse the open connection.

    On a real pool this is a plain engine.connect() / engine.begin(), except
    when ``ambient`` is set: then the connection is published for the rest of
    the context, so work done inside an explicit transaction() joins it instead
    of opening its own checkout and committing separately.

    Args:
        engine: Async engine to open the connection on
        transactional: Whether the block needs a transaction of its own
        ambient: Whether nested work in this context should join this connection

    Yields:
        AsyncConnection to run statements on
    """
    key = _engine_key(engine)
    if _needs_serialization(engine):
        scope = _engine_scope(
            engine,
            lambda: _AsyncSingleConnectionScope(memory_database=_is_memory_database(engine)),
        )
        async with scope.connection(engine, transactional) as conn:
            yield conn
        return

    state = _ambient_state(key)
    if state is not None:
        async with _nested_async(state, engine, transactional) as conn:
            yield conn
        return

    conn = await _open_async(engine, transactional)
    state = _ScopeState(conn, transactional, key)
    token = _publish(key, state) if ambient else None
    try:
        async with _async_block(conn, transactional) as conn:
            yield conn
    finally:
        state.active = False
        if token is not None:
            _unpublish(token)
        if state.nested == 0:
            # state.conn, not conn: a nested block may have reopened it. The
            # pool wrappers cannot reach that case (ambient always comes with a
            # transaction, and reopening one is refused), so this guards direct
            # scoped_connection(transactional=False, ambient=True) callers.
            await state.conn.close()


@contextmanager
def sync_scoped_connection(
    engine: Engine, transactional: bool = False, ambient: bool = False
) -> Iterator[Connection]:
    """
    Sync counterpart of scoped_connection().

    Args:
        engine: Sync engine to open the connection on
        transactional: Whether the block needs a transaction of its own
        ambient: Whether nested work in this context should join this connection

    Yields:
        Connection to run statements on
    """
    key = _engine_key(engine)
    if _needs_serialization(engine):
        scope = _engine_scope(
            engine,
            lambda: _SyncSingleConnectionScope(
                cross_thread=isinstance(engine.pool, StaticPool),
                memory_database=_is_memory_database(engine),
            ),
        )
        with scope.connection(engine, transactional) as conn:
            yield conn
        return

    state = _ambient_state(key)
    if state is not None:
        with _nested_sync(state, engine, transactional) as conn:
            yield conn
        return

    conn = _open_sync(engine, transactional)
    state = _ScopeState(conn, transactional, key)
    token = _publish(key, state) if ambient else None
    try:
        with _sync_block(conn, transactional) as conn:
            yield conn
    finally:
        state.active = False
        if token is not None:
            _unpublish(token)
        if state.nested == 0:
            # state.conn, not conn: a nested block may have reopened it.
            state.conn.close()


class AsyncConnectionPool:
    """
    Async connection pool wrapper for AsyncEngine.

    Provides context manager for acquiring connections with automatic cleanup.
    SQLAlchemy AsyncEngine handles the actual pooling.
    """

    def __init__(self, engine: AsyncEngine):
        """
        Initialize pool with async engine.

        Args:
            engine: SQLAlchemy AsyncEngine with built-in connection pool
        """
        self._engine = engine

    @asynccontextmanager
    async def acquire(self, ambient: bool = False) -> AsyncIterator[AsyncConnection]:
        """
        Acquire connection from pool as async context manager.

        Args:
            ambient: Publish the connection so table operations running inside
                this block join its transaction instead of opening their own

        Yields:
            AsyncConnection from the pool

        Raises:
            ConnectionError: If connection cannot be acquired

        Examples:
            >>> async with pool.acquire() as conn:
            ...     result = await conn.execute(stmt)
        """
        async with scoped_connection(
            self._engine, transactional=True, ambient=ambient
        ) as conn:
            yield conn

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        """
        Get connection without starting transaction.

        The connection runs in AUTOCOMMIT where the dialect has it, so every
        statement - the block's own and any table operation nested inside it -
        commits on its own and nothing here can be rolled back. Use acquire()
        or transaction() when the boundary matters.

        Yields:
            AsyncConnection without transaction

        Examples:
            >>> async with pool.connect() as conn:
            ...     result = await conn.execute(stmt)
        """
        async with scoped_connection(self._engine) as conn:
            yield conn

    async def close(self):
        """
        Close all connections in pool and dispose engine.

        Should be called when shutting down application.
        """
        await self._engine.dispose()


class SyncConnectionPool:
    """
    Sync connection pool wrapper for Engine.

    Provides context manager for acquiring connections with automatic cleanup.
    SQLAlchemy Engine handles the actual pooling.
    """

    def __init__(self, engine: Engine):
        """
        Initialize pool with sync engine.

        Args:
            engine: SQLAlchemy Engine with built-in connection pool
        """
        self._engine = engine

    @contextmanager
    def acquire(self, ambient: bool = False) -> Iterator[Connection]:
        """
        Acquire connection from pool as context manager.

        Args:
            ambient: Publish the connection so table operations running inside
                this block join its transaction instead of opening their own

        Yields:
            Connection from the pool

        Raises:
            ConnectionError: If connection cannot be acquired

        Examples:
            >>> with pool.acquire() as conn:
            ...     result = conn.execute(stmt)
        """
        with sync_scoped_connection(
            self._engine, transactional=True, ambient=ambient
        ) as conn:
            yield conn

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        """
        Get connection without starting transaction.

        The connection runs in AUTOCOMMIT where the dialect has it, so every
        statement - the block's own and any table operation nested inside it -
        commits on its own and nothing here can be rolled back. Use acquire()
        or transaction() when the boundary matters.

        Yields:
            Connection without transaction

        Examples:
            >>> with pool.connect() as conn:
            ...     result = conn.execute(stmt)
        """
        with sync_scoped_connection(self._engine) as conn:
            yield conn

    def close(self):
        """
        Close all connections in pool and dispose engine.

        Should be called when shutting down application.
        """
        self._engine.dispose()


def create_pool_config(
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: float = 30.0,
    pool_recycle: int = 3600,
) -> dict:
    """
    Create connection pool configuration for SQLAlchemy engine.

    Args:
        pool_size: Number of connections to maintain in pool (default: 5)
        max_overflow: Max connections beyond pool_size (default: 10)
        pool_timeout: Seconds to wait for connection (default: 30.0)
        pool_recycle: Recycle connections after N seconds (default: 3600)

    Returns:
        Dictionary of pool configuration for create_engine/create_async_engine

    Examples:
        >>> config = create_pool_config(pool_size=10, max_overflow=20)
        >>> engine = create_async_engine(url, **config)
    """
    return {
        'pool_size': pool_size,
        'max_overflow': max_overflow,
        'pool_timeout': pool_timeout,
        'pool_recycle': pool_recycle,
        'pool_pre_ping': True,  # Test connections before using
    }


# ---------- PostgreSQL driver for a URL without one ----------

#: Drivers dbset documents, in order of preference, with the module each imports.
_PG_SYNC_DRIVERS = (("psycopg2", "psycopg2"), ("psycopg", "psycopg"))
_PG_ASYNC_DRIVERS = (("asyncpg", "asyncpg"), ("psycopg", "psycopg"))
_PG_DRIVER_MODULES = {"psycopg2": "psycopg2", "psycopg": "psycopg", "asyncpg": "asyncpg", "pg8000": "pg8000"}
_PG_ASYNC_CAPABLE = {"asyncpg", "psycopg"}


def _has_module(name: str) -> bool:
    import importlib.util

    return importlib.util.find_spec(name) is not None


def _sqlalchemy_default_pg_driver() -> str:
    from sqlalchemy.engine import make_url

    return make_url("postgresql://").get_dialect().driver


def resolve_postgres_driver(url, *, is_async: bool, has_module=None, default_driver: str | None = None):
    """Name an installed driver in a `postgresql://` URL that has none.

    SQLAlchemy 2.1 made psycopg (v3) the default driver of `postgresql://`, 2.0 had
    psycopg2; the `postgres` extra installs psycopg2 and asyncpg. When SQLAlchemy's
    default driver is installed (and, for async, can do asyncio) the URL is returned
    unchanged; otherwise the first installed of psycopg2/psycopg (sync) or
    asyncpg/psycopg (async) is put in. A URL with a driver, another database, or no
    usable driver installed is returned unchanged (SQLAlchemy then reports the problem).
    """
    from sqlalchemy.engine import make_url

    parsed = make_url(url)
    if parsed.drivername != "postgresql":
        return url
    has_module = has_module or _has_module
    default = default_driver or _sqlalchemy_default_pg_driver()
    default_ok = has_module(_PG_DRIVER_MODULES.get(default, default)) and (
        not is_async or default in _PG_ASYNC_CAPABLE
    )
    if default_ok:
        return url
    for driver, module in _PG_ASYNC_DRIVERS if is_async else _PG_SYNC_DRIVERS:
        if has_module(module):
            new = parsed.set(drivername=f"postgresql+{driver}")
            return new.render_as_string(hide_password=False) if isinstance(url, str) else new
    return url
