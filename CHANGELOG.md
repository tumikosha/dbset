# Changelog

## 1.2.0

### Fixed

- A checkout could roll back another one's uncommitted work on a
  single-connection pool. `sqlite+aiosqlite:///:memory:` uses SQLAlchemy's
  `StaticPool`, which hands the same DBAPI connection to every concurrent
  checkout, so closing one "connection" emitted a `ROLLBACK` on whatever
  another coroutine had open: no exception, `rowcount` already `1`, `COMMIT`
  returning cleanly, and the value unchanged in the database. Checkouts on
  such a pool are now serialized, and a nested checkout reuses the connection
  its scope already holds instead of opening a second copy of it. The schema
  manager, which opens connections straight on the engine, goes through the
  same path.
- `AsyncTable.find_one` returned from inside `async for`, leaving the `find()`
  generator suspended for the event loop to finalize later, in a separate
  task. It closes the generator itself now (`aclosing`, `closing` in the sync
  twin).
- `find`, `all`, `query`, `distinct` and `find_similar` no longer hold a
  connection across their yields: rows are read while the connection is held
  and yielded after it is released, so an abandoned generator cannot pin a
  connection.
- `transaction()` raised on every call and every pool
  (`This connection has already initialized a SQLAlchemy Transaction()`), and
  table operations inside the block committed independently of it on a real
  pool. The block now runs on one connection and commits or rolls back as a
  unit, table operations included.
- `pool.acquire()` / `pool.connect()` rewrote every exception raised inside
  the caller's block as `ConnectionError`. Only the connection attempt is
  wrapped now.
- A sqlite `":memory:"` database silently replaced by the pool (its connection
  invalidated, a fresh empty one opened in its place) now raises instead of
  continuing against empty tables.
- Closing the connection `transaction()` yields is reported instead of being
  silently reopened without its transaction.

### Behavior change

- `pool.connect()` scopes run in `AUTOCOMMIT` where the dialect has it. Every
  statement in such a scope commits on its own; use `pool.acquire()` or
  `transaction()` when a rollback boundary matters.
- A nested operation takes a `SAVEPOINT` on backends with usable savepoints
  (PostgreSQL, MySQL) and joins the open transaction on sqlite, where pysqlite
  releases a savepoint as a commit. A failed operation inside `transaction()`
  undoes itself rather than poisoning the transaction.
- `find()` and `all()` materialize the result set before yielding the first
  row. The sync sqlite driver used to fetch lazily, so a large table now costs
  more memory unless `_stream=True` is passed.

### Added

- `find(_stream=True)` and `all(_stream=True)` iterate the result set from the
  database, holding the connection for the iteration.
- `pool.acquire(ambient=True)` publishes the connection so nested work joins
  it; this is what `transaction()` uses.

## 1.1.1

### Docs

- README: surface Hybrid Search in the Features list and refresh the Status
  section (hybrid search, `skip_null_columns`, current test count).

## 1.1.0

### Behavior change

- `connect` / `async_connect` now default to `skip_null_columns=True`. A dict
  key with value `None` whose column does not yet exist is no longer
  auto-created as a `TEXT` column — it is silently skipped (the column is not
  created and the key is dropped from the statement). Columns that already
  exist still receive `NULL`. Pass `skip_null_columns=False` to restore the
  previous behavior.

### Added

- New `skip_null_columns: bool = True` parameter on `connect`,
  `async_connect`, `Database.connect`, and `AsyncDatabase.connect`.
- Shared helper `dbset.types.strip_uncreatable_nulls(row, table)` used by both
  the sync and async cores; applied in `insert`, `insert_many`, `update`,
  `upsert`, and `upsert_many`.
