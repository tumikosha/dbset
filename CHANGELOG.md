# Changelog

## Unreleased

### Added

- libSQL (Turso's SQLite fork) support: `sqlite+dbsetlibsql://` (sync) and
  `sqlite+dbsetaiolibsql://` (async) URLs, extra `dbset[libsql]`. Vector columns are
  created as `F32_BLOB(dim)` and stored as float32 blobs; `find_similar` computes
  cosine and L2 distances in SQL; `create_vector_index` builds a DiskANN index
  (`libsql_vector_idx`), optionally partial (`where={'col': value}`), which
  `find_similar` and `hybrid_search` use; with filters they oversample candidates
  (`limit` × 10, × 100, × 1000) and scan exactly when the index cannot supply
  `limit` matches.
  Local files and `:memory:` only. The URL names are dbset's own, so the
  `sqlite+libsql://` dialects of the `sqlalchemy-libsql` package (whose async
  dialect does not run) keep working in the same process.
- `deserialize_vector` reads float32 blobs.

### Changed

- `create_vector_index(method=None)`: the default is now `None`, meaning
  `'hnsw'` on PostgreSQL (as before) and `'diskann'` on libSQL.

## 1.3.0

### Fixed

- A `postgresql://` URL without a driver failed with SQLAlchemy 2.1 and the
  `postgres` extra: 2.1 made psycopg (v3) the default driver, the extra installs
  psycopg2 and asyncpg, so `connect()` raised `No module named 'psycopg'`.
  When SQLAlchemy's default driver is not installed (or, for `async_connect`,
  cannot do asyncio), dbset now puts in an installed one: psycopg2 or psycopg
  for `connect`, asyncpg or psycopg for `async_connect`. URLs that name a
  driver are unchanged.
- `hybrid_search` with an operator filter (`n={'>=': 2}`, `tags={'?|': [...]}`,
  `{'in': [...]}`) failed on the full-text side (`can't adapt type 'dict'`,
  asyncpg: `'dict' object has no attribute 'encode'`), on PostgreSQL and SQLite,
  sync and async: the filter was bound as a value of `col = :param`. The
  full-text query now applies only plain equality filters; the row fetch
  applies them all, as it already did for dotted JSONB keys.

- `insert_many` failed on a new table when a row after the first had `None` in
  a column: `None` keys without a column were stripped from each row before the
  columns were created from the first row, so the rows reached `executemany`
  with different keys (`A value is required for bind parameter`). Only keys
  that are `None` in every row are dropped now.
- `insert_many` took column types from the first row only: a key that appears
  only in later rows was silently lost, a key missing from a later row failed
  the batch, and an `int` then a `float` made an `INTEGER` column. Types now
  come from all rows (merged), and each run of consecutive rows with the same
  keys is one `executemany`, so a row that leaves a column out gets its
  default, as with `insert`.
- `insert_many` ignored the configured primary key: with
  `primary_key_type='uuid'` it created an `INTEGER` key on a new table and
  failed with `NOT NULL constraint failed` on an existing one. It now creates
  the table with the configured key and generates missing key values, as
  `insert` does.
- `insert_many` committed each chunk on its own, so a failure in a later chunk
  left the earlier ones written. All chunks now run in one transaction: all
  rows or none, and one commit instead of one per chunk.
- `upsert_many` took column types from the first row only: a key that appears
  only in later rows failed with `Unconsumed column names` after the earlier
  rows were already written, and an `int` then a `float` made an `INTEGER`
  column. Types now come from all rows.
- `upsert_many` was not atomic: each row was its own `SELECT` and `INSERT` or
  `UPDATE`, each committed on its own. It now runs in one transaction (and
  joins an outer `db.transaction()`).

### Changed

- `upsert_many` writes in bulk: one `SELECT` of the existing keys per batch,
  then `executemany` `INSERT` for new rows and `executemany` `UPDATE` for
  existing ones. 10 000 rows on SQLite: 10 s before, 0.04 s now. Rows with the
  same keys in one call are merged in order, which leaves the table as
  upserting them one by one did. Rows whose key value is `None`, or whose key
  is not a column, still go row by row, with the same results as before.

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
