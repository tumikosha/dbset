# Changelog

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
