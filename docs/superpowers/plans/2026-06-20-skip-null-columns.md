# skip_null_columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `skip_null_columns: bool = True` parameter to `connect()`/`async_connect()` so that a dict key with value `None` whose column does not yet exist is silently skipped instead of auto-creating an ambiguous `Text` column.

**Architecture:** The flag is stored on `Database`/`AsyncDatabase`, threaded into `Table`/`AsyncTable`. A shared pure function `strip_uncreatable_nulls(row, table)` lives in `dbset/types.py` (next to `TypeInference`, matching how `FilterBuilder`/`TypeInference` are shared between the sync and async cores) and removes `None`-valued keys that have no existing column. It is applied at the start of every column-creating / value-building method (`insert`, `insert_many`, `update`, `upsert`, `upsert_many`) before type inference, so stripped keys never create columns and never reach the SQL statement. Columns that already exist keep their `None` and get written as `NULL`.

**Tech Stack:** Python, SQLAlchemy, pytest, SQLite (`sqlite:///:memory:` sync, `sqlite+aiosqlite:///:memory:` async).

## Global Constraints

- Default value MUST be `skip_null_columns: bool = True` (this is an intentional behavior change vs. the previous always-create-Text behavior).
- `skip_null_columns=False` MUST reproduce the previous behavior exactly (`None` → `Text` column).
- The parameter MUST be an explicit keyword argument in BOTH the top-level `connect`/`async_connect` wrappers AND `Database.connect`/`AsyncDatabase.connect` — it MUST NOT be passed via `**kwargs`, because `**kwargs` is forwarded to `create_engine`.
- A column that already exists MUST still receive `NULL` for a `None` value, in both modes.
- Shared function name, verbatim: `strip_uncreatable_nulls` (module-level in `dbset/types.py`, imported by both cores — NOT duplicated as a method in each class). Attribute name, verbatim: `self._skip_null_columns`.

---

### Task 1: Sync — parameter plumbing, helper, wiring, tests

**Files:**
- Modify: `dbset/src/dbset/types.py` (add module-level `strip_uncreatable_nulls`)
- Modify: `dbset/src/dbset/__init__.py` (top-level `connect`, ~line 40 signature + `return Database.connect(...)`)
- Modify: `dbset/src/dbset/sync_core.py` (import `strip_uncreatable_nulls`; `Database.connect` 83-180, `Database.__init__` 48-80, `Database.__getitem__` 201-209, `Table.__init__` 326-344, plus `insert` 415, `insert_many` 485, `update` 644, `upsert` 714, `upsert_many` 798)
- Test: `dbset/tests/test_skip_null_columns.py`

**Interfaces:**
- Produces: `dbset.types.strip_uncreatable_nulls(row: dict, table) -> dict` (shared by sync and async cores); `connect(url, ..., skip_null_columns: bool = True) -> Database`; `Database.connect(url, ..., skip_null_columns: bool = True)`; attribute `Database._skip_null_columns: bool`, `Table._skip_null_columns: bool`.

- [ ] **Step 1: Write the first failing test**

Create `dbset/tests/test_skip_null_columns.py`:

```python
"""Tests for the skip_null_columns parameter (sync)."""

from sqlalchemy import inspect

from dbset import connect

DB_URI = 'sqlite:///:memory:'


def _column_names(db, table_name):
    return {c['name'] for c in inspect(db._engine).get_columns(table_name)}


def test_skip_true_does_not_create_column_for_none():
    db = connect(DB_URI)  # default skip_null_columns=True
    users = db['users']
    pk = users.insert({'name': 'John', 'note': None})
    assert pk is not None
    cols = _column_names(db, 'users')
    assert 'name' in cols
    assert 'note' not in cols
    db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest dbset/tests/test_skip_null_columns.py::test_skip_true_does_not_create_column_for_none -v`
Expected: FAIL — currently `note` column IS created (assert `'note' not in cols` fails).

- [ ] **Step 3: Add the parameter to the top-level `connect` wrapper**

In `dbset/src/dbset/__init__.py`, in `def connect(...)`, add the parameter (after `pk_config`, before `**kwargs`):

```python
    pk_config: PrimaryKeyConfig | None = None,
    skip_null_columns: bool = True,
    **kwargs,
```

And in its `return Database.connect(...)` call, pass it explicitly:

```python
    return Database.connect(
        url,
        read_only=read_only,
        ensure_schema=ensure_schema,
        primary_key_type=primary_key_type,
        primary_key_column=primary_key_column,
        pk_config=pk_config,
        skip_null_columns=skip_null_columns,
        **kwargs,
    )
```

(Keep the existing keyword arguments already present in the call; only add the `skip_null_columns=skip_null_columns` line.)

- [ ] **Step 4: Add the parameter to `Database.connect` and forward to the constructor**

In `dbset/src/dbset/sync_core.py`, `Database.connect` signature (around line 94), add after `text_index_prefix: int = 255,`:

```python
        text_index_prefix: int = 255,
        skip_null_columns: bool = True,
        **engine_kwargs,
```

In the `return cls(...)` block (around line 171-180), add the argument:

```python
            text_index_prefix=text_index_prefix,
            skip_null_columns=skip_null_columns,
        )
```

- [ ] **Step 5: Store the flag on `Database` and thread it into `Table`**

In `Database.__init__` (around 48-80), add the parameter after `text_index_prefix: int = 255,`:

```python
        text_index_prefix: int = 255,
        skip_null_columns: bool = True,
    ):
```

and store it next to `self._text_index_prefix = text_index_prefix`:

```python
        self._text_index_prefix = text_index_prefix
        self._skip_null_columns = skip_null_columns
```

In `Database.__getitem__` (around 201-209), pass it into the `Table(...)` construction, after `text_index_prefix=self._text_index_prefix,`:

```python
            text_index_prefix=self._text_index_prefix,
            skip_null_columns=self._skip_null_columns,
        )
```

- [ ] **Step 6: Add the shared function to `types.py`, import it, store the flag on `Table`**

In `dbset/src/dbset/types.py`, add a module-level function (place it after the `TypeInference` class, at the end of the file):

```python
def strip_uncreatable_nulls(row: dict, table) -> dict:
    """Drop None-valued keys that have no existing column.

    When skip_null_columns is enabled, a key whose value is None and whose
    column does not yet exist cannot have its type inferred, so we skip it
    entirely (no column is created, the key never reaches the statement).
    Keys whose column already exists are kept so the value is written as NULL.
    """
    existing = {c.name for c in table.columns}
    return {k: v for k, v in row.items() if v is not None or k in existing}
```

In `dbset/src/dbset/sync_core.py`, add it to the existing import from `.types` (the line `from .types import ... TypeInference`):

```python
from .types import (
    PrimaryKeyConfig,
    PrimaryKeyType,
    TypeInference,
    strip_uncreatable_nulls,
)
```

(Adapt to the actual current import form — just add `strip_uncreatable_nulls` to whatever is imported from `.types`.)

In `Table.__init__` (around 326-344), add the parameter after `text_index_prefix: int = 255,`:

```python
        text_index_prefix: int = 255,
        skip_null_columns: bool = True,
    ):
```

and store it next to `self._text_index_prefix = text_index_prefix`:

```python
        self._text_index_prefix = text_index_prefix
        self._skip_null_columns = skip_null_columns
```

- [ ] **Step 7: Wire the function into `insert`**

In `insert` (around line 419, immediately after the `table = self._schema.get_table(...)` block that ends at line 419):

```python
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)
```

- [ ] **Step 8: Run the first test to verify it passes**

Run: `pytest dbset/tests/test_skip_null_columns.py::test_skip_true_does_not_create_column_for_none -v`
Expected: PASS

- [ ] **Step 9: Wire the function into `insert_many`, `update`, `upsert`, `upsert_many`**

In `insert_many` (after `table = self._schema.get_table(self._name, ensure_exists=ensure)`, around line 485):

```python
        if self._skip_null_columns:
            rows = [strip_uncreatable_nulls(r, table) for r in rows]
```

In `update` (immediately after `table = self._get_table()`, around line 644):

```python
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)
```

In `upsert`, inside the `if ensure:` block, immediately after the `table = self._schema.get_table(... ensure_exists=True ...)` call (around line 718, before the `inferred_types = ...` line at 721):

```python
            if self._skip_null_columns:
                row = strip_uncreatable_nulls(row, table)
```

In `upsert_many`, inside the `if ensure:` block, immediately after its `table = self._schema.get_table(... ensure_exists=True ...)` call (around line 802, before the `inferred_types = ...` line at 805):

```python
            if self._skip_null_columns:
                rows = [strip_uncreatable_nulls(r, table) for r in rows]
```

- [ ] **Step 10: Add the remaining behavior tests**

Append to `dbset/tests/test_skip_null_columns.py`:

```python
def test_skip_true_writes_null_for_existing_column():
    db = connect(DB_URI)
    users = db['users']
    users.insert({'name': 'Ann', 'note': 'hello'})   # creates note column
    pk = users.insert({'name': 'Bob', 'note': None})  # existing column -> NULL
    row = users.find_one(id=pk)
    assert row['note'] is None
    db.close()


def test_skip_false_creates_text_column_for_none():
    db = connect(DB_URI, skip_null_columns=False)
    users = db['users']
    users.insert({'name': 'John', 'note': None})
    cols = _column_names(db, 'users')
    assert 'note' in cols
    db.close()


def test_skip_true_insert_many_skips_none_column():
    db = connect(DB_URI)
    users = db['users']
    count = users.insert_many([
        {'name': 'A', 'note': None},
        {'name': 'B', 'note': None},
    ])
    assert count == 2
    assert 'note' not in _column_names(db, 'users')
    db.close()


def test_skip_true_update_drops_none_for_missing_column():
    db = connect(DB_URI)
    users = db['users']
    pk = users.insert({'name': 'A', 'age': 1})
    # 'note' has no column; None for it is stripped, 'age' still updates, no crash
    updated = users.update({'age': 2, 'note': None}, id=pk)
    assert updated == 1
    row = users.find_one(id=pk)
    assert row['age'] == 2
    assert 'note' not in _column_names(db, 'users')
    db.close()
```

- [ ] **Step 11: Run the full sync test file**

Run: `pytest dbset/tests/test_skip_null_columns.py -v`
Expected: PASS (all 5 tests)

- [ ] **Step 12: Run the existing sync suite to check for regressions**

Run: `pytest dbset/tests/test_sync_core.py dbset/tests/test_types.py -v`
Expected: PASS (no regressions introduced by the new default)

- [ ] **Step 13: Commit**

```bash
git add dbset/src/dbset/__init__.py dbset/src/dbset/sync_core.py dbset/tests/test_skip_null_columns.py
git commit -m "feat: add skip_null_columns parameter (sync), default True"
```

---

### Task 2: Async — mirror plumbing, helper, wiring, tests

**Files:**
- Modify: `dbset/src/dbset/__init__.py` (top-level `async_connect`, signature ~71 + `return await AsyncDatabase.connect(...)` ~133)
- Modify: `dbset/src/dbset/async_core.py` (`AsyncDatabase.connect` 84-187, `AsyncDatabase.__init__` 49-80, `AsyncDatabase.__getitem__` ~215, `AsyncTable.__init__` 347-375, plus `insert` 455, `insert_many` 527, `update` 708, `upsert` 783, `upsert_many` 867)
- Test: `dbset/tests/test_skip_null_columns_async.py`

**Interfaces:**
- Consumes: `dbset.types.strip_uncreatable_nulls(row, table) -> dict` (created in Task 1); same semantics as Task 1.
- Produces: `async_connect(url, ..., skip_null_columns: bool = True)`; `AsyncDatabase.connect(url, ..., skip_null_columns: bool = True)`; attribute `AsyncDatabase._skip_null_columns`, `AsyncTable._skip_null_columns`.

- [ ] **Step 1: Write the first failing async test**

Create `dbset/tests/test_skip_null_columns_async.py`:

```python
"""Tests for the skip_null_columns parameter (async)."""

import pytest
from sqlalchemy import inspect

from dbset import async_connect

DB_URI = 'sqlite+aiosqlite:///:memory:'


def _column_names(db, table_name):
    # async engine: use the sync inspection via the underlying sync engine
    return {c['name'] for c in inspect(db._engine.sync_engine).get_columns(table_name)}


@pytest.mark.asyncio
async def test_skip_true_does_not_create_column_for_none():
    db = await async_connect(DB_URI)  # default skip_null_columns=True
    users = db['users']
    pk = await users.insert({'name': 'John', 'note': None})
    assert pk is not None
    async with db._engine.connect() as conn:
        cols = await conn.run_sync(
            lambda sync_conn: {c['name'] for c in inspect(sync_conn).get_columns('users')}
        )
    assert 'name' in cols
    assert 'note' not in cols
    await db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest dbset/tests/test_skip_null_columns_async.py::test_skip_true_does_not_create_column_for_none -v`
Expected: FAIL — `note` column is created.

- [ ] **Step 3: Add the parameter to the top-level `async_connect` wrapper**

In `dbset/src/dbset/__init__.py`, `def async_connect(...)`, add `skip_null_columns: bool = True,` after `pk_config` and before `**kwargs`, and add `skip_null_columns=skip_null_columns,` to the `return await AsyncDatabase.connect(...)` call (around line 133).

- [ ] **Step 4: Add the parameter to `AsyncDatabase.connect` and forward to the constructor**

In `dbset/src/dbset/async_core.py`, `AsyncDatabase.connect` signature (around line 95), add `skip_null_columns: bool = True,` after `text_index_prefix: int = 255,` (before `**engine_kwargs`). In the `return cls(...)` block (around line 187), add `skip_null_columns=skip_null_columns,` after `text_index_prefix=text_index_prefix,`.

- [ ] **Step 5: Store the flag on `AsyncDatabase` and thread it into `AsyncTable`**

In `AsyncDatabase.__init__` (around 49-80), add `skip_null_columns: bool = True,` after `text_index_prefix: int = 255,`, and store:

```python
        self._text_index_prefix = text_index_prefix
        self._skip_null_columns = skip_null_columns
```

In `AsyncDatabase.__getitem__` (the `AsyncTable(...)` construction, around line 215), add `skip_null_columns=self._skip_null_columns,` after `text_index_prefix=self._text_index_prefix,`.

- [ ] **Step 6: Import the shared function and store the flag on `AsyncTable`**

In `dbset/src/dbset/async_core.py`, add `strip_uncreatable_nulls` to the existing import from `.types` (currently `from .types import PrimaryKeyConfig, PrimaryKeyType, TypeInference`):

```python
from .types import (
    PrimaryKeyConfig,
    PrimaryKeyType,
    TypeInference,
    strip_uncreatable_nulls,
)
```

In `AsyncTable.__init__` (around 347-375), add `skip_null_columns: bool = True,` after `text_index_prefix: int = 255,`, and store:

```python
        self._text_index_prefix = text_index_prefix
        self._skip_null_columns = skip_null_columns
```

(The function already exists in `dbset/types.py` from Task 1 — do NOT redefine it.)

- [ ] **Step 7: Wire the function into the async `insert`**

In `insert` (immediately after `table = await self._schema.get_table(...)`, around line 460):

```python
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)
```

- [ ] **Step 8: Run the first async test to verify it passes**

Run: `pytest dbset/tests/test_skip_null_columns_async.py::test_skip_true_does_not_create_column_for_none -v`
Expected: PASS

- [ ] **Step 9: Wire the function into async `insert_many`, `update`, `upsert`, `upsert_many`**

In `insert_many` (after `table = await self._schema.get_table(self._name, ensure_exists=ensure)`, around line 527):

```python
        if self._skip_null_columns:
            rows = [strip_uncreatable_nulls(r, table) for r in rows]
```

In `update` (immediately after `table = await self._get_table()`, around line 708):

```python
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)
```

In `upsert`, inside the `if ensure:` block, after the `table = await self._schema.get_table(... ensure_exists=True ...)` call (around line 787, before `inferred_types = ...` at 790):

```python
            if self._skip_null_columns:
                row = strip_uncreatable_nulls(row, table)
```

In `upsert_many`, inside the `if ensure:` block, after its `table = await self._schema.get_table(... ensure_exists=True ...)` call (around line 871, before `inferred_types = ...` at 874):

```python
            if self._skip_null_columns:
                rows = [strip_uncreatable_nulls(r, table) for r in rows]
```

- [ ] **Step 10: Add the remaining async behavior tests**

Append to `dbset/tests/test_skip_null_columns_async.py`:

```python
@pytest.mark.asyncio
async def test_skip_true_writes_null_for_existing_column():
    db = await async_connect(DB_URI)
    users = db['users']
    await users.insert({'name': 'Ann', 'note': 'hello'})
    pk = await users.insert({'name': 'Bob', 'note': None})
    row = await users.find_one(id=pk)
    assert row['note'] is None
    await db.close()


@pytest.mark.asyncio
async def test_skip_false_creates_text_column_for_none():
    db = await async_connect(DB_URI, skip_null_columns=False)
    users = db['users']
    await users.insert({'name': 'John', 'note': None})
    async with db._engine.connect() as conn:
        cols = await conn.run_sync(
            lambda sync_conn: {c['name'] for c in inspect(sync_conn).get_columns('users')}
        )
    assert 'note' in cols
    await db.close()


@pytest.mark.asyncio
async def test_skip_true_insert_many_skips_none_column():
    db = await async_connect(DB_URI)
    users = db['users']
    count = await users.insert_many([
        {'name': 'A', 'note': None},
        {'name': 'B', 'note': None},
    ])
    assert count == 2
    async with db._engine.connect() as conn:
        cols = await conn.run_sync(
            lambda sync_conn: {c['name'] for c in inspect(sync_conn).get_columns('users')}
        )
    assert 'note' not in cols
    await db.close()
```

- [ ] **Step 11: Run the full async test file**

Run: `pytest dbset/tests/test_skip_null_columns_async.py -v`
Expected: PASS (all 4 tests)

- [ ] **Step 12: Run the existing async suite to check for regressions**

Run: `pytest dbset/tests/test_async_core.py -v`
Expected: PASS (no regressions)

- [ ] **Step 13: Commit**

```bash
git add dbset/src/dbset/__init__.py dbset/src/dbset/async_core.py dbset/tests/test_skip_null_columns_async.py
git commit -m "feat: add skip_null_columns parameter (async), default True"
```

---

### Task 3: Documentation and version bump

**Files:**
- Modify: `README.md` (document `skip_null_columns` near the schema/auto-create section)
- Modify: `pyproject.toml` (version bump to `1.1.0`)
- Modify: changelog location if present (e.g. a `CHANGELOG` section in `README.md` or a `CHANGELOG.md`)

**Interfaces:**
- Consumes: the finished feature from Tasks 1-2.

- [ ] **Step 1: Confirm the current version and changelog location**

Run: `grep -n "version" pyproject.toml && ls CHANGELOG* 2>/dev/null && grep -n -i "changelog\|## " README.md | head`
Expected: shows current version (`1.0.13`) and where release notes live.

- [ ] **Step 2: Document the parameter in README.md**

Add a short subsection under the auto-schema section describing:
- `skip_null_columns: bool = True` on `connect`/`async_connect`.
- Behavior: `None` for a non-existent column is skipped (no column created); existing columns still receive `NULL`.
- Note that `skip_null_columns=False` restores the previous always-create-`Text` behavior.

- [ ] **Step 3: Bump the version**

In `pyproject.toml`, change the version from `1.0.13` to `1.1.0`.

- [ ] **Step 4: Add a changelog entry**

Add an entry for `1.1.0` recording the behavior change:
> **Behavior change:** `connect`/`async_connect` now default to `skip_null_columns=True`. A dict key with value `None` whose column does not exist is no longer auto-created as a `Text` column; it is silently skipped. Pass `skip_null_columns=False` to restore the previous behavior.

- [ ] **Step 5: Run the whole test suite**

Run: `pytest dbset/tests -v`
Expected: PASS (pre-existing known failure in `test_json_med_records.py::test_sync_insert_records_with_json_field` may remain — confirm it is unchanged, not newly broken).

- [ ] **Step 6: Commit**

```bash
git add README.md pyproject.toml CHANGELOG.md
git commit -m "docs: document skip_null_columns; bump version to 1.1.0"
```

---

## Self-Review

**Spec coverage:**
- API param in `connect`/`async_connect` → Task 1 Steps 3-5, Task 2 Steps 3-5. ✓
- Stored on Database/Table, threaded → Task 1 Steps 5-6, Task 2 Steps 5-6. ✓
- Shared function `strip_uncreatable_nulls` in types.py → Task 1 Step 6; imported & reused in async → Task 2 Step 6. ✓
- Wiring into insert/insert_many/update/upsert/upsert_many → Task 1 Steps 7,9; Task 2 Steps 7,9. ✓ (Note: `update` does not auto-create columns; wiring there makes its `None`-on-missing-column behavior consistent and is in the spec's method table.)
- Existing column → NULL → Task 1 Step 10 (`test_skip_true_writes_null_for_existing_column`), Task 2 Step 10. ✓
- Default True regression / False restores old behavior → Task 1 Step 10, Task 2 Step 10. ✓
- Compatibility note + version bump → Task 3. ✓

**Placeholder scan:** No TBD/TODO; all code blocks present. ✓

**Type consistency:** `strip_uncreatable_nulls(row, table) -> dict` (one definition in types.py), `self._skip_null_columns` used identically in sync and async. ✓

**Note on line numbers:** all line numbers are approximate anchors from the current files; the engineer should locate the named call/site (e.g. "after `table = self._schema.get_table(...)`") rather than trusting the exact line, since earlier edits in the same file shift later lines.
