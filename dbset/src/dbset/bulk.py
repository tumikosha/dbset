"""Bulk upsert planning shared by the sync and async tables.

upsert_many reads which keys already exist with one SELECT per batch of keys, then
writes new rows with executemany INSERT and existing ones with executemany UPDATE.
Everything here is plain SQLAlchemy Core, so both tables execute the same statements.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import bindparam, select, tuple_, update
from sqlalchemy.sql import Select, Update

_KEY = "_dbset_key_"  # bind names must differ from column names in an UPDATE
_SET = "_dbset_set_"


def split_upsert_rows(
    rows: list[dict[str, Any]], keys: list[str], table
) -> tuple[dict[tuple, dict[str, Any]], list[dict[str, Any]]]:
    """Rows for the bulk path, merged by key values in first-seen order, and rows left for row-by-row upsert.

    Rows with the same key values are merged ({**earlier, **later}), which is what
    upserting them one after another leaves in the table. A row goes row by row when
    a key is not a column of the table or a key value is None (it matches IS NULL).
    """
    columns = set(table.c.keys())
    if not keys or any(k not in columns for k in keys):
        return {}, list(rows)
    bulk: dict[tuple, dict[str, Any]] = {}
    rowwise: list[dict[str, Any]] = []
    for row in rows:
        values = tuple(row.get(k) for k in keys)
        if any(v is None for v in values):
            rowwise.append(row)
        elif values in bulk:
            bulk[values] = {**bulk[values], **row}
        else:
            bulk[values] = dict(row)
    return bulk, rowwise


def existing_keys_select(table, keys: list[str], values: list[tuple]) -> Select:
    """SELECT of the key columns of rows whose keys are among values."""
    cols = [table.c[k] for k in keys]
    if len(cols) == 1:
        return select(*cols).where(cols[0].in_([v[0] for v in values]))
    return select(*cols).where(tuple_(*cols).in_(values))


def key_batch_size(keys: list[str]) -> int:
    """Keys per SELECT: well under the 999 bind parameters of old SQLite builds."""
    return max(1, 500 // len(keys))


def is_existing(values: tuple, found: set[tuple], found_str: set[tuple]) -> bool:
    """values was found; the string match covers a key stored as another type that the database still equates."""
    return values in found or tuple(map(str, values)) in found_str


def update_params(row: dict[str, Any], keys: list[str]) -> dict[str, Any] | None:
    """Bind parameters of one row for update_by_keys, or None when the row has nothing but keys."""
    params = {_KEY + k: row[k] for k in keys}
    sets = {_SET + c: v for c, v in row.items() if c not in keys}
    return {**params, **sets} if sets else None


def update_by_keys(table, keys: list[str], params: dict[str, Any]) -> Update:
    """UPDATE ... SET <the columns in params> WHERE <keys> = <their values>, for executemany."""
    stmt = update(table)
    for k in keys:
        stmt = stmt.where(table.c[k] == bindparam(_KEY + k))
    columns = [name[len(_SET):] for name in params if name.startswith(_SET)]
    return stmt.values({c: bindparam(_SET + c) for c in columns})
