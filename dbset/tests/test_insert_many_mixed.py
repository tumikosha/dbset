"""insert_many with rows that differ from each other: None values, missing and extra keys, mixed types, UUID keys.

Runs on SQLite, and on PostgreSQL / MySQL when DBSET_PG_URL / DBSET_MYSQL_URL are set (see _backends.py).
"""

import uuid

import pytest
from sqlalchemy import Float, inspect

from ._backends import table_name


def _columns(db, table):
    return {c['name']: c['type'] for c in inspect(db.engine).get_columns(table)}


def _rows(t, *cols):
    return [tuple(r.get(c) for c in cols) for r in t.find(_order_by='id')]


# ---------- sync ----------


def test_none_in_a_later_row_of_a_new_table(make_db):
    t = make_db()[table_name()]
    assert t.insert_many([{'p': 'x'}, {'p': None}, {'p': 'z'}]) == 3
    assert _rows(t, 'p') == [('x',), (None,), ('z',)]


def test_key_only_in_a_later_row_is_not_lost(make_db):
    t = make_db()[table_name()]
    t.insert_many([{'a': 1}, {'a': 2, 'b': 'x'}])
    assert _rows(t, 'a', 'b') == [(1, None), (2, 'x')]


def test_key_missing_in_a_later_row(make_db):
    t = make_db()[table_name()]
    t.insert_many([{'a': 1, 'b': 'x'}, {'a': 2}, {'a': 3, 'b': 'z'}])
    assert _rows(t, 'a', 'b') == [(1, 'x'), (2, None), (3, 'z')]


def test_key_that_is_none_in_every_row_creates_no_column(make_db):
    db = make_db()
    name = table_name()
    db[name].insert_many([{'a': 1, 'note': None}, {'a': 2, 'note': None}])
    assert 'note' not in _columns(db, name)


def test_column_type_covers_all_rows(make_db):
    db = make_db()
    name = table_name()
    db[name].insert_many([{'v': 1}, {'v': 2.5}])
    assert isinstance(_columns(db, name)['v'], Float)
    assert [r['v'] for r in db[name].find(_order_by='id')] == [1, 2.5]


def test_failed_insert_many_writes_nothing(make_db):
    t = make_db()[table_name()]
    with pytest.raises(Exception):
        t.insert_many([{'id': 1}, {'id': 2}, {'id': 1}], chunk_size=2)  # the duplicate is in the 2nd chunk
    assert t.count() == 0


def test_uuid_keys_are_generated(make_db):
    db = make_db(primary_key_type='uuid')
    t = db[table_name()]
    t.insert({'name': 'first'})  # existing UUID table
    t.insert_many([{'name': 'a'}, {'name': 'b'}])
    ids = [r['id'] for r in t.all()]
    assert len(ids) == 3 and all(uuid.UUID(str(i)) for i in ids)
    t2 = db[table_name()]
    t2.insert_many([{'name': 'a'}])  # a table created by insert_many gets the UUID key too
    assert uuid.UUID(str(next(iter(t2.all()))['id']))


def test_input_rows_are_not_modified(make_db):
    t = make_db(primary_key_type='uuid')[table_name()]
    rows = [{'name': 'a', 'note': None}]
    t.insert_many(rows)
    assert rows == [{'name': 'a', 'note': None}]


def test_column_types_from_values_whose_type_depends_on_the_value():
    import decimal

    import numpy as np

    from dbset.types import infer_types_from_rows
    from dbset.vector import Vector

    types = infer_types_from_rows([
        {'vec': np.zeros(64), 'amount': decimal.Decimal('1.5')},
        {'vec': np.zeros(128), 'amount': decimal.Decimal('12345.678')},
    ])
    assert isinstance(types['vec'], Vector) and types['vec'].dim is None  # two sizes: no fixed dim
    amount = types['amount']
    assert amount.scale >= 3 and amount.precision - amount.scale >= 5  # fits 12345.678, not only 1.5


# ---------- async ----------


async def _async_rows(t, *cols):
    return [tuple(r.get(c) for c in cols) async for r in t.find(_order_by='id')]


async def test_async_none_in_a_later_row_of_a_new_table(make_async_db):
    t = (await make_async_db())[table_name()]
    assert await t.insert_many([{'p': 'x'}, {'p': None}, {'p': 'z'}]) == 3
    assert await _async_rows(t, 'p') == [('x',), (None,), ('z',)]


async def test_async_key_only_in_a_later_row_is_not_lost(make_async_db):
    t = (await make_async_db())[table_name()]
    await t.insert_many([{'a': 1}, {'a': 2, 'b': 'x'}])
    assert await _async_rows(t, 'a', 'b') == [(1, None), (2, 'x')]


async def test_async_key_missing_in_a_later_row(make_async_db):
    t = (await make_async_db())[table_name()]
    await t.insert_many([{'a': 1, 'b': 'x'}, {'a': 2}, {'a': 3, 'b': 'z'}])
    assert await _async_rows(t, 'a', 'b') == [(1, 'x'), (2, None), (3, 'z')]


async def test_async_failed_insert_many_writes_nothing(make_async_db):
    t = (await make_async_db())[table_name()]
    with pytest.raises(Exception):
        await t.insert_many([{'id': 1}, {'id': 2}, {'id': 1}], chunk_size=2)
    assert await t.count() == 0


async def test_async_uuid_keys_are_generated(make_async_db):
    t = (await make_async_db(primary_key_type='uuid'))[table_name()]
    await t.insert({'name': 'first'})
    await t.insert_many([{'name': 'a'}, {'name': 'b'}])
    ids = [r['id'] async for r in t.all()]
    assert len(ids) == 3 and all(uuid.UUID(str(i)) for i in ids)
