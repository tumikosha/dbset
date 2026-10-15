"""upsert_many: rows that differ from each other, atomicity, and the same results as row-by-row upsert.

Runs on SQLite, and on PostgreSQL / MySQL when DBSET_PG_URL / DBSET_MYSQL_URL are set (see _backends.py).
"""

import uuid

import pytest
from sqlalchemy import Float, inspect

from ._backends import table_name


def _rows(t, *cols):
    return [tuple(r.get(c) for c in cols) for r in t.find(_order_by='id')]


# ---------- sync ----------


def test_key_only_in_a_later_row_is_written(make_db):
    t = make_db()[table_name()]
    assert t.upsert_many([{'k': 'a', 'v': 1}, {'k': 'b', 'v': 2, 'note': 'x'}, {'k': 'c', 'v': 3}], keys=['k']) == 3
    assert _rows(t, 'k', 'note') == [('a', None), ('b', 'x'), ('c', None)]


def test_failed_upsert_many_changes_nothing(make_db):
    t = make_db()[table_name()]
    t.upsert_many([{'k': 'x', 'v': 1}], keys=['k'])
    with pytest.raises(Exception):  # two new rows with the same primary key
        t.upsert_many([{'k': 'x', 'v': 2}, {'k': 'a', 'id': 5}, {'k': 'b', 'id': 5}], keys=['k'])
    assert _rows(t, 'k', 'v') == [('x', 1)]


def test_column_type_covers_all_rows(make_db):
    db = make_db()
    name = table_name()
    db[name].upsert_many([{'k': 'a', 'v': 1}, {'k': 'b', 'v': 2.5}], keys=['k'])
    types = {c['name']: c['type'] for c in inspect(db.engine).get_columns(name)}
    assert isinstance(types['v'], Float)


def test_same_key_twice_in_a_batch_acts_like_two_upserts(make_db):
    t = make_db()[table_name()]
    t.upsert_many([{'k': 'a', 'v': 1, 'w': 1}, {'k': 'a', 'v': 2}], keys=['k'])
    assert _rows(t, 'k', 'v', 'w') == [('a', 2, 1)]


def test_existing_rows_are_updated_new_rows_inserted(make_db):
    t = make_db()[table_name()]
    t.insert({'k': 'a', 'v': 1, 'w': 'keep'})
    t.upsert_many([{'k': 'a', 'v': 10}, {'k': 'b', 'v': 20}], keys=['k'])
    assert _rows(t, 'k', 'v', 'w') == [('a', 10, 'keep'), ('b', 20, None)]


def test_none_value_sets_null_on_an_existing_row(make_db):
    t = make_db()[table_name()]
    t.upsert_many([{'k': 'a', 'p': 'x'}, {'k': 'b', 'p': 'y'}], keys=['k'])
    t.upsert_many([{'k': 'a', 'p': None}], keys=['k'])
    assert _rows(t, 'k', 'p') == [('a', None), ('b', 'y')]


def test_composite_keys(make_db):
    t = make_db()[table_name()]
    t.upsert_many([{'a': 1, 'b': 'x', 'v': 1}, {'a': 1, 'b': 'y', 'v': 2}], keys=['a', 'b'])
    t.upsert_many([{'a': 1, 'b': 'y', 'v': 20}, {'a': 2, 'b': 'x', 'v': 3}], keys=['a', 'b'])
    assert _rows(t, 'a', 'b', 'v') == [(1, 'x', 1), (1, 'y', 20), (2, 'x', 3)]


def test_none_key_value_matches_a_null_row_as_upsert_does(make_db):
    t = make_db()[table_name()]
    t.insert({'grp': 'g', 'v': 0})
    t.insert({'grp': None, 'v': 0})
    t.upsert_many([{'grp': None, 'v': 5}], keys=['grp'])
    assert _rows(t, 'grp', 'v') == [('g', 0), (None, 5)]


def test_key_that_is_no_column_inserts_as_upsert_does(make_db):
    t = make_db()[table_name()]
    assert t.upsert_many([{'a': 1}, {'a': 2}], keys=['nope']) == 2
    assert t.count() == 2


def test_key_value_of_another_python_type_still_matches(make_db):
    if make_db.backend != 'sqlite':
        pytest.skip("only SQLite equates '1' and 1; PostgreSQL rejects text = integer")
    t = make_db()[table_name()]
    t.insert({'k': '1', 'v': 0})  # TEXT column
    t.upsert_many([{'k': 1, 'v': 9}], keys=['k'])  # sqlite compares '1' = 1 as equal, so must upsert
    assert t.count() == 1 and next(iter(t.all()))['v'] == 9


def test_uuid_keys_for_new_rows_only(make_db):
    t = make_db(primary_key_type='uuid')[table_name()]
    t.upsert_many([{'k': 'a'}], keys=['k'])
    first_id = next(iter(t.all()))['id']
    t.upsert_many([{'k': 'a', 'v': 1}, {'k': 'b', 'v': 2}], keys=['k'])
    ids = {r['k']: r['id'] for r in t.all()}
    assert ids['a'] == first_id and uuid.UUID(str(ids['b']))


def test_input_rows_are_not_modified(make_db):
    t = make_db(primary_key_type='uuid')[table_name()]
    rows = [{'k': 'a', 'note': None}]
    t.upsert_many(rows, keys=['k'])
    assert rows == [{'k': 'a', 'note': None}]


def test_joins_an_outer_transaction(make_db):
    db = make_db()
    t = db[table_name()]
    t.upsert_many([{'k': 'x', 'v': 1}], keys=['k'])
    with pytest.raises(RuntimeError):
        with db.transaction():
            t.upsert_many([{'k': 'x', 'v': 2}, {'k': 'y', 'v': 3}], keys=['k'])
            raise RuntimeError('roll back')
    assert _rows(t, 'k', 'v') == [('x', 1)]


# ---------- async ----------


async def _async_rows(t, *cols):
    return [tuple(r.get(c) for c in cols) async for r in t.find(_order_by='id')]


async def test_async_key_only_in_a_later_row_is_written(make_async_db):
    t = (await make_async_db())[table_name()]
    assert await t.upsert_many([{'k': 'a', 'v': 1}, {'k': 'b', 'v': 2, 'note': 'x'}], keys=['k']) == 2
    assert await _async_rows(t, 'k', 'note') == [('a', None), ('b', 'x')]


async def test_async_failed_upsert_many_changes_nothing(make_async_db):
    t = (await make_async_db())[table_name()]
    await t.upsert_many([{'k': 'x', 'v': 1}], keys=['k'])
    with pytest.raises(Exception):
        await t.upsert_many([{'k': 'x', 'v': 2}, {'k': 'a', 'id': 5}, {'k': 'b', 'id': 5}], keys=['k'])
    assert await _async_rows(t, 'k', 'v') == [('x', 1)]


async def test_async_existing_rows_are_updated_new_rows_inserted(make_async_db):
    t = (await make_async_db())[table_name()]
    await t.insert({'k': 'a', 'v': 1, 'w': 'keep'})
    await t.upsert_many([{'k': 'a', 'v': 10}, {'k': 'b', 'v': 20}, {'k': 'a', 'w': 'new'}], keys=['k'])
    assert await _async_rows(t, 'k', 'v', 'w') == [('a', 10, 'new'), ('b', 20, None)]


async def test_async_none_key_value_matches_a_null_row(make_async_db):
    t = (await make_async_db())[table_name()]
    await t.insert({'grp': 'g', 'v': 0})
    await t.insert({'grp': None, 'v': 0})
    await t.upsert_many([{'grp': None, 'v': 5}], keys=['grp'])
    assert await _async_rows(t, 'grp', 'v') == [('g', 0), (None, 5)]
