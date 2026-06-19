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
