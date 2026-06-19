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
