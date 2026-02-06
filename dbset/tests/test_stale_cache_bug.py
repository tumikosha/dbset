"""Regression test: stale cache in create_index after ensure_columns."""
import pytest
from dbset import async_connect


@pytest.mark.asyncio
async def test_upsert_new_key_columns_stale_cache():
    """
    BUG: upsert с ensure=True падает с ColumnNotFoundError когда keys содержат
    колонки, которых не было в таблице до вызова.

    Сценарий:
    1. insert создаёт таблицу с колонкой 'name' → self._table кэшируется
    2. upsert с keys=['email'] и ensure=True:
       - ensure_columns добавляет 'email' в БД
       - create_index(keys) → _get_table() → возвращает старый self._table без 'email'
       - schema.create_index валидирует колонки → ColumnNotFoundError
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    table = db['test_stale_cache']

    # Шаг 1: создать таблицу, закэшировать self._table
    await table.insert({'name': 'alice'}, ensure=True)

    # Force cache population via find_one (calls _get_table which caches)
    await table.find_one(name='alice')

    # Шаг 2: upsert с НОВОЙ колонкой в keys — должен работать, но падает
    pk = await table.upsert(
        {'name': 'alice', 'email': 'alice@test.com'},
        keys=['email'],
        ensure=True,
    )
    assert pk is not None

    found = await table.find_one(email='alice@test.com')
    assert found is not None
    assert found['name'] == 'alice'

    await db.close()


@pytest.mark.asyncio
async def test_upsert_many_new_key_columns_stale_cache():
    """
    BUG: upsert_many с ensure=True падает с ColumnNotFoundError когда keys
    содержат новые колонки (аналогично upsert).
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    table = db['test_stale_cache_many']

    # Шаг 1: создать таблицу с одной колонкой
    await table.insert({'name': 'seed'}, ensure=True)

    # Force cache population via find_one (calls _get_table which caches)
    await table.find_one(name='seed')

    # Шаг 2: upsert_many с НОВЫМИ колонками как keys
    rows = [
        {'channel': 'chan1', 'post_id': '1', 'views': 100},
        {'channel': 'chan2', 'post_id': '2', 'views': 200},
    ]
    count = await table.upsert_many(rows, keys=['channel', 'post_id'], ensure=True)
    assert count == 2

    found = await table.find_one(channel='chan1')
    assert found is not None
    assert found['views'] == 100

    await db.close()
