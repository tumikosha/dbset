"""Unit tests for async_core.py - AsyncDatabase and AsyncTable."""

from decimal import Decimal

import pytest
from dbset import async_connect, ReadOnlyError, QueryError


async def test_database_connect():
    """Test basic async database connection."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    assert db is not None
    assert db.read_only is False
    await db.close()


async def test_database_connect_read_only():
    """Test read-only database connection."""
    db = await async_connect('sqlite+aiosqlite:///:memory:', read_only=True)
    assert db.read_only is True
    await db.close()


async def test_table_access():
    """Test dict-like table access."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']
    assert users is not None
    assert users.name == 'users'
    await db.close()


async def test_insert_and_find():
    """Test basic insert and find operations."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert
    pk = await users.insert({'name': 'John', 'age': 30})
    assert pk is not None

    # Find
    found = await users.find_one(name='John')
    assert found is not None
    assert found['name'] == 'John'
    assert found['age'] == 30

    await db.close()


async def test_insert_with_auto_schema():
    """Test automatic table/column creation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:', ensure_schema=True)
    users = db['users']

    # Insert - should auto-create table and columns
    pk = await users.insert({'name': 'Alice', 'age': 25, 'email': 'alice@example.com'})
    assert pk is not None

    # Verify data
    found = await users.find_one(name='Alice')
    assert found['email'] == 'alice@example.com'

    await db.close()


async def test_find_with_filters():
    """Test find with various filter operators."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert test data
    await users.insert({'name': 'John', 'age': 30})
    await users.insert({'name': 'Jane', 'age': 25})
    await users.insert({'name': 'Bob', 'age': 35})

    # Test equality filter
    results = []
    async for user in users.find(name='John'):
        results.append(user)
    assert len(results) == 1
    assert results[0]['name'] == 'John'

    # Test comparison operator
    results = []
    async for user in users.find(age={'>=': 30}):
        results.append(user)
    assert len(results) == 2  # John and Bob

    # Test multiple filters (AND)
    results = []
    async for user in users.find(age={'>=': 30}, name='John'):
        results.append(user)
    assert len(results) == 1
    assert results[0]['name'] == 'John'

    await db.close()


async def test_count():
    """Test count operation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert test data
    await users.insert({'name': 'John', 'age': 30})
    await users.insert({'name': 'Jane', 'age': 25})
    await users.insert({'name': 'Bob', 'age': 35})

    # Count all
    total = await users.count()
    assert total == 3

    # Count with filter
    adults = await users.count(age={'>=': 30})
    assert adults == 2

    await db.close()


async def test_update():
    """Test update operation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert
    await users.insert({'name': 'John', 'age': 30})

    # Update
    count = await users.update({'age': 31}, name='John')
    assert count == 1

    # Verify
    found = await users.find_one(name='John')
    assert found['age'] == 31

    await db.close()


async def test_update_with_nonexistent_key_column():
    """Test update with keys that include non-existent columns.

    This matches dataset library behavior: non-existent keys are ignored,
    and update proceeds with valid keys only.
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial row
    await users.insert({'name': 'John', 'age': 30})

    # Update with keys=['name', 'nonexistent'] - 'nonexistent' should be ignored
    count = await users.update(
        {'name': 'John', 'age': 99, 'nonexistent': 'val'},
        keys=['name', 'nonexistent']
    )
    assert count == 1

    # Verify update happened
    found = await users.find_one(name='John')
    assert found['age'] == 99

    await db.close()


async def test_update_with_all_nonexistent_keys():
    """Test update when ALL keys are non-existent columns.

    Should raise QueryError because no valid WHERE clause can be built.
    """
    from dbset import QueryError

    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial row
    await users.insert({'name': 'John', 'age': 30})

    # Update with keys=['foo', 'bar'] - all non-existent
    # Since no valid keys, filters will be empty, should raise error
    with pytest.raises(QueryError):
        await users.update(
            {'name': 'John', 'age': 99, 'foo': 'a', 'bar': 'b'},
            keys=['foo', 'bar']
        )

    await db.close()


async def test_delete():
    """Test delete operation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert
    await users.insert({'name': 'John', 'age': 30})
    await users.insert({'name': 'Jane', 'age': 25})

    # Delete
    count = await users.delete(name='John')
    assert count == 1

    # Verify
    remaining = await users.count()
    assert remaining == 1

    await db.close()


async def test_read_only_mode():
    """Test read-only validation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:', read_only=True)
    users = db['users']

    # SELECT should work (but no data yet, so just test it doesn't error)
    count = await users.count()
    assert count == 0

    # INSERT should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        await users.insert({'name': 'John'})

    # UPDATE should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        await users.update({'age': 31}, name='John')

    # DELETE should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        await users.delete(name='John')

    await db.close()


async def test_upsert():
    """Test upsert (insert or update) operation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # First upsert - should insert
    pk1 = await users.upsert({'name': 'John', 'age': 30}, keys=['name'])
    assert pk1 is not None

    # Second upsert - should update
    pk2 = await users.upsert({'name': 'John', 'age': 31}, keys=['name'])

    # Verify only one row exists with updated age
    count = await users.count()
    assert count == 1

    found = await users.find_one(name='John')
    assert found['age'] == 31

    await db.close()


async def test_upsert_with_nonexistent_key_column():
    """Test upsert with keys that include non-existent columns.

    This matches dataset library behavior: when keys contain columns that
    don't exist in the table, the query finds no match and inserts a new row.
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial row
    await users.insert({'name': 'John', 'age': 30})
    assert await users.count() == 1

    # Upsert with keys=['name', 'nonexistent'] - should INSERT (not update)
    # because 'nonexistent' column doesn't exist, causing no match
    await users.upsert({'name': 'John', 'age': 99}, keys=['name', 'nonexistent'])

    # Should have 2 rows now (original + new insert)
    assert await users.count() == 2

    # Both rows should exist
    rows = [row async for row in users.find(name='John')]
    assert len(rows) == 2
    ages = {row['age'] for row in rows}
    assert ages == {30, 99}

    await db.close()


async def test_upsert_with_all_nonexistent_keys():
    """Test upsert when ALL keys are non-existent columns.

    Should insert a new row every time since no columns match.
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial row
    await users.insert({'name': 'John', 'age': 30})
    assert await users.count() == 1

    # Upsert with keys=['foo', 'bar'] - all non-existent
    await users.upsert({'name': 'Jane', 'age': 25}, keys=['foo', 'bar'])

    # Should have 2 rows
    assert await users.count() == 2

    # Upsert again with same data but non-existent keys - should insert again
    await users.upsert({'name': 'Jane', 'age': 25}, keys=['foo', 'bar'])

    # Should have 3 rows
    assert await users.count() == 3

    await db.close()


async def test_upsert_with_only_key_fields():
    """Test upsert when record contains only key fields (no fields to update)."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # First insert a row
    await users.insert({'email': 'john@example.com', 'name': 'John', 'age': 30})

    # Upsert with only the key field - should not fail with SQL syntax error
    # This tests the fix for: UPDATE table SET WHERE ... (empty SET clause)
    pk = await users.upsert({'email': 'john@example.com'}, keys=['email'])

    # Verify the row still exists and is unchanged
    found = await users.find_one(email='john@example.com')
    assert found is not None
    assert found['name'] == 'John'
    assert found['age'] == 30

    # Verify count is still 1
    count = await users.count()
    assert count == 1

    await db.close()


async def test_upsert_many_with_only_key_fields():
    """Test upsert_many when records contain only key fields."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    urls = db['urls']

    # First insert some rows
    await urls.insert({'url': 'https://example1.com', 'status': 'active'})
    await urls.insert({'url': 'https://example2.com', 'status': 'active'})

    # Upsert_many with only key fields - should not fail
    records = [
        {'url': 'https://example1.com'},
        {'url': 'https://example2.com'},
        {'url': 'https://example3.com'},  # This one is new
    ]
    count = await urls.upsert_many(records, keys=['url'])
    assert count == 3

    # Verify existing rows are unchanged
    found1 = await urls.find_one(url='https://example1.com')
    assert found1['status'] == 'active'

    # Verify new row was inserted (with only the url field)
    found3 = await urls.find_one(url='https://example3.com')
    assert found3 is not None
    assert found3['url'] == 'https://example3.com'

    await db.close()


async def test_upsert_many_with_nonexistent_key_column():
    """Test upsert_many with keys that include non-existent columns.

    This matches dataset library behavior: when keys contain columns that
    don't exist in the table, upsert_many inserts new rows instead of updating.
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial rows
    await users.insert({'name': 'John', 'age': 30})
    await users.insert({'name': 'Jane', 'age': 25})
    assert await users.count() == 2

    # Upsert_many with keys=['name', 'nonexistent'] - should INSERT all rows
    # because 'nonexistent' column doesn't exist
    records = [
        {'name': 'John', 'age': 99},
        {'name': 'Jane', 'age': 88},
    ]
    count = await users.upsert_many(records, keys=['name', 'nonexistent'])
    assert count == 2

    # Should have 4 rows now (2 original + 2 new inserts)
    assert await users.count() == 4

    # Original rows should still exist with original values
    johns = [row async for row in users.find(name='John')]
    assert len(johns) == 2
    ages = {row['age'] for row in johns}
    assert ages == {30, 99}

    await db.close()


async def test_upsert_many_with_all_nonexistent_keys():
    """Test upsert_many when ALL keys are non-existent columns.

    Should insert all rows every time since no columns match.
    """
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert initial row
    await users.insert({'name': 'John', 'age': 30})
    assert await users.count() == 1

    # Upsert_many with keys=['foo', 'bar'] - all non-existent
    records = [
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 35},
    ]
    count = await users.upsert_many(records, keys=['foo', 'bar'])
    assert count == 2

    # Should have 3 rows
    assert await users.count() == 3

    # Upsert_many again with same data - should insert again
    count = await users.upsert_many(records, keys=['foo', 'bar'])
    assert count == 2

    # Should have 5 rows
    assert await users.count() == 5

    await db.close()


async def test_insert_many():
    """Test batch insert operation."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    rows = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 35},
    ]

    count = await users.insert_many(rows)
    assert count == 3

    # Verify
    total = await users.count()
    assert total == 3

    await db.close()


async def test_order_by():
    """Test ordering results."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert out of order
    await users.insert({'name': 'Charlie', 'age': 35})
    await users.insert({'name': 'Alice', 'age': 25})
    await users.insert({'name': 'Bob', 'age': 30})

    # Order by name ascending
    results = []
    async for user in users.find(_order_by='name'):
        results.append(user['name'])
    assert results == ['Alice', 'Bob', 'Charlie']

    # Order by age descending
    results = []
    async for user in users.find(_order_by='-age'):
        results.append(user['age'])
    assert results == [35, 30, 25]

    await db.close()


async def test_limit_and_offset():
    """Test pagination with limit and offset."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert test data
    for i in range(10):
        await users.insert({'name': f'User{i}', 'age': 20 + i})

    # Test limit
    results = []
    async for user in users.find(_limit=3):
        results.append(user)
    assert len(results) == 3

    # Test offset
    results = []
    async for user in users.find(_offset=5, _limit=3):
        results.append(user)
    assert len(results) == 3

    await db.close()


async def test_distinct():
    """Test distinct values query."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert with duplicate statuses
    await users.insert({'name': 'John', 'status': 'active'})
    await users.insert({'name': 'Jane', 'status': 'active'})
    await users.insert({'name': 'Bob', 'status': 'inactive'})

    # Get distinct statuses
    statuses = []
    async for row in users.distinct('status'):
        statuses.append(row['status'])

    assert len(statuses) == 2
    assert 'active' in statuses
    assert 'inactive' in statuses

    await db.close()


async def test_insert_with_decimal_values():
    """Test Decimal round-trip - insert and retrieve with precision preserved."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    products = db['products']

    # Insert with Decimal values
    await products.insert({
        'name': 'Widget',
        'price': Decimal('99.99'),
        'tax_rate': Decimal('0.085'),
    })

    # Retrieve and verify precision is preserved
    product = await products.find_one(name='Widget')
    assert product['name'] == 'Widget'
    assert product['price'] == Decimal('99.99')
    assert product['tax_rate'] == Decimal('0.085')
    assert isinstance(product['price'], Decimal)
    assert isinstance(product['tax_rate'], Decimal)

    await db.close()


async def test_decimal_and_float_compatibility():
    """Test mixed Decimal/float values in same table."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    items = db['items']

    # Insert Decimal
    await items.insert({'value': Decimal('100.00')})

    # Insert float - should work with same table
    await items.insert({'value': 50.5})

    # Verify both rows exist
    count = await items.count()
    assert count == 2

    # Retrieve both
    rows = []
    async for row in items.all():
        rows.append(row)

    assert len(rows) == 2

    await db.close()


async def test_decimal_in_batch_insert():
    """Test Decimal values in batch insert operations."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    transactions = db['transactions']

    # Batch insert with Decimal values
    data = [
        {'amount': Decimal('100.50'), 'description': 'Payment 1'},
        {'amount': Decimal('200.75'), 'description': 'Payment 2'},
        {'amount': Decimal('50.25'), 'description': 'Payment 3'},
    ]

    for record in data:
        await transactions.insert(record)

    # Verify all inserted with correct precision
    count = await transactions.count()
    assert count == 3

    # Check first transaction
    tx = await transactions.find_one(description='Payment 1')
    assert tx['amount'] == Decimal('100.50')
    assert isinstance(tx['amount'], Decimal)

    await db.close()


async def test_decimal_query_filtering():
    """Test querying with Decimal values."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    products = db['products']

    # Insert products with different prices
    await products.insert({'name': 'Cheap', 'price': Decimal('10.00')})
    await products.insert({'name': 'Medium', 'price': Decimal('50.00')})
    await products.insert({'name': 'Expensive', 'price': Decimal('100.00')})

    # Query by exact Decimal value
    cheap = await products.find_one(price=Decimal('10.00'))
    assert cheap['name'] == 'Cheap'

    await db.close()
