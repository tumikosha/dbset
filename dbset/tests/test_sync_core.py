"""Unit tests for sync_core.py - Database and Table."""

from decimal import Decimal

import pytest
from dbset import connect, ReadOnlyError


def test_database_connect():
    """Test basic sync database connection."""
    db = connect('sqlite:///:memory:')
    assert db is not None
    assert db.read_only is False
    db.close()


def test_database_connect_read_only():
    """Test read-only database connection."""
    db = connect('sqlite:///:memory:', read_only=True)
    assert db.read_only is True
    db.close()


def test_table_access():
    """Test dict-like table access."""
    db = connect('sqlite:///:memory:')
    users = db['users']
    assert users is not None
    assert users.name == 'users'
    db.close()


def test_insert_and_find():
    """Test basic insert and find operations."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert
    pk = users.insert({'name': 'John', 'age': 30})
    assert pk is not None

    # Find
    found = users.find_one(name='John')
    assert found is not None
    assert found['name'] == 'John'
    assert found['age'] == 30

    db.close()


def test_insert_with_auto_schema():
    """Test automatic table/column creation."""
    db = connect('sqlite:///:memory:', ensure_schema=True)
    users = db['users']

    # Insert - should auto-create table and columns
    pk = users.insert({'name': 'Alice', 'age': 25, 'email': 'alice@example.com'})
    assert pk is not None

    # Verify data
    found = users.find_one(name='Alice')
    assert found['email'] == 'alice@example.com'

    db.close()


def test_find_with_filters():
    """Test find with various filter operators."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert test data
    users.insert({'name': 'John', 'age': 30})
    users.insert({'name': 'Jane', 'age': 25})
    users.insert({'name': 'Bob', 'age': 35})

    # Test equality filter
    results = list(users.find(name='John'))
    assert len(results) == 1
    assert results[0]['name'] == 'John'

    # Test comparison operator
    results = list(users.find(age={'>=': 30}))
    assert len(results) == 2  # John and Bob

    # Test multiple filters (AND)
    results = list(users.find(age={'>=': 30}, name='John'))
    assert len(results) == 1
    assert results[0]['name'] == 'John'

    db.close()


def test_count():
    """Test count operation."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert test data
    users.insert({'name': 'John', 'age': 30})
    users.insert({'name': 'Jane', 'age': 25})
    users.insert({'name': 'Bob', 'age': 35})

    # Count all
    total = users.count()
    assert total == 3

    # Count with filter
    adults = users.count(age={'>=': 30})
    assert adults == 2

    db.close()


def test_update():
    """Test update operation."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert
    users.insert({'name': 'John', 'age': 30})

    # Update
    count = users.update({'age': 31}, name='John')
    assert count == 1

    # Verify
    found = users.find_one(name='John')
    assert found['age'] == 31

    db.close()


def test_delete():
    """Test delete operation."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert
    users.insert({'name': 'John', 'age': 30})
    users.insert({'name': 'Jane', 'age': 25})

    # Delete
    count = users.delete(name='John')
    assert count == 1

    # Verify
    remaining = users.count()
    assert remaining == 1

    db.close()


def test_read_only_mode():
    """Test read-only validation."""
    db = connect('sqlite:///:memory:', read_only=True)
    users = db['users']

    # SELECT should work
    count = users.count()
    assert count == 0

    # INSERT should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        users.insert({'name': 'John'})

    # UPDATE should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        users.update({'age': 31}, name='John')

    # DELETE should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        users.delete(name='John')

    db.close()


def test_upsert():
    """Test upsert (insert or update) operation."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # First upsert - should insert
    pk1 = users.upsert({'name': 'John', 'age': 30}, keys=['name'])
    assert pk1 is not None

    # Second upsert - should update
    pk2 = users.upsert({'name': 'John', 'age': 31}, keys=['name'])

    # Verify only one row exists with updated age
    count = users.count()
    assert count == 1

    found = users.find_one(name='John')
    assert found['age'] == 31

    db.close()


def test_insert_many():
    """Test batch insert operation."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    rows = [
        {'name': 'John', 'age': 30},
        {'name': 'Jane', 'age': 25},
        {'name': 'Bob', 'age': 35},
    ]

    count = users.insert_many(rows)
    assert count == 3

    # Verify
    total = users.count()
    assert total == 3

    db.close()


def test_order_by():
    """Test ordering results."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert out of order
    users.insert({'name': 'Charlie', 'age': 35})
    users.insert({'name': 'Alice', 'age': 25})
    users.insert({'name': 'Bob', 'age': 30})

    # Order by name ascending
    results = [user['name'] for user in users.find(_order_by='name')]
    assert results == ['Alice', 'Bob', 'Charlie']

    # Order by age descending
    results = [user['age'] for user in users.find(_order_by='-age')]
    assert results == [35, 30, 25]

    db.close()


def test_limit_and_offset():
    """Test pagination with limit and offset."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert test data
    for i in range(10):
        users.insert({'name': f'User{i}', 'age': 20 + i})

    # Test limit
    results = list(users.find(_limit=3))
    assert len(results) == 3

    # Test offset
    results = list(users.find(_offset=5, _limit=3))
    assert len(results) == 3

    db.close()


def test_distinct():
    """Test distinct values query."""
    db = connect('sqlite:///:memory:')
    users = db['users']

    # Insert with duplicate statuses
    users.insert({'name': 'John', 'status': 'active'})
    users.insert({'name': 'Jane', 'status': 'active'})
    users.insert({'name': 'Bob', 'status': 'inactive'})

    # Get distinct statuses
    statuses = [row['status'] for row in users.distinct('status')]

    assert len(statuses) == 2
    assert 'active' in statuses
    assert 'inactive' in statuses

    db.close()


def test_insert_with_decimal_values():
    """Test Decimal round-trip - insert and retrieve with precision preserved."""
    db = connect('sqlite:///:memory:')
    products = db['products']

    # Insert with Decimal values
    products.insert({
        'name': 'Widget',
        'price': Decimal('99.99'),
        'tax_rate': Decimal('0.085'),
    })

    # Retrieve and verify precision is preserved
    product = products.find_one(name='Widget')
    assert product['name'] == 'Widget'
    assert product['price'] == Decimal('99.99')
    assert product['tax_rate'] == Decimal('0.085')
    assert isinstance(product['price'], Decimal)
    assert isinstance(product['tax_rate'], Decimal)

    db.close()


def test_decimal_and_float_compatibility():
    """Test mixed Decimal/float values in same table."""
    db = connect('sqlite:///:memory:')
    items = db['items']

    # Insert Decimal
    items.insert({'value': Decimal('100.00')})

    # Insert float - should work with same table
    items.insert({'value': 50.5})

    # Verify both rows exist
    count = items.count()
    assert count == 2

    # Retrieve both
    rows = list(items.all())
    assert len(rows) == 2

    db.close()


def test_decimal_in_batch_insert():
    """Test Decimal values in batch insert operations."""
    db = connect('sqlite:///:memory:')
    transactions = db['transactions']

    # Batch insert with Decimal values
    data = [
        {'amount': Decimal('100.50'), 'description': 'Payment 1'},
        {'amount': Decimal('200.75'), 'description': 'Payment 2'},
        {'amount': Decimal('50.25'), 'description': 'Payment 3'},
    ]

    for record in data:
        transactions.insert(record)

    # Verify all inserted with correct precision
    count = transactions.count()
    assert count == 3

    # Check first transaction
    tx = transactions.find_one(description='Payment 1')
    assert tx['amount'] == Decimal('100.50')
    assert isinstance(tx['amount'], Decimal)

    db.close()


def test_decimal_query_filtering():
    """Test querying with Decimal values."""
    db = connect('sqlite:///:memory:')
    products = db['products']

    # Insert products with different prices
    products.insert({'name': 'Cheap', 'price': Decimal('10.00')})
    products.insert({'name': 'Medium', 'price': Decimal('50.00')})
    products.insert({'name': 'Expensive', 'price': Decimal('100.00')})

    # Query by exact Decimal value
    cheap = products.find_one(price=Decimal('10.00'))
    assert cheap['name'] == 'Cheap'

    db.close()
