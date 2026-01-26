"""Tests for UUID primary key support."""

import pytest
from uuid import UUID, uuid4

from dbset import (
    PrimaryKeyConfig,
    PrimaryKeyType,
    async_connect,
    connect,
)
from sqlalchemy import String


class TestAsyncUUIDPrimaryKey:
    """Test UUID primary key in async API."""

    @pytest.mark.asyncio
    async def test_uuid_primary_key_basic(self):
        """Test basic UUID primary key functionality."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert with auto-generated UUID
        pk = await users.insert({'name': 'John', 'age': 30})

        # Verify it's a string UUID
        assert isinstance(pk, str)
        # Verify it's a valid UUID format
        UUID(pk)  # Will raise ValueError if invalid

        # Find by UUID
        found = await users.find_one(id=pk)
        assert found is not None
        assert found['name'] == 'John'
        assert found['id'] == pk

        await db.close()

    @pytest.mark.asyncio
    async def test_uuid_primary_key_enum(self):
        """Test UUID primary key with enum."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type=PrimaryKeyType.UUID
        )
        users = db['users']

        pk = await users.insert({'name': 'Jane', 'email': 'jane@example.com'})

        assert isinstance(pk, str)
        UUID(pk)  # Validate UUID

        found = await users.find_one(id=pk)
        assert found['name'] == 'Jane'

        await db.close()

    @pytest.mark.asyncio
    async def test_custom_uuid_value(self):
        """Test inserting with custom UUID value."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert with custom UUID
        custom_id = str(uuid4())
        pk = await users.insert({'id': custom_id, 'name': 'Jane'})
        assert pk == custom_id

        # Verify
        found = await users.find_one(id=custom_id)
        assert found is not None
        assert found['name'] == 'Jane'
        assert found['id'] == custom_id

        await db.close()

    @pytest.mark.asyncio
    async def test_uuid_multiple_inserts(self):
        """Test multiple inserts with UUID primary keys."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert multiple rows
        pk1 = await users.insert({'name': 'Alice', 'age': 25})
        pk2 = await users.insert({'name': 'Bob', 'age': 30})
        pk3 = await users.insert({'name': 'Charlie', 'age': 35})

        # Verify all have different UUIDs
        assert pk1 != pk2
        assert pk2 != pk3
        assert pk1 != pk3

        # Verify all are valid UUIDs
        UUID(pk1)
        UUID(pk2)
        UUID(pk3)

        # Verify count
        count = await users.count()
        assert count == 3

        await db.close()

    @pytest.mark.asyncio
    async def test_integer_pk_default_backward_compat(self):
        """Test that Integer PK is still default (backward compatibility)."""
        db = await async_connect('sqlite+aiosqlite:///:memory:')
        users = db['users']

        # Should use Integer PK (default)
        pk = await users.insert({'name': 'John'})
        assert isinstance(pk, int)

        # Verify find works
        found = await users.find_one(id=pk)
        assert found is not None
        assert found['name'] == 'John'

        await db.close()

    @pytest.mark.asyncio
    async def test_custom_pk_column_name_uuid(self):
        """Test custom primary key column name with UUID."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type='uuid',
            primary_key_column='user_id'
        )
        users = db['users']

        # Insert - should use 'user_id' column
        pk = await users.insert({'name': 'John', 'age': 30})
        assert isinstance(pk, str)
        UUID(pk)  # Validate UUID

        # Find by custom column name
        found = await users.find_one(user_id=pk)
        assert found is not None
        assert found['name'] == 'John'
        assert found['user_id'] == pk

        # Verify 'id' column does NOT exist
        assert 'id' not in found

        await db.close()

    @pytest.mark.asyncio
    async def test_custom_pk_column_name_integer(self):
        """Test custom primary key column name with Integer."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_column='order_id'
        )
        orders = db['orders']

        # Insert
        pk = await orders.insert({'total': 100, 'status': 'pending'})
        assert isinstance(pk, int)

        # Find by custom column
        found = await orders.find_one(order_id=pk)
        assert found is not None
        assert found['total'] == 100
        assert 'order_id' in found
        assert 'id' not in found

        await db.close()

    @pytest.mark.asyncio
    async def test_advanced_pk_config(self):
        """Test advanced PrimaryKeyConfig usage."""
        # Custom generator with uppercase UUIDs
        pk_config = PrimaryKeyConfig(
            pk_type='uuid',
            column_name='custom_uuid_id',
            generator=lambda: str(uuid4()).upper()
        )

        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            pk_config=pk_config
        )
        users = db['users']

        pk = await users.insert({'name': 'John'})
        assert isinstance(pk, str)
        assert pk == pk.upper()  # Uppercase UUID

        found = await users.find_one(custom_uuid_id=pk)
        assert found is not None
        assert found['name'] == 'John'

        await db.close()

    @pytest.mark.asyncio
    async def test_custom_pk_type_with_prefix(self):
        """Test CUSTOM pk_type with prefix generator."""
        def custom_id_generator():
            return f"USER_{uuid4()}"

        pk_config = PrimaryKeyConfig(
            pk_type=PrimaryKeyType.CUSTOM,
            column_name='user_id',
            generator=custom_id_generator,
            sqlalchemy_type=String(50)
        )

        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            pk_config=pk_config
        )
        users = db['users']

        pk = await users.insert({'name': 'Alice'})
        assert isinstance(pk, str)
        assert pk.startswith('USER_')

        found = await users.find_one(user_id=pk)
        assert found is not None
        assert found['name'] == 'Alice'

        await db.close()

    @pytest.mark.asyncio
    async def test_uuid_find_operations(self):
        """Test various find operations with UUID primary keys."""
        db = await async_connect(
            'sqlite+aiosqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert test data
        pk1 = await users.insert({'name': 'Alice', 'age': 25})
        pk2 = await users.insert({'name': 'Bob', 'age': 30})
        pk3 = await users.insert({'name': 'Charlie', 'age': 35})

        # Test find with filters
        results = []
        async for user in users.find(age={'>=': 30}):
            results.append(user)

        assert len(results) == 2
        assert any(r['name'] == 'Bob' for r in results)
        assert any(r['name'] == 'Charlie' for r in results)

        # Test find_one
        found = await users.find_one(name='Alice')
        assert found is not None
        assert found['id'] == pk1

        # Test count
        count = await users.count(age={'<': 30})
        assert count == 1

        await db.close()


class TestSyncUUIDPrimaryKey:
    """Test UUID primary key in sync API."""

    def test_uuid_primary_key_sync(self):
        """Test UUID primary key in sync API."""
        db = connect(
            'sqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert with auto-generated UUID
        pk = users.insert({'name': 'John', 'age': 30})
        assert isinstance(pk, str)
        UUID(pk)  # Validate UUID

        # Find by UUID
        found = users.find_one(id=pk)
        assert found is not None
        assert found['name'] == 'John'

        db.close()

    def test_custom_uuid_value_sync(self):
        """Test inserting with custom UUID value in sync API."""
        db = connect(
            'sqlite:///:memory:',
            primary_key_type='uuid'
        )
        users = db['users']

        # Insert with custom UUID
        custom_id = str(uuid4())
        pk = users.insert({'id': custom_id, 'name': 'Jane'})
        assert pk == custom_id

        # Verify
        found = users.find_one(id=custom_id)
        assert found is not None
        assert found['name'] == 'Jane'

        db.close()

    def test_custom_pk_column_name_sync(self):
        """Test custom primary key column name in sync API."""
        db = connect(
            'sqlite:///:memory:',
            primary_key_column='order_id'
        )
        orders = db['orders']

        # Insert
        pk = orders.insert({'total': 100})
        assert isinstance(pk, int)

        # Find by custom column
        found = orders.find_one(order_id=pk)
        assert found is not None
        assert found['total'] == 100
        assert 'order_id' in found
        assert 'id' not in found

        db.close()

    def test_integer_pk_default_sync(self):
        """Test that Integer PK is still default in sync API."""
        db = connect('sqlite:///:memory:')
        users = db['users']

        pk = users.insert({'name': 'John'})
        assert isinstance(pk, int)

        db.close()


class TestPrimaryKeyConfig:
    """Test PrimaryKeyConfig class directly."""

    def test_primary_key_config_integer(self):
        """Test PrimaryKeyConfig for Integer type."""
        pk_config = PrimaryKeyConfig(pk_type=PrimaryKeyType.INTEGER)

        assert pk_config.pk_type == PrimaryKeyType.INTEGER
        assert pk_config.column_name == 'id'
        assert pk_config.autoincrement is True
        assert pk_config.generator is None

        # Test column creation
        col = pk_config.get_column()
        assert col.name == 'id'
        assert col.primary_key is True

        # Test value generation
        value = pk_config.generate_value()
        assert value is None  # Integer uses DB autoincrement

    def test_primary_key_config_uuid(self):
        """Test PrimaryKeyConfig for UUID type."""
        pk_config = PrimaryKeyConfig(pk_type='uuid')

        assert pk_config.pk_type == PrimaryKeyType.UUID
        assert pk_config.column_name == 'id'
        assert pk_config.autoincrement is False
        assert pk_config.generator is not None

        # Test column creation
        col = pk_config.get_column()
        assert col.name == 'id'
        assert col.primary_key is True

        # Test value generation
        value = pk_config.generate_value()
        assert isinstance(value, str)
        UUID(value)  # Validate UUID

    def test_primary_key_config_custom_column_name(self):
        """Test PrimaryKeyConfig with custom column name."""
        pk_config = PrimaryKeyConfig(
            pk_type='uuid',
            column_name='user_uuid'
        )

        assert pk_config.column_name == 'user_uuid'

        col = pk_config.get_column()
        assert col.name == 'user_uuid'

    def test_primary_key_config_custom_generator(self):
        """Test PrimaryKeyConfig with custom generator."""
        def custom_gen():
            return 'CUSTOM_ID_123'

        pk_config = PrimaryKeyConfig(
            pk_type='uuid',
            generator=custom_gen
        )

        value = pk_config.generate_value()
        assert value == 'CUSTOM_ID_123'

    def test_primary_key_config_custom_type(self):
        """Test PrimaryKeyConfig with CUSTOM pk_type."""
        def custom_gen():
            return f"CUSTOM_{uuid4()}"

        pk_config = PrimaryKeyConfig(
            pk_type=PrimaryKeyType.CUSTOM,
            column_name='custom_id',
            generator=custom_gen,
            sqlalchemy_type=String(50)
        )

        assert pk_config.pk_type == PrimaryKeyType.CUSTOM
        assert pk_config.column_name == 'custom_id'

        value = pk_config.generate_value()
        assert value.startswith('CUSTOM_')

    def test_primary_key_config_custom_requires_generator(self):
        """Test that CUSTOM pk_type requires generator."""
        with pytest.raises(ValueError, match="CUSTOM pk_type requires generator"):
            PrimaryKeyConfig(
                pk_type=PrimaryKeyType.CUSTOM,
                sqlalchemy_type=String(50)
            )

    def test_primary_key_config_custom_requires_type(self):
        """Test that CUSTOM pk_type requires sqlalchemy_type."""
        with pytest.raises(ValueError, match="CUSTOM pk_type requires sqlalchemy_type"):
            PrimaryKeyConfig(
                pk_type=PrimaryKeyType.CUSTOM,
                generator=lambda: 'test'
            )


class TestBackwardCompatibility:
    """Test backward compatibility with existing code."""

    @pytest.mark.asyncio
    async def test_existing_code_continues_to_work(self):
        """Test that existing code without UUID params still works."""
        # This is the most important test - existing code must work unchanged
        db = await async_connect('sqlite+aiosqlite:///:memory:')
        users = db['users']

        # Old-style usage
        pk1 = await users.insert({'name': 'Alice', 'age': 25})
        pk2 = await users.insert({'name': 'Bob', 'age': 30})

        # Both should be integers
        assert isinstance(pk1, int)
        assert isinstance(pk2, int)

        # Find operations should work
        found = await users.find_one(id=pk1)
        assert found is not None
        assert found['name'] == 'Alice'

        # Count should work
        count = await users.count()
        assert count == 2

        await db.close()

    def test_existing_sync_code_continues_to_work(self):
        """Test that existing sync code still works."""
        db = connect('sqlite:///:memory:')
        users = db['users']

        pk1 = users.insert({'name': 'Alice', 'age': 25})
        pk2 = users.insert({'name': 'Bob', 'age': 30})

        assert isinstance(pk1, int)
        assert isinstance(pk2, int)

        found = users.find_one(id=pk1)
        assert found is not None

        db.close()
