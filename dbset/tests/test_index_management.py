"""Tests for index management functionality in AsyncDataset."""

import pytest
from sqlalchemy import text

from dbset import async_connect, connect
from dbset.exceptions import ColumnNotFoundError


@pytest.mark.asyncio
class TestAsyncIndexManagement:
    """Test async index creation and checking."""

    async def test_create_index_single_column(self, async_db):
        """Test creating index on single column."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index
        idx_name = await table.create_index("email")

        # Verify index name follows convention
        assert idx_name == "idx_users_email"

        # Verify index exists
        assert await table.has_index("email") is True

    async def test_create_index_compound(self, async_db):
        """Test creating compound index on multiple columns."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "country": "US", "city": "NYC"})

        # Create compound index
        idx_name = await table.create_index(["country", "city"])

        # Verify index name
        assert idx_name == "idx_users_country_city"

        # Verify index exists
        assert await table.has_index(["country", "city"]) is True

    async def test_create_index_custom_name(self, async_db):
        """Test creating index with custom name."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index with custom name
        idx_name = await table.create_index("email", name="custom_email_idx")

        # Verify custom name was used
        assert idx_name == "custom_email_idx"

        # Verify index exists
        assert await table.has_index("email") is True

    async def test_create_index_unique(self, async_db):
        """Test creating unique index."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "username": "alice"})

        # Create unique index
        idx_name = await table.create_index("username", unique=True)

        # Verify index was created
        assert idx_name == "idx_users_username"
        assert await table.has_index("username") is True

        # Verify uniqueness constraint works
        await table.insert({"name": "Bob", "username": "bob"})

        # Attempting to insert duplicate should fail
        with pytest.raises(Exception):  # Will be IntegrityError
            await table.insert({"name": "Charlie", "username": "alice"})

    async def test_create_index_idempotent(self, async_db):
        """Test that creating same index twice is idempotent."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index first time
        idx_name1 = await table.create_index("email")

        # Create same index again
        idx_name2 = await table.create_index("email")

        # Should return same name without error
        assert idx_name1 == idx_name2
        assert await table.has_index("email") is True

    async def test_has_index_returns_true(self, async_db):
        """Test has_index returns True for existing index."""
        table = async_db["users"]

        # Insert test data and create index
        await table.insert({"name": "Alice", "email": "alice@example.com"})
        await table.create_index("email")

        # Check index exists
        assert await table.has_index("email") is True

    async def test_has_index_returns_false(self, async_db):
        """Test has_index returns False for non-existent index."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Check index doesn't exist
        assert await table.has_index("email") is False

    async def test_create_index_nonexistent_column(self, async_db):
        """Test creating index on non-existent column raises error."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice"})

        # Try to create index on non-existent column
        with pytest.raises(ColumnNotFoundError):
            await table.create_index("nonexistent_column")

    async def test_create_index_auto_name_generation(self, async_db):
        """Test automatic index name generation."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index without name
        idx_name = await table.create_index("email")

        # Verify name follows convention
        assert idx_name.startswith("idx_users_")
        assert "email" in idx_name

    async def test_create_index_string_input(self, async_db):
        """Test create_index accepts string input for single column."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index with string input
        idx_name = await table.create_index("email")

        assert idx_name == "idx_users_email"
        assert await table.has_index("email") is True

    async def test_has_index_string_input(self, async_db):
        """Test has_index accepts string input for single column."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "email": "alice@example.com"})

        # Check with string input (before creation)
        assert await table.has_index("email") is False

        # Create and check again
        await table.create_index("email")
        assert await table.has_index("email") is True

    async def test_create_index_empty_columns(self, async_db):
        """Test creating index with empty columns list raises error."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice"})

        # Try to create index with empty list
        with pytest.raises(ValueError):
            await table.create_index([])

    async def test_create_index_long_name_truncation(self, async_db):
        """Test that long index names are truncated with hash."""
        table = async_db["users"]

        # Insert test data with long column names
        long_col1 = "very_long_column_name_that_exceeds_limits_part1"
        long_col2 = "very_long_column_name_that_exceeds_limits_part2"

        await table.insert({long_col1: "value1", long_col2: "value2"})

        # Create compound index with long name
        idx_name = await table.create_index([long_col1, long_col2])

        # Verify name is truncated to 63 chars (PostgreSQL limit)
        assert len(idx_name) <= 63
        assert idx_name.startswith("idx_users_")

    async def test_has_index_compound(self, async_db):
        """Test has_index works with compound indexes."""
        table = async_db["users"]

        # Insert test data
        await table.insert({"name": "Alice", "country": "US", "city": "NYC"})

        # Check compound index doesn't exist
        assert await table.has_index(["country", "city"]) is False

        # Create compound index
        await table.create_index(["country", "city"])

        # Check compound index exists
        assert await table.has_index(["country", "city"]) is True


@pytest.mark.asyncio
class TestAutoIndexOnUpsert:
    """Test automatic index creation on upsert with ensure=True."""

    async def test_upsert_with_ensure_creates_index(self, async_db):
        """Verify upsert with ensure=True auto-creates index on keys."""
        table = async_db['users']

        # First upsert with ensure=True
        await table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice'},
            keys=['email'],
            ensure=True
        )

        # Verify index was created automatically
        assert await table.has_index(['email']) is True

    async def test_upsert_compound_keys_creates_compound_index(self, async_db):
        """Verify compound keys create compound index."""
        table = async_db['users']

        await table.upsert(
            {'email': 'alice@example.com', 'country': 'US', 'age': 30},
            keys=['email', 'country'],
            ensure=True
        )

        # Verify compound index created
        assert await table.has_index(['email', 'country']) is True

    async def test_upsert_without_ensure_no_index(self, async_db):
        """Verify upsert with ensure=False does NOT create index."""
        table = async_db['users']

        # Insert data first (to create table)
        await table.insert({'email': 'alice@example.com', 'name': 'Alice'})

        # Upsert without ensure
        await table.upsert(
            {'email': 'bob@example.com', 'name': 'Bob'},
            keys=['email'],
            ensure=False
        )

        # Verify no index created
        assert await table.has_index(['email']) is False

    async def test_upsert_many_creates_index_once(self, async_db):
        """Verify upsert_many creates index once, not per row."""
        table = async_db['users']

        rows = [
            {'email': f'user{i}@example.com', 'name': f'User{i}'}
            for i in range(100)
        ]

        # Batch upsert with ensure
        await table.upsert_many(rows, keys=['email'], ensure=True)

        # Verify index exists
        assert await table.has_index(['email']) is True

        # Verify all rows inserted
        count = await table.count()
        assert count == 100

    async def test_upsert_idempotent_index_creation(self, async_db):
        """Verify multiple upserts don't fail when index already exists."""
        table = async_db['users']

        # First upsert creates index
        await table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice'},
            keys=['email'],
            ensure=True
        )

        # Second upsert should not fail (idempotent)
        await table.upsert(
            {'email': 'bob@example.com', 'name': 'Bob'},
            keys=['email'],
            ensure=True
        )

        # Third upsert on same key (update)
        await table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice Updated'},
            keys=['email'],
            ensure=True
        )

        # Verify index still exists and data correct
        assert await table.has_index(['email']) is True
        alice = await table.find_one(email='alice@example.com')
        assert alice['name'] == 'Alice Updated'

    async def test_upsert_many_with_updates(self, async_db):
        """Verify upsert_many handles both inserts and updates correctly."""
        table = async_db['users']

        # Initial batch
        initial_rows = [
            {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
            {'email': 'bob@example.com', 'name': 'Bob', 'age': 25}
        ]
        await table.upsert_many(initial_rows, keys=['email'], ensure=True)

        # Batch with some updates and some new inserts
        update_rows = [
            {'email': 'alice@example.com', 'name': 'Alice Updated', 'age': 31},  # Update
            {'email': 'bob@example.com', 'name': 'Bob', 'age': 26},  # Update
            {'email': 'charlie@example.com', 'name': 'Charlie', 'age': 35}  # Insert
        ]
        await table.upsert_many(update_rows, keys=['email'], ensure=True)

        # Verify index exists
        assert await table.has_index(['email']) is True

        # Verify counts
        count = await table.count()
        assert count == 3

        # Verify updates worked
        alice = await table.find_one(email='alice@example.com')
        assert alice['name'] == 'Alice Updated'
        assert alice['age'] == 31


class TestSyncIndexManagement:
    """Test sync index creation and checking."""

    def test_create_index_single_column(self, sync_db):
        """Test creating index on single column (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index
        idx_name = table.create_index("email")

        # Verify index name
        assert idx_name == "idx_users_email"

        # Verify index exists
        assert table.has_index("email") is True

    def test_create_index_compound(self, sync_db):
        """Test creating compound index (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "country": "US", "city": "NYC"})

        # Create compound index
        idx_name = table.create_index(["country", "city"])

        # Verify index name
        assert idx_name == "idx_users_country_city"

        # Verify index exists
        assert table.has_index(["country", "city"]) is True

    def test_create_index_custom_name(self, sync_db):
        """Test creating index with custom name (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index with custom name
        idx_name = table.create_index("email", name="custom_email_idx")

        # Verify custom name
        assert idx_name == "custom_email_idx"
        assert table.has_index("email") is True

    def test_create_index_unique(self, sync_db):
        """Test creating unique index (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "username": "alice"})

        # Create unique index
        idx_name = table.create_index("username", unique=True)

        # Verify index
        assert idx_name == "idx_users_username"
        assert table.has_index("username") is True

        # Verify uniqueness constraint
        table.insert({"name": "Bob", "username": "bob"})

        with pytest.raises(Exception):  # Will be IntegrityError
            table.insert({"name": "Charlie", "username": "alice"})

    def test_create_index_idempotent(self, sync_db):
        """Test idempotent index creation (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index twice
        idx_name1 = table.create_index("email")
        idx_name2 = table.create_index("email")

        # Should return same name
        assert idx_name1 == idx_name2
        assert table.has_index("email") is True

    def test_has_index_returns_true(self, sync_db):
        """Test has_index returns True (sync)."""
        table = sync_db["users"]

        # Insert data and create index
        table.insert({"name": "Alice", "email": "alice@example.com"})
        table.create_index("email")

        # Check index exists
        assert table.has_index("email") is True

    def test_has_index_returns_false(self, sync_db):
        """Test has_index returns False (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Check index doesn't exist
        assert table.has_index("email") is False

    def test_create_index_nonexistent_column(self, sync_db):
        """Test error on non-existent column (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice"})

        # Try to create index on non-existent column
        with pytest.raises(ColumnNotFoundError):
            table.create_index("nonexistent_column")

    def test_create_index_auto_name_generation(self, sync_db):
        """Test auto name generation (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create index
        idx_name = table.create_index("email")

        # Verify name convention
        assert idx_name.startswith("idx_users_")
        assert "email" in idx_name

    def test_create_index_string_input(self, sync_db):
        """Test string input (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Create with string
        idx_name = table.create_index("email")

        assert idx_name == "idx_users_email"
        assert table.has_index("email") is True

    def test_has_index_string_input(self, sync_db):
        """Test has_index string input (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "email": "alice@example.com"})

        # Check with string
        assert table.has_index("email") is False

        table.create_index("email")
        assert table.has_index("email") is True

    def test_create_index_empty_columns(self, sync_db):
        """Test empty columns error (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice"})

        # Try empty list
        with pytest.raises(ValueError):
            table.create_index([])

    def test_has_index_compound(self, sync_db):
        """Test compound index checking (sync)."""
        table = sync_db["users"]

        # Insert test data
        table.insert({"name": "Alice", "country": "US", "city": "NYC"})

        # Check doesn't exist
        assert table.has_index(["country", "city"]) is False

        # Create and check
        table.create_index(["country", "city"])
        assert table.has_index(["country", "city"]) is True


class TestSyncAutoIndexOnUpsert:
    """Test sync version of auto-index creation."""

    def test_upsert_with_ensure_creates_index_sync(self, sync_db):
        """Verify sync upsert auto-creates index."""
        table = sync_db['users']

        table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice'},
            keys=['email'],
            ensure=True
        )

        assert table.has_index(['email']) is True

    def test_upsert_many_sync_creates_index(self, sync_db):
        """Verify sync upsert_many auto-creates index."""
        table = sync_db['users']

        rows = [
            {'email': f'user{i}@example.com', 'name': f'User{i}'}
            for i in range(50)
        ]

        table.upsert_many(rows, keys=['email'], ensure=True)

        assert table.has_index(['email']) is True
        assert table.count() == 50

    def test_upsert_compound_keys_sync(self, sync_db):
        """Verify sync upsert creates compound index."""
        table = sync_db['users']

        table.upsert(
            {'email': 'alice@example.com', 'country': 'US', 'age': 30},
            keys=['email', 'country'],
            ensure=True
        )

        assert table.has_index(['email', 'country']) is True

    def test_upsert_without_ensure_no_index_sync(self, sync_db):
        """Verify sync upsert with ensure=False does NOT create index."""
        table = sync_db['users']

        # Insert data first
        table.insert({'email': 'alice@example.com', 'name': 'Alice'})

        # Upsert without ensure
        table.upsert(
            {'email': 'bob@example.com', 'name': 'Bob'},
            keys=['email'],
            ensure=False
        )

        # Verify no index created
        assert table.has_index(['email']) is False

    def test_upsert_idempotent_index_creation_sync(self, sync_db):
        """Verify sync multiple upserts don't fail when index exists."""
        table = sync_db['users']

        # First upsert creates index
        table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice'},
            keys=['email'],
            ensure=True
        )

        # Second upsert should not fail
        table.upsert(
            {'email': 'bob@example.com', 'name': 'Bob'},
            keys=['email'],
            ensure=True
        )

        # Third upsert on same key (update)
        table.upsert(
            {'email': 'alice@example.com', 'name': 'Alice Updated'},
            keys=['email'],
            ensure=True
        )

        # Verify index still exists and data correct
        assert table.has_index(['email']) is True
        alice = table.find_one(email='alice@example.com')
        assert alice['name'] == 'Alice Updated'


# Fixtures for testing
@pytest.fixture
async def async_db():
    """Create async database for testing."""
    db = await async_connect("sqlite+aiosqlite:///:memory:")
    yield db
    await db.close()


@pytest.fixture
def sync_db():
    """Create sync database for testing."""
    db = connect("sqlite:///:memory:")
    yield db
    db.close()
