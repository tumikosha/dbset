# DBset aka AsyncDataset - Thin Wrapper on SQLAlchemy with Async Support

A Python library for simplified database operations, inspired by the original `dataset` library but with native async/await support and dual sync/async APIs.

## Features

- **Built on SQLAlchemy 2.x**: Thin wrapper providing Pythonic API over SQLAlchemy
- **Dual API**: Both sync and async interfaces with identical APIs
- **Automatic Schema Management**: Auto-create tables and columns on insert
- **Read-Only Mode**: Built-in safety for marketing queries
- **Connection Pooling**: Efficient connection reuse via SQLAlchemy
- **Dict-Based Filtering**: Pythonic query API with advanced filters
- **Type Inference**: Automatic Python → SQLAlchemy type mapping (TEXT for all strings)
- **JSON/JSONB Support**: Native handling of nested dicts and lists (JSONB for PostgreSQL)

## Installation

```bash
pip install dbset                  # base installation
pip install 'dbset[asyncpg]'         # + async PostgreSQL driver
pip install 'dbset[psycopg2]'        # + sync PostgreSQL driver
pip install 'dbset[postgres]'        # + all PostgreSQL drivers
pip install 'dbset[aiosqlite]'       # + async SQLite driver
pip install 'dbset[all]'             # all drivers
pip install 'dbset[dev]'             # development dependencies
```

### Dependencies

- `sqlalchemy>=2.0` (core dependency)
- `greenlet>=3.0` (for SQLAlchemy async support)
- `asyncpg>=0.29.0` (async PostgreSQL driver, optional)
- `psycopg2-binary>=2.9.9` (sync PostgreSQL driver, optional)
- `aiosqlite>=0.19.0` (async SQLite driver, optional)

## Quick Start
    db = connect('sqlite:///:memory:')
    # db = connect('sqlite:///db.sqlite')
    # db = connect('postgresql://localhost/mydb')
    users = db['users']                                 # Get table
    pk = users.insert({'name': 'John', 'age': 30})      # Insert data 
    for user in users.find(age={'>=': 18}):             # Find with filters
        print(user)
    db.close()

### Async API (Recommended)

```python
import asyncio
from dbset import connect, async_connect

async def main():
    # Connect to database   
    # db = await async_connect('postgresql+asyncpg://localhost/mydb')
    # db = await async_connect('sqlite+aiosqlite:///db.sqlite')
    db = await async_connect('sqlite+aiosqlite:///:memory:')

    # Get table (auto-creates if doesn't exist)
    users = db['users']

    # Insert data
    pk = await users.insert({'name': 'John', 'age': 30})

    # Find with filters
    async for user in users.find(age={'>=': 18}):
        print(user)

    # Update
    await users.update({'age': 31}, name='John')

    # Delete
    await users.delete(name='John')

    # Close connection
    await db.close()

if __name__ == '__main__':
    result = asyncio.run(main())
    print(result)
```

### Sync API (For Simple Scripts)

```python
from dbset import connect

# Connect to database
# db = connect('sqlite:///:memory:')
# db = connect('sqlite:///db.sqlite')
db = connect('postgresql://localhost/mydb')

# Get table
users = db['users']

# Insert data
pk = users.insert({'name': 'John', 'age': 30})

# Find with filters
for user in users.find(age={'>=': 18}):
    print(user)

# Close connection
db.close()
```

### Read-Only Mode (For Safety)

```python
# Marketing queries with read-only safety
db = await async_connect(
    'postgresql+asyncpg://localhost/clinic',
    read_only=True  # Only SELECT allowed
)

patients = db['patients']

# This works - SELECT query
async for patient in patients.find(last_visit={'<': '2024-01-01'}):
    print(patient)

# This raises ReadOnlyError
await patients.insert({'name': 'Hacker'})  # ❌ Blocked!
```

## Advanced Usage

### Complex Filters

```python
# Comparison operators
users.find(age={'>=': 18})
users.find(age={'<': 65})
users.find(status={'!=': 'deleted'})

# IN queries
users.find(status={'in': ['active', 'pending', 'approved']})

# LIKE patterns
users.find(email={'like': '%@gmail.com'})
users.find(name={'startswith': 'John'})
users.find(name={'endswith': 'son'})

# BETWEEN
users.find(age={'between': [18, 65]})

# NULL checks
users.find(deleted_at={'is': None})

# Multiple conditions (AND)
users.find(age={'>=': 18}, status='active')
```

### Ordering and Pagination

```python
# Order by column (ascending)
async for user in users.find(_order_by='age'):
    print(user)

# Order by column (descending)
async for user in users.find(_order_by='-age'):
    print(user)

# Multiple order columns
async for user in users.find(_order_by=['name', '-age']):
    print(user)

# Pagination
async for user in users.find(_limit=10, _offset=20):
    print(user)
```

### Batch Operations

```python
# Insert many rows
rows = [
    {'name': 'John', 'age': 30},
    {'name': 'Jane', 'age': 25},
    {'name': 'Bob', 'age': 35},
]
count = await users.insert_many(rows)

# Upsert (insert or update)
await users.upsert(
    {'name': 'John', 'age': 31},
    keys=['name']  # Check if name exists
)
```

### Handling Non-Existent Keys

The `keys` parameter in `update()`, `upsert()`, and `upsert_many()` supports graceful handling of non-existent columns, matching the `dataset` library behavior.

**Why this matters:** When working with dynamic schemas, external data sources, or evolving codebases, your key columns might not always exist in the table. DBSet handles this gracefully instead of raising errors.

#### Behavior Summary

| Function | Some keys exist | All keys non-existent |
|----------|-----------------|----------------------|
| `upsert()` | INSERT new row (no match found) | INSERT new row |
| `upsert_many()` | INSERT new rows | INSERT new rows |
| `update()` | Update using valid keys only | Raises `QueryError` |

#### upsert() with Non-Existent Keys

When `keys` contains columns that don't exist in the table, the lookup query returns no match, causing an INSERT instead of UPDATE:

```python
# If 'nonexistent' column doesn't exist, this inserts a new row
await users.upsert(
    {'name': 'John', 'age': 31},
    keys=['name', 'nonexistent']  # 'nonexistent' not in table → INSERT
)

# Even if 'name' exists and matches, the non-existent key causes no match
# Result: New row inserted, not updated
```

**Use case:** Safe handling of schema mismatches between different environments or data versions.

#### update() with Non-Existent Keys

The `update()` function filters out non-existent keys and proceeds with valid ones:

```python
# With keys=['name', 'nonexistent'], only 'name' is used for WHERE clause
count = users.update(
    {'name': 'John', 'age': 99, 'nonexistent': 'val'},
    keys=['name', 'nonexistent']  # 'nonexistent' ignored, uses name='John'
)

# If ALL keys are non-existent, raises QueryError (no valid WHERE clause)
from dbset import QueryError
try:
    users.update({'age': 99}, keys=['foo', 'bar'])  # All keys invalid
except QueryError as e:
    print("Cannot update: no valid keys")  # ← This is raised
```

**Why the difference?**
- `upsert()` has a fallback (INSERT), so it can safely proceed
- `update()` without a WHERE clause would update ALL rows, which is dangerous

### Transactions

```python
# Async transactions
async with db.transaction():
    await users.insert({'name': 'Alice'})
    await orders.insert({'user_id': 1, 'total': 100})
    # Both committed together

# Sync transactions
with db.transaction():
    users.insert({'name': 'Alice'})
    orders.insert({'user_id': 1, 'total': 100})
```

### JSON/JSONB Support

DBset automatically handles nested Python dicts and lists, storing them as JSON columns. For PostgreSQL, the optimized **JSONB** type is used automatically.

```python
# Insert data with nested structures - no manual serialization needed!
await users.insert({
    'name': 'John',
    'metadata': {
        'role': 'admin',
        'permissions': ['read', 'write', 'delete']
    },
    'tags': ['python', 'sql', 'async'],
    'orders': [
        {'product': 'Book', 'qty': 2, 'price': 29.99},
        {'product': 'Pen', 'qty': 5, 'price': 4.99}
    ]
})

# Data is stored as:
# - PostgreSQL: JSONB columns (fast queries, indexable)
# - SQLite/others: JSON columns

# Query and use - data comes back as Python dicts/lists
user = await users.find_one(name='John')
print(user['metadata']['role'])  # 'admin'
print(user['orders'][0]['product'])  # 'Book'
```

**Type mapping by database:**

| Python Type | PostgreSQL | SQLite | Other |
|-------------|------------|--------|-------|
| `dict` | JSONB | JSON | JSON |
| `list` | JSONB | JSON | JSON |

**Why JSONB for PostgreSQL?**
- Binary storage format - faster reads
- Supports GIN indexes for fast JSON queries
- Native operators: `->`, `->>`, `@>`, `?`
- No duplicate keys, no whitespace preservation

### Index Management

AsyncDataset automatically manages indexes for optimal performance.

#### Automatic Index Creation

When using `upsert()` or `upsert_many()` with `ensure=True`, indexes are **automatically created** on the key columns:

```python
# Automatic index creation on upsert
await table.upsert(
    {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
    keys=['email'],
    ensure=True  # Auto-creates table, columns, AND index on 'email'
)

# Verify index was created
assert await table.has_index(['email']) is True

# Compound keys create compound indexes
await table.upsert(
    {'email': 'bob@example.com', 'country': 'US', 'age': 25},
    keys=['email', 'country'],
    ensure=True  # Auto-creates index on ['email', 'country']
)

# Batch operations create index once before processing
rows = [
    {'email': f'user{i}@example.com', 'name': f'User{i}'}
    for i in range(1000)
]
await table.upsert_many(rows, keys=['email'], ensure=True)
# Index created once, then 1000 fast upserts

# Sync API works identically
table.upsert(
    {'email': 'charlie@example.com', 'name': 'Charlie'},
    keys=['email'],
    ensure=True
)
assert table.has_index(['email']) is True
```

**Why auto-indexing on upsert?**
- Upsert performs lookup (`find_one`) on every call using the `keys` parameter
- Without an index, this is a full table scan - O(n) complexity
- With an index, lookups are O(log n) - dramatically faster for large tables
- `ensure=True` means "set up everything needed for optimal operation"

**When indexes are NOT auto-created:**
- `insert()` / `insert_many()` - no lookup needed, so no critical performance benefit
- `upsert()` with `ensure=False` - user has explicit control
- `update()` methods - updates use existing keys, not critical path

#### Manual Index Creation

You can always create indexes explicitly for fine-grained control:

```python
# Create single column index
idx_name = await table.create_index('email')
# Returns: 'idx_users_email'

# Create compound index on multiple columns
idx_name = await table.create_index(['country', 'city'])
# Returns: 'idx_users_country_city'

# Create unique index with custom name
idx_name = await table.create_index(
    'username',
    name='unique_username',
    unique=True
)

# Idempotent - creating same index twice succeeds
idx_name = await table.create_index('email')  # First time
idx_name = await table.create_index('email')  # Second time - no error

# Check if index exists
if not await table.has_index('email'):
    await table.create_index('email')

# Check compound index
has_compound = await table.has_index(['country', 'city'])

# Database-specific features (PostgreSQL partial index)
from sqlalchemy import text
idx_name = await table.create_index(
    'email',
    postgresql_where=text("status = 'active'")
)

# Sync API works identically
idx_name = table.create_index('email')
assert table.has_index('email') is True
```

**Index Naming Convention:**
- Auto-generated names follow pattern: `idx_{table}_{col1}_{col2}`
- Long names are truncated to 63 characters (PostgreSQL limit) with hash suffix
- Custom names can be provided via the `name` parameter

**When to Use Indexes:**
- Columns frequently used in WHERE clauses
- Columns used in JOIN conditions
- Columns used for sorting (ORDER BY)
- Foreign key columns
- Email/username fields for authentication lookups

**Best Practices:**
- Create indexes after bulk data imports for better performance
- Use compound indexes for queries filtering on multiple columns together
- Use unique indexes to enforce data integrity constraints
- Monitor index usage - unused indexes slow down writes

### Direct SQLAlchemy Access

```python
from sqlalchemy import select, func

# Get SQLAlchemy Table object
users_table = await users.table

# Build complex query with SQLAlchemy
stmt = (
    select(users_table.c.name, func.count().label('count'))
    .where(users_table.c.age > 18)
    .group_by(users_table.c.name)
    .order_by(func.count().desc())
)

# Execute via dataset
async for row in db.query(stmt):
    print(row)
```

## Architecture

### Components

```
dataset/
├── __init__.py           # Public API (connect, async_connect)
├── async_core.py         # AsyncDatabase, AsyncTable (async API)
├── sync_core.py          # Database, Table (sync API)
├── schema.py             # Schema management (DDL operations)
├── query.py              # FilterBuilder (dict → SQLAlchemy WHERE)
├── types.py              # TypeInference (Python → SQLAlchemy types)
├── validators.py         # ReadOnlyValidator (SQL safety)
├── connection.py         # Connection pooling
└── exceptions.py         # Exception hierarchy
```

### How It Works

1. **Schema Discovery**: Reflects database schema using SQLAlchemy MetaData
2. **Auto-Create**: Automatically creates tables/columns on insert
3. **Type Inference**: Infers SQLAlchemy types from Python values (including JSON/JSONB for dicts/lists)
4. **Query Building**: Translates dict filters to SQLAlchemy WHERE clauses
5. **Validation**: Checks SQL safety in read-only mode
6. **Execution**: Executes via SQLAlchemy async/sync engines

### SQLAlchemy Integration

AsyncDataset is a **thin wrapper** on SQLAlchemy:

```python
# Dataset simplified API
await table.insert({'name': 'John', 'age': 30})

# Translates to SQLAlchemy under the hood
from sqlalchemy import insert
stmt = insert(table._table).values(name='John', age=30)
await conn.execute(stmt)
```

**You always have direct SQLAlchemy access:**
- `table.table` → SQLAlchemy Table object
- `db.query(sqlalchemy_statement)` → Execute SQLAlchemy statements
- `db.engine` → SQLAlchemy Engine
- `db.metadata` → SQLAlchemy MetaData

## Testing

```bash
# Run all tests
uv run pytest tests/unit/dataset/ -v

# Run specific test file
uv run pytest tests/unit/dataset/test_async_core.py -v

# With coverage
uv run pytest tests/unit/dataset/ --cov=src/dataset --cov-report=html
```


### Marketing Churn Query

```python
from dbset import async_connect
from datetime import datetime, timedelta


async def find_churn_customers(db_url: str):
    """Find patients who haven't visited in 6+ months."""
    db = await async_connect(db_url, read_only=True)

    six_months_ago = datetime.now() - timedelta(days=180)
    patients = db['patients']

    churn_list = []
    async for patient in patients.find(
            last_visit={'<': six_months_ago},
            status='active',
            _limit=100,
            _order_by='-last_visit'
    ):
        churn_list.append(patient)

    await db.close()
    return churn_list
```

### CSV Import with Auto-Schema

```python
from dbset import connect
import csv


def import_customers(csv_path: str):
    """Import CSV with automatic table creation."""
    db = connect('postgresql://localhost/clinic')
    customers = db['customers']

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Auto-creates table with columns from CSV headers
    count = customers.insert_many(rows, ensure=True)

    print(f"Imported {count} rows")
    db.close()
```

## Status

**Phase 1-3 Complete:**
- ✅ Infrastructure (exceptions, types, validators, connection, query)
- ✅ Schema management (DDL operations)
- ✅ Async API (AsyncDatabase, AsyncTable)
- ✅ Sync API (Database, Table)
- ✅ JSON/JSONB support (auto-detection by dialect)
- ✅ Unit tests (170+ tests passing)

**Remaining Phases:**
- [ ] Integration tests with PostgreSQL
- [ ] Performance benchmarks
- [ ] Documentation and examples

## Design Philosophy

**AsyncDataset = Simplified API + SQLAlchemy Power**

- Use dataset's simple API for common operations (80% use case)
- Use SQLAlchemy directly for complex queries (20% use case)
- No magic - everything translates to standard SQLAlchemy code
- Always possible to drop down to SQLAlchemy when needed

## License
Apache-2.0
