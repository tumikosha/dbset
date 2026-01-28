# DBSet - Async Database Library Documentation

## Overview

**DBSet** is a Python library for simplified database operations, built on SQLAlchemy 2.x with async support. The library is inspired by the original `dataset` library but provides native async/await support and a dual synchronous/asynchronous API.

### Key Features

- **SQLAlchemy 2.x** - Thin wrapper over SQLAlchemy with Pythonic API
- **Dual API** - Synchronous and asynchronous interfaces with identical API
- **Automatic Schema Management** - Auto-create tables and columns on insert
- **Read-Only Mode** - Built-in security for marketing queries
- **Connection Pooling** - Efficient connection reuse through SQLAlchemy
- **Dict-based Filtering** - Pythonic query API with advanced filters
- **Automatic Type Inference** - Automatic Python → SQLAlchemy type mapping
- **JSON/JSONB Support** - Native handling of nested dicts and lists (JSONB for PostgreSQL)
- **UUID Support** - UUID-based primary keys
- **Index Management** - Automatic and manual index creation
- **Transactions** - Transaction support via context managers

### Supported Databases

- PostgreSQL (asyncpg, psycopg2)
- SQLite (aiosqlite, built-in driver)
- MySQL (planned)
- MongoDB (planned)

---

## Installation

```toml
dependencies = [
    "sqlalchemy[asyncio]>=2.0.25",
    "asyncpg>=0.29.0",           # Async PostgreSQL driver
    "psycopg2-binary>=2.9.9",    # Sync PostgreSQL driver
    "aiosqlite>=0.19.0",         # Async SQLite driver
]
```

To install dependencies:

```bash
cd agent
uv sync
```

---

## Quick Start

### Async API (Recommended)

```python
from dbset import async_connect

async def main():
    # Connect to database
    db = await async_connect('postgresql+asyncpg://localhost/mydb')

    # Get table (auto-created if doesn't exist)
    users = db['users']

    # Insert data
    pk = await users.insert({'name': 'John', 'age': 30})
    print(f"Inserted user with ID: {pk}")

    # Search with filters
    async for user in users.find(age={'>=': 18}):
        print(f"{user['name']}: {user['age']} years old")

    # Update
    await users.update({'age': 31}, name='John')

    # Delete
    await users.delete(name='John')

    # Close connection
    await db.close()
```

### Sync API (For Simple Scripts)

```python
from dbset import connect

# Connect to database
db = connect('postgresql://localhost/mydb')

# Get table
users = db['users']

# Insert data
pk = users.insert({'name': 'John', 'age': 30})

# Search with filters
for user in users.find(age={'>=': 18}):
    print(f"{user['name']}: {user['age']} years old")

# Close connection
db.close()
```

### Read-Only Mode

```python
# Marketing queries with write protection
db = await async_connect(
    'postgresql+asyncpg://localhost/clinic',
    read_only=True  # Only SELECT queries allowed
)

patients = db['patients']

# This works - SELECT query
async for patient in patients.find(last_visit={'<': '2024-01-01'}):
    print(patient)

# This will raise ReadOnlyError
await patients.insert({'name': 'Hacker'})  # Blocked!
```

---

## API Reference

### Database Connection

#### `async_connect(url, **kwargs)`

Creates an asynchronous database connection.

**Parameters:**
- `url` (str): Database URL with async driver
  - PostgreSQL: `postgresql+asyncpg://user:pass@host/db`
  - SQLite: `sqlite+aiosqlite:///path/to/db.sqlite`
- `read_only` (bool): If True, only SELECT queries are allowed (default: False)
- `ensure_schema` (bool): If True, auto-create tables/columns (default: True)
- `primary_key_type` (str | PrimaryKeyType): Primary key type ('integer', 'uuid')
- `primary_key_column` (str): Primary key column name (default: 'id')
- `pk_config` (PrimaryKeyConfig): Advanced primary key configuration

**Returns:** `AsyncDatabase`

**Example:**
```python
# PostgreSQL with Integer PK
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# UUID primary keys
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)

# Custom PK column name
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid',
    primary_key_column='user_id'
)

# SQLite for testing
db = await async_connect('sqlite+aiosqlite:///:memory:')
```

#### `connect(url, **kwargs)`

Creates a synchronous database connection. Accepts the same parameters as `async_connect()`.

**Example:**
```python
db = connect('postgresql://localhost/mydb')
db = connect('sqlite:///:memory:')
```

---

### AsyncDatabase Class

#### Methods

##### `db[table_name]` - Get Table

Returns an `AsyncTable` object for the specified table. The table is automatically created on first insert.

```python
users = db['users']
orders = db['orders']
```

##### `await db.close()` - Close Connection

Closes the database connection and releases resources.

```python
await db.close()
```

##### `async with db.transaction()` - Transaction

Context manager for executing operations within a transaction.

```python
async with db.transaction():
    await users.insert({'name': 'Alice'})
    await orders.insert({'user_id': 1, 'total': 100})
    # Both operations will be committed together
```

##### `async for row in db.query(stmt)` - Execute SQLAlchemy Query

Executes a SQLAlchemy statement directly.

```python
from sqlalchemy import select, func

users_table = await users.table
stmt = select(func.count()).select_from(users_table)
async for row in db.query(stmt):
    print(row)
```

---

### AsyncTable Class

#### Inserting Data

##### `await table.insert(row, ensure=True)`

Inserts a single row into the table.

**Parameters:**
- `row` (dict): Dictionary with data
- `ensure` (bool): Auto-create table/columns if they don't exist

**Returns:** Primary key of the inserted row

**Example:**
```python
pk = await users.insert({
    'name': 'John',
    'age': 30,
    'email': 'john@example.com'
})
print(f"Inserted with ID: {pk}")
```

##### `await table.insert_many(rows, chunk_size=1000, ensure=True)`

Inserts multiple rows at once.

**Parameters:**
- `rows` (list[dict]): List of dictionaries with data
- `chunk_size` (int): Batch size for insertion (default: 1000)
- `ensure` (bool): Auto-create table/columns

**Returns:** Number of inserted rows

**Example:**
```python
rows = [
    {'name': 'John', 'age': 30},
    {'name': 'Jane', 'age': 25},
    {'name': 'Bob', 'age': 35},
]
count = await users.insert_many(rows)
print(f"Inserted {count} rows")
```

#### Finding Data

##### `async for row in table.find(**filters)`

Finds rows matching the specified filters.

**Parameters:**
- `**filters`: Search filters (see "Filters" section)
- `_order_by` (str | list[str]): Sorting (e.g., 'age', '-age', ['name', '-age'])
- `_limit` (int): Maximum number of rows
- `_offset` (int): Offset for pagination

**Returns:** AsyncIterator[dict]

**Examples:**
```python
# Simple filter
async for user in users.find(age=30):
    print(user)

# Filters with operators
async for user in users.find(age={'>=': 18}):
    print(user)

# Multiple filters (AND)
async for user in users.find(age={'>=': 18}, status='active'):
    print(user)

# With sorting
async for user in users.find(_order_by='-age', _limit=10):
    print(user)

# Pagination
async for user in users.find(_limit=20, _offset=40):
    print(user)
```

##### `await table.find_one(**filters)`

Finds the first row matching the filters.

**Returns:** dict | None

**Example:**
```python
user = await users.find_one(email='john@example.com')
if user:
    print(f"Found: {user['name']}")
```

##### `await table.all()`

Returns all rows from the table.

**Returns:** list[dict]

**Example:**
```python
all_users = await users.all()
print(f"Total users: {len(all_users)}")
```

#### Updating Data

##### `await table.update(data, **filters)`

Updates rows matching the filters.

**Parameters:**
- `data` (dict): New values for update
- `**filters`: Filters for selecting rows

**Returns:** Number of updated rows

**Example:**
```python
# Update age for John
updated = await users.update({'age': 31}, name='John')
print(f"Updated {updated} rows")

# Update all users over 30
updated = await users.update({'status': 'senior'}, age={'>': 30})
```

##### `await table.upsert(row, keys, ensure=True)`

Inserts a row or updates if it exists.

**Parameters:**
- `row` (dict): Data to insert/update
- `keys` (list[str]): Columns for existence check
- `ensure` (bool): Auto-create table/columns and indexes

**Returns:** Primary key of the row

**Example:**
```python
# Insert or update by email
pk = await users.upsert(
    {'email': 'john@example.com', 'name': 'John', 'age': 31},
    keys=['email']
)

# Insert or update by compound key
pk = await users.upsert(
    {'email': 'bob@example.com', 'country': 'US', 'age': 25},
    keys=['email', 'country']
)
```

**Note:** With `ensure=True`, an index is automatically created on the `keys` columns for optimal performance.

##### `await table.upsert_many(rows, keys, chunk_size=1000, ensure=True)`

Bulk upsert operation.

**Example:**
```python
rows = [
    {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
    {'email': 'bob@example.com', 'name': 'Bob', 'age': 25},
]
count = await users.upsert_many(rows, keys=['email'], ensure=True)
```

#### Deleting Data

##### `await table.delete(**filters)`

Deletes rows matching the filters.

**Returns:** Number of deleted rows

**Example:**
```python
# Delete specific user
deleted = await users.delete(name='John')

# Delete all inactive users
deleted = await users.delete(status='inactive')

# Delete ALL rows (careful!)
deleted = await users.delete()
```

#### Aggregation

##### `await table.count(**filters)`

Counts rows with filters.

**Example:**
```python
# Total users
total = await users.count()

# Adult users
adults = await users.count(age={'>=': 18})
```

##### `async for row in table.distinct(column, **filters)`

Returns unique values of a column.

**Example:**
```python
# Unique ages
async for row in users.distinct('age'):
    print(f"Age: {row['age']}")

# Unique countries of active users
async for row in users.distinct('country', status='active'):
    print(f"Country: {row['country']}")
```

#### Index Management

##### `await table.create_index(columns, name=None, unique=False, **kwargs)`

Creates an index on specified columns.

**Parameters:**
- `columns` (str | list[str]): Column or list of columns
- `name` (str): Index name (optional, auto-generated)
- `unique` (bool): Create unique index
- `**kwargs`: Additional parameters (e.g., `postgresql_where`)

**Returns:** Name of the created index

**Examples:**
```python
# Index on single column
idx_name = await users.create_index('email')
# Returns: 'idx_users_email'

# Compound index
idx_name = await users.create_index(['country', 'city'])
# Returns: 'idx_users_country_city'

# Unique index with custom name
idx_name = await users.create_index(
    'username',
    name='unique_username',
    unique=True
)

# Partial index (PostgreSQL)
from sqlalchemy import text
idx_name = await users.create_index(
    'email',
    postgresql_where=text("status = 'active'")
)
```

##### `await table.has_index(columns)`

Checks if an index exists on the columns.

**Returns:** bool

**Example:**
```python
if not await users.has_index('email'):
    await users.create_index('email')

# Check compound index
has_compound = await users.has_index(['country', 'city'])
```

#### SQLAlchemy Access

##### `await table.table` - Get SQLAlchemy Table

Returns the `sqlalchemy.Table` object for direct SQLAlchemy API usage.

**Example:**
```python
from sqlalchemy import select, func

users_table = await users.table

# Complex query with SQLAlchemy
stmt = (
    select(users_table.c.name, func.count().label('count'))
    .where(users_table.c.age > 18)
    .group_by(users_table.c.name)
    .order_by(func.count().desc())
)

async for row in db.query(stmt):
    print(f"{row['name']}: {row['count']}")
```

---

## Query Filters

DBSet supports a powerful dict-based filter system for queries.

### Simple Filters

```python
# Exact match
users.find(status='active')
users.find(age=30)

# Multiple conditions (AND)
users.find(status='active', age=30)
```

### Comparison Operators

```python
# Greater/less than
users.find(age={'>': 18})
users.find(age={'>=': 18})
users.find(age={'<': 65})
users.find(age={'<=': 65})

# Not equal
users.find(status={'!=': 'deleted'})
```

### IN Queries

```python
# IN list of values
users.find(status={'in': ['active', 'pending', 'approved']})
users.find(age={'in': [25, 30, 35]})
```

### LIKE Patterns

```python
# LIKE with wildcards
users.find(email={'like': '%@gmail.com'})

# Special operators
users.find(name={'startswith': 'John'})  # name LIKE 'John%'
users.find(name={'endswith': 'son'})      # name LIKE '%son'
users.find(name={'contains': 'doe'})      # name LIKE '%doe%'
```

### BETWEEN

```python
# BETWEEN (inclusive)
users.find(age={'between': [18, 65]})
users.find(created_at={'between': ['2024-01-01', '2024-12-31']})
```

### NULL Checks

```python
# IS NULL
users.find(deleted_at={'is': None})

# IS NOT NULL
users.find(deleted_at={'is_not': None})
```

### Combined Filters

```python
# All conditions are joined with AND
async for user in users.find(
    age={'>=': 18, '<': 65},
    status='active',
    country={'in': ['US', 'UK', 'CA']},
    email={'like': '%@gmail.com'}
):
    print(user)
```

---

## Usage Examples

### Example 1: Marketing Query - Finding Customer Churn

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
        churn_list.append({
            'name': patient['name'],
            'email': patient['email'],
            'last_visit': patient['last_visit']
        })

    await db.close()
    return churn_list
```

### Example 2: CSV Import with Auto-Schema Creation

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

    # Auto-create table with columns from CSV headers
    count = customers.insert_many(rows, ensure=True)

    print(f"Imported {count} rows")
    db.close()
```

### Example 3: Upsert with UUID Primary Keys

```python
from dbset import async_connect

async def sync_users_with_uuid():
    """Sync users with UUID primary keys."""
    db = await async_connect(
        'postgresql+asyncpg://localhost/mydb',
        primary_key_type='uuid'
    )

    users = db['users']

    # Upsert will automatically create an index on email
    await users.upsert(
        {
            'email': 'alice@example.com',
            'name': 'Alice',
            'age': 30
        },
        keys=['email'],
        ensure=True
    )

    # Bulk sync
    new_users = [
        {'email': 'bob@example.com', 'name': 'Bob', 'age': 25},
        {'email': 'charlie@example.com', 'name': 'Charlie', 'age': 35},
    ]
    await users.upsert_many(new_users, keys=['email'])

    await db.close()
```

### Example 4: Transactions

```python
async def transfer_money(from_user_id: int, to_user_id: int, amount: float):
    """Transfer money between users with a transaction."""
    db = await async_connect('postgresql+asyncpg://localhost/bank')
    accounts = db['accounts']

    async with db.transaction():
        # Deduct from sender
        await accounts.update(
            {'balance': {'decrement': amount}},
            user_id=from_user_id
        )

        # Add to recipient
        await accounts.update(
            {'balance': {'increment': amount}},
            user_id=to_user_id
        )

        # If error - automatic rollback

    await db.close()
```

### Example 5: Direct SQLAlchemy Query

```python
from dbset import async_connect
from sqlalchemy import select, func, and_

async def advanced_analytics():
    """Advanced analytics with SQLAlchemy."""
    db = await async_connect('postgresql+asyncpg://localhost/clinic')

    patients = db['patients']
    appointments = db['appointments']

    patients_table = await patients.table
    appointments_table = await appointments.table

    # Complex query with JOIN and aggregation
    stmt = (
        select(
            patients_table.c.name,
            func.count(appointments_table.c.id).label('visit_count'),
            func.max(appointments_table.c.date).label('last_visit')
        )
        .select_from(
            patients_table.join(
                appointments_table,
                patients_table.c.id == appointments_table.c.patient_id
            )
        )
        .where(appointments_table.c.status == 'completed')
        .group_by(patients_table.c.id, patients_table.c.name)
        .having(func.count(appointments_table.c.id) > 5)
        .order_by(func.count(appointments_table.c.id).desc())
    )

    async for row in db.query(stmt):
        print(f"{row['name']}: {row['visit_count']} visits, last: {row['last_visit']}")

    await db.close()
```

---

## Architecture

### Module Structure

```
dbset/
├── __init__.py           # Public API (connect, async_connect)
├── async_core.py         # AsyncDatabase, AsyncTable (async API)
├── sync_core.py          # Database, Table (sync API)
├── schema.py             # Schema management (DDL operations)
├── query.py              # FilterBuilder (dict → SQLAlchemy WHERE)
├── types.py              # TypeInference (Python → SQLAlchemy types)
├── validators.py         # ReadOnlyValidator (SQL security)
├── connection.py         # Connection pooling
└── exceptions.py         # Exception hierarchy
```

### How It Works

1. **Schema Discovery**: Reflects DB schema using SQLAlchemy MetaData
2. **Auto-Create**: Automatically creates tables/columns on insert
3. **Type Inference**: Infers SQLAlchemy types from Python values
4. **Query Building**: Translates dict filters to SQLAlchemy WHERE conditions
5. **Validation**: Validates SQL safety in read-only mode
6. **Execution**: Executes through SQLAlchemy async/sync engines

### SQLAlchemy Integration

DBSet is a **thin wrapper** over SQLAlchemy:

```python
# Simplified DBSet API
await table.insert({'name': 'John', 'age': 30})

# Translates to SQLAlchemy under the hood
from sqlalchemy import insert
stmt = insert(table._table).values(name='John', age=30)
await conn.execute(stmt)
```

**You always have direct access to SQLAlchemy:**
- `table.table` → SQLAlchemy Table object
- `db.query(sqlalchemy_statement)` → Execute SQLAlchemy statements
- `db.engine` → SQLAlchemy Engine
- `db.metadata` → SQLAlchemy MetaData

---

## Primary Key Configuration

### PrimaryKeyType

Enum with supported primary key types:

- `PrimaryKeyType.INTEGER` - Auto-increment integer (default)
- `PrimaryKeyType.UUID` - UUID strings (String(36))
- `PrimaryKeyType.CUSTOM` - Custom type with user-defined generator

### PrimaryKeyConfig

Class for advanced primary key configuration.

**Parameters:**
- `pk_type` (PrimaryKeyType | str): Primary key type
- `column_name` (str): PK column name (default: 'id')
- `generator` (Callable): Value generation function (for UUID/CUSTOM)
- `sqlalchemy_type` (TypeEngine): SQLAlchemy type (for CUSTOM)

**Examples:**

```python
from dbset import async_connect, PrimaryKeyConfig, PrimaryKeyType
from uuid import uuid4
from sqlalchemy import String

# Integer auto-increment (default)
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# UUID primary keys
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)

# UUID with custom column name
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid',
    primary_key_column='user_id'
)

# Uppercase UUID via PrimaryKeyConfig
pk_config = PrimaryKeyConfig(
    pk_type='uuid',
    generator=lambda: str(uuid4()).upper()
)
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pk_config=pk_config
)

# Fully custom primary key
pk_config = PrimaryKeyConfig(
    pk_type='custom',
    column_name='custom_id',
    generator=lambda: f"USER_{uuid4()}",
    sqlalchemy_type=String(50)
)
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pk_config=pk_config
)
```

---

## Exceptions

### Exception Hierarchy

```
DatasetError (base class)
├── ConnectionError         - Database connection error
├── TableNotFoundError      - Table doesn't exist
├── ColumnNotFoundError     - Column doesn't exist
├── ReadOnlyError          - Write attempt in read-only mode
├── TransactionError       - Transaction error
├── ValidationError        - Data validation error
├── SchemaError           - DDL operation error
├── QueryError            - Query execution error
└── TypeInferenceError    - Type inference error
```

### Handling Examples

```python
from dbset import async_connect
from dbset import (
    ReadOnlyError,
    TableNotFoundError,
    ValidationError
)

async def safe_operation():
    db = await async_connect(
        'postgresql+asyncpg://localhost/mydb',
        read_only=True
    )

    try:
        users = db['users']
        await users.insert({'name': 'John'})
    except ReadOnlyError as e:
        print(f"Write operation blocked: {e}")
    except TableNotFoundError as e:
        print(f"Table not found: {e.table_name}")
    except ValidationError as e:
        print(f"Validation failed: {e}")
    finally:
        await db.close()
```

---

## Index Management

### Automatic Index Creation

When using `upsert()` or `upsert_many()` with `ensure=True`, indexes are **automatically created** on key columns:

```python
# Automatic index creation on upsert
await table.upsert(
    {'email': 'alice@example.com', 'name': 'Alice', 'age': 30},
    keys=['email'],
    ensure=True  # Auto-creates table, columns AND index on 'email'
)

# Verify index creation
assert await table.has_index(['email']) is True

# Compound keys create compound indexes
await table.upsert(
    {'email': 'bob@example.com', 'country': 'US', 'age': 25},
    keys=['email', 'country'],
    ensure=True  # Auto-creates index on ['email', 'country']
)
```

### Why Automatic Indexes on Upsert?

- Upsert performs a lookup (`find_one`) on each call using the `keys` parameter
- Without an index, this is a full table scan - O(n) complexity
- With an index, lookup is O(log n) - dramatically faster for large tables
- `ensure=True` means "set up everything needed for optimal operation"

### When Indexes Are NOT Automatically Created:

- `insert()` / `insert_many()` - No lookup required
- `upsert()` with `ensure=False` - Explicit user control
- `update()` methods - Use existing keys

### Manual Index Creation

```python
# Index on single column
idx_name = await table.create_index('email')
# Returns: 'idx_users_email'

# Compound index
idx_name = await table.create_index(['country', 'city'])
# Returns: 'idx_users_country_city'

# Unique index with custom name
idx_name = await table.create_index(
    'username',
    name='unique_username',
    unique=True
)

# Idempotent - repeated creation doesn't cause error
idx_name = await table.create_index('email')  # First time
idx_name = await table.create_index('email')  # Second time - no error

# Check existence
if not await table.has_index('email'):
    await table.create_index('email')

# Partial index (PostgreSQL)
from sqlalchemy import text
idx_name = await table.create_index(
    'email',
    postgresql_where=text("status = 'active'")
)
```

### Index Naming

- Auto-generated names: `idx_{table}_{col1}_{col2}`
- Long names are truncated to 63 characters (PostgreSQL limit) with hash suffix
- Custom names can be specified via the `name` parameter

### When to Use Indexes

- Columns frequently used in WHERE conditions
- Columns for JOIN operations
- Columns for sorting (ORDER BY)
- Foreign key columns
- Email/username fields for authentication

### Best Practices

- Create indexes after bulk data imports for better performance
- Use compound indexes for queries filtering on multiple columns
- Use unique indexes to ensure data integrity
- Monitor index usage - unused indexes slow down writes

---

## Type Inference

### TypeInference

Class for automatic SQLAlchemy type inference from Python values.

**Supported Types:**

| Python Type | SQLAlchemy Type | Notes |
|-------------|-----------------|-------|
| `int` | `Integer()` | |
| `float` | `Float()` | |
| `Decimal` | `Numeric(p, s)` | Auto-calculated precision/scale |
| `bool` | `Boolean()` | Checked before int (bool is int subclass) |
| `str` | `String(255)` or `Text()` | Text for strings >255 chars |
| `bytes` | `Text()` | May be improved for binary types |
| `datetime` | `DateTime()` | |
| `date` | `Date()` | |
| `dict` | `JSON()` or `JSONB()` | JSONB for PostgreSQL, JSON for others |
| `list` | `JSON()` or `JSONB()` | JSONB for PostgreSQL, JSON for others |
| `None` | `String(255)` | Nullable by default |

**Examples:**

```python
from dbset.types import TypeInference
from decimal import Decimal

# Infer types from values
TypeInference.infer_type(42)                    # Integer()
TypeInference.infer_type(3.14)                  # Float()
TypeInference.infer_type(Decimal('123.45'))     # Numeric(5, 2)
TypeInference.infer_type(True)                  # Boolean()
TypeInference.infer_type('hello')               # String(255)
TypeInference.infer_type('x' * 300)             # Text()
TypeInference.infer_type(datetime.now())        # DateTime()

# Infer types from row
row = {'name': 'John', 'age': 30, 'active': True}
types = TypeInference.infer_types_from_row(row)
# {'name': String(255), 'age': Integer(), 'active': Boolean()}

# Merge types (for multiple rows)
TypeInference.merge_types(Integer(), Float())   # Float()
TypeInference.merge_types(String(50), String(100))  # String(100)
TypeInference.merge_types(Date(), DateTime())   # DateTime()

# JSON types (auto-detect dialect)
TypeInference.infer_type({'key': 'value'})                    # JSON()
TypeInference.infer_type({'key': 'value'}, dialect='postgresql')  # JSONB()
TypeInference.infer_type([1, 2, 3], dialect='postgresql')     # JSONB()
```

---

## JSON/JSONB Support

DBSet automatically handles nested Python dicts and lists, storing them as JSON columns. For PostgreSQL, the optimized **JSONB** type is used automatically.

### Inserting JSON Data

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
```

### Querying JSON Data

```python
# Data comes back as Python dicts/lists
user = await users.find_one(name='John')
print(user['metadata']['role'])       # 'admin'
print(user['orders'][0]['product'])   # 'Book'
print(user['tags'])                   # ['python', 'sql', 'async']
```

### Type Mapping by Database

| Python Type | PostgreSQL | SQLite | Other |
|-------------|------------|--------|-------|
| `dict` | JSONB | JSON | JSON |
| `list` | JSONB | JSON | JSON |

### Why JSONB for PostgreSQL?

- **Binary storage format** - faster reads and queries
- **Supports GIN indexes** - fast JSON content queries
- **Native operators** - `->`, `->>`, `@>`, `?` for querying inside JSON
- **No duplicate keys** - automatically deduplicated
- **No whitespace preservation** - more compact storage

### Advanced: SQLAlchemy JSON Queries

For complex JSON queries, use SQLAlchemy directly:

```python
from sqlalchemy import select

users_table = await users.table

# PostgreSQL JSONB operators via SQLAlchemy
stmt = select(users_table).where(
    users_table.c.metadata['role'].astext == 'admin'
)

async for row in db.query(stmt):
    print(row)
```

---

## Testing

### Running Tests

```bash
# All tests
uv run pytest tests/unit/dbset/ -v

# Specific file
uv run pytest tests/unit/dbset/test_async_core.py -v

# Specific test
uv run pytest tests/unit/dbset/test_async_core.py::test_insert -v

# With code coverage
uv run pytest tests/unit/dbset/ --cov=src/dbset --cov-report=html
```

### Test Examples

```python
import pytest
from dbset import async_connect

@pytest.mark.asyncio
async def test_insert_and_find():
    """Test insert and find."""
    db = await async_connect('sqlite+aiosqlite:///:memory:')
    users = db['users']

    # Insert
    pk = await users.insert({'name': 'John', 'age': 30})
    assert pk is not None

    # Find
    user = await users.find_one(name='John')
    assert user['name'] == 'John'
    assert user['age'] == 30

    await db.close()

@pytest.mark.asyncio
async def test_read_only_mode():
    """Test read-only mode."""
    db = await async_connect(
        'sqlite+aiosqlite:///:memory:',
        read_only=True
    )
    users = db['users']

    # Insert attempt should raise ReadOnlyError
    with pytest.raises(ReadOnlyError):
        await users.insert({'name': 'Hacker'})

    await db.close()
```

---

## Best Practices

### 1. Use Async API for Modern Applications

```python
# Recommended
db = await async_connect('postgresql+asyncpg://localhost/mydb')

# Only for simple scripts
db = connect('postgresql://localhost/mydb')
```

### 2. Always Close Connections

```python
# With context manager (future version)
async with await async_connect(url) as db:
    users = db['users']
    await users.insert({'name': 'John'})

# Manual closing
db = await async_connect(url)
try:
    users = db['users']
    await users.insert({'name': 'John'})
finally:
    await db.close()
```

### 3. Use Read-Only Mode for Security

```python
# For marketing queries and analytics
db = await async_connect(db_url, read_only=True)
```

### 4. Create Indexes for Frequently Used Columns

```python
# With upsert ensure=True, indexes are created automatically
await users.upsert(
    {'email': 'alice@example.com', 'name': 'Alice'},
    keys=['email'],
    ensure=True
)

# Or create manually for complex cases
await users.create_index(['country', 'city'])
```

### 5. Use Transactions for Related Operations

```python
async with db.transaction():
    await users.insert({'name': 'Alice'})
    await orders.insert({'user_id': 1, 'total': 100})
    # Both operations will be committed together
```

### 6. Use Batch Operations for Large Volumes

```python
# Efficient
rows = [{'name': f'User{i}', 'age': i} for i in range(1000)]
await users.insert_many(rows, chunk_size=500)

# Inefficient
for i in range(1000):
    await users.insert({'name': f'User{i}', 'age': i})
```

### 7. Use SQLAlchemy Directly for Complex Queries

```python
from sqlalchemy import select, func

users_table = await users.table
stmt = (
    select(users_table.c.country, func.count().label('count'))
    .group_by(users_table.c.country)
    .order_by(func.count().desc())
)

async for row in db.query(stmt):
    print(f"{row['country']}: {row['count']}")
```

### 8. Use UUID for Distributed Systems

```python
# UUID is better for distributed systems without a central ID generator
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    primary_key_type='uuid'
)
```

---

## Performance

### Connection Pooling

DBSet uses SQLAlchemy's connection pooling for efficient connection reuse:

```python
db = await async_connect(
    'postgresql+asyncpg://localhost/mydb',
    pool_size=10,        # Pool size (default: 5)
    max_overflow=20,     # Additional connections (default: 10)
)
```

### Batch Operations

Use `insert_many()` and `upsert_many()` for inserting large volumes of data:

```python
# Insert 10,000 rows in batches of 1000
rows = [{'name': f'User{i}', 'age': i % 100} for i in range(10000)]
await users.insert_many(rows, chunk_size=1000)
```

### Indexes

Create indexes for columns used in WHERE, JOIN, ORDER BY:

```python
# Index is automatically created with upsert ensure=True
await users.upsert(data, keys=['email'], ensure=True)

# Or create manually
await users.create_index('email')
await users.create_index(['country', 'city'])
```

### Limit and Offset

Use pagination for large result sets:

```python
# First page (0-20)
async for user in users.find(_limit=20, _offset=0):
    print(user)

# Second page (20-40)
async for user in users.find(_limit=20, _offset=20):
    print(user)
```

---

## FAQ

### Q: How is DBSet different from SQLAlchemy ORM?

**A:** DBSet is a thin wrapper over SQLAlchemy Core (not ORM), providing a simplified dict-based API. Unlike ORM, there's no need to define model classes - tables are created automatically from data.

### Q: Can DBSet be used with existing databases?

**A:** Yes! DBSet reflects existing schema and works with it. Auto-creation only triggers for non-existent tables/columns.

### Q: How to handle schema migrations?

**A:** DBSet is not designed for complex migrations. For production, use Alembic or other migration tools. DBSet automatically adds new columns on insert.

### Q: Are JOIN queries supported?

**A:** For JOINs, use the direct SQLAlchemy API via `db.query()` with SQLAlchemy statements.

### Q: Can DBSet be used in production?

**A:** Yes, DBSet is built on SQLAlchemy 2.x and uses its connection pooling and security. Thorough testing is recommended for critical systems.

### Q: How to handle connection errors?

**A:** Use try/except to catch `ConnectionError`:

```python
from dbset import ConnectionError

try:
    db = await async_connect('postgresql+asyncpg://bad-url')
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

---

## Development Status

**Phases 1-3 Complete:**
- ✅ Infrastructure (exceptions, types, validators, connection, query)
- ✅ Schema management (DDL operations)
- ✅ Async API (AsyncDatabase, AsyncTable)
- ✅ Sync API (Database, Table)
- ✅ JSON/JSONB support (auto-detection by dialect)
- ✅ Unit tests (170+ tests)

**Remaining Phases:**
- [ ] Integration tests with PostgreSQL
- [ ] Performance benchmarks
- [ ] Extended documentation and examples

---

## Design Philosophy

**DBSet = Simplified API + SQLAlchemy Power**

- Use simple DBSet API for common operations (80% of cases)
- Use SQLAlchemy directly for complex queries (20% of cases)
- No magic - everything translates to standard SQLAlchemy code
- Always possible to fall back to SQLAlchemy when needed

---