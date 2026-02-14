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
- **Vector Support**: Store and search embeddings with auto-detection from Python lists (no numpy required)

## Installation

```bash
pip install dbset                  # base installation
pip install 'dbset[asyncpg]'         # + async PostgreSQL driver
pip install 'dbset[psycopg2]'        # + sync PostgreSQL driver
pip install 'dbset[postgres]'        # + all PostgreSQL drivers
pip install 'dbset[aiosqlite]'       # + async SQLite driver
pip install 'dbset[vector]'            # + numpy for vector support
pip install 'dbset[all]'             # all drivers + numpy
pip install 'dbset[dev]'             # development dependencies
```

### Dependencies

- `sqlalchemy>=2.0` (core dependency)
- `greenlet>=3.0` (for SQLAlchemy async support)
- `asyncpg>=0.29.0` (async PostgreSQL driver, optional)
- `psycopg2-binary>=2.9.9` (sync PostgreSQL driver, optional)
- `aiosqlite>=0.19.0` (async SQLite driver, optional)
- `numpy>=1.24.0` (for vector type inference from numpy arrays, optional)

## Quick Start
    db = connect('sqlite:///:memory:')
    # db = await async_connect('sqlite+aiosqlite:///:memory:')
    # db = connect('sqlite:///db.sqlite')
    # db = connect('postgresql://user:password@localhost:5432/database_name')
    # db = connect('postgresql+asyncpg://user:password@localhost:5432/database_name',)

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
| `list` (≥64 numeric) | VECTOR(dim) | TEXT | VECTOR(dim) |
| `numpy.ndarray` | VECTOR(dim) | TEXT | VECTOR(dim) |

**Note:** Lists with ≥64 numeric elements are automatically detected as vectors, not JSON.

**Why JSONB for PostgreSQL?**
- Binary storage format - faster reads
- Supports GIN indexes for fast JSON queries
- Native operators: `->`, `->>`, `@>`, `?`
- No duplicate keys, no whitespace preservation

### Vector Support (Embeddings & Similarity Search)

DBset supports storing and searching vector embeddings for AI/ML workloads such as semantic search, RAG, and recommendation systems. Vectors are stored efficiently per database dialect and can be queried with similarity search.

#### Inserting Vectors

```python
from dbset import connect, Vector

db = connect('postgresql://user:password@localhost/mydb')
items = db['items']

# Auto-detection: Lists with ≥64 numeric elements are automatically inferred as vectors
# No numpy required - plain Python lists work directly
embedding = [0.1, 0.2, ...] * 128  # 128-dim embedding from your ML model
items.insert({'name': 'doc1', 'embedding': embedding})  # Auto-inferred as Vector(128)

# Insert numpy arrays (also auto-inferred)
import numpy as np
items.insert({'name': 'doc2', 'embedding': np.array([0.4, 0.5, 0.6] * 100)})

# Full numpy workflow example
import numpy as np

# Simulate embeddings from a model (e.g., sentence-transformers)
embeddings = np.random.randn(3, 768).astype(np.float32)  # 3 docs, 768-dim

# Insert - numpy arrays auto-detected as Vector(768)
for i, emb in enumerate(embeddings):
    items.insert({'name': f'doc{i}', 'embedding': emb})

# Search with numpy query vector
query_vec = np.random.randn(768).astype(np.float32)
for row in items.find_similar('embedding', query_vec, limit=5):
    print(row['name'], row['_distance'])

# Explicit type for short vectors (< 64 elements)
items.insert(
    {'name': 'doc3', 'embedding': '[0.1, 0.2, 0.3]'},
    types={'embedding': Vector(dim=3)}
)
```

**Automatic Vector Detection:**
- Lists with **≥64 numeric elements** (int or float) are automatically detected as vectors
- `numpy.ndarray` (1D) is also auto-detected as Vector with dimension inferred from array length
- No need for numpy or explicit `types=` parameter for typical ML embeddings (128, 256, 512, 768, 1024, etc.)
- Vectors are serialized correctly for each database dialect (pgvector format for PostgreSQL)
- For short vectors (< 64 elements), use explicit `types={'column': Vector(dim=N)}`

#### Similarity Search

Use `find_similar()` to find the closest vectors by distance:

```python
# Find 5 most similar items by cosine distance
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], metric='cosine', limit=5):
    print(row['name'], row['_distance'])

# L2 (Euclidean) distance
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], metric='l2', limit=5):
    print(row['name'], row['_distance'])

# Inner product
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], metric='inner_product'):
    print(row['name'], row['_distance'])

# With distance threshold (only return results within threshold)
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], metric='cosine', threshold=0.5):
    print(row['name'], row['_distance'])

# With additional filters
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], category='science'):
    print(row['name'], row['_distance'])

# Without distance in results
for row in items.find_similar('embedding', [0.1, 0.2, 0.3], include_distance=False):
    print(row['name'])
```

#### Async API

```python
from dbset import async_connect, Vector
import numpy as np

db = await async_connect('sqlite+aiosqlite:///:memory:')
items = db['items']

await items.insert({'name': 'doc1', 'embedding': np.array([0.1, 0.2, 0.3])})

async for row in items.find_similar('embedding', [0.1, 0.2, 0.3], metric='cosine', limit=5):
    print(row['name'], row['_distance'])

await db.close()
```

#### Vector Indexes (PostgreSQL with pgvector)

On PostgreSQL with the pgvector extension, you can create HNSW or IVFFlat indexes for fast approximate nearest-neighbor search:

```python
# Create HNSW index (recommended for most use cases)
items.create_vector_index('embedding', method='hnsw', metric='cosine')

# Create IVFFlat index with custom parameters
items.create_vector_index('embedding', method='ivfflat', metric='l2', lists=100)

# Async version
await items.create_vector_index('embedding', method='hnsw', metric='cosine')
```

#### Database-Specific Storage

| Database | Storage Type | Similarity Search | Index Support |
|----------|-------------|-------------------|---------------|
| SQLite | TEXT (JSON string) | Python-side computation | No |
| PostgreSQL | VECTOR(dim) via pgvector | Native operators (`<->`, `<=>`, `<#>`) | HNSW, IVFFlat |
| MySQL 9+ | VECTOR | `VECTOR_DISTANCE()` function | No |

#### Distance Metrics

| Metric | Constant | Description | Lower = More Similar |
|--------|----------|-------------|---------------------|
| Cosine | `DistanceMetric.COSINE` | Cosine distance (1 - cosine similarity) | Yes |
| L2 | `DistanceMetric.L2` | Euclidean distance | Yes |
| Inner Product | `DistanceMetric.INNER_PRODUCT` | Negative dot product | Yes |

**Notes:**
- **numpy is fully optional.** Plain Python lists with ≥64 numeric elements are automatically detected as vectors. No JSON strings or explicit types needed for typical ML embeddings.
- `find_similar()` accepts both `list[float]` and numpy arrays as query vectors.
- For SQLite, similarity search fetches all rows and computes distances in Python. This works well for small-to-medium datasets. For large-scale vector search, use PostgreSQL with pgvector.

### Hybrid Search (BM25 + Vector)

Hybrid search combines **full-text search (BM25)** with **vector similarity** for superior retrieval quality. This is especially useful for RAG (Retrieval-Augmented Generation) applications where you want both semantic understanding and keyword matching.

#### Basic Usage

```python
from dbset import connect

db = connect('postgresql://localhost/mydb')
docs = db['documents']

# Hybrid search with automatic index creation
for row in docs.hybrid_search(
    vector_column='embedding',
    text_column='content',
    query_vector=[0.1, 0.2, ...],  # Your query embedding
    query_text='machine learning tutorial',
    limit=10,
    ensure=True,  # Auto-create FTS index if not exists
):
    print(row['title'], row['_score'])
```

#### Fusion Methods

Two fusion algorithms are available:

**RRF (Reciprocal Rank Fusion)** - Default, recommended:
```python
# RRF combines rankings, not raw scores
# Formula: score = 1/(k + rank_vector) + 1/(k + rank_bm25)
results = docs.hybrid_search(
    vector_column='embedding',
    text_column='content',
    query_vector=query_vec,
    query_text='python async',
    fusion='rrf',     # Default
    rrf_k=60,         # K parameter (default: 60)
)
```

**Linear Fusion** - Weighted combination of normalized scores:
```python
# Linear combines normalized scores with weights
# Formula: score = α * norm(vector_sim) + (1-α) * norm(bm25)
results = docs.hybrid_search(
    vector_column='embedding',
    text_column='content',
    query_vector=query_vec,
    query_text='python async',
    fusion='linear',
    vector_weight=0.7,  # 70% vector, 30% BM25
)
```

#### Distance Metrics

```python
from dbset import DistanceMetric

# Cosine distance (default) - best for normalized embeddings
docs.hybrid_search(..., vector_metric=DistanceMetric.COSINE)

# L2 / Euclidean distance
docs.hybrid_search(..., vector_metric=DistanceMetric.L2)

# Dot product / Inner product
docs.hybrid_search(..., vector_metric=DistanceMetric.INNER_PRODUCT)

# Or use strings
docs.hybrid_search(..., vector_metric='cosine')
docs.hybrid_search(..., vector_metric='l2')
docs.hybrid_search(..., vector_metric='inner_product')
```

#### Multi-Column Text Search

Search across multiple text columns:

```python
# Search in both title and content
results = docs.hybrid_search(
    vector_column='embedding',
    text_column=['title', 'content'],  # Multiple columns
    query_vector=query_vec,
    query_text='neural networks',
    ensure=True,
)
```

#### JSONB Array Filtering (`@>` operator)

Filter by JSONB array containment (e.g., tags):

```python
# Filter posts where tags JSONB array contains 'ai'
results = posts.hybrid_search(
    vector_column='embedding',
    text_column='text',
    query_vector=query_vec,
    query_text='artificial intelligence',
    tags={'@>': 'ai'},  # JSONB containment
)

# Multiple values (must contain all)
results = posts.hybrid_search(
    ...
    tags={'@>': ['ai', 'ml']},  # Must have both tags
)

# Alternative syntax
results = posts.find(tags={'jsonb_contains': 'python'})
```

**JSONB List (array) - containment and existence operators:**
```python
# Data: tags = ['ai', 'ml', 'python']

# Find where tags contains 'ai'
posts.find(tags={'@>': 'ai'})
# SQL: tags @> '["ai"]'::jsonb

# Find where tags contains ALL of these (AND logic)
posts.find(tags={'@>': ['ai', 'ml']})
# SQL: tags @> '["ai", "ml"]'::jsonb
# Matches: ['ai', 'ml', 'python'] ✓, ['ai', 'python'] ✗

# Find where tags contains ANY of these (OR logic)
posts.find(tags={'?|': ['ai', 'ml', 'data']})
# SQL: tags ?| array['ai', 'ml', 'data']
# Matches: ['ai'] ✓, ['data', 'other'] ✓, ['none'] ✗

# Alternative readable syntax
posts.find(tags={'jsonb_any': ['ai', 'ml']})  # same as ?|
posts.find(tags={'jsonb_all': ['ai', 'ml']})  # same as ?&

# Combine with hybrid search
posts.hybrid_search(
    vector_column='embedding',
    text_column='text',
    query_vector=vec,
    query_text='machine learning',
    tags={'?|': ['ai', 'ml', 'data']},  # ANY of these tags
)
```

**JSONB Dict (object) - use dot notation:**
```python
# Data: metadata = {'category': 'tech', 'author': {'name': 'John', 'role': 'admin'}}

# Simple field access
posts.find(**{'metadata.category': 'tech'})
# SQL: metadata->>'category' = 'tech'

# Nested field access
posts.find(**{'metadata.author.name': 'John'})
# SQL: metadata #>> '{author,name}' = 'John'

# With comparison operators
posts.find(**{'metadata.views': {'>=': 1000}})

# Combined with hybrid search
posts.hybrid_search(
    vector_column='embedding',
    text_column='text',
    query_vector=vec,
    query_text='python',
    **{'metadata.category': 'tech'},  # JSONB dict filter
)
```

**JSONB Dict containment - use `@>` with dict:**
```python
# Check if JSONB object contains key-value pair
posts.find(metadata={'@>': {'category': 'tech'}})
# SQL: metadata @> '{"category": "tech"}'::jsonb

# Check nested structure
posts.find(metadata={'@>': {'author': {'role': 'admin'}}})
# SQL: metadata @> '{"author": {"role": "admin"}}'::jsonb
```

**Supported JSONB operators:**

| Operator | Syntax | Description |
|----------|--------|-------------|
| `@>` | `tags={'@>': 'ai'}` | Contains ALL (AND) |
| `<@` | `tags={'<@': ['a','b']}` | Contained in value |
| `?|` | `tags={'?|': ['ai','ml']}` | Contains ANY (OR) |
| `?&` | `tags={'?&': ['ai','ml']}` | Contains ALL (same as `@>` for arrays) |
| `jsonb_contains` | `tags={'jsonb_contains': 'ai'}` | Alias for `@>` |
| `jsonb_any` | `tags={'jsonb_any': ['ai','ml']}` | Alias for `?|` |
| `jsonb_all` | `tags={'jsonb_all': ['ai','ml']}` | Alias for `?&` |
| dot notation | `{'meta.field': 'val'}` | Access nested field (equality) |

**Database support:**
- **PostgreSQL:** `column @> '["ai"]'::jsonb`
- **MySQL:** `JSON_CONTAINS(column, '["ai"]')`
- **SQLite:** `EXISTS (SELECT 1 FROM json_each(column) WHERE value = 'ai')`

#### FTS Index Management

```python
# Manually create FTS index
docs.create_fts_index(['title', 'content'], language='english')

# Check if FTS index exists
if docs.has_fts_index(['title', 'content']):
    print("FTS index ready")

# Auto-create with ensure=True (recommended)
docs.hybrid_search(..., ensure=True)  # Creates index if missing
```

#### Async API

```python
from dbset import async_connect

db = await async_connect('postgresql+asyncpg://localhost/mydb')
docs = db['documents']

async for row in docs.hybrid_search(
    vector_column='embedding',
    text_column='content',
    query_vector=query_vec,
    query_text='deep learning',
    ensure=True,
):
    print(row['title'], row['_score'])

await db.close()
```

#### Database-Specific FTS Implementation

| Database | FTS Technology | Ranking Function | Index Type |
|----------|----------------|------------------|------------|
| PostgreSQL | tsvector + GIN | `ts_rank_cd()` | GIN index |
| SQLite | FTS5 virtual table | `bm25()` | FTS5 triggers |
| MySQL | FULLTEXT index | `MATCH() AGAINST()` | FULLTEXT |

#### Complete Example

```python
from dbset import connect, DistanceMetric
import numpy as np

db = connect('postgresql://localhost/mydb')
posts = db['tg_posts']

# Get query embedding from your model
query_embedding = model.encode("искусственный интеллект")

# Hybrid search with all features
results = list(posts.hybrid_search(
    vector_column='embedding',
    text_column='text',
    query_vector=query_embedding,
    query_text='нейросеть машинное обучение',
    limit=10,
    fusion='rrf',
    rrf_k=60,
    vector_metric=DistanceMetric.COSINE,
    ensure=True,
    language='russian',
    tags={'@>': 'ai'},  # Only posts tagged 'ai'
))

for r in results:
    print(f"[{r['channel']}] score={r['_score']:.4f}")
    print(f"  {r['text'][:100]}...")

db.close()
```

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
├── vector.py             # Vector type, serialization, distance computation
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
- ✅ Vector support (embeddings, similarity search, pgvector integration)
- ✅ Unit tests (230+ tests passing)

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
