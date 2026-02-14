"""Integration tests for hybrid search with PostgreSQL.

Requires:
- PostgreSQL database with pgvector extension
- .env file with DB_URL variable
"""
import os
import json
import pytest
from pathlib import Path

# Load .env file
def load_env():
    env_paths = [
        Path(__file__).parent.parent.parent / '.env',
        Path(__file__).parent.parent / '.env',
        Path(__file__).parent / '.env',
        Path.cwd() / '.env',
    ]
    for env_path in env_paths:
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ.setdefault(key.strip(), value.strip())
            break

load_env()

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

from dbset import connect, async_connect
from sqlalchemy import text

# Database URLs
DB_URL = os.environ.get('DB_URL') or os.environ.get('db_url') or os.environ.get('DATABASE_URL', '')
if 'asyncpg' in DB_URL:
    DB_URL = DB_URL.replace('postgresql+asyncpg', 'postgresql')

ASYNC_DB_URL = os.environ.get('DB_URL') or os.environ.get('db_url') or os.environ.get('DATABASE_URL', '')
if ASYNC_DB_URL and 'postgresql' in ASYNC_DB_URL and 'asyncpg' not in ASYNC_DB_URL:
    ASYNC_DB_URL = ASYNC_DB_URL.replace('postgresql://', 'postgresql+asyncpg://')
    ASYNC_DB_URL = ASYNC_DB_URL.replace('postgresql+psycopg2://', 'postgresql+asyncpg://')

# Skip if no PostgreSQL
pytestmark = pytest.mark.skipif(
    not DB_URL or 'postgresql' not in DB_URL.lower(),
    reason="PostgreSQL DB_URL not configured"
)


def drop_table(db, table_name):
    """Drop table and associated FTS tables."""
    with db._pool.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}_fts CASCADE"))
        conn.commit()


async def async_drop_table(db, table_name):
    """Drop table async."""
    async with db._pool.connect() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        await conn.execute(text(f"DROP TABLE IF EXISTS {table_name}_fts CASCADE"))
        await conn.commit()


# ============================================================
# SYNC TESTS
# ============================================================

@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_create_fts_index_postgresql():
    """Test creating FTS index with PostgreSQL tsvector."""
    db = connect(DB_URL)
    table_name = 'test_fts_create'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Python programming guide', 'content': 'Learn Python basics'})
        docs.insert({'title': 'Machine learning tutorial', 'content': 'Introduction to ML'})
        docs.insert({'title': 'Database design', 'content': 'SQL and NoSQL databases'})

        index_name = docs.create_fts_index('title')

        assert index_name is not None
        assert 'fts' in index_name.lower()
        assert docs.has_fts_index('title')
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_create_fts_index_multiple_columns():
    """Test FTS index on multiple columns."""
    db = connect(DB_URL)
    table_name = 'test_fts_multi'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Python guide', 'description': 'A comprehensive programming guide'})

        index_name = docs.create_fts_index(['title', 'description'])

        assert index_name is not None
        assert docs.has_fts_index(['title', 'description'])
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_fts_search_returns_results():
    """Test that FTS search returns relevant results."""
    db = connect(DB_URL)
    table_name = 'test_fts_search'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Python programming', 'embedding': np.array([1.0, 0.0, 0.0])})
        docs.insert({'title': 'JavaScript basics', 'embedding': np.array([0.0, 1.0, 0.0])})
        docs.insert({'title': 'Python machine learning', 'embedding': np.array([0.8, 0.2, 0.0])})

        docs.create_fts_index('title')

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python',
            limit=10,
        ))

        assert len(results) >= 1
        titles = [r['title'] for r in results]
        assert any('Python' in t for t in titles)
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_basic():
    """Test basic hybrid search functionality."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_basic'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Deep learning fundamentals', 'embedding': np.array([0.9, 0.1, 0.0])})
        docs.insert({'title': 'Traditional machine learning', 'embedding': np.array([0.7, 0.3, 0.0])})
        docs.insert({'title': 'Web development with React', 'embedding': np.array([0.0, 0.0, 1.0])})

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.85, 0.15, 0.0],
            query_text='learning',
            ensure=True,
            limit=10,
        ))

        assert len(results) >= 1
        assert '_score' in results[0]
        top_titles = [r['title'] for r in results[:2]]
        assert any('learning' in t.lower() for t in top_titles)
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_rrf_fusion():
    """Test RRF fusion method."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_rrf'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Neural networks explained', 'embedding': np.array([0.5, 0.5, 0.0])})

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.5, 0.5, 0.0],
            query_text='neural',
            fusion='rrf',
            rrf_k=60,
            ensure=True,
        ))

        assert len(results) >= 1
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_linear_fusion():
    """Test linear fusion method."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_linear'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Data science handbook', 'embedding': np.array([0.3, 0.3, 0.4])})

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.3, 0.3, 0.4],
            query_text='data science',
            fusion='linear',
            vector_weight=0.6,
            ensure=True,
        ))

        assert len(results) >= 1
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_with_filters():
    """Test hybrid search with additional filters."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_filter'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Python for beginners', 'category': 'programming', 'embedding': np.array([1.0, 0.0, 0.0])})
        docs.insert({'title': 'Python cookbook', 'category': 'programming', 'embedding': np.array([0.9, 0.1, 0.0])})
        docs.insert({'title': 'Python for data analysis', 'category': 'data', 'embedding': np.array([0.8, 0.2, 0.0])})

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python',
            ensure=True,
            category='programming',
        ))

        for r in results:
            assert r['category'] == 'programming'
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_ensure_creates_index():
    """Test that ensure=True creates FTS index."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_ensure'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Test document', 'embedding': np.array([0.1, 0.2, 0.3])})

        assert not docs.has_fts_index('title')

        list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.1, 0.2, 0.3],
            query_text='test',
            ensure=True,
        ))

        assert docs.has_fts_index('title')
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_hybrid_search_with_jsonb():
    """Test hybrid search with JSONB metadata."""
    db = connect(DB_URL)
    table_name = 'test_hybrid_jsonb'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({
            'title': 'Machine learning basics',
            'metadata': json.dumps({'category': 'ai', 'tags': ['ml', 'python']}),
            'embedding': np.array([0.8, 0.2, 0.0]),
        })
        docs.insert({
            'title': 'Advanced deep learning',
            'metadata': json.dumps({'category': 'ai', 'tags': ['dl', 'pytorch']}),
            'embedding': np.array([0.7, 0.3, 0.0]),
        })

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.75, 0.25, 0.0],
            query_text='learning',
            ensure=True,
        ))

        assert len(results) >= 1
        assert any('learning' in r['title'].lower() for r in results[:2])
    finally:
        drop_table(db, table_name)
        db.close()


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
def test_gin_index_created():
    """Verify GIN index is created for tsvector."""
    db = connect(DB_URL)
    table_name = 'test_gin_index'

    try:
        drop_table(db, table_name)
        docs = db[table_name]

        docs.insert({'title': 'Test document', 'content': 'Some content here'})
        docs.create_fts_index('title')

        with db._pool.connect() as conn:
            result = conn.execute(text(f"""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = '{table_name}'
                AND indexdef LIKE '%gin%'
            """))
            indexes = result.fetchall()

        assert len(indexes) >= 1
        index_defs = [idx[1].lower() for idx in indexes]
        assert any('gin' in d and 'tsv' in d for d in index_defs)
    finally:
        drop_table(db, table_name)
        db.close()


# ============================================================
# ASYNC TESTS
# ============================================================

@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_create_fts_index():
    """Test creating FTS index asynchronously."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_fts_create'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Python programming', 'content': 'Learn basics'})
        await docs.insert({'title': 'Machine learning', 'content': 'ML introduction'})

        index_name = await docs.create_fts_index('title')

        assert index_name is not None
        assert 'fts' in index_name.lower()
        assert await docs.has_fts_index('title')
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_fts_multiple_columns():
    """Test async FTS index on multiple columns."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_fts_multi'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Guide', 'description': 'Comprehensive guide'})

        index_name = await docs.create_fts_index(['title', 'description'])

        assert index_name is not None
        assert await docs.has_fts_index(['title', 'description'])
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_basic():
    """Test basic async hybrid search."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_basic'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Deep learning fundamentals', 'embedding': np.array([0.9, 0.1, 0.0])})
        await docs.insert({'title': 'Web development basics', 'embedding': np.array([0.0, 0.0, 1.0])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.9, 0.1, 0.0],
            query_text='learning',
            ensure=True,
        ):
            results.append(row)

        assert len(results) >= 1
        assert '_score' in results[0]
        assert 'learning' in results[0]['title'].lower()
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_rrf():
    """Test async RRF fusion."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_rrf'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Neural networks explained', 'embedding': np.array([0.5, 0.5, 0.0])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.5, 0.5, 0.0],
            query_text='neural',
            fusion='rrf',
            rrf_k=60,
            ensure=True,
        ):
            results.append(row)

        assert len(results) >= 1
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_linear():
    """Test async linear fusion."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_linear'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Data science handbook', 'embedding': np.array([0.3, 0.3, 0.4])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.3, 0.3, 0.4],
            query_text='data',
            fusion='linear',
            vector_weight=0.6,
            ensure=True,
        ):
            results.append(row)

        assert len(results) >= 1
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_with_filters():
    """Test async hybrid search with filters."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_filter'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Python basics', 'category': 'programming', 'embedding': np.array([1.0, 0.0, 0.0])})
        await docs.insert({'title': 'Python for data', 'category': 'data', 'embedding': np.array([0.8, 0.2, 0.0])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python',
            ensure=True,
            category='programming',
        ):
            results.append(row)

        for r in results:
            assert r['category'] == 'programming'
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_jsonb_any():
    """Test async hybrid search with ?| JSONB operator."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_jsonb_any'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'AI fundamentals', 'tags': ['ai', 'ml'], 'embedding': np.array([0.9, 0.1, 0.0])})
        await docs.insert({'title': 'Business analytics', 'tags': ['business', 'analytics'], 'embedding': np.array([0.5, 0.5, 0.0])})
        await docs.insert({'title': 'Science tech', 'tags': ['science', 'tech'], 'embedding': np.array([0.3, 0.7, 0.0])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.7, 0.3, 0.0],
            query_text='fundamentals analytics',
            ensure=True,
            tags={'?|': ['ai', 'business']},
        ):
            results.append(row)

        for r in results:
            tags = r['tags']
            assert 'ai' in tags or 'business' in tags
    finally:
        await async_drop_table(db, table_name)
        await db.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
async def test_async_hybrid_search_jsonb_contains():
    """Test async hybrid search with @> JSONB operator."""
    if not ASYNC_DB_URL or 'asyncpg' not in ASYNC_DB_URL:
        pytest.skip("Async PostgreSQL URL not configured")

    db = await async_connect(ASYNC_DB_URL)
    table_name = 'test_async_hybrid_jsonb_contains'

    try:
        await async_drop_table(db, table_name)
        docs = db[table_name]

        await docs.insert({'title': 'Machine learning guide', 'tags': ['ml', 'python', 'tensorflow'], 'embedding': np.array([0.8, 0.2, 0.0])})
        await docs.insert({'title': 'Web development', 'tags': ['javascript', 'react'], 'embedding': np.array([0.0, 1.0, 0.0])})

        results = []
        async for row in docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.8, 0.2, 0.0],
            query_text='learning guide',
            ensure=True,
            tags={'@>': 'ml'},
        ):
            results.append(row)

        for r in results:
            assert 'ml' in r['tags']
    finally:
        await async_drop_table(db, table_name)
        await db.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
