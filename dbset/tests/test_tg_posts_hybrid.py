"""Test hybrid search on real tg_posts table."""
import os
from pathlib import Path

# Load .env
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

from dbset import connect

DB_URL = os.environ.get('DB_URL') or os.environ.get('db_url') or os.environ.get('DATABASE_URL', '')
if 'asyncpg' in DB_URL:
    DB_URL = DB_URL.replace('postgresql+asyncpg', 'postgresql+psycopg2')


def parse_vector(v):
    """Parse vector from string or return as-is."""
    if isinstance(v, str):
        # PostgreSQL vector format: '[0.1,0.2,...]'
        import json
        return json.loads(v.replace(' ', ''))
    return v


def test_hybrid_search_tg_posts():
    """Hybrid search on tg_posts with tags='ai' filter using @> operator."""
    from sqlalchemy import text as sql_text

    db = connect(DB_URL)
    posts = db['tg_posts']

    # Get a sample embedding from posts with 'ai' tag
    with db._pool.connect() as conn:
        result = conn.execute(sql_text(
            "SELECT embedding FROM tg_posts WHERE tags @> '[\"ai\"]' AND embedding IS NOT NULL LIMIT 1"
        ))
        row = result.fetchone()
        if not row:
            print("No posts with 'ai' tag and embeddings found")
            return
        query_embedding = parse_vector(row[0])

    # Test 1: @> operator (contains)
    print("\n=== Test @> operator (contains 'ai') ===\n")
    results = list(posts.hybrid_search(
        vector_column='embedding',
        text_column='text',
        query_vector=query_embedding,
        query_text='искусственный интеллект',
        ensure=True,
        limit=3,
        tags={'@>': 'ai'},
    ))
    print(f"Found {len(results)} results with @>")
    for r in results:
        print(f"  tags: {r.get('tags')}")

    # Test 2: ?| operator (ANY of tags)
    print("\n=== Test ?| operator (ANY of ['ai', 'business', 'science_tech']) ===\n")
    results = list(posts.hybrid_search(
        vector_column='embedding',
        text_column='text',
        query_vector=query_embedding,
        query_text='технологии бизнес',
        ensure=True,
        limit=5,
        tags={'?|': ['ai', 'business', 'science_tech']},
    ))
    print(f"Found {len(results)} results with ?|")
    for r in results:
        print(f"  tags: {r.get('tags')}")
        text = (r.get('text') or '')[:100].replace('\n', ' ')
        print(f"   text: {text}...")
        print()

    db.close()


if __name__ == '__main__':
    test_hybrid_search_tg_posts()
