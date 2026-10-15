"""hybrid_search with operator filters ({'>=': 2}, JSONB {'?|': [...]}, {'in': [...]}) on the full-text side.

Runs on SQLite, and on PostgreSQL when DBSET_PG_URL is set (see _backends.py). MySQL has no FTS here.
"""

import pytest

from dbset.fts import _filter_clause

from ._backends import table_name


def _vec(i: int) -> list[float]:
    v = [0.0] * 64
    v[i] = 1.0
    return v


ROWS = [
    {'body': 'red apple pie', 'n': 1, 'tags': ['food'], 'embedding': _vec(0)},
    {'body': 'green apple tart', 'n': 5, 'tags': ['food', 'ai'], 'embedding': _vec(1)},
    {'body': 'blue car', 'n': 9, 'tags': ['ai'], 'embedding': _vec(2)},
]

# the vector side finds only "blue car" (close to _vec(2)); "apple" rows come from full-text search
CASES = [
    pytest.param({'n': {'>=': 2}}, ['blue car', 'green apple tart'], id='gte'),
    pytest.param({'n': {'in': [1, 9]}}, ['blue car', 'red apple pie'], id='in'),
    pytest.param({'n': 5}, ['green apple tart'], id='equal'),
    pytest.param({'tags': {'?|': ['ai']}}, ['blue car', 'green apple tart'], id='jsonb-any'),
]


def test_filter_clause_renders_only_plain_equality():
    clause, params = _filter_clause({'n': 5, 'm': None, 'a': {'>=': 2}, 'b': {'?|': ['x']}, 'c': [1, 2], 'x.y': 1})
    assert clause == 'AND n = :flt_0 AND m IS NULL'
    assert params == {'flt_0': 5}


@pytest.mark.parametrize('filters, expected', CASES)
def test_operator_filters(make_db, filters, expected):
    if make_db.backend == 'mysql':
        pytest.skip('no full-text search on MySQL')
    if make_db.backend == 'sqlite' and '?|' in str(filters):
        pytest.skip('JSONB operators are PostgreSQL only')
    t = make_db()[table_name()]
    t.insert_many(ROWS)
    rows = t.hybrid_search('embedding', 'body', _vec(2), 'apple', ensure=True, limit=5,
                           vector_max_distance=0.5, **filters)
    assert sorted(r['body'] for r in rows) == expected


@pytest.mark.parametrize('filters, expected', CASES)
async def test_async_operator_filters(make_async_db, filters, expected):
    if make_async_db.backend == 'mysql':
        pytest.skip('no full-text search on MySQL')
    if make_async_db.backend == 'sqlite' and '?|' in str(filters):
        pytest.skip('JSONB operators are PostgreSQL only')
    t = (await make_async_db())[table_name()]
    await t.insert_many(ROWS)
    rows = [r async for r in t.hybrid_search('embedding', 'body', _vec(2), 'apple', ensure=True, limit=5,
                                             vector_max_distance=0.5, **filters)]
    assert sorted(r['body'] for r in rows) == expected
