"""Integration tests for vector support with SQLite."""
import json
import math

import pytest

from dbset import connect, Vector, DistanceMetric, VectorError

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


@pytest.fixture
def db():
    """Create an in-memory SQLite database."""
    database = connect('sqlite:///:memory:')
    yield database
    database.close()


class TestVectorInsertAndRetrieve:
    def test_insert_vector_with_explicit_type(self, db):
        items = db['items']
        pk = items.insert(
            {'name': 'doc1', 'embedding': json.dumps([0.1, 0.2, 0.3])},
            types={'embedding': Vector(dim=3)}
        )
        assert pk is not None

        row = items.find_one(name='doc1')
        assert row is not None
        assert row['name'] == 'doc1'

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_insert_numpy_vector(self, db):
        items = db['items']
        embedding = np.array([0.4, 0.5, 0.6])
        pk = items.insert({'name': 'doc2', 'embedding': embedding})
        assert pk is not None

        row = items.find_one(name='doc2')
        assert row is not None
        # The stored value should be a JSON string
        stored = json.loads(row['embedding'])
        assert len(stored) == 3
        assert abs(stored[0] - 0.4) < 1e-7

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_round_trip_numpy(self, db):
        items = db['items']
        original = np.array([0.1, 0.2, 0.3])
        items.insert({'name': 'doc1', 'embedding': original})

        row = items.find_one(name='doc1')
        from dbset.vector import deserialize_vector
        retrieved = deserialize_vector(row['embedding'])
        assert len(retrieved) == 3
        for a, b in zip(original.tolist(), retrieved):
            assert abs(a - b) < 1e-7

    def test_insert_list_as_json_vector(self, db):
        items = db['items']
        vec_str = json.dumps([1.0, 2.0, 3.0])
        items.insert(
            {'name': 'doc1', 'embedding': vec_str},
            types={'embedding': Vector(dim=3)}
        )

        row = items.find_one(name='doc1')
        from dbset.vector import deserialize_vector
        retrieved = deserialize_vector(row['embedding'])
        assert retrieved == [1.0, 2.0, 3.0]


class TestFindSimilar:
    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_basic(self, db):
        items = db['items']

        # Insert some vectors
        vectors = [
            ('doc1', np.array([1.0, 0.0, 0.0])),
            ('doc2', np.array([0.9, 0.1, 0.0])),
            ('doc3', np.array([0.0, 1.0, 0.0])),
            ('doc4', np.array([0.0, 0.0, 1.0])),
        ]
        for name, vec in vectors:
            items.insert({'name': name, 'embedding': vec})

        # Search for vectors similar to [1, 0, 0]
        results = list(items.find_similar('embedding', [1.0, 0.0, 0.0], metric='cosine', limit=2))

        assert len(results) == 2
        # doc1 should be closest (identical), then doc2
        assert results[0]['name'] == 'doc1'
        assert results[1]['name'] == 'doc2'
        # Check distance is included
        assert '_distance' in results[0]
        assert results[0]['_distance'] < results[1]['_distance']

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_l2(self, db):
        items = db['items']

        vectors = [
            ('close', np.array([1.0, 0.0, 0.0])),
            ('far', np.array([10.0, 10.0, 10.0])),
        ]
        for name, vec in vectors:
            items.insert({'name': name, 'embedding': vec})

        results = list(items.find_similar('embedding', [1.0, 0.0, 0.0], metric='l2'))
        assert results[0]['name'] == 'close'
        assert results[0]['_distance'] < results[1]['_distance']

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_with_threshold(self, db):
        items = db['items']

        vectors = [
            ('close', np.array([1.0, 0.0, 0.0])),
            ('far', np.array([0.0, 0.0, 1.0])),
        ]
        for name, vec in vectors:
            items.insert({'name': name, 'embedding': vec})

        # Use a tight threshold to only get close results
        results = list(items.find_similar(
            'embedding', [1.0, 0.0, 0.0],
            metric='cosine',
            threshold=0.1,
        ))
        assert len(results) == 1
        assert results[0]['name'] == 'close'

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_without_distance(self, db):
        items = db['items']
        items.insert({'name': 'doc1', 'embedding': np.array([1.0, 0.0, 0.0])})

        results = list(items.find_similar(
            'embedding', [1.0, 0.0, 0.0],
            include_distance=False,
        ))
        assert len(results) == 1
        assert '_distance' not in results[0]

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_with_filters(self, db):
        items = db['items']

        items.insert({'name': 'doc1', 'category': 'A', 'embedding': np.array([1.0, 0.0, 0.0])})
        items.insert({'name': 'doc2', 'category': 'B', 'embedding': np.array([0.9, 0.1, 0.0])})
        items.insert({'name': 'doc3', 'category': 'A', 'embedding': np.array([0.0, 1.0, 0.0])})

        results = list(items.find_similar(
            'embedding', [1.0, 0.0, 0.0],
            category='A',
        ))
        # Only category A results
        assert all(r['category'] == 'A' for r in results)

    def test_find_similar_with_json_strings(self, db):
        items = db['items']

        # Insert as JSON strings (without numpy)
        items.insert(
            {'name': 'doc1', 'embedding': json.dumps([1.0, 0.0, 0.0])},
            types={'embedding': Vector(dim=3)}
        )
        items.insert(
            {'name': 'doc2', 'embedding': json.dumps([0.0, 1.0, 0.0])},
            types={'embedding': Vector(dim=3)}
        )

        results = list(items.find_similar('embedding', [1.0, 0.0, 0.0], metric='cosine'))
        assert len(results) == 2
        assert results[0]['name'] == 'doc1'

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_find_similar_inner_product(self, db):
        items = db['items']

        items.insert({'name': 'high', 'embedding': np.array([1.0, 1.0, 1.0])})
        items.insert({'name': 'low', 'embedding': np.array([0.1, 0.1, 0.1])})

        results = list(items.find_similar(
            'embedding', [1.0, 1.0, 1.0],
            metric='inner_product',
        ))
        # Inner product: higher = more similar, but distance = -dot, so lower distance = more similar
        assert results[0]['name'] == 'high'


class TestInsertMany:
    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_insert_many_with_numpy(self, db):
        items = db['items']
        rows = [
            {'name': f'doc{i}', 'embedding': np.array([float(i), 0.0, 0.0])}
            for i in range(5)
        ]
        count = items.insert_many(rows)
        assert count == 5

        total = items.count()
        assert total == 5


class TestVectorIndex:
    def test_create_vector_index_raises_on_sqlite(self, db):
        items = db['items']
        items.insert(
            {'name': 'doc1', 'embedding': json.dumps([0.1, 0.2, 0.3])},
            types={'embedding': Vector(dim=3)}
        )

        with pytest.raises(VectorError, match="PostgreSQL"):
            items.create_vector_index('embedding')
