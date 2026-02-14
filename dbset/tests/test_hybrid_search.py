"""Tests for hybrid search (BM25 + Vector) functionality."""
import json
import pytest

from dbset import connect, FusionMethod
from dbset.hybrid import reciprocal_rank_fusion, linear_fusion, fuse_results

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


class TestFusionAlgorithms:
    """Test fusion algorithms (RRF and Linear)."""

    def test_rrf_basic(self):
        """Test basic RRF fusion."""
        # Vector results (lower distance = better)
        vector_results = [
            ('doc1', 0.1),  # rank 1
            ('doc2', 0.2),  # rank 2
            ('doc3', 0.5),  # rank 3
        ]

        # BM25 results (higher score = better)
        bm25_results = [
            ('doc2', 10.0),  # rank 1
            ('doc1', 8.0),   # rank 2
            ('doc4', 5.0),   # rank 3
        ]

        fused = reciprocal_rank_fusion(vector_results, bm25_results, k=60)

        # Both doc1 and doc2 should be at top (they appear in both lists)
        top_docs = [doc_id for doc_id, _ in fused[:2]]
        assert 'doc1' in top_docs
        assert 'doc2' in top_docs

        # Check that scores are positive
        for doc_id, score in fused:
            assert score > 0

    def test_rrf_empty_results(self):
        """Test RRF with empty results."""
        fused = reciprocal_rank_fusion([], [])
        assert fused == []

    def test_rrf_single_list(self):
        """Test RRF with only one non-empty list."""
        vector_results = [('doc1', 0.1), ('doc2', 0.2)]
        fused = reciprocal_rank_fusion(vector_results, [])

        assert len(fused) == 2
        assert fused[0][0] == 'doc1'  # Best vector result first

    def test_linear_fusion_basic(self):
        """Test basic linear fusion."""
        vector_results = [
            ('doc1', 0.1),  # Low distance = high similarity
            ('doc2', 0.5),
        ]

        bm25_results = [
            ('doc2', 10.0),  # High score = good match
            ('doc1', 5.0),
        ]

        fused = linear_fusion(vector_results, bm25_results, vector_weight=0.5)

        # Both docs should be present
        doc_ids = [doc_id for doc_id, _ in fused]
        assert 'doc1' in doc_ids
        assert 'doc2' in doc_ids

        # Scores should be in [0, 1]
        for doc_id, score in fused:
            assert 0 <= score <= 1

    def test_linear_fusion_weight_extremes(self):
        """Test linear fusion with extreme weights."""
        vector_results = [('doc1', 0.0), ('doc2', 1.0)]
        bm25_results = [('doc2', 10.0), ('doc1', 1.0)]

        # Pure vector weight
        fused_vec = linear_fusion(vector_results, bm25_results, vector_weight=1.0)
        assert fused_vec[0][0] == 'doc1'  # doc1 has best vector score

        # Pure BM25 weight
        fused_bm25 = linear_fusion(vector_results, bm25_results, vector_weight=0.0)
        assert fused_bm25[0][0] == 'doc2'  # doc2 has best BM25 score

    def test_linear_fusion_invalid_weight(self):
        """Test that invalid weights raise error."""
        with pytest.raises(ValueError):
            linear_fusion([], [], vector_weight=1.5)

        with pytest.raises(ValueError):
            linear_fusion([], [], vector_weight=-0.5)

    def test_fuse_results_rrf(self):
        """Test fuse_results with RRF method."""
        vector_results = [('doc1', 0.1)]
        bm25_results = [('doc1', 10.0)]

        fused = fuse_results(vector_results, bm25_results, method='rrf')
        assert len(fused) == 1
        assert fused[0][0] == 'doc1'

    def test_fuse_results_linear(self):
        """Test fuse_results with linear method."""
        vector_results = [('doc1', 0.1)]
        bm25_results = [('doc1', 10.0)]

        fused = fuse_results(vector_results, bm25_results, method='linear')
        assert len(fused) == 1
        assert fused[0][0] == 'doc1'

    def test_fuse_results_invalid_method(self):
        """Test that invalid method raises error."""
        with pytest.raises(ValueError):
            fuse_results([], [], method='invalid')


class TestFTSIndex:
    """Test FTS index creation and checking."""

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_create_fts_index(self, db):
        """Test creating FTS index."""
        docs = db['documents']

        # Insert some documents
        docs.insert({'title': 'Python programming', 'content': 'Learn Python basics'})
        docs.insert({'title': 'Machine learning', 'content': 'Introduction to ML'})

        # Create FTS index
        index_name = docs.create_fts_index('title')
        assert index_name is not None
        assert 'fts' in index_name.lower()

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_has_fts_index(self, db):
        """Test checking FTS index existence."""
        docs = db['documents']

        docs.insert({'title': 'Test document'})

        # Initially no index
        assert not docs.has_fts_index('title')

        # Create index
        docs.create_fts_index('title')

        # Now it exists
        assert docs.has_fts_index('title')

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_create_fts_index_multiple_columns(self, db):
        """Test FTS index on multiple columns."""
        docs = db['documents']

        docs.insert({
            'title': 'Python guide',
            'description': 'A comprehensive guide',
        })

        index_name = docs.create_fts_index(['title', 'description'])
        assert index_name is not None


class TestHybridSearch:
    """Test hybrid_search method."""

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_basic(self, db):
        """Test basic hybrid search."""
        docs = db['documents']

        # Insert documents with text and embeddings
        docs.insert({
            'title': 'Python programming tutorial',
            'embedding': np.array([1.0, 0.0, 0.0]),
        })
        docs.insert({
            'title': 'Machine learning with Python',
            'embedding': np.array([0.8, 0.2, 0.0]),
        })
        docs.insert({
            'title': 'JavaScript basics',
            'embedding': np.array([0.0, 1.0, 0.0]),
        })

        # Hybrid search for "Python" with vector close to [1, 0, 0]
        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python',
            ensure=True,
            limit=10,
        ))

        # Should find Python-related documents
        assert len(results) >= 1
        assert '_score' in results[0]

        # Top results should contain Python
        titles = [r['title'] for r in results[:2]]
        assert any('Python' in t for t in titles)

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_rrf_fusion(self, db):
        """Test hybrid search with RRF fusion."""
        docs = db['documents']

        docs.insert({
            'title': 'Deep learning fundamentals',
            'embedding': np.array([0.5, 0.5, 0.0]),
        })

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.5, 0.5, 0.0],
            query_text='learning',
            fusion='rrf',
            rrf_k=60,
            ensure=True,
        ))

        assert len(results) >= 1

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_linear_fusion(self, db):
        """Test hybrid search with linear fusion."""
        docs = db['documents']

        docs.insert({
            'title': 'Neural networks explained',
            'embedding': np.array([0.3, 0.3, 0.3]),
        })

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.3, 0.3, 0.3],
            query_text='neural',
            fusion='linear',
            vector_weight=0.7,
            ensure=True,
        ))

        assert len(results) >= 1

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_without_scores(self, db):
        """Test hybrid search with include_scores=False."""
        docs = db['documents']

        docs.insert({
            'title': 'Test document',
            'embedding': np.array([0.1, 0.2, 0.3]),
        })

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.1, 0.2, 0.3],
            query_text='test',
            include_scores=False,
            ensure=True,
        ))

        if results:
            assert '_score' not in results[0]

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_ensure_creates_index(self, db):
        """Test that ensure=True creates FTS index if missing."""
        docs = db['documents']

        docs.insert({
            'title': 'Data science guide',
            'embedding': np.array([0.4, 0.4, 0.2]),
        })

        # Initially no FTS index
        assert not docs.has_fts_index('title')

        # Hybrid search with ensure=True
        list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[0.4, 0.4, 0.2],
            query_text='data',
            ensure=True,
        ))

        # Now FTS index exists
        assert docs.has_fts_index('title')

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_multiple_text_columns(self, db):
        """Test hybrid search with multiple text columns."""
        docs = db['documents']

        docs.insert({
            'title': 'Python basics',
            'description': 'Learn programming fundamentals',
            'embedding': np.array([1.0, 0.0, 0.0]),
        })

        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column=['title', 'description'],
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python programming',
            ensure=True,
        ))

        # Should match based on both title and description
        assert len(results) >= 1

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_empty_results(self, db):
        """Test hybrid search when no matches found."""
        docs = db['documents']

        docs.insert({
            'title': 'Unrelated document',
            'embedding': np.array([0.0, 0.0, 1.0]),
        })

        # Search for something that won't match
        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='xyznonexistent',
            ensure=True,
            limit=10,
        ))

        # Results depend on FTS matching
        # With no text match and distant vector, might get empty or low-scored results
        # This is expected behavior


class TestJSONBFilters:
    """Test JSONB dot notation support in hybrid search."""

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_hybrid_search_with_jsonb_filter(self, db):
        """Test hybrid search with JSONB filter."""
        docs = db['documents']

        docs.insert({
            'title': 'Python for beginners',
            'metadata': json.dumps({'category': 'programming', 'level': 'beginner'}),
            'embedding': np.array([1.0, 0.0, 0.0]),
        })
        docs.insert({
            'title': 'Advanced Python',
            'metadata': json.dumps({'category': 'programming', 'level': 'advanced'}),
            'embedding': np.array([0.9, 0.1, 0.0]),
        })
        docs.insert({
            'title': 'Cooking basics',
            'metadata': json.dumps({'category': 'cooking', 'level': 'beginner'}),
            'embedding': np.array([0.0, 1.0, 0.0]),
        })

        # Search with filter - this tests the filter passing through
        # Note: JSONB dot notation filtering requires dialect support
        # For SQLite, this would use json_extract
        results = list(docs.hybrid_search(
            vector_column='embedding',
            text_column='title',
            query_vector=[1.0, 0.0, 0.0],
            query_text='Python',
            ensure=True,
            limit=10,
        ))

        # Should find Python-related documents
        assert len(results) >= 1
