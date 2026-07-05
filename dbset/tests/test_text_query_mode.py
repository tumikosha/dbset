"""Tests for hybrid_search(text_query_mode=...) — AND vs OR word semantics
in the BM25 branch (SQLite FTS5 here; PostgreSQL path shares the same flag)."""
import pytest

from dbset import connect
from dbset.fts import FTSManager

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


@pytest.fixture
def db():
    database = connect('sqlite:///:memory:')
    yield database
    database.close()


@pytest.fixture
def docs(db):
    """Three documents: only one contains ALL the words of the test query."""
    table = db['documents']
    rows = [
        'angular react javascript developer',       # partial match (no "css")
        'react frontend engineer',                  # partial match
        'angular react javascript css layout',      # full match
    ]
    for i, title in enumerate(rows):
        row = {'title': title}
        if HAS_NUMPY:
            row['embedding'] = np.eye(3)[min(i, 2)]
        table.insert(row)
    table.create_fts_index('title')
    return table


class TestSqliteMatchQuery:
    def test_and_mode_keeps_query_verbatim(self):
        assert FTSManager._sqlite_match_query('foo bar', 'and') == 'foo bar'

    def test_or_mode_joins_quoted_tokens(self):
        assert FTSManager._sqlite_match_query('foo, bar-baz', 'or') == '"foo" OR "bar" OR "baz"'

    def test_or_mode_empty_query(self):
        assert FTSManager._sqlite_match_query('', 'or') == ''


class TestPgTsquery:
    def test_and_mode_uses_plainto(self):
        fn, q = FTSManager._pg_tsquery('foo bar', 'and')
        assert fn == 'plainto_tsquery' and q == 'foo bar'

    def test_or_mode_joins_tokens_with_pipe(self):
        fn, q = FTSManager._pg_tsquery('angular, react javascript', 'or')
        assert fn == 'to_tsquery' and q == 'angular | react | javascript'

    def test_or_mode_strips_operators(self):
        # Tokens come from \w+ so tsquery operators can't be injected.
        fn, q = FTSManager._pg_tsquery("a & b | c' --", 'or')
        assert q == 'a | b | c'


@pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
class TestFtsSearchQueryMode:
    """BM25 branch semantics, asserted at the FTSManager level — the hybrid
    fusion always unions in the vector branch, so absence can only be checked
    on the text search itself."""

    QUERY = 'angular react javascript css'

    def _fts(self, db, mode):
        with db.engine.connect() as conn:
            return FTSManager.fts_search_sync(
                conn, 'documents', ['title'], self.QUERY, 'sqlite',
                pk_column='id', query_mode=mode,
            )

    def test_and_mode_requires_all_words(self, db, docs):
        rows = self._fts(db, 'and')
        # Only the full match contains all four words.
        assert [pk for pk, _ in rows] == [3]

    def test_or_mode_matches_any_word(self, db, docs):
        rows = self._fts(db, 'or')
        # All three documents match at least one word…
        assert {pk for pk, _ in rows} == {1, 2, 3}
        # …and the fullest match still ranks first.
        assert rows[0][0] == 3

    def test_hybrid_search_accepts_the_flag(self, docs):
        # End-to-end smoke: the kwarg flows through hybrid_search unchanged.
        for mode in ('and', 'or'):
            results = list(docs.hybrid_search(
                vector_column='embedding', text_column='title',
                query_vector=[1.0, 0.0, 0.0], query_text=self.QUERY,
                text_query_mode=mode, limit=10,
            ))
            assert results, mode
