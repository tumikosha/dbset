"""Vectors on libSQL: F32_BLOB storage, SQL distances, DiskANN index (sync and async)."""

import math
import struct

import pytest
from sqlalchemy import exc, text

from dbset import DistanceMetric, Vector, VectorError, async_connect, connect
from dbset.vector import compute_distance, deserialize_vector, parse_libsql_vector_index

from ._backends import HAS_LIBSQL

pytestmark = pytest.mark.skipif(not HAS_LIBSQL, reason="libsql not installed")

np = pytest.importorskip("numpy")

DOCS = [
    # name, cat, vector
    ('a1', 'a', [1.0, 0.0, 0.0]),
    ('a2', 'a', [0.9, 0.1, 0.0]),
    ('a3', 'a', [0.8, 0.2, 0.0]),
    ('b1', 'b', [0.0, 1.0, 0.0]),
    ('b2', 'b', [0.1, 0.9, 0.3]),
    ('c1', 'c', [-0.1, 0.0, 1.0]),  # no distance ties with b1
]
QUERY = [1.0, 0.0, 0.0]


def expected_order(metric, docs=DOCS, cat=None):
    rows = [(compute_distance(QUERY, v, metric), n) for n, c, v in docs if cat is None or c == cat]
    return [n for _, n in sorted(rows)]


@pytest.fixture
def db():
    database = connect('sqlite+dbsetlibsql://')
    yield database
    database.close()


@pytest.fixture
def items(db):
    table = db['items']
    for name, cat, vec in DOCS:
        table.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
    return table


def column_decl(db, table, column):
    with db.engine.connect() as conn:
        sql = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND name=:n"), {'n': table}
        ).scalar()
    return sql


class TestStorage:
    def test_dialect_is_sqlite_with_libsql_driver(self, db):
        assert db.engine.dialect.name == 'sqlite'
        assert db.engine.dialect.driver == 'libsql'

    def test_vector_column_is_f32_blob(self, db, items):
        assert 'F32_BLOB(3)' in column_decl(db, 'items', 'embedding')

    def test_vector_stored_as_float32_blob_and_round_trips(self, items):
        row = items.find_one(name='a2')
        assert isinstance(row['embedding'], bytes)
        assert len(row['embedding']) == 3 * 4
        assert deserialize_vector(row['embedding']) == pytest.approx([0.9, 0.1, 0.0], abs=1e-6)

    def test_added_vector_column_is_f32_blob(self, db):
        table = db['later']
        table.insert({'name': 'x'})
        table.insert({'name': 'y', 'embedding': np.array([0.1, 0.2])})
        assert 'F32_BLOB(2)' in column_decl(db, 'later', 'embedding')

    def test_reflected_column_type_is_vector(self, db, items):
        db._schema.reflect()
        col = items._get_table().c.embedding
        assert isinstance(col.type, Vector)
        assert col.type.dim == 3

    def test_long_float_list_is_a_vector(self, db):
        table = db['lists']
        table.insert({'name': 'x', 'embedding': [0.5] * 64})
        assert 'F32_BLOB(64)' in column_decl(db, 'lists', 'embedding')
        assert deserialize_vector(table.find_one(name='x')['embedding']) == [0.5] * 64

    def test_deserialize_rejects_odd_blob(self):
        with pytest.raises(ValueError):
            deserialize_vector(b'abc')

    def test_deserialize_blob(self):
        assert deserialize_vector(struct.pack('<2f', 1.5, -2.0)) == [1.5, -2.0]


class TestFindSimilarExact:
    @pytest.mark.parametrize('metric', [DistanceMetric.COSINE, DistanceMetric.L2, DistanceMetric.INNER_PRODUCT])
    def test_order_and_distance(self, items, metric):
        rows = list(items.find_similar('embedding', QUERY, metric=metric, limit=len(DOCS)))
        assert [r['name'] for r in rows] == expected_order(metric)
        by_name = {n: v for n, _, v in DOCS}
        for r in rows:
            assert r['_distance'] == pytest.approx(compute_distance(QUERY, by_name[r['name']], metric), abs=1e-5)

    def test_filters_and_threshold(self, items):
        rows = list(items.find_similar('embedding', QUERY, cat='a', threshold=0.05))
        assert [r['name'] for r in rows] == ['a1', 'a2', 'a3']
        assert all(r['_distance'] <= 0.05 for r in rows)

    def test_include_distance_false(self, items):
        rows = list(items.find_similar('embedding', QUERY, limit=2, include_distance=False))
        assert len(rows) == 2 and '_distance' not in rows[0]

    def test_json_text_vector_in_f32_column(self, db):
        """A JSON string given with types={'embedding': Vector(dim)} is searchable too."""
        table = db['json_vecs']
        table.insert({'name': 'x', 'embedding': '[1, 0, 0]'}, types={'embedding': Vector(dim=3)})
        table.insert({'name': 'y', 'embedding': '[0, 1, 0]'})
        assert [r['name'] for r in table.find_similar('embedding', QUERY)] == ['x', 'y']


class TestVectorIndex:
    def test_create_index(self, db, items):
        name = items.create_vector_index('embedding')
        assert name == 'idx_items_embedding_diskann_cosine'
        with db.engine.connect() as conn:
            sql = conn.execute(text("SELECT sql FROM sqlite_master WHERE name=:n"), {'n': name}).scalar()
        assert 'libsql_vector_idx' in sql and "'metric=cosine'" in sql
        assert items.create_vector_index('embedding') == name  # IF NOT EXISTS

    @pytest.mark.parametrize('metric', [DistanceMetric.COSINE, DistanceMetric.L2])
    def test_search_uses_index_and_matches_exact(self, items, metric, monkeypatch):
        exact = list(items.find_similar('embedding', QUERY, metric=metric, limit=3))
        items.create_vector_index('embedding', metric=metric)

        import dbset.sync_core as sync_core
        calls = []
        real = sync_core.libsql_candidates_query
        monkeypatch.setattr(sync_core, 'libsql_candidates_query', lambda *a, **k: calls.append(a[4]) or real(*a, **k))

        ann = list(items.find_similar('embedding', QUERY, metric=metric, limit=3))
        assert calls == [f'idx_items_embedding_diskann_{metric}']
        assert [r['name'] for r in ann] == [r['name'] for r in exact]
        assert [r['_distance'] for r in ann] == pytest.approx([r['_distance'] for r in exact], abs=1e-6)
        assert set(ann[0]) == set(exact[0])

    def test_index_of_other_metric_is_not_used(self, items, monkeypatch):
        items.create_vector_index('embedding', metric='l2')
        import dbset.sync_core as sync_core
        monkeypatch.setattr(sync_core, 'libsql_candidates_query', lambda *a, **k: pytest.fail('ANN used'))
        rows = list(items.find_similar('embedding', QUERY, metric='cosine', limit=2))
        assert [r['name'] for r in rows] == expected_order('cosine')[:2]

    def test_filters_with_index_still_return_matches(self, items):
        """vector_top_k picks rows before a WHERE; filtered searches must not lose rows."""
        items.create_vector_index('embedding')
        rows = list(items.find_similar('embedding', QUERY, limit=2, cat='b'))
        assert [r['name'] for r in rows] == expected_order('cosine', cat='b')

    def test_threshold_and_no_distance_with_index(self, items):
        items.create_vector_index('embedding')
        rows = list(items.find_similar('embedding', QUERY, limit=10, threshold=0.05))
        assert [r['name'] for r in rows] == [n for n in expected_order('cosine')
                                              if compute_distance(QUERY, dict((d[0], d[2]) for d in DOCS)[n]) <= 0.05]
        rows = list(items.find_similar('embedding', QUERY, limit=1, include_distance=False))
        assert rows[0]['name'] == 'a1' and '_distance' not in rows[0]

    def test_index_with_params(self, items):
        items.create_vector_index('embedding', max_neighbors=16, compress_neighbors='float8')
        assert list(items.find_similar('embedding', QUERY, limit=1))[0]['name'] == 'a1'

    def test_rows_inserted_after_index_are_found(self, items):
        items.create_vector_index('embedding')
        items.insert({'name': 'new', 'cat': 'a', 'embedding': np.array([1.0, 0.0, 0.0001])})
        names = [r['name'] for r in items.find_similar('embedding', QUERY, limit=2)]
        assert set(names) == {'a1', 'new'}

    @pytest.mark.parametrize('kwargs, match', [
        ({'method': 'ivfflat'}, 'DiskANN'),
        ({'method': 'hnsw'}, 'DiskANN'),
        ({'metric': 'inner_product'}, 'metrics'),
        ({'max_neighbors': "1'); DROP TABLE items; --"}, 'Invalid'),
    ])
    def test_rejected_index_arguments(self, items, kwargs, match):
        with pytest.raises(VectorError, match=match):
            items.create_vector_index('embedding', **kwargs)
        assert items.count() == len(DOCS)

    def test_text_column_from_plain_sqlite_cannot_be_indexed(self, tmp_path):
        path = tmp_path / 'plain.db'
        plain = connect(f'sqlite:///{path}')
        plain['items'].insert({'name': 'a', 'embedding': np.array([1.0, 0.0])})
        plain.close()

        db = connect(f'sqlite+dbsetlibsql:///{path}')
        try:
            table = db['items']
            with pytest.raises(VectorError, match='unexpected vector column type: TEXT'):
                table.create_vector_index('embedding')
            # exact search still works on the JSON text vectors
            assert list(table.find_similar('embedding', [1.0, 0.0]))[0]['name'] == 'a'
        finally:
            db.close()

    def test_uuid_primary_key(self):
        db = connect('sqlite+dbsetlibsql://', primary_key_type='uuid')
        try:
            table = db['docs']
            for name, cat, vec in DOCS:
                table.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
            table.create_vector_index('embedding')
            rows = list(table.find_similar('embedding', QUERY, limit=2))
            assert [r['name'] for r in rows] == ['a1', 'a2']
            assert isinstance(rows[0]['id'], str) and len(rows[0]['id']) == 36
        finally:
            db.close()

    def test_file_database_persists_index(self, tmp_path):
        url = f'sqlite+dbsetlibsql:///{tmp_path / "v.db"}'
        db = connect(url)
        for name, cat, vec in DOCS:
            db['items'].insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
        db['items'].create_vector_index('embedding')
        db.close()

        db = connect(url)
        try:
            rows = list(db['items'].find_similar('embedding', QUERY, limit=2))
            assert [r['name'] for r in rows] == ['a1', 'a2']
        finally:
            db.close()


class TestNullVectors:
    """Rows inserted before the vector column existed have NULL there."""

    @pytest.fixture
    def table(self, db):
        t = db['nulls']
        t.insert({'name': 'none', 'cat': 'a'})
        for name, cat, vec in DOCS:
            t.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
        t.insert({'name': 'none2', 'cat': 'b'})
        return t

    @pytest.mark.parametrize('metric', ['cosine', 'l2', 'inner_product'])
    def test_exact_search_skips_nulls(self, table, metric):
        rows = list(table.find_similar('embedding', QUERY, metric=metric, limit=20))
        assert [r['name'] for r in rows] == expected_order(metric)

    def test_filtered_and_indexed_search_skips_nulls(self, table):
        table.create_vector_index('embedding')
        assert [r['name'] for r in table.find_similar('embedding', QUERY, limit=20)] == expected_order('cosine')
        rows = list(table.find_similar('embedding', QUERY, limit=20, cat='b', threshold=2.0))
        assert [r['name'] for r in rows] == expected_order('cosine', cat='b')


class TestQuotedNames:
    @pytest.mark.parametrize('table_name, column', [('my-items', 'my emb'), ('it"ems', 'e"mb'), ('a_b', 'c')])
    def test_index_on_names_needing_quotes(self, db, table_name, column):
        table = db[table_name]
        for name, cat, vec in DOCS:
            table.insert({'name': name, 'cat': cat, column: np.array(vec)})
        index_name = table.create_vector_index(column)
        assert index_name.replace('_', '').isalnum()
        rows = list(table.find_similar(column, QUERY, limit=2))
        assert [r['name'] for r in rows] == ['a1', 'a2']
        assert [r['name'] for r in table.find_similar(column, QUERY, limit=2, cat='b')] == ['b2', 'b1']

    def test_sanitised_names_do_not_collide(self, db):
        from dbset.vector import libsql_create_index_sql
        preparer = db.engine.dialect.identifier_preparer
        for name, _, vec in DOCS[:1]:
            db['a-b'].insert({'name': name, 'c': np.array(vec)})
            db['a'].insert({'name': name, 'b-c': np.array(vec)})
        n1, _ = libsql_create_index_sql(db['a-b']._get_table(), 'c', 'cosine', {}, preparer)
        n2, _ = libsql_create_index_sql(db['a']._get_table(), 'b-c', 'cosine', {}, preparer)
        assert n1 != n2


class TestParseIndex:
    @pytest.mark.parametrize('sql, expected', [
        ("CREATE INDEX i ON t(libsql_vector_idx(emb))", ('emb', 'cosine')),
        ("CREATE INDEX i ON t (libsql_vector_idx(\"my col\", 'metric=l2'))", ('my col', 'l2')),
        ("CREATE INDEX i ON t(libsql_vector_idx(`e`, 'max_neighbors=8', 'metric = cosine'))", ('e', 'cosine')),
        ("CREATE INDEX i ON t(LIBSQL_VECTOR_IDX([e], 'metric=L2'))", ('e', 'l2')),
        ("CREATE INDEX i ON t(name)", None),
        (None, None),
    ])
    def test_parse(self, sql, expected):
        assert parse_libsql_vector_index(sql) == expected


class TestUrls:
    def test_does_not_shadow_sqlalchemy_libsql(self):
        """sqlite+libsql:// stays the sqlalchemy-libsql package's dialect (if installed)."""
        from sqlalchemy.dialects import registry
        try:
            cls = registry.load('sqlite.libsql')
        except exc.NoSuchModuleError:
            pytest.skip('sqlalchemy-libsql not installed')
        assert not cls.__module__.startswith('dbset')

    def test_remote_url_rejected(self):
        with pytest.raises(exc.ArgumentError, match='remote'):
            connect('sqlite+dbsetlibsql://example.turso.io/db')

    async def test_async_remote_url_rejected(self):
        with pytest.raises(exc.ArgumentError, match='remote'):
            await async_connect('sqlite+dbsetaiolibsql://example.turso.io/db')


class TestAsync:
    async def test_async_vectors_and_index(self, tmp_path):
        db = await async_connect(f'sqlite+dbsetaiolibsql:///{tmp_path / "a.db"}')
        try:
            assert db.engine.dialect.driver == 'aiolibsql'
            table = db['items']
            for name, cat, vec in DOCS:
                await table.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})

            row = await table.find_one(name='a1')
            assert isinstance(row['embedding'], bytes)

            exact = [r async for r in table.find_similar('embedding', QUERY, limit=3)]
            assert [r['name'] for r in exact] == expected_order('cosine')[:3]

            assert await table.create_vector_index('embedding') == 'idx_items_embedding_diskann_cosine'
            import dbset.async_core as async_core
            calls = []
            real = async_core.libsql_candidates_query
            async_core.libsql_candidates_query = lambda *a, **k: calls.append(a[4]) or real(*a, **k)
            try:
                ann = [r async for r in table.find_similar('embedding', QUERY, limit=3)]
            finally:
                async_core.libsql_candidates_query = real
            assert calls == ['idx_items_embedding_diskann_cosine']
            assert [r['name'] for r in ann] == [r['name'] for r in exact]
            assert [r['_distance'] for r in ann] == pytest.approx([r['_distance'] for r in exact], abs=1e-6)

            filtered = [r async for r in table.find_similar('embedding', QUERY, limit=2, cat='b')]
            assert [r['name'] for r in filtered] == expected_order('cosine', cat='b')

            ip = [r async for r in table.find_similar('embedding', QUERY, metric='inner_product', limit=2)]
            assert [r['name'] for r in ip] == expected_order('inner_product')[:2]

            with pytest.raises(VectorError):
                await table.create_vector_index('embedding', metric='inner_product')
        finally:
            await db.close()

    async def test_async_memory_database(self):
        db = await async_connect('sqlite+dbsetaiolibsql://')
        try:
            table = db['items']
            await table.insert({'name': 'x', 'embedding': np.array([0.0, 1.0])})
            await table.create_vector_index('embedding', metric='l2')
            rows = [r async for r in table.find_similar('embedding', [0.0, 1.0], metric='l2')]
            assert rows[0]['name'] == 'x' and math.isclose(rows[0]['_distance'], 0.0, abs_tol=1e-6)
        finally:
            await db.close()


def _spy(monkeypatch, module, name):
    """Record the calls of module.name (positional args) and keep its behaviour."""
    calls = []
    real = getattr(module, name)
    monkeypatch.setattr(module, name, lambda *a, **k: calls.append(a) or real(*a, **k))
    return calls


def _exact(rows_by_name, query, metric, names):
    return [n for _, n in sorted((compute_distance(query, rows_by_name[n], metric), n) for n in names)]


class TestFilteredAnn:
    """Filters with a DiskANN index: oversampled candidates, then an exact scan if needed."""

    @pytest.fixture
    def big(self, db):
        rng = np.random.default_rng(7)
        table = db['big']
        vecs = {f'r{i}': rng.standard_normal(8).tolist() for i in range(300)}
        table.insert_many([
            {'name': n, 'cat': 'a' if i % 2 else 'b', 'rare': 'yes' if i in (5, 77) else 'no',
             'embedding': np.array(v)}
            for i, (n, v) in enumerate(vecs.items())
        ])
        table.create_vector_index('embedding')
        return table, vecs, rng.standard_normal(8).tolist()

    def test_filtered_search_uses_index(self, big, monkeypatch):
        import dbset.sync_core as sync_core
        table, vecs, query = big
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        rows = list(table.find_similar('embedding', query, limit=5, cat='a'))
        assert len(ann) == 1 and ann[0][5] == 50  # k = limit * 10
        assert exact == []
        names_a = [n for i, n in enumerate(vecs) if i % 2]
        assert [r['name'] for r in rows] == _exact(vecs, query, 'cosine', names_a)[:5]
        assert all(r['cat'] == 'a' for r in rows)

    def test_selective_filter_hits_search_l_cap_then_scans(self, big, monkeypatch):
        """vector_top_k yields at most search_l (200) candidates: k=500 cannot cover 300 rows."""
        import dbset.sync_core as sync_core
        table, vecs, query = big
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        rows = list(table.find_similar('embedding', query, limit=5, rare='yes'))
        assert [a[5] for a in ann] == [50, 500]
        assert len(exact) == 1
        assert sorted(r['name'] for r in rows) == ['r5', 'r77']

    def test_larger_search_l_covers_the_index(self, db, monkeypatch):
        import dbset.sync_core as sync_core
        rng = np.random.default_rng(3)
        table = db['wide']
        table.insert_many([{'name': f'r{i}', 'rare': 'yes' if i in (5, 77) else 'no',
                            'embedding': rng.standard_normal(8)} for i in range(300)])
        table.create_vector_index('embedding', search_l=1000)
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        rows = list(table.find_similar('embedding', rng.standard_normal(8).tolist(), limit=5, rare='yes'))
        assert [a[5] for a in ann] == [50, 500]  # 500 candidates >= 300 indexed rows: complete
        assert exact == []
        assert sorted(r['name'] for r in rows) == ['r5', 'r77']

    def test_small_index_is_exhausted_without_scan(self, items, monkeypatch):
        import dbset.sync_core as sync_core
        items.create_vector_index('embedding')
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        assert [r['name'] for r in items.find_similar('embedding', QUERY, limit=5, cat='c')] == ['c1']
        assert [a[5] for a in ann] == [50] and exact == []

    def test_falls_back_to_exact_scan(self, db, monkeypatch):
        import dbset.sync_core as sync_core
        table = db['fallback']
        rows = [{'name': f'n{i}', 'cat': 'common', 'embedding': np.array([1.0, i / 2000])} for i in range(1200)]
        rows.append({'name': 'far', 'cat': 'rare', 'embedding': np.array([-1.0, 0.0])})
        table.insert_many(rows)
        table.create_vector_index('embedding')
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        found = list(table.find_similar('embedding', [1.0, 0.0], limit=1, cat='rare'))
        assert [a[5] for a in ann] == [10, 100, 1000]  # 1000 < 1201 indexed rows
        assert len(exact) == 1
        assert [r['name'] for r in found] == ['far']

    def test_operator_filter_with_index(self, big):
        table, vecs, query = big
        rows = list(table.find_similar('embedding', query, limit=3, name={'in': ['r1', 'r2', 'r3', 'r4']}))
        assert [r['name'] for r in rows] == _exact(vecs, query, 'cosine', ['r1', 'r2', 'r3', 'r4'])[:3]

    def test_threshold_with_filters(self, big):
        table, vecs, query = big
        rows = list(table.find_similar('embedding', query, limit=50, cat='b', threshold=0.5))
        expected = [n for n in _exact(vecs, query, 'cosine', [n for i, n in enumerate(vecs) if i % 2 == 0])
                    if compute_distance(query, vecs[n]) <= 0.5]
        assert [r['name'] for r in rows] == expected[:50]

    def test_hybrid_search_with_filter_uses_index(self, db, monkeypatch):
        import dbset.sync_core as sync_core
        table = db['hyb']
        texts = ['cat sits on mat', 'dog runs in park', 'cat and dog play', 'fish swims', 'bird flies high'] * 20
        rng = np.random.default_rng(1)
        table.insert_many([{'body': t, 'cat': 'a' if i % 2 else 'b', 'emb': rng.random(8)} for i, t in enumerate(texts)])
        table.create_vector_index('emb')
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        rows = list(table.hybrid_search('emb', 'body', rng.random(8), 'cat', limit=3, ensure=True, cat='a'))
        assert ann and rows and all(r['cat'] == 'a' for r in rows)


class TestPartialIndex:
    def test_partial_index_serves_its_filter(self, db, items, monkeypatch):
        import dbset.sync_core as sync_core
        name = items.create_vector_index('embedding', where={'cat': 'b'})
        assert name.startswith('idx_items_embedding_diskann_cosine_')
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        rows = list(items.find_similar('embedding', QUERY, limit=5, cat='b'))
        assert [r['name'] for r in rows] == expected_order('cosine', cat='b')
        assert [(a[4], a[5], a[6]) for a in ann] == [(name, 5, None)]  # no residual filter, k = limit

    def test_partial_index_plus_residual_filter(self, items, monkeypatch):
        import dbset.sync_core as sync_core
        name = items.create_vector_index('embedding', where={'cat': 'a'})
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        rows = list(items.find_similar('embedding', QUERY, limit=5, cat='a', name={'!=': 'a1'}))
        assert [r['name'] for r in rows] == ['a2', 'a3']
        assert ann[0][4] == name and ann[0][6] is not None

    def test_partial_index_not_used_for_other_values(self, items, monkeypatch):
        import dbset.sync_core as sync_core
        items.create_vector_index('embedding', where={'cat': 'b'})
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        assert [r['name'] for r in items.find_similar('embedding', QUERY, limit=5, cat='a')] == ['a1', 'a2', 'a3']
        assert [r['name'] for r in items.find_similar('embedding', QUERY, limit=2)] == ['a1', 'a2']
        assert ann == []

    def test_most_specific_index_wins(self, items, monkeypatch):
        import dbset.sync_core as sync_core
        items.create_vector_index('embedding')
        partial = items.create_vector_index('embedding', where={'cat': 'b'})
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        list(items.find_similar('embedding', QUERY, limit=2, cat='b'))
        list(items.find_similar('embedding', QUERY, limit=2, cat='a'))
        assert [a[4] for a in ann] == [partial, 'idx_items_embedding_diskann_cosine']

    def test_quoted_value_and_numeric_conditions(self, db):
        table = db['q']
        for i, (name, _, vec) in enumerate(DOCS):
            table.insert({'name': name, 'tag': "it's" if i < 3 else 'x', 'n': i % 2, 'embedding': np.array(vec)})
        table.create_vector_index('embedding', where={'tag': "it's", 'n': 0})
        rows = list(table.find_similar('embedding', QUERY, limit=5, tag="it's", n=0))
        assert [r['name'] for r in rows] == ['a1', 'a3']
        assert table.count() == len(DOCS)

    @pytest.mark.parametrize('where', [{'nope': 1}, {'cat': [1]}, {'cat': float('nan')}])
    def test_bad_where(self, items, where):
        with pytest.raises(VectorError):
            items.create_vector_index('embedding', where=where)

    def test_where_on_plain_sqlite_rejected(self):
        db = connect('sqlite:///:memory:')
        try:
            db['t'].insert({'name': 'x', 'embedding': np.array([1.0, 0.0])})
            with pytest.raises(VectorError, match='libSQL only'):
                db['t'].create_vector_index('embedding', where={'name': 'x'})
        finally:
            db.close()


class TestParseWhere:
    @pytest.mark.parametrize('sql, expected', [
        ("CREATE INDEX i ON t(libsql_vector_idx(e))", {}),
        ("CREATE INDEX i ON t(libsql_vector_idx(e, 'metric=l2')) WHERE cat = 'b'", {'cat': 'b'}),
        ("CREATE INDEX i ON t(libsql_vector_idx(e)) WHERE \"my col\" = 'it''s' AND n = -3", {'my col': "it's", 'n': -3}),
        ("CREATE INDEX i ON t(libsql_vector_idx(e)) WHERE x = 1.5 and y='a AND b'", {'x': 1.5, 'y': 'a AND b'}),
        ("CREATE INDEX i ON t(libsql_vector_idx(e)) WHERE x > 1", None),
        ("CREATE INDEX i ON t(libsql_vector_idx(e)) WHERE x = 1 OR y = 2", None),
        ("CREATE INDEX i ON t(libsql_vector_idx(e)) WHERE x = 1 AND x = 2", None),
        ("CREATE INDEX i ON t(name)", None),
    ])
    def test_parse(self, sql, expected):
        from dbset.vector import parse_libsql_index_where
        assert parse_libsql_index_where(sql) == expected


class TestAsyncFilteredAnn:
    async def test_async_filtered_and_partial(self, monkeypatch):
        import dbset.async_core as async_core
        db = await async_connect('sqlite+dbsetaiolibsql://')
        try:
            table = db['items']
            for name, cat, vec in DOCS:
                await table.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
            await table.create_vector_index('embedding')
            partial = await table.create_vector_index('embedding', where={'cat': 'b'})
            ann = _spy(monkeypatch, async_core, 'libsql_candidates_query')
            rows = [r async for r in table.find_similar('embedding', QUERY, limit=2, cat='a')]
            assert [r['name'] for r in rows] == ['a1', 'a2']
            rows = [r async for r in table.find_similar('embedding', QUERY, limit=5, cat='b')]
            assert [r['name'] for r in rows] == expected_order('cosine', cat='b')
            assert [a[4] for a in ann] == ['idx_items_embedding_diskann_cosine', partial]
            rows = [r async for r in table.find_similar('embedding', QUERY, limit=5, cat='c', threshold=2.0)]
            assert [r['name'] for r in rows] == ['c1']
        finally:
            await db.close()


class TestAnnCompleteness:
    """Review findings: search_l cap without filters, NULL distances, strict literal matching."""

    def test_limit_above_search_l_without_filters(self, db, monkeypatch):
        import dbset.sync_core as sync_core
        rng = np.random.default_rng(5)
        table = db['many']
        table.insert_many([{'name': f'r{i}', 'embedding': rng.standard_normal(8)} for i in range(300)])
        table.create_vector_index('embedding')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        rows = list(table.find_similar('embedding', rng.standard_normal(8).tolist(), limit=250))
        assert len(rows) == 250  # vector_top_k yields ~200: the search scans exactly
        assert len(exact) == 1

    def test_partial_index_fully_covering_filter_is_not_cut(self, db):
        rng = np.random.default_rng(6)
        table = db['many2']
        table.insert_many([{'name': f'r{i}', 'cat': 'a' if i % 2 else 'b', 'embedding': rng.standard_normal(8)}
                           for i in range(600)])
        table.create_vector_index('embedding', where={'cat': 'a'})
        rows = list(table.find_similar('embedding', rng.standard_normal(8).tolist(), limit=280, cat='a'))
        assert len(rows) == 280 and all(r['cat'] == 'a' for r in rows)

    def test_threshold_on_ann_path(self, items, monkeypatch):
        import dbset.sync_core as sync_core
        items.create_vector_index('embedding')
        exact = _spy(monkeypatch, sync_core, 'vector_distance_sql')
        rows = list(items.find_similar('embedding', QUERY, limit=2, cat='a', threshold=0.01))
        assert [r['name'] for r in rows] == ['a1', 'a2']
        rows = list(items.find_similar('embedding', QUERY, limit=5, threshold=0.01))
        assert [r['name'] for r in rows] == ['a1', 'a2']
        assert exact == []

    @pytest.mark.parametrize('threshold', [None, 0.5])
    def test_zero_vector_distance_is_null(self, db, threshold):
        """vector_distance_cos of a zero vector is NULL: ANN and exact scan agree, no TypeError."""
        table = db['zeros']
        for name, cat, vec in DOCS + [('zero', 'z', [0.0, 0.0, 0.0])]:
            table.insert({'name': name, 'cat': cat, 'embedding': np.array(vec)})
        exact = [(r['name'], r['_distance']) for r in table.find_similar('embedding', QUERY, limit=20, threshold=threshold)]
        table.create_vector_index('embedding')
        ann = [(r['name'], r['_distance']) for r in table.find_similar('embedding', QUERY, limit=20, threshold=threshold)]
        assert [n for n, _ in ann] == [n for n, _ in exact]
        assert ('zero' in [n for n, _ in ann]) == (threshold is None)

    def test_literal_type_must_match_partial_index(self, db, monkeypatch):
        import dbset.sync_core as sync_core
        table = db['codes']
        for code, vec in (('1', [1.0, 0.0]), ('1.0', [0.9, 0.1])):
            table.insert({'code': code, 'embedding': np.array(vec)})
        table.create_vector_index('embedding', where={'code': 1.0})
        ann = _spy(monkeypatch, sync_core, 'libsql_candidates_query')
        assert [r['code'] for r in table.find_similar('embedding', [1.0, 0.0], code=1)] == ['1']
        assert ann == []

    def test_zero_and_negative_limit(self, items):
        items.create_vector_index('embedding')
        assert list(items.find_similar('embedding', QUERY, limit=0)) == []
        assert len(list(items.find_similar('embedding', QUERY, limit=-1))) == len(DOCS)
