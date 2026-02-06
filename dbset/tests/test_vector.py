"""Unit tests for vector.py module."""
import json
import math

import pytest

from dbset.vector import (
    DistanceMetric,
    Vector,
    compute_distance,
    deserialize_vector,
    serialize_vector,
    vector_distance_sql,
)

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class TestVector:
    def test_init_with_dim(self):
        v = Vector(dim=3)
        assert v.dim == 3

    def test_init_without_dim(self):
        v = Vector()
        assert v.dim is None

    def test_get_col_spec_with_dim(self):
        v = Vector(dim=128)
        assert v.get_col_spec() == "VECTOR(128)"

    def test_get_col_spec_without_dim(self):
        v = Vector()
        assert v.get_col_spec() == "VECTOR"

    def test_cache_ok(self):
        assert Vector.cache_ok is True


class TestDistanceMetric:
    def test_constants(self):
        assert DistanceMetric.L2 == 'l2'
        assert DistanceMetric.COSINE == 'cosine'
        assert DistanceMetric.INNER_PRODUCT == 'inner_product'


class TestSerializeVector:
    def test_serialize_list_sqlite(self):
        result = serialize_vector([0.1, 0.2, 0.3], 'sqlite')
        assert result == json.dumps([0.1, 0.2, 0.3])

    def test_serialize_list_postgresql(self):
        result = serialize_vector([0.1, 0.2, 0.3], 'postgresql')
        assert result == '[0.1,0.2,0.3]'

    def test_serialize_list_mysql(self):
        result = serialize_vector([0.1, 0.2, 0.3], 'mysql')
        parsed = json.loads(result)
        assert parsed == [0.1, 0.2, 0.3]

    def test_serialize_tuple(self):
        result = serialize_vector((1.0, 2.0, 3.0), 'sqlite')
        parsed = json.loads(result)
        assert parsed == [1.0, 2.0, 3.0]

    def test_serialize_already_string(self):
        result = serialize_vector('[1,2,3]', 'sqlite')
        assert result == '[1,2,3]'

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_serialize_numpy_sqlite(self):
        arr = np.array([0.1, 0.2, 0.3])
        result = serialize_vector(arr, 'sqlite')
        parsed = json.loads(result)
        assert len(parsed) == 3
        assert abs(parsed[0] - 0.1) < 1e-7

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_serialize_numpy_postgresql(self):
        arr = np.array([0.1, 0.2, 0.3])
        result = serialize_vector(arr, 'postgresql')
        assert result.startswith('[')
        assert result.endswith(']')
        assert ',' in result


class TestDeserializeVector:
    def test_deserialize_none(self):
        assert deserialize_vector(None) is None

    def test_deserialize_list(self):
        result = deserialize_vector([1.0, 2.0, 3.0])
        assert result == [1.0, 2.0, 3.0]

    def test_deserialize_json_string(self):
        result = deserialize_vector('[0.1, 0.2, 0.3]')
        assert len(result) == 3
        assert abs(result[0] - 0.1) < 1e-7

    def test_deserialize_pgvector_string(self):
        result = deserialize_vector('[0.1,0.2,0.3]')
        assert len(result) == 3
        assert abs(result[0] - 0.1) < 1e-7

    def test_deserialize_invalid(self):
        with pytest.raises(ValueError, match="Cannot deserialize"):
            deserialize_vector("not_a_vector")

    @pytest.mark.skipif(not HAS_NUMPY, reason="numpy not installed")
    def test_deserialize_numpy(self):
        arr = np.array([1.0, 2.0, 3.0])
        result = deserialize_vector(arr)
        assert result == [1.0, 2.0, 3.0]


class TestComputeDistance:
    def test_l2_distance(self):
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        dist = compute_distance(a, b, DistanceMetric.L2)
        assert abs(dist - math.sqrt(2)) < 1e-10

    def test_l2_distance_same(self):
        a = [1.0, 2.0, 3.0]
        dist = compute_distance(a, a, DistanceMetric.L2)
        assert dist == 0.0

    def test_cosine_distance_identical(self):
        a = [1.0, 0.0, 0.0]
        dist = compute_distance(a, a, DistanceMetric.COSINE)
        assert abs(dist) < 1e-10

    def test_cosine_distance_orthogonal(self):
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        dist = compute_distance(a, b, DistanceMetric.COSINE)
        assert abs(dist - 1.0) < 1e-10

    def test_cosine_distance_zero_vector(self):
        a = [0.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]
        dist = compute_distance(a, b, DistanceMetric.COSINE)
        assert dist == 1.0

    def test_inner_product(self):
        a = [1.0, 2.0, 3.0]
        b = [4.0, 5.0, 6.0]
        dist = compute_distance(a, b, DistanceMetric.INNER_PRODUCT)
        # -(1*4 + 2*5 + 3*6) = -(4+10+18) = -32
        assert dist == -32.0

    def test_dimension_mismatch(self):
        a = [1.0, 2.0]
        b = [1.0, 2.0, 3.0]
        with pytest.raises(ValueError, match="dimension mismatch"):
            compute_distance(a, b, DistanceMetric.L2)

    def test_unknown_metric(self):
        with pytest.raises(ValueError, match="Unknown metric"):
            compute_distance([1.0], [1.0], 'unknown')


class TestVectorDistanceSql:
    def test_postgresql_cosine(self):
        expr, direction = vector_distance_sql('embedding', [0.1, 0.2], 'cosine', 'postgresql')
        assert '<=>' in expr
        assert direction == 'ASC'

    def test_postgresql_l2(self):
        expr, direction = vector_distance_sql('embedding', [0.1, 0.2], 'l2', 'postgresql')
        assert '<->' in expr
        assert direction == 'ASC'

    def test_postgresql_inner_product(self):
        expr, direction = vector_distance_sql('embedding', [0.1, 0.2], 'inner_product', 'postgresql')
        assert '<#>' in expr
        assert direction == 'ASC'

    def test_mysql_cosine(self):
        expr, direction = vector_distance_sql('embedding', [0.1, 0.2], 'cosine', 'mysql')
        assert 'VECTOR_DISTANCE' in expr
        assert 'COSINE' in expr
        assert direction == 'ASC'

    def test_sqlite_fallback(self):
        expr, direction = vector_distance_sql('embedding', [0.1, 0.2], 'cosine', 'sqlite')
        assert expr == '__python_distance__'
        assert direction == 'ASC'
