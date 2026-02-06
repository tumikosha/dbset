"""Vector type support for storing and searching embeddings."""
from __future__ import annotations

import json
import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.types import UserDefinedType

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    np = None
    HAS_NUMPY = False


class DistanceMetric:
    """Distance metric constants for vector similarity search."""
    L2 = 'l2'
    COSINE = 'cosine'
    INNER_PRODUCT = 'inner_product'


class Vector(UserDefinedType):
    """
    SQLAlchemy custom type for vector/embedding columns.

    Used for DDL generation and type annotation. Serialization/deserialization
    is handled explicitly in _preprocess_row() and find_similar() because
    SQLAlchemy schema reflection does not preserve custom types.

    Args:
        dim: Vector dimension (number of elements). None for variable-length.
    """
    cache_ok = True

    def __init__(self, dim: int | None = None):
        self.dim = dim

    def get_col_spec(self) -> str:
        if self.dim is not None:
            return f"VECTOR({self.dim})"
        return "VECTOR"


def serialize_vector(vector: Any, dialect_name: str = 'sqlite') -> str:
    """
    Serialize a vector (list or numpy array) to a string for storage.

    Args:
        vector: list[float] or numpy.ndarray
        dialect_name: Database dialect name

    Returns:
        Serialized string representation
    """
    if HAS_NUMPY and isinstance(vector, np.ndarray):
        values = vector.tolist()
    elif isinstance(vector, (list, tuple)):
        values = list(vector)
    else:
        return vector  # Already serialized or unknown type

    if dialect_name == 'postgresql':
        return '[' + ','.join(str(float(v)) for v in values) + ']'
    else:
        # SQLite and MySQL: JSON array string
        return json.dumps([float(v) for v in values])


def deserialize_vector(value: Any) -> list[float] | None:
    """
    Deserialize a stored vector string back to list[float].

    Args:
        value: Stored value (string or already a list)

    Returns:
        list[float] or None if value is None
    """
    if value is None:
        return None

    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]

    if HAS_NUMPY and isinstance(value, np.ndarray):
        return value.tolist()

    if isinstance(value, str):
        value = value.strip()
        # PostgreSQL format: [0.1,0.2,0.3]
        # JSON format: [0.1, 0.2, 0.3]
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [float(v) for v in parsed]
        except (json.JSONDecodeError, ValueError):
            pass

    raise ValueError(f"Cannot deserialize vector from: {value!r}")


def vector_distance_sql(
    column_name: str,
    query_vector: list[float],
    metric: str = DistanceMetric.COSINE,
    dialect_name: str = 'sqlite',
) -> tuple[str, str]:
    """
    Generate SQL expressions for vector distance computation.

    Returns a tuple of (distance_expr, order_direction) for use in
    SELECT and ORDER BY clauses.

    Args:
        column_name: Name of the vector column
        query_vector: Query vector as list of floats
        metric: Distance metric (l2, cosine, inner_product)
        dialect_name: Database dialect

    Returns:
        Tuple of (distance_expression_sql, order_direction)
    """
    if dialect_name == 'postgresql':
        vec_literal = "'[" + ','.join(str(float(v)) for v in query_vector) + "]'::vector"
        if metric == DistanceMetric.L2:
            return f"{column_name} <-> {vec_literal}", "ASC"
        elif metric == DistanceMetric.COSINE:
            return f"{column_name} <=> {vec_literal}", "ASC"
        elif metric == DistanceMetric.INNER_PRODUCT:
            return f"{column_name} <#> {vec_literal}", "ASC"

    elif dialect_name == 'mysql':
        vec_json = json.dumps([float(v) for v in query_vector])
        metric_map = {
            DistanceMetric.COSINE: 'COSINE',
            DistanceMetric.L2: 'EUCLIDEAN',
            DistanceMetric.INNER_PRODUCT: 'DOT',
        }
        mysql_metric = metric_map.get(metric, 'COSINE')
        return (
            f"VECTOR_DISTANCE({column_name}, STRING_TO_VECTOR('{vec_json}'), '{mysql_metric}')",
            "ASC",
        )

    else:
        # SQLite: compute in Python or use sqlite-vec if available
        # Fallback: compute cosine/L2 distance in Python after fetch
        # For now, return a placeholder that signals Python-side computation
        return "__python_distance__", "ASC"

    raise ValueError(f"Unknown metric: {metric}")


def compute_distance(
    vec_a: list[float],
    vec_b: list[float],
    metric: str = DistanceMetric.COSINE,
) -> float:
    """
    Compute distance between two vectors in Python.

    Used as fallback for SQLite which lacks native vector distance functions
    (unless sqlite-vec extension is loaded).

    Args:
        vec_a: First vector
        vec_b: Second vector
        metric: Distance metric

    Returns:
        Distance value (lower = more similar for L2/cosine)
    """
    if len(vec_a) != len(vec_b):
        raise ValueError(
            f"Vector dimension mismatch: {len(vec_a)} vs {len(vec_b)}"
        )

    if metric == DistanceMetric.L2:
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec_a, vec_b)))

    elif metric == DistanceMetric.COSINE:
        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))
        if norm_a == 0 or norm_b == 0:
            return 1.0  # Maximum cosine distance
        return 1.0 - (dot / (norm_a * norm_b))

    elif metric == DistanceMetric.INNER_PRODUCT:
        # Negative inner product (so lower = more similar, consistent with pgvector <#>)
        return -sum(a * b for a, b in zip(vec_a, vec_b))

    raise ValueError(f"Unknown metric: {metric}")
