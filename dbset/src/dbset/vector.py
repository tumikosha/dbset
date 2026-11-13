"""Vector type support for storing and searching embeddings."""
from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import UserDefinedType

#: dialect_name the vector helpers take for libSQL (its SQLAlchemy dialect name is 'sqlite').
LIBSQL = 'libsql'

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

    def __init__(self, dim: int | str | None = None):
        # Reflection (libSQL F32_BLOB(dim)) passes the dimension as a string.
        self.dim = int(dim) if dim is not None else None

    def get_col_spec(self) -> str:
        if self.dim is not None:
            return f"VECTOR({self.dim})"
        return "VECTOR"


@compiles(Vector, 'sqlite')
def _compile_vector_sqlite(type_, compiler, **kw):
    """libSQL stores a sized vector as F32_BLOB(dim), the only type its DiskANN index accepts."""
    from .libsql import is_libsql  # libsql.py imports this module for reflection

    if is_libsql(compiler.dialect) and type_.dim is not None:
        return f"F32_BLOB({type_.dim})"
    return type_.get_col_spec()


def vector_dialect(dialect) -> str:
    """Dialect name for the vector helpers: 'libsql' for libSQL, else ``dialect.name``."""
    from .libsql import is_libsql

    return LIBSQL if is_libsql(dialect) else dialect.name


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
    elif dialect_name == LIBSQL:
        # F32_BLOB: little-endian float32, what libSQL's vector functions read.
        return struct.pack(f'<{len(values)}f', *(float(v) for v in values))
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

    if isinstance(value, (bytes, bytearray, memoryview)):
        # libSQL F32_BLOB: little-endian float32
        raw = bytes(value)
        if len(raw) % 4:
            raise ValueError(f"Cannot deserialize vector from a {len(raw)}-byte blob")
        return list(struct.unpack(f'<{len(raw) // 4}f', raw))

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

    elif dialect_name == LIBSQL:
        functions = {
            DistanceMetric.COSINE: 'vector_distance_cos',
            DistanceMetric.L2: 'vector_distance_l2',
        }
        if metric in functions:
            return f"{functions[metric]}({column_name}, {libsql_vector_literal(query_vector)})", "ASC"
        if metric == DistanceMetric.INNER_PRODUCT:
            # libSQL has no inner product function: computed in Python like SQLite.
            return "__python_distance__", "ASC"

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


# ---------- libSQL DiskANN index ----------

#: Metrics a libSQL vector index supports (libsql_vector_idx 'metric=...' values).
LIBSQL_INDEX_METRICS = {DistanceMetric.COSINE: 'cosine', DistanceMetric.L2: 'l2'}

_LIBSQL_INDEX_RE = re.compile(
    r"libsql_vector_idx\s*\(\s*(?P<col>\"(?:[^\"]|\"\")+\"|`[^`]+`|\[[^\]]+\]|\w+)(?P<opts>[^)]*)\)",
    re.IGNORECASE,
)
_LIBSQL_METRIC_RE = re.compile(r"'\s*metric\s*=\s*(\w+)\s*'", re.IGNORECASE)
_LIBSQL_PARAM_RE = re.compile(r"^[A-Za-z_]+$")
_LIBSQL_VALUE_RE = re.compile(r"^[A-Za-z0-9_.]+$")


def libsql_vector_literal(query_vector: list[float]) -> str:
    """SQL literal for a query vector: vector32('[...]')."""
    return "vector32('[" + ','.join(repr(float(v)) for v in query_vector) + "]')"


def parse_libsql_vector_index(sql: str | None) -> tuple[str, str] | None:
    """(column, metric) of a libSQL vector index from its CREATE INDEX text, else None."""
    if not sql:
        return None
    match = _LIBSQL_INDEX_RE.search(sql)
    if match is None:
        return None
    column = match.group('col')
    if column[0] in '"`[':
        column = column[1:-1].replace('""', '"') if column[0] == '"' else column[1:-1]
    metric_match = _LIBSQL_METRIC_RE.search(match.group('opts'))
    metric = metric_match.group(1).lower() if metric_match else 'cosine'
    for name, libsql_name in LIBSQL_INDEX_METRICS.items():
        if metric == libsql_name:
            return column, name
    return column, metric


def libsql_index_options(metric: str, params: dict[str, Any]) -> list[str]:
    """The quoted option strings of libsql_vector_idx(col, ...)."""
    options = [f"'metric={LIBSQL_INDEX_METRICS[metric]}'"]
    for key, value in params.items():
        value = str(value)
        if not _LIBSQL_PARAM_RE.match(key) or not _LIBSQL_VALUE_RE.match(value):
            raise ValueError(f"Invalid libSQL vector index parameter: {key}={value!r}")
        options.append(f"'{key}={value}'")
    return options


#: The indexes of one table, for find_libsql_vector_index().
LIBSQL_TABLE_INDEXES_SQL = text(
    "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = :table_name"
)


#: Oversampling steps of a filtered ANN search: vector_top_k fetches limit * factor
#: candidates, the filter runs on them; the next factor is tried while fewer than
#: `limit` rows pass, then the search falls back to an exact scan.
LIBSQL_ANN_OVERSAMPLE = (10, 100, 1000)

_IDENT = r'"(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\]|\w+'
_WHERE_TERM_RE = re.compile(
    rf"\s*(?P<col>{_IDENT})\s*=\s*(?P<val>'(?:[^']|'')*'|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*"
)
_WHERE_AND_RE = re.compile(r"AND\b", re.IGNORECASE)
_WHERE_RE = re.compile(r"WHERE\s+(?P<where>.*)$", re.IGNORECASE | re.DOTALL)


def _unquote_ident(ident: str) -> str:
    if ident[0] == '"':
        return ident[1:-1].replace('""', '"')
    if ident[0] in '`[':
        return ident[1:-1]
    return ident


def _parse_sql_literal(literal: str) -> Any:
    if literal[0] == "'":
        return literal[1:-1].replace("''", "'")
    try:
        return int(literal)
    except ValueError:
        return float(literal)


def _sql_literal(value: Any) -> str:
    """SQL literal of a partial-index condition value (str, int, float, bool)."""
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Partial index value must be finite, got {value!r}")
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"Partial index values must be str, int, float or bool, got {value!r}")


def _equality_literal(value: Any) -> str | None:
    """SQL literal of a plain equality filter value, as a partial index condition is written;
    None for anything else (operators, lists, None, NaN). Comparing literal text keeps
    1 and 1.0 apart, as SQLite does on a TEXT column."""
    if isinstance(value, (str, int, float)):
        try:
            return _sql_literal(value)
        except ValueError:
            return None
    return None


def parse_libsql_index_where(sql: str | None) -> dict[str, Any] | None:
    """Conditions of a libSQL vector index: {} for a full index, {col: value} for a partial
    index whose WHERE is `col = literal [AND ...]`, None for any other WHERE."""
    if not sql:
        return None
    match = _LIBSQL_INDEX_RE.search(sql)
    if match is None:
        return None
    rest = sql[match.end():].strip()
    if not rest.startswith(')'):
        return None
    rest = rest[1:].strip().rstrip(';').strip()
    if not rest:
        return {}
    where_match = _WHERE_RE.match(rest)
    if where_match is None:
        return None
    where = where_match.group('where')
    conditions: dict[str, Any] = {}
    pos = 0
    while True:
        term = _WHERE_TERM_RE.match(where, pos)
        if term is None:
            return None
        name = _unquote_ident(term.group('col'))
        if name in conditions:
            return None  # a repeated column (e.g. a = 1 AND a = 2) is not a plain equality set
        conditions[name] = _parse_sql_literal(term.group('val'))
        pos = term.end()
        if pos == len(where):
            return conditions
        conj = _WHERE_AND_RE.match(where, pos)
        if conj is None:
            return None
        pos = conj.end()


def find_libsql_vector_index(index_rows, column: str, metric: str, filters: dict[str, Any] | None = None):
    """The libSQL vector index that serves a search, from (name, sql) rows of the table's indexes.

    An index qualifies when it is on ``column`` for ``metric`` and its WHERE (if any) is a
    subset of the plain equality filters. Of those, the one with the most conditions wins.
    Returns (index_name, index_conditions, residual_filters) or None.
    """
    filters = filters or {}
    equalities = {
        key: _equality_literal(value)
        for key, value in filters.items()
        if '.' not in key and _equality_literal(value) is not None
    }
    best = None
    for name, sql in index_rows:
        if parse_libsql_vector_index(sql) != (column, metric):
            continue
        conditions = parse_libsql_index_where(sql)
        if conditions is None:
            continue
        if all(key in equalities and equalities[key] == _sql_literal(value) for key, value in conditions.items()):
            if best is None or len(conditions) > len(best[1]):
                best = (name, conditions)
    if best is None:
        return None
    name, conditions = best
    residual = {key: value for key, value in filters.items() if key not in conditions}
    return name, conditions, residual


def libsql_create_index_sql(table, column: str, metric: str, params: dict[str, Any], preparer,
                            where: dict[str, Any] | None = None) -> tuple[str, str]:
    """(index_name, CREATE INDEX statement) of a libSQL DiskANN index, partial when ``where``
    ({column: value}, equality only) is given."""
    where = dict(sorted((where or {}).items()))
    for key in where:
        if key not in table.c:
            raise ValueError(f"Partial index condition on unknown column {key!r}")
    where_sql = ' AND '.join(f"{preparer.quote(key)} = {_sql_literal(value)}" for key, value in where.items())

    # libSQL's DiskANN cannot initialise an index whose name needs quoting: keep it to
    # [A-Za-z0-9_], with a hash of the real names (and conditions) so names cannot collide.
    raw = f"idx_{table.name}_{column}_diskann_{metric}"
    index_name = re.sub(r'\W', '_', raw, flags=re.ASCII)
    if index_name != raw or where:
        key = f"{table.name}\0{column}\0{metric}\0{where_sql}"
        index_name = f"{index_name}_{hashlib.sha1(key.encode()).hexdigest()[:8]}"
    options = ', '.join(libsql_index_options(metric, params))
    create_sql = (
        f"CREATE INDEX IF NOT EXISTS {preparer.quote(index_name)} ON {preparer.format_table(table)} "
        f"(libsql_vector_idx({preparer.quote(column)}, {options}))"
    )
    if where_sql:
        create_sql += f" WHERE {where_sql}"
    return index_name, create_sql


def libsql_candidates_query(table, column: str, query_vector: list[float], metric: str, index_name: str,
                            k: int, where_clause, preparer):
    """The candidates vector_top_k yields for ``k``: (rowid, _distance, _match), one row each.

    vector_top_k picks candidates before any WHERE, so the filter is a column here (_match)
    rather than a WHERE: the caller sees how many candidates there were (at most the index's
    search_l, 200 by default, whatever ``k`` is) and how many pass, from one statement.
    """
    from sqlalchemy import Float, Integer, case, func, literal, literal_column, select

    quoted_table = preparer.format_table(table)
    distance_sql, _ = vector_distance_sql(
        f"{quoted_table}.{preparer.quote(column)}", query_vector, metric, LIBSQL
    )
    top = func.vector_top_k(
        literal(index_name), literal_column(libsql_vector_literal(query_vector)), literal(k)
    ).table_valued('id').alias('v')
    rowid = literal_column(f"{quoted_table}.rowid", Integer)
    match = literal(1) if where_clause is None else case((where_clause, 1), else_=0)
    return select(
        rowid.label('rowid'),
        literal_column(distance_sql, Float).label('_distance'),
        match.label('_match'),
    ).select_from(table.join(top, rowid == top.c.id))


#: Label of the rowid selected next to a table's columns by libsql_rows_query.
LIBSQL_ROWID_KEY = '__dbset_rowid__'


def libsql_rows_query(table, rowids: list[int], preparer):
    """Full rows (plus their rowid under LIBSQL_ROWID_KEY) of the chosen candidates."""
    from sqlalchemy import Integer, literal_column, select

    rowid = literal_column(f"{preparer.format_table(table)}.rowid", Integer)
    return select(table, rowid.label(LIBSQL_ROWID_KEY)).where(rowid.in_(rowids))


def pick_libsql_candidates(candidates, limit: int, threshold: float | None):
    """Choose the result among vector_top_k candidates.

    ``candidates``: (rowid, distance, match) rows of one k. Returns (complete, [(rowid, distance)])
    ordered like the exact scan (NULL distance first; dropped by a threshold). complete=False
    means a larger k may still find more.
    """
    def order(item):
        return (item[1] is not None, item[1] if item[1] is not None else 0.0)

    passing = sorted(((rowid, dist) for rowid, dist, ok in candidates if ok), key=order)
    if threshold is not None:
        passing = [(rowid, dist) for rowid, dist in passing if dist is not None and dist <= threshold]
    if len(passing) >= limit:
        return True, passing[:limit]
    # A shortfall. Distances only grow beyond the candidates, so with a threshold that the
    # farthest candidate already exceeds, nothing further can pass.
    distances = [dist for _, dist, _ in candidates if dist is not None]
    if threshold is not None and distances and max(distances) > threshold:
        return True, passing
    return False, passing


def libsql_indexed_count_query(table, column: str, conditions: dict[str, Any]):
    """How many rows a (partial) vector index holds: non-NULL vectors meeting its conditions."""
    from sqlalchemy import func, select

    stmt = select(func.count()).select_from(table).where(table.c[column].isnot(None))
    for key, value in conditions.items():
        stmt = stmt.where(table.c[key] == value)
    return stmt
