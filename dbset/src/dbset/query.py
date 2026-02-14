"""Query builder - translates dict-based filters to SQLAlchemy WHERE clauses."""
from __future__ import annotations

from typing import Any

import json

from sqlalchemy import Table, and_, or_, not_, text, func
from sqlalchemy.sql.elements import BooleanClauseList, ColumnElement

from .exceptions import QueryError


# JSONB containment operators (need special handling)
_JSONB_CONTAINMENT_OPS = frozenset({'@>', '<@', 'jsonb_contains', 'jsonb_contained_by'})

# JSONB key/element existence operators
_JSONB_EXISTS_OPS = frozenset({'?|', '?&', 'jsonb_any', 'jsonb_all'})


def _parse_json_path(column_name: str) -> tuple[str, str | None]:
    """
    Parse column name to extract JSONB path.

    Args:
        column_name: Column name, possibly with dot notation (e.g., 'metadata.category')

    Returns:
        Tuple of (base_column, json_path) where json_path is None for regular columns

    Examples:
        >>> _parse_json_path('name')
        ('name', None)
        >>> _parse_json_path('metadata.category')
        ('metadata', 'category')
        >>> _parse_json_path('metadata.nested.field')
        ('metadata', 'nested.field')
    """
    if '.' in column_name:
        parts = column_name.split('.', 1)
        return parts[0], parts[1]
    return column_name, None


def _build_json_accessor(table: Table, base_column: str, json_path: str, dialect: str):
    """
    Build SQLAlchemy expression for JSONB field access.

    Args:
        table: SQLAlchemy Table object
        base_column: Base column name (e.g., 'metadata')
        json_path: JSON path (e.g., 'category' or 'nested.field')
        dialect: Database dialect name

    Returns:
        SQLAlchemy expression for accessing the JSON field
    """
    column = table.c[base_column]

    # Handle nested paths (convert 'a.b.c' to '$.a.b.c' or PostgreSQL syntax)
    if dialect == 'postgresql':
        # PostgreSQL: Use ->> for text extraction
        # For nested paths like 'a.b', use #>> operator with path array
        if '.' in json_path:
            # Nested path: metadata #>> '{a,b,c}'
            path_parts = json_path.split('.')
            path_array = '{' + ','.join(path_parts) + '}'
            return text(f"{base_column} #>> '{path_array}'")
        else:
            # Simple path: metadata->>'category'
            return text(f"{base_column}->>'{json_path}'")
    elif dialect == 'sqlite':
        # SQLite: json_extract(column, '$.path')
        full_path = '$.' + json_path.replace('.', '.')
        return func.json_extract(column, full_path)
    else:
        # MySQL/MariaDB: JSON_UNQUOTE(JSON_EXTRACT(column, '$.path'))
        full_path = '$.' + json_path
        return func.JSON_UNQUOTE(func.JSON_EXTRACT(column, full_path))

# Vector distance operator names
_VECTOR_DISTANCE_OPS = frozenset({'cosine_distance', 'l2_distance', 'max_inner_product'})


class FilterBuilder:
    """
    Builds SQLAlchemy WHERE clauses from dictionary filters.

    Provides Pythonic dict-based filtering API that translates to
    SQLAlchemy expressions under the hood.

    Supports:
    - Simple equality: {'age': 30}
    - Comparison operators: {'age': {'>=': 18}}
    - Multiple conditions (AND): {'age': {'>=': 18}, 'status': 'active'}
    - IN queries: {'status': {'in': ['active', 'pending']}}
    - LIKE patterns: {'email': {'like': '%@gmail.com'}}
    - NULL checks: {'deleted_at': {'is': None}}
    - BETWEEN: {'age': {'between': [18, 65]}}
    """

    # Operator mapping: dict key -> SQLAlchemy operator function
    OPERATORS = {
        '=': lambda col, val: col == val,
        '==': lambda col, val: col == val,
        '!=': lambda col, val: col != val,
        '>': lambda col, val: col > val,
        '>=': lambda col, val: col >= val,
        '<': lambda col, val: col < val,
        '<=': lambda col, val: col <= val,
        'in': lambda col, val: col.in_(val),
        'not_in': lambda col, val: col.notin_(val),
        'like': lambda col, val: col.like(val),
        'ilike': lambda col, val: col.ilike(val),  # Case-insensitive LIKE
        'not_like': lambda col, val: col.notlike(val),
        'startswith': lambda col, val: col.like(f"{val}%"),
        'endswith': lambda col, val: col.like(f"%{val}"),
        'contains': lambda col, val: col.like(f"%{val}%"),
        'is': lambda col, val: col.is_(val),  # For NULL checks
        'is_not': lambda col, val: col.is_not(val),
        'between': lambda col, val: col.between(val[0], val[1]),
        'cosine_distance': None,  # Handled specially in build()
        'l2_distance': None,  # Handled specially in build()
        'max_inner_product': None,  # Handled specially in build()
        # JSONB containment operators (handled specially in build())
        '@>': None,  # PostgreSQL: column @> '["value"]'::jsonb
        '<@': None,  # PostgreSQL: column <@ '["value"]'::jsonb
        'jsonb_contains': None,  # Alias for @>
        'jsonb_contained_by': None,  # Alias for <@
        # JSONB existence operators (handled specially in build())
        '?|': None,  # PostgreSQL: column ?| array['a','b'] (ANY)
        '?&': None,  # PostgreSQL: column ?& array['a','b'] (ALL)
        'jsonb_any': None,  # Alias for ?|
        'jsonb_all': None,  # Alias for ?&
    }

    @staticmethod
    def build(
        table: Table,
        filters: dict[str, Any],
        conjunction: str = 'AND',
        dialect: str | None = None,
    ) -> BooleanClauseList | ColumnElement | None:
        """
        Build SQLAlchemy WHERE clause from dict filters.

        Args:
            table: SQLAlchemy Table object
            filters: Dictionary of column_name -> value or operator dict
            conjunction: 'AND' or 'OR' for combining multiple conditions
            dialect: Database dialect name (for JSONB support)

        Returns:
            SQLAlchemy BooleanClauseList or None if no filters

        Raises:
            QueryError: If filter syntax is invalid

        Examples:
            Simple equality:
            >>> build(users_table, {'age': 30})
            users_table.c.age == 30

            Comparison operator:
            >>> build(users_table, {'age': {'>=': 18}})
            users_table.c.age >= 18

            Multiple conditions (AND):
            >>> build(users_table, {'age': {'>=': 18}, 'status': 'active'})
            (users_table.c.age >= 18) AND (users_table.c.status == 'active')

            IN query:
            >>> build(users_table, {'status': {'in': ['active', 'pending']}})
            users_table.c.status.in_(['active', 'pending'])

            LIKE pattern:
            >>> build(users_table, {'email': {'like': '%@gmail.com'}})
            users_table.c.email.like('%@gmail.com')

            NULL check:
            >>> build(users_table, {'deleted_at': {'is': None}})
            users_table.c.deleted_at.is_(None)

            BETWEEN:
            >>> build(users_table, {'age': {'between': [18, 65]}})
            users_table.c.age.between(18, 65)

            JSONB dot notation (requires dialect):
            >>> build(users_table, {'metadata.category': 'tech'}, dialect='postgresql')
            metadata->>'category' = 'tech'
        """
        if not filters:
            return None

        clauses = []

        for column_name, value in filters.items():
            # Parse for JSONB dot notation
            base_column, json_path = _parse_json_path(column_name)

            # Check base column exists
            if base_column not in table.c:
                raise QueryError(
                    f"Column '{base_column}' not found in table '{table.name}'"
                )

            # Get column accessor (regular column or JSONB path)
            if json_path is not None:
                if dialect is None:
                    raise QueryError(
                        f"JSONB dot notation '{column_name}' requires dialect parameter"
                    )
                column = _build_json_accessor(table, base_column, json_path, dialect)
            else:
                column = table.c[column_name]

            # Handle advanced filters: {'age': {'>=': 18}}
            if isinstance(value, dict):
                for operator, op_value in value.items():
                    if operator not in FilterBuilder.OPERATORS:
                        raise QueryError(
                            f"Unknown operator: '{operator}'. "
                            f"Valid operators: {', '.join(FilterBuilder.OPERATORS.keys())}"
                        )

                    # Validate BETWEEN has exactly 2 values
                    if operator == 'between':
                        if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
                            raise QueryError(
                                f"BETWEEN operator requires list/tuple of 2 values, "
                                f"got: {op_value}"
                            )

                    # Validate IN/NOT_IN has list
                    if operator in ('in', 'not_in'):
                        if not isinstance(op_value, (list, tuple)):
                            raise QueryError(
                                f"{operator.upper()} operator requires list/tuple, "
                                f"got: {type(op_value).__name__}"
                            )

                    # Handle vector distance operators
                    if operator in _VECTOR_DISTANCE_OPS:
                        if not isinstance(op_value, (list, tuple)) or len(op_value) != 2:
                            raise QueryError(
                                f"{operator} requires [query_vector, threshold], "
                                f"got: {op_value}"
                            )
                        query_vec, threshold = op_value
                        if not isinstance(query_vec, (list, tuple)):
                            raise QueryError(
                                f"{operator} query_vector must be a list, "
                                f"got: {type(query_vec).__name__}"
                            )
                        # Vector distance filtering is handled by find_similar() or
                        # Python-side computation, not as SQL WHERE clause.
                        # Store as metadata for the caller to interpret.
                        from .vector import compute_distance, DistanceMetric
                        metric_map = {
                            'cosine_distance': DistanceMetric.COSINE,
                            'l2_distance': DistanceMetric.L2,
                            'max_inner_product': DistanceMetric.INNER_PRODUCT,
                        }
                        # Skip adding SQL clause - vector filtering is handled separately
                        continue

                    # Handle JSONB containment operators
                    if operator in _JSONB_CONTAINMENT_OPS:
                        # Normalize operator name
                        is_contains = operator in ('@>', 'jsonb_contains')

                        # Convert value to JSON string
                        if isinstance(op_value, str):
                            # Already a string, assume it's valid JSON or wrap in array
                            if not op_value.startswith('[') and not op_value.startswith('{'):
                                json_val = json.dumps([op_value])
                            else:
                                json_val = op_value
                        elif isinstance(op_value, (list, dict)):
                            json_val = json.dumps(op_value)
                        else:
                            # Single value - wrap in array
                            json_val = json.dumps([op_value])

                        # Build dialect-specific clause
                        if dialect == 'postgresql':
                            op_symbol = '@>' if is_contains else '<@'
                            clause = text(f"{base_column} {op_symbol} '{json_val}'::jsonb")
                        elif dialect == 'sqlite':
                            # SQLite: Use json_each to check containment
                            if is_contains:
                                # Check if array contains value
                                if isinstance(op_value, str) and not op_value.startswith('['):
                                    clause = text(
                                        f"EXISTS (SELECT 1 FROM json_each({base_column}) "
                                        f"WHERE json_each.value = '{op_value}')"
                                    )
                                else:
                                    # For array containment, check each element
                                    clause = text(
                                        f"EXISTS (SELECT 1 FROM json_each('{json_val}') AS needle "
                                        f"WHERE needle.value IN (SELECT value FROM json_each({base_column})))"
                                    )
                            else:
                                raise QueryError(
                                    f"Operator '<@' not supported for SQLite"
                                )
                        elif dialect in ('mysql', 'mariadb'):
                            # MySQL: JSON_CONTAINS(column, value)
                            if is_contains:
                                clause = text(f"JSON_CONTAINS({base_column}, '{json_val}')")
                            else:
                                clause = text(f"JSON_CONTAINS('{json_val}', {base_column})")
                        else:
                            raise QueryError(
                                f"JSONB containment operator '{operator}' requires dialect parameter"
                            )

                        clauses.append(clause)
                        continue

                    # Handle JSONB existence operators (?| and ?&)
                    if operator in _JSONB_EXISTS_OPS:
                        # Normalize operator name
                        is_any = operator in ('?|', 'jsonb_any')

                        # Ensure value is a list
                        if isinstance(op_value, str):
                            values = [op_value]
                        elif isinstance(op_value, (list, tuple)):
                            values = list(op_value)
                        else:
                            values = [op_value]

                        # Build dialect-specific clause
                        if dialect == 'postgresql':
                            # PostgreSQL: column ?| array['a','b'] or column ?& array['a','b']
                            op_symbol = '?|' if is_any else '?&'
                            array_str = "array[" + ",".join(f"'{v}'" for v in values) + "]"
                            clause = text(f"{base_column} {op_symbol} {array_str}")
                        elif dialect == 'sqlite':
                            # SQLite: Use json_each with EXISTS
                            if is_any:
                                # ANY: at least one value matches
                                values_str = ",".join(f"'{v}'" for v in values)
                                clause = text(
                                    f"EXISTS (SELECT 1 FROM json_each({base_column}) "
                                    f"WHERE json_each.value IN ({values_str}))"
                                )
                            else:
                                # ALL: all values must match
                                conditions = " AND ".join(
                                    f"EXISTS (SELECT 1 FROM json_each({base_column}) WHERE json_each.value = '{v}')"
                                    for v in values
                                )
                                clause = text(f"({conditions})")
                        elif dialect in ('mysql', 'mariadb'):
                            # MySQL: Use JSON_CONTAINS for each value
                            if is_any:
                                # ANY: OR conditions
                                conditions = " OR ".join(
                                    f"JSON_CONTAINS({base_column}, '\"{v}\"')"
                                    for v in values
                                )
                                clause = text(f"({conditions})")
                            else:
                                # ALL: AND conditions
                                conditions = " AND ".join(
                                    f"JSON_CONTAINS({base_column}, '\"{v}\"')"
                                    for v in values
                                )
                                clause = text(f"({conditions})")
                        else:
                            raise QueryError(
                                f"JSONB existence operator '{operator}' requires dialect parameter"
                            )

                        clauses.append(clause)
                        continue

                    try:
                        clause = FilterBuilder.OPERATORS[operator](column, op_value)
                        clauses.append(clause)
                    except Exception as e:
                        raise QueryError(
                            f"Error building filter for {column_name} {operator} {op_value}: {e}"
                        )
            else:
                # Simple equality filter: {'age': 30}
                clauses.append(column == value)

        # Combine clauses with AND/OR
        if not clauses:
            return None

        if len(clauses) == 1:
            return clauses[0]

        if conjunction.upper() == 'AND':
            return and_(*clauses)
        elif conjunction.upper() == 'OR':
            return or_(*clauses)
        else:
            raise QueryError(
                f"Invalid conjunction: '{conjunction}'. Must be 'AND' or 'OR'"
            )

    @staticmethod
    def parse_order_by(
        table: Table,
        order_by: str | list[str],
    ) -> list[ColumnElement]:
        """
        Parse order_by string/list into SQLAlchemy order_by clauses.

        Args:
            table: SQLAlchemy Table object
            order_by: Column name(s) with optional '-' prefix for DESC
                      Examples: 'age', '-age', ['name', '-age']

        Returns:
            List of SQLAlchemy order_by clauses

        Raises:
            QueryError: If column not found

        Examples:
            >>> parse_order_by(users_table, 'age')
            [users_table.c.age.asc()]

            >>> parse_order_by(users_table, '-age')
            [users_table.c.age.desc()]

            >>> parse_order_by(users_table, ['name', '-age'])
            [users_table.c.name.asc(), users_table.c.age.desc()]
        """
        if isinstance(order_by, str):
            order_by = [order_by]

        order_clauses = []
        for col_spec in order_by:
            # Check for DESC prefix
            if col_spec.startswith('-'):
                column_name = col_spec[1:]
                desc = True
            else:
                column_name = col_spec
                desc = False

            # Validate column exists
            if column_name not in table.c:
                raise QueryError(
                    f"Column '{column_name}' not found in table '{table.name}'"
                )

            column = table.c[column_name]
            order_clauses.append(column.desc() if desc else column.asc())

        return order_clauses
