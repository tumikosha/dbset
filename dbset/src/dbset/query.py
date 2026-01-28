"""Query builder - translates dict-based filters to SQLAlchemy WHERE clauses."""
from __future__ import annotations

from typing import Any

from sqlalchemy import Table, and_, or_, not_
from sqlalchemy.sql.elements import BooleanClauseList, ColumnElement

from .exceptions import QueryError


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
    }

    @staticmethod
    def build(
        table: Table,
        filters: dict[str, Any],
        conjunction: str = 'AND',
    ) -> BooleanClauseList | ColumnElement | None:
        """
        Build SQLAlchemy WHERE clause from dict filters.

        Args:
            table: SQLAlchemy Table object
            filters: Dictionary of column_name -> value or operator dict
            conjunction: 'AND' or 'OR' for combining multiple conditions

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
        """
        if not filters:
            return None

        clauses = []

        for column_name, value in filters.items():
            # Check column exists
            if column_name not in table.c:
                raise QueryError(
                    f"Column '{column_name}' not found in table '{table.name}'"
                )

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
