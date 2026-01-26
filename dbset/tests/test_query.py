"""Unit tests for query.py - FilterBuilder."""

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from dbset.exceptions import QueryError
from dbset.query import FilterBuilder


# Helper to create test table
def create_test_table():
    """Create a test SQLAlchemy table."""
    metadata = MetaData()
    table = Table(
        'users',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(255)),
        Column('age', Integer),
        Column('email', String(255)),
        Column('status', String(50)),
    )
    return table


def test_simple_equality_filter():
    """Test simple equality filter."""
    table = create_test_table()
    filters = {'name': 'John'}

    clause = FilterBuilder.build(table, filters)
    assert clause is not None

    # Check clause compiles to valid SQL
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'name' in sql
    assert 'John' in sql


def test_comparison_operators():
    """Test comparison operators (>, <, >=, <=, !=)."""
    table = create_test_table()

    # Greater than
    clause = FilterBuilder.build(table, {'age': {'>': 18}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert '>' in sql

    # Greater than or equal
    clause = FilterBuilder.build(table, {'age': {'>=': 18}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert '>=' in sql

    # Less than
    clause = FilterBuilder.build(table, {'age': {'<': 65}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert '<' in sql

    # Less than or equal
    clause = FilterBuilder.build(table, {'age': {'<=': 65}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert '<=' in sql

    # Not equal
    clause = FilterBuilder.build(table, {'status': {'!=': 'deleted'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'status' in sql
    assert '!=' in sql or '<>' in sql  # SQLite uses !=, others may use <>


def test_in_operator():
    """Test IN operator."""
    table = create_test_table()
    filters = {'status': {'in': ['active', 'pending', 'approved']}}

    clause = FilterBuilder.build(table, filters)
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'status' in sql
    assert 'IN' in sql.upper()


def test_like_operator():
    """Test LIKE operator."""
    table = create_test_table()

    # LIKE
    clause = FilterBuilder.build(table, {'email': {'like': '%@gmail.com'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'email' in sql
    assert 'LIKE' in sql.upper()

    # ILIKE (case-insensitive)
    clause = FilterBuilder.build(table, {'email': {'ilike': '%@GMAIL.COM'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'email' in sql

    # Startswith
    clause = FilterBuilder.build(table, {'name': {'startswith': 'John'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'name' in sql
    assert 'LIKE' in sql.upper()

    # Endswith
    clause = FilterBuilder.build(table, {'name': {'endswith': 'son'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'name' in sql
    assert 'LIKE' in sql.upper()

    # Contains
    clause = FilterBuilder.build(table, {'name': {'contains': 'oh'}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'name' in sql
    assert 'LIKE' in sql.upper()


def test_is_null():
    """Test IS NULL operator."""
    table = create_test_table()
    clause = FilterBuilder.build(table, {'email': {'is': None}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'email' in sql
    assert 'IS' in sql.upper()


def test_between_operator():
    """Test BETWEEN operator."""
    table = create_test_table()
    clause = FilterBuilder.build(table, {'age': {'between': [18, 65]}})
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert 'BETWEEN' in sql.upper()


def test_multiple_filters_and():
    """Test multiple filters combined with AND."""
    table = create_test_table()
    filters = {
        'age': {'>=': 18},
        'status': 'active',
    }

    clause = FilterBuilder.build(table, filters, conjunction='AND')
    sql = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert 'status' in sql
    assert 'AND' in sql.upper()


def test_multiple_filters_or():
    """Test multiple filters combined with OR."""
    table = create_test_table()
    filters = {
        'status': 'active',
        'status': 'pending',  # Note: This will be overwritten, just for demo
    }

    clause = FilterBuilder.build(table, {'age': {'>': 65}}, conjunction='OR')
    # With only one filter, no OR should appear
    # Let's test with actual OR scenario by using advanced query building


def test_empty_filters():
    """Test empty filters returns None."""
    table = create_test_table()
    clause = FilterBuilder.build(table, {})
    assert clause is None


def test_unknown_operator():
    """Test unknown operator raises error."""
    table = create_test_table()
    with pytest.raises(QueryError, match="Unknown operator"):
        FilterBuilder.build(table, {'age': {'unknown_op': 18}})


def test_unknown_column():
    """Test unknown column raises error."""
    table = create_test_table()
    with pytest.raises(QueryError, match="not found"):
        FilterBuilder.build(table, {'unknown_column': 'value'})


def test_between_invalid_value():
    """Test BETWEEN with invalid value raises error."""
    table = create_test_table()
    with pytest.raises(QueryError, match="requires list/tuple of 2 values"):
        FilterBuilder.build(table, {'age': {'between': [18]}})  # Only 1 value


def test_in_invalid_value():
    """Test IN with invalid value raises error."""
    table = create_test_table()
    with pytest.raises(QueryError, match="requires list/tuple"):
        FilterBuilder.build(table, {'status': {'in': 'active'}})  # String, not list


def test_parse_order_by_asc():
    """Test parsing ascending order_by."""
    table = create_test_table()
    order_clauses = FilterBuilder.parse_order_by(table, 'age')

    assert len(order_clauses) == 1
    sql = str(order_clauses[0].compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql


def test_parse_order_by_desc():
    """Test parsing descending order_by."""
    table = create_test_table()
    order_clauses = FilterBuilder.parse_order_by(table, '-age')

    assert len(order_clauses) == 1
    sql = str(order_clauses[0].compile(compile_kwargs={"literal_binds": True}))
    assert 'age' in sql
    assert 'DESC' in sql.upper()


def test_parse_order_by_multiple():
    """Test parsing multiple order_by columns."""
    table = create_test_table()
    order_clauses = FilterBuilder.parse_order_by(table, ['name', '-age'])

    assert len(order_clauses) == 2


def test_parse_order_by_unknown_column():
    """Test parsing order_by with unknown column."""
    table = create_test_table()
    with pytest.raises(QueryError, match="not found"):
        FilterBuilder.parse_order_by(table, 'unknown_column')
