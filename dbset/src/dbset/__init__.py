"""
AsyncDataset - Thin wrapper on top of SQLAlchemy with async/await support.

A Python library for simplified database operations, similar to the original
'dataset' library but with native async support and dual sync/async APIs.

Built on SQLAlchemy 2.x, provides:
- Pythonic dict-based API for common operations
- Automatic schema creation (auto-create tables/columns)
- Read-only mode for safe querying
- Connection pooling for performance
- Both sync and async APIs

Examples:
    Async API (recommended for modern applications):
    >>> from dbset import async_connect
    >>> db = await async_connect('postgresql+asyncpg://localhost/mydb')
    >>> users = db['users']
    >>> pk = await users.insert({'name': 'John', 'age': 30})
    >>> async for user in users.find(age={'>=': 18}):
    ...     print(user)
    >>> await db.close()

    Sync API (for simple scripts):
    >>> from dbset import connect
    >>> db = connect('postgresql://localhost/mydb')
    >>> users = db['users']
    >>> pk = users.insert({'name': 'John', 'age': 30})
    >>> for user in users.find(age={'>=': 18}):
    ...     print(user)
    >>> db.close()

    Read-only mode:
    >>> db = await async_connect('postgresql+asyncpg://...', read_only=True)
    >>> # Only SELECT queries allowed - safe for marketing queries

    Direct SQLAlchemy access:
    >>> from sqlalchemy import select
    >>> users_table = await users.table  # Get SQLAlchemy Table
    >>> stmt = select(users_table).where(users_table.c.age > 18)
    >>> async for row in db.query(stmt):
    ...     print(row)
"""
from __future__ import annotations

from .async_core import AsyncDatabase, AsyncTable
from .exceptions import (
    ColumnNotFoundError,
    ConnectionError,
    DatasetError,
    QueryError,
    ReadOnlyError,
    SchemaError,
    TableNotFoundError,
    TransactionError,
    TypeInferenceError,
    ValidationError,
)
from .sync_core import Database, Table
from .types import PrimaryKeyConfig, PrimaryKeyType

__version__ = "0.1.0"
__author__ = "TriggerAI Team"


# Convenience functions for creating database connections


async def async_connect(
    url: str,
    read_only: bool = False,
    ensure_schema: bool = True,
    primary_key_type: str | PrimaryKeyType = PrimaryKeyType.INTEGER,
    primary_key_column: str = 'id',
    pk_config: PrimaryKeyConfig | None = None,
    **kwargs,
) -> AsyncDatabase:
    """
    Create async database connection.

    This is the recommended way to connect for modern async applications.

    Args:
        url: Database URL with async driver
             - PostgreSQL: 'postgresql+asyncpg://user:pass@host/db'
             - SQLite: 'sqlite+aiosqlite:///path/to/db.sqlite'
        read_only: If True, only SELECT queries allowed (default: False)
        ensure_schema: If True, auto-create tables/columns (default: True)
        primary_key_type: Type of primary key for auto-created tables
                         ('integer', 'uuid', or PrimaryKeyType enum)
        primary_key_column: Name of primary key column (default: 'id')
        pk_config: Advanced PK configuration (overrides primary_key_type/column)
        **kwargs: Additional arguments for create_async_engine

    Returns:
        AsyncDatabase instance

    Examples:
        >>> # PostgreSQL with asyncpg (default Integer PK)
        >>> db = await async_connect('postgresql+asyncpg://localhost/mydb')

        >>> # UUID primary keys
        >>> db = await async_connect(
        ...     'postgresql+asyncpg://localhost/mydb',
        ...     primary_key_type='uuid'
        ... )

        >>> # Custom PK column name
        >>> db = await async_connect(
        ...     'postgresql+asyncpg://localhost/mydb',
        ...     primary_key_type='uuid',
        ...     primary_key_column='user_id'
        ... )

        >>> # Read-only mode for safety
        >>> db = await async_connect(
        ...     'postgresql+asyncpg://localhost/mydb',
        ...     read_only=True
        ... )

        >>> # SQLite for testing
        >>> db = await async_connect('sqlite+aiosqlite:///:memory:')

        >>> # Custom pool settings
        >>> db = await async_connect(
        ...     'postgresql+asyncpg://localhost/mydb',
        ...     pool_size=10,
        ...     max_overflow=20
        ... )
    """
    return await AsyncDatabase.connect(
        url=url,
        read_only=read_only,
        ensure_schema=ensure_schema,
        primary_key_type=primary_key_type,
        primary_key_column=primary_key_column,
        pk_config=pk_config,
        **kwargs,
    )


def connect(
    url: str,
    read_only: bool = False,
    ensure_schema: bool = True,
    primary_key_type: str | PrimaryKeyType = PrimaryKeyType.INTEGER,
    primary_key_column: str = 'id',
    pk_config: PrimaryKeyConfig | None = None,
    **kwargs,
) -> Database:
    """
    Create sync database connection.

    Use this for simple scripts or when async is not needed.

    Args:
        url: Database URL with sync driver
             - PostgreSQL: 'postgresql://user:pass@host/db' or 'postgresql+psycopg2://...'
             - SQLite: 'sqlite:///path/to/db.sqlite'
        read_only: If True, only SELECT queries allowed (default: False)
        ensure_schema: If True, auto-create tables/columns (default: True)
        primary_key_type: Type of primary key for auto-created tables
                         ('integer', 'uuid', or PrimaryKeyType enum)
        primary_key_column: Name of primary key column (default: 'id')
        pk_config: Advanced PK configuration (overrides primary_key_type/column)
        **kwargs: Additional arguments for create_engine

    Returns:
        Database instance

    Examples:
        >>> # PostgreSQL with psycopg2 (default Integer PK)
        >>> db = connect('postgresql://localhost/mydb')

        >>> # UUID primary keys
        >>> db = connect('postgresql://localhost/mydb', primary_key_type='uuid')

        >>> # Custom PK column name
        >>> db = connect(
        ...     'postgresql://localhost/mydb',
        ...     primary_key_type='uuid',
        ...     primary_key_column='user_id'
        ... )

        >>> # Read-only mode
        >>> db = connect('postgresql://localhost/mydb', read_only=True)

        >>> # SQLite for testing
        >>> db = connect('sqlite:///:memory:')
    """
    return Database.connect(
        url=url,
        read_only=read_only,
        ensure_schema=ensure_schema,
        primary_key_type=primary_key_type,
        primary_key_column=primary_key_column,
        pk_config=pk_config,
        **kwargs,
    )


__all__ = [
    # Version
    "__version__",
    # Async API
    "async_connect",
    "AsyncDatabase",
    "AsyncTable",
    # Sync API
    "connect",
    "Database",
    "Table",
    # Primary Key Configuration
    "PrimaryKeyType",
    "PrimaryKeyConfig",
    # Exceptions
    "DatasetError",
    "ConnectionError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "ReadOnlyError",
    "TransactionError",
    "ValidationError",
    "SchemaError",
    "QueryError",
    "TypeInferenceError",
]
