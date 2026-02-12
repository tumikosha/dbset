"""Async core API - AsyncDatabase and AsyncTable classes built on SQLAlchemy async."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from sqlalchemy import MetaData, asc, delete, desc, insert, literal_column, select, text, update, func
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .connection import AsyncConnectionPool, create_pool_config
from .exceptions import ReadOnlyError, QueryError, TableNotFoundError, VectorError
from .query import FilterBuilder
from .schema import AsyncSchemaManager
from .types import PrimaryKeyConfig, PrimaryKeyType, TypeInference
from .validators import ReadOnlyValidator
from .vector import (
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
    np = None
    HAS_NUMPY = False


class AsyncDatabase:
    """
    Async database connection - thin wrapper over SQLAlchemy AsyncEngine.

    Provides Pythonic dict-based API for database operations with
    automatic schema management and read-only mode support.

    Examples:
        >>> db = await AsyncDatabase.connect('postgresql+asyncpg://localhost/mydb')
        >>> table = db['users']
        >>> await table.insert({'name': 'John', 'age': 30})
        >>> async for user in table.find(age={'>=': 18}):
        ...     print(user)
        >>> await db.close()
    """

    def __init__(
        self,
        engine: AsyncEngine,
        metadata: MetaData,
        schema_manager: AsyncSchemaManager,
        pool: AsyncConnectionPool,
        read_only: bool = False,
        ensure_schema: bool = True,
        pk_config: PrimaryKeyConfig | None = None,
        text_index_prefix: int = 255,
    ):
        """
        Initialize database connection.

        Args:
            engine: SQLAlchemy AsyncEngine
            metadata: SQLAlchemy MetaData
            schema_manager: AsyncSchemaManager for DDL operations
            pool: AsyncConnectionPool for connection management
            read_only: If True, only SELECT queries allowed
            ensure_schema: If True, auto-create tables/columns
            pk_config: Primary key configuration for auto-created tables
            text_index_prefix: Prefix length for TEXT column indexes (MySQL/MariaDB)
        """
        self._engine = engine
        self._metadata = metadata
        self._schema = schema_manager
        self._pool = pool
        self._read_only = read_only
        self._ensure_schema = ensure_schema
        self._pk_config = pk_config or PrimaryKeyConfig()
        self._text_index_prefix = text_index_prefix
        self._tables: dict[str, 'AsyncTable'] = {}

    @classmethod
    async def connect(
        cls,
        url: str,
        read_only: bool = False,
        ensure_schema: bool = True,
        schema: str | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        primary_key_type: str | PrimaryKeyType = PrimaryKeyType.INTEGER,
        primary_key_column: str = 'id',
        pk_config: PrimaryKeyConfig | None = None,
        text_index_prefix: int = 255,
        **engine_kwargs,
    ) -> 'AsyncDatabase':
        """
        Create async database connection.

        Args:
            url: Database URL (e.g., 'postgresql+asyncpg://user:pass@host/db')
            read_only: If True, only SELECT queries allowed
            ensure_schema: If True, auto-create tables/columns
            schema: Database schema name (optional)
            pool_size: Connection pool size (default: 5)
            max_overflow: Max connections beyond pool_size (default: 10)
            primary_key_type: Type of primary key for auto-created tables
                             ('integer', 'uuid', or PrimaryKeyType enum)
            primary_key_column: Name of primary key column (default: 'id')
            pk_config: Advanced PK configuration (overrides primary_key_type/column)
            text_index_prefix: Prefix length for TEXT column indexes in MySQL/MariaDB (default: 255)
            **engine_kwargs: Additional arguments for create_async_engine

        Returns:
            AsyncDatabase instance

        Examples:
            >>> # Full access with default Integer PK
            >>> db = await AsyncDatabase.connect('postgresql+asyncpg://localhost/mydb')

            >>> # UUID primary keys
            >>> db = await AsyncDatabase.connect(
            ...     'postgresql+asyncpg://localhost/mydb',
            ...     primary_key_type='uuid'
            ... )

            >>> # Custom PK column name
            >>> db = await AsyncDatabase.connect(
            ...     'postgresql+asyncpg://localhost/mydb',
            ...     primary_key_type='uuid',
            ...     primary_key_column='user_id'
            ... )

            >>> # Read-only mode for safety
            >>> db = await AsyncDatabase.connect(
            ...     'postgresql+asyncpg://localhost/mydb',
            ...     read_only=True
            ... )

            >>> # Custom pool settings
            >>> db = await AsyncDatabase.connect(
            ...     'postgresql+asyncpg://localhost/mydb',
            ...     pool_size=10,
            ...     max_overflow=20
            ... )
        """
        # Create engine config
        engine_config = {**engine_kwargs}

        # Only apply pool config for non-SQLite databases
        # (SQLite uses SingletonThreadPool which doesn't accept these params)
        if not url.startswith('sqlite'):
            pool_config = create_pool_config(
                pool_size=pool_size,
                max_overflow=max_overflow,
            )
            engine_config.update(pool_config)

        # Create async engine
        engine = create_async_engine(url, **engine_config)

        # Create metadata
        metadata = MetaData(schema=schema)

        # Create schema manager
        schema_manager = AsyncSchemaManager(engine, metadata, schema)

        # Create connection pool
        pool = AsyncConnectionPool(engine)

        # Create primary key config
        if pk_config is None:
            pk_config = PrimaryKeyConfig(
                pk_type=primary_key_type,
                column_name=primary_key_column
            )

        return cls(
            engine=engine,
            metadata=metadata,
            schema_manager=schema_manager,
            pool=pool,
            read_only=read_only,
            ensure_schema=ensure_schema,
            pk_config=pk_config,
            text_index_prefix=text_index_prefix,
        )

    def __getitem__(self, table_name: str) -> 'AsyncTable':
        """
        Get table by name (dict-like access).

        Args:
            table_name: Name of table

        Returns:
            AsyncTable instance

        Examples:
            >>> users = db['users']
            >>> await users.insert({'name': 'John'})
        """
        # Return cached table if exists
        if table_name in self._tables:
            return self._tables[table_name]

        # Create new table wrapper
        table = AsyncTable(
            db=self,
            name=table_name,
            schema_manager=self._schema,
            pool=self._pool,
            read_only=self._read_only,
            ensure_schema=self._ensure_schema,
            text_index_prefix=self._text_index_prefix,
        )

        # Cache it
        self._tables[table_name] = table

        return table

    async def query(
        self,
        sql: str | Any,
        **params,
    ) -> AsyncIterator[dict]:
        """
        Execute raw SQL query or SQLAlchemy statement.

        Accepts both raw SQL strings and SQLAlchemy select() statements
        for advanced queries.

        Args:
            sql: SQL string or SQLAlchemy statement
            **params: Query parameters for raw SQL

        Yields:
            Rows as dictionaries

        Raises:
            ReadOnlyError: If write query in read-only mode

        Examples:
            >>> # Raw SQL
            >>> async for row in db.query("SELECT * FROM users WHERE age >= :age", age=18):
            ...     print(row)

            >>> # SQLAlchemy statement
            >>> from sqlalchemy import select
            >>> stmt = select(users_table).where(users_table.c.age >= 18)
            >>> async for row in db.query(stmt):
            ...     print(row)
        """
        # Validate read-only mode for raw SQL
        if self._read_only and isinstance(sql, str):
            ReadOnlyValidator.validate_sql(sql)

        async with self._pool.connect() as conn:
            if isinstance(sql, str):
                # Raw SQL string
                result = await conn.execute(sql, params)
            else:
                # SQLAlchemy statement
                result = await conn.execute(sql)

            # Yield rows as dicts
            for row in result:
                yield dict(row._mapping)

    @asynccontextmanager
    async def transaction(self):
        """
        Start explicit transaction context.

        All operations within the context will be committed together
        or rolled back on exception.

        Examples:
            >>> async with db.transaction():
            ...     await db['users'].insert({'name': 'Alice'})
            ...     await db['orders'].insert({'user_id': 1, 'total': 100})
            ...     # Both committed together
        """
        if self._read_only:
            raise ReadOnlyError("Transactions not allowed in read-only mode")

        async with self._pool.acquire() as conn:
            async with conn.begin():
                yield conn

    async def tables(self) -> list[str]:
        """
        Get list of all table names in database.

        Returns:
            List of table names

        Examples:
            >>> tables = await db.tables()
            >>> print(tables)
            ['users', 'orders', 'products']
        """
        return await self._schema.get_table_names()

    async def close(self):
        """
        Close database connection and dispose engine.

        Should be called when shutting down application.

        Examples:
            >>> await db.close()
        """
        await self._pool.close()

    @property
    def read_only(self) -> bool:
        """Check if database is in read-only mode."""
        return self._read_only

    @property
    def engine(self) -> AsyncEngine:
        """Get underlying SQLAlchemy AsyncEngine."""
        return self._engine

    @property
    def metadata(self) -> MetaData:
        """Get SQLAlchemy MetaData."""
        return self._metadata


class AsyncTable:
    """
    Async table wrapper - provides Pythonic dict-based API for table operations.

    Examples:
        >>> table = db['users']
        >>> pk = await table.insert({'name': 'John', 'age': 30})
        >>> async for user in table.find(age={'>=': 18}):
        ...     print(user)
        >>> await table.update({'age': 31}, keys=['name'])
        >>> await table.delete(name='John')
    """

    def __init__(
        self,
        db: AsyncDatabase,
        name: str,
        schema_manager: AsyncSchemaManager,
        pool: AsyncConnectionPool,
        read_only: bool = False,
        ensure_schema: bool = True,
        text_index_prefix: int = 255,
    ):
        """
        Initialize table wrapper.

        Args:
            db: Parent AsyncDatabase
            name: Table name
            schema_manager: AsyncSchemaManager for DDL operations
            pool: AsyncConnectionPool
            read_only: If True, only SELECT queries allowed
            ensure_schema: If True, auto-create table/columns
            text_index_prefix: Prefix length for TEXT column indexes (MySQL/MariaDB)
        """
        self._db = db
        self._name = name
        self._schema = schema_manager
        self._pool = pool
        self._read_only = read_only
        self._ensure_schema = ensure_schema
        self._text_index_prefix = text_index_prefix
        self._table = None  # SQLAlchemy Table (lazy loaded)

    async def _get_table(self):
        """Lazy load SQLAlchemy Table object."""
        if self._table is None:
            self._table = await self._schema.get_table(
                self._name,
                ensure_exists=self._ensure_schema,
            )
        return self._table

    @property
    async def table(self):
        """
        Get underlying SQLAlchemy Table object.

        Provides direct access to SQLAlchemy for advanced queries.

        Returns:
            SQLAlchemy Table object

        Examples:
            >>> sqla_table = await table.table
            >>> stmt = select(sqla_table).where(sqla_table.c.age > 18)
            >>> async for row in db.query(stmt):
            ...     print(row)
        """
        return await self._get_table()

    @property
    def name(self) -> str:
        """Get table name."""
        return self._name

    @property
    def _dialect(self) -> str:
        """Get database dialect name (e.g., 'postgresql', 'sqlite')."""
        return self._db._engine.dialect.name

    async def insert(
        self,
        row: dict[str, Any],
        ensure: bool | None = None,
        types: dict[str, Any] | None = None,
    ) -> Any:
        """
        Insert single row into table.

        Args:
            row: Dictionary of column_name -> value
            ensure: If True, auto-create table/columns (default: from database)
            types: Optional dict of column_name -> SQLAlchemy type for type hints

        Returns:
            Primary key of inserted row

        Raises:
            ReadOnlyError: If in read-only mode

        Examples:
            >>> pk = await table.insert({'name': 'John', 'age': 30})
            >>> print(pk)  # 1 (Integer) or UUID string

            >>> # With type hints
            >>> pk = await table.insert(
            ...     {'price': 99.99},
            ...     types={'price': Float()}
            ... )

            >>> # With custom UUID value
            >>> from uuid import uuid4
            >>> pk = await table.insert({'id': uuid4(), 'name': 'John'})
        """
        if self._read_only:
            raise ReadOnlyError("INSERT not allowed in read-only mode")

        ensure = ensure if ensure is not None else self._ensure_schema

        # Get or create table
        table = await self._schema.get_table(
            self._name,
            ensure_exists=ensure,
            pk_config=self._db._pk_config if ensure else None
        )

        # Generate primary key value if needed (for UUID/CUSTOM types)
        pk_col = self._db._pk_config.column_name
        if pk_col not in row and self._db._pk_config.generator:
            row[pk_col] = self._db._pk_config.generate_value()

        # Infer types and ensure columns exist
        if ensure:
            inferred_types = TypeInference.infer_types_from_row(row, dialect=self._dialect)
            # Override with user-provided types
            if types:
                inferred_types.update(types)
            await self._schema.ensure_columns(table, inferred_types)

            # Clear cached table and reload
            self._table = None
            table = await self._schema.get_table(self._name, ensure_exists=False)

        # Preprocess row (serialize vectors)
        row = self._preprocess_row(row)

        # Insert row
        stmt = insert(table).values(**row)

        async with self._pool.acquire() as conn:
            result = await conn.execute(stmt)
            # For UUID/CUSTOM, return the generated value from row
            # For Integer, return from inserted_primary_key
            if self._db._pk_config.pk_type != PrimaryKeyType.INTEGER:
                return row.get(pk_col)
            else:
                return result.inserted_primary_key[0]

    async def insert_many(
        self,
        rows: list[dict[str, Any]],
        ensure: bool | None = None,
        chunk_size: int = 1000,
    ) -> int:
        """
        Insert multiple rows (batch operation).

        Args:
            rows: List of dictionaries (rows to insert)
            ensure: If True, auto-create table/columns
            chunk_size: Number of rows per batch (default: 1000)

        Returns:
            Number of rows inserted

        Examples:
            >>> rows = [
            ...     {'name': 'John', 'age': 30},
            ...     {'name': 'Jane', 'age': 25},
            ... ]
            >>> count = await table.insert_many(rows)
            >>> print(count)  # 2
        """
        if self._read_only:
            raise ReadOnlyError("INSERT not allowed in read-only mode")

        if not rows:
            return 0

        ensure = ensure if ensure is not None else self._ensure_schema

        # Get or create table
        table = await self._schema.get_table(self._name, ensure_exists=ensure)

        # Infer types from first row and ensure columns
        if ensure:
            inferred_types = TypeInference.infer_types_from_row(rows[0], dialect=self._dialect)
            await self._schema.ensure_columns(table, inferred_types)

            # Clear cached table and reload
            self._table = None
            table = await self._schema.get_table(self._name, ensure_exists=False)

        # Preprocess rows (serialize vectors)
        rows = [self._preprocess_row(r) for r in rows]

        # Insert in chunks
        total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            async with self._pool.acquire() as conn:
                await conn.execute(insert(table), chunk)
                total += len(chunk)

        return total

    async def find(
        self,
        _limit: int | None = None,
        _offset: int = 0,
        _order_by: str | list[str] | None = None,
        **filters,
    ) -> AsyncIterator[dict]:
        """
        Find rows matching filters.

        Args:
            _limit: Maximum number of rows to return
            _offset: Number of rows to skip
            _order_by: Column(s) to order by (prefix with '-' for DESC)
            **filters: Column filters (see FilterBuilder for syntax)

        Yields:
            Rows as dictionaries

        Examples:
            >>> # Find all active users over 18
            >>> async for user in table.find(age={'>=': 18}, status='active'):
            ...     print(user)

            >>> # With ordering and limit
            >>> async for user in table.find(_order_by='-age', _limit=10):
            ...     print(user)

            >>> # Advanced filters
            >>> async for user in table.find(
            ...     email={'like': '%@gmail.com'},
            ...     age={'between': [18, 65]},
            ...     status={'in': ['active', 'pending']}
            ... ):
            ...     print(user)
        """
        table = await self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build SELECT statement
        stmt = select(table)

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Add ordering
        if _order_by:
            order_clauses = FilterBuilder.parse_order_by(table, _order_by)
            stmt = stmt.order_by(*order_clauses)

        # Add pagination
        if _offset:
            stmt = stmt.offset(_offset)
        if _limit:
            stmt = stmt.limit(_limit)

        # Execute and yield rows
        async with self._pool.connect() as conn:
            result = await conn.execute(stmt)
            for row in result:
                yield dict(row._mapping)

    async def find_one(self, **filters) -> dict | None:
        """
        Find single row matching filters.

        Args:
            **filters: Column filters

        Returns:
            Row as dictionary or None if not found

        Examples:
            >>> user = await table.find_one(name='John')
            >>> if user:
            ...     print(user['age'])
        """
        async for row in self.find(_limit=1, **filters):
            return row
        return None

    async def all(self) -> AsyncIterator[dict]:
        """
        Get all rows in table.

        Yields:
            All rows as dictionaries

        Examples:
            >>> async for user in table.all():
            ...     print(user)
        """
        async for row in self.find():
            yield row

    async def count(self, **filters) -> int:
        """
        Count rows matching filters.

        Args:
            **filters: Column filters

        Returns:
            Number of matching rows

        Examples:
            >>> total = await table.count()
            >>> adults = await table.count(age={'>=': 18})
        """
        table = await self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build COUNT statement
        stmt = select(func.count()).select_from(table)

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Execute
        async with self._pool.connect() as conn:
            result = await conn.execute(stmt)
            return result.scalar()

    async def update(
        self,
        row: dict[str, Any],
        keys: list[str] | None = None,
        **filters,
    ) -> int:
        """
        Update rows matching filters.

        Args:
            row: Dictionary of column_name -> new_value
            keys: List of key columns for WHERE clause (alternative to filters)
            **filters: Column filters for WHERE clause

        Returns:
            Number of rows updated

        Raises:
            ReadOnlyError: If in read-only mode

        Examples:
            >>> # Update by filter
            >>> count = await table.update({'age': 31}, name='John')

            >>> # Update by keys
            >>> count = await table.update({'age': 31}, keys=['name'])
        """
        if self._read_only:
            raise ReadOnlyError("UPDATE not allowed in read-only mode")

        table = await self._get_table()

        # Build WHERE clause
        if keys:
            # Filter keys to only include columns that exist in the table
            # This matches dataset behavior: non-existent keys are ignored
            table_cols = {col.name for col in table.columns}
            valid_keys = [k for k in keys if k in table_cols and k in row]

            filters = {k: row[k] for k in valid_keys}
            # Remove keys from update values
            row = {k: v for k, v in row.items() if k not in keys}

            # If row is empty after removing keys, nothing to update
            if not row:
                return 0

        where_clause = FilterBuilder.build(table, filters)

        if where_clause is None:
            raise QueryError("UPDATE requires WHERE clause (filters or keys)")

        # Build UPDATE statement
        row = self._preprocess_row(row)  # serialize vectors for asyncpg
        stmt = update(table).where(where_clause).values(**row)

        # Execute
        async with self._pool.acquire() as conn:
            result = await conn.execute(stmt)
            return result.rowcount

    async def upsert(
        self,
        row: dict[str, Any],
        keys: list[str],
        ensure: bool | None = None,
        types: dict[str, Any] | None = None,
    ) -> Any:
        """
        Insert or update row (upsert).

        Args:
            row: Dictionary of column_name -> value
            keys: List of key columns to check for existing row
            ensure: If True, auto-create table/columns/index (default: from database)
            types: Optional dict of column_name -> SQLAlchemy type for type hints

        Returns:
            Primary key of inserted/updated row

        Examples:
            >>> # Insert if not exists, update if exists
            >>> pk = await table.upsert(
            ...     {'name': 'John', 'age': 31},
            ...     keys=['name']
            ... )

            >>> # With ensure=True (auto-creates table, columns, and index on keys)
            >>> pk = await table.upsert(
            ...     {'email': 'alice@example.com', 'name': 'Alice'},
            ...     keys=['email'],
            ...     ensure=True
            ... )
        """
        if self._read_only:
            raise ReadOnlyError("UPSERT not allowed in read-only mode")

        ensure = ensure if ensure is not None else self._ensure_schema

        # Store original keys for query - they may include non-existent columns
        original_keys = keys

        # Ensure table, columns, and index exist if requested
        if ensure:
            # Get or create table
            table = await self._schema.get_table(
                self._name,
                ensure_exists=True,
                pk_config=self._db._pk_config
            )

            # Infer types and ensure columns exist
            inferred_types = TypeInference.infer_types_from_row(row, dialect=self._dialect)
            if types:
                inferred_types.update(types)
            await self._schema.ensure_columns(table, inferred_types)

            # Filter keys to only include columns that exist in the table (for index creation)
            # After ensure_columns, columns from row + original table columns all exist
            table_cols = {col.name for col in table.columns} | set(row.keys())
            index_keys = [k for k in keys if k in table_cols]

            # Get fresh table from schema (metadata is up-to-date after ensure_columns)
            fresh_table = await self._schema.get_table(self._name, ensure_exists=False)

            # Auto-create index on valid keys for performance, bypassing stale cache
            if index_keys:
                await self.create_index(index_keys, table=fresh_table)

            # Clear cached table
            self._table = None

        # Check if row exists using original keys (with .get() for missing keys)
        # This matches dataset behavior: non-existent keys cause query to fail/return None
        filters = {k: row.get(k) for k in original_keys}
        try:
            existing = await self.find_one(**filters)
        except QueryError:
            # Table/columns don't exist yet - insert
            existing = None

        if existing:
            # Update existing
            await self.update(row, keys=keys, ensure=ensure)
            return existing.get('id')
        else:
            # Insert new
            return await self.insert(row, ensure=ensure, types=types)

    async def upsert_many(
        self,
        rows: list[dict[str, Any]],
        keys: list[str],
        chunk_size: int = 1000,
        ensure: bool | None = None,
        types: dict[str, Any] | None = None,
    ) -> int:
        """
        Upsert multiple rows (batch operation).

        Args:
            rows: List of dictionaries (rows to upsert)
            keys: List of key columns to check for existing rows
            chunk_size: Number of rows per batch (default: 1000)
            ensure: If True, auto-create table/columns/index (default: from database)
            types: Optional dict of column_name -> SQLAlchemy type for type hints

        Returns:
            Number of rows upserted

        Examples:
            >>> rows = [
            ...     {'email': 'alice@example.com', 'name': 'Alice'},
            ...     {'email': 'bob@example.com', 'name': 'Bob'}
            ... ]
            >>> count = await table.upsert_many(rows, keys=['email'], ensure=True)
            >>> print(count)  # 2
        """
        if not rows:
            return 0

        if self._read_only:
            raise ReadOnlyError("UPSERT not allowed in read-only mode")

        ensure = ensure if ensure is not None else self._ensure_schema

        # Ensure table, columns, and index exist if requested (once for all rows)
        if ensure:
            # Get or create table
            table = await self._schema.get_table(
                self._name,
                ensure_exists=True,
                pk_config=self._db._pk_config
            )

            # Infer types from first row and ensure columns exist
            inferred_types = TypeInference.infer_types_from_row(rows[0], dialect=self._dialect)
            if types:
                inferred_types.update(types)
            await self._schema.ensure_columns(table, inferred_types)

            # Filter keys to only include columns that exist in the table (for index creation)
            # After ensure_columns, columns from row + original table columns all exist
            table_cols = {col.name for col in table.columns} | set(rows[0].keys())
            index_keys = [k for k in keys if k in table_cols]

            # Get fresh table from schema (metadata is up-to-date after ensure_columns)
            fresh_table = await self._schema.get_table(self._name, ensure_exists=False)

            # Auto-create index on valid keys for performance (once for batch), bypassing stale cache
            if index_keys:
                await self.create_index(index_keys, table=fresh_table)

            # Clear cached table
            self._table = None

        # Process each row (upsert handles non-existent keys gracefully)
        for row in rows:
            await self.upsert(row, keys=keys, ensure=False, types=types)

        return len(rows)

    async def delete(self, **filters) -> int:
        """
        Delete rows matching filters.

        Args:
            **filters: Column filters for WHERE clause

        Returns:
            Number of rows deleted

        Raises:
            ReadOnlyError: If in read-only mode

        Examples:
            >>> count = await table.delete(name='John')
            >>> count = await table.delete(age={'<': 18})
        """
        if self._read_only:
            raise ReadOnlyError("DELETE not allowed in read-only mode")

        if not filters:
            raise QueryError("DELETE requires WHERE clause (provide filters)")

        table = await self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build DELETE statement
        stmt = delete(table).where(where_clause)

        # Execute
        async with self._pool.acquire() as conn:
            result = await conn.execute(stmt)
            return result.rowcount

    async def distinct(
        self,
        *columns: str,
        **filters,
    ) -> AsyncIterator[dict]:
        """
        Get distinct values for specified columns.

        Args:
            *columns: Column names to select
            **filters: Column filters

        Yields:
            Distinct rows as dictionaries

        Examples:
            >>> # Get distinct statuses
            >>> async for row in table.distinct('status'):
            ...     print(row['status'])

            >>> # Get distinct combinations
            >>> async for row in table.distinct('city', 'country'):
            ...     print(row)
        """
        if not columns:
            raise QueryError("DISTINCT requires at least one column")

        table = await self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build SELECT DISTINCT statement
        selected_columns = [table.c[col] for col in columns]
        stmt = select(*selected_columns).distinct()

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Execute and yield rows
        async with self._pool.connect() as conn:
            result = await conn.execute(stmt)
            for row in result:
                yield dict(row._mapping)

    async def create_index(
        self,
        columns: str | list[str],
        name: str | None = None,
        unique: bool = False,
        table=None,
        **kw,
    ) -> str:
        """
        Create index on table columns for query performance optimization.

        Indexes speed up queries that filter or sort by the indexed columns.
        Creating an existing index is idempotent (returns name without error).

        Args:
            columns: Column name (string) or list of column names for compound index
            name: Custom index name (auto-generated if None following pattern idx_{table}_{col1}_{col2})
            unique: Create unique index (enforces uniqueness constraint)
            **kw: Additional SQLAlchemy Index kwargs (e.g., postgresql_where for partial indexes)

        Returns:
            Index name (either custom or auto-generated)

        Raises:
            ColumnNotFoundError: If any specified column doesn't exist in table
            ValueError: If columns is empty

        Examples:
            >>> # Single column index
            >>> idx_name = await table.create_index('email')
            >>> # Returns: 'idx_tablename_email'

            >>> # Compound index on multiple columns
            >>> idx_name = await table.create_index(['country', 'city'])
            >>> # Returns: 'idx_tablename_country_city'

            >>> # Unique index with custom name
            >>> idx_name = await table.create_index(
            ...     'username',
            ...     name='unique_username',
            ...     unique=True
            ... )

            >>> # Idempotent - creating again returns same name
            >>> idx_name_again = await table.create_index('email')
            >>> assert idx_name == idx_name_again

            >>> # Database-specific features (PostgreSQL partial index)
            >>> from sqlalchemy import text
            >>> idx_name = await table.create_index(
            ...     'email',
            ...     postgresql_where=text("status = 'active'")
            ... )
        """
        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]

        if table is None:
            table = await self._get_table()
        return await self._schema.create_index(
            table, columns, name, unique,
            text_index_prefix=self._text_index_prefix,
            **kw
        )

    async def has_index(self, columns: str | list[str]) -> bool:
        """
        Check if index exists on specified column(s).

        Useful for conditional index creation to avoid unnecessary operations.

        Args:
            columns: Column name (string) or list of column names

        Returns:
            True if matching index exists on these columns, False otherwise

        Examples:
            >>> # Check single column index
            >>> if not await table.has_index('email'):
            ...     await table.create_index('email')

            >>> # Check compound index
            >>> has_compound = await table.has_index(['country', 'city'])

            >>> # Verify index creation
            >>> await table.create_index('email')
            >>> assert await table.has_index('email') is True
        """
        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]

        table = await self._get_table()
        return await self._schema.index_exists(table, columns)

    def _preprocess_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Preprocess a row before insertion, serializing vector values.

        Args:
            row: Dictionary of column_name -> value

        Returns:
            New dictionary with vector values serialized
        """
        result = {}
        for key, value in row.items():
            if HAS_NUMPY and isinstance(value, np.ndarray):
                result[key] = serialize_vector(value, self._dialect)
            elif isinstance(value, list) and TypeInference._is_vector_list(value):
                result[key] = serialize_vector(value, self._dialect)
            else:
                result[key] = value
        return result

    async def find_similar(
        self,
        column: str,
        query_vector: Any,
        metric: str = DistanceMetric.COSINE,
        limit: int = 10,
        threshold: float | None = None,
        include_distance: bool = True,
        **filters,
    ) -> AsyncIterator[dict]:
        """
        Find rows similar to query vector, ordered by distance.

        Args:
            column: Name of the vector column
            query_vector: Query vector (list[float] or numpy array)
            metric: Distance metric ('cosine', 'l2', 'inner_product')
            limit: Maximum number of results
            threshold: Maximum distance threshold (None for no filter)
            include_distance: If True, include '_distance' key in results
            **filters: Additional column filters

        Yields:
            Rows as dictionaries, ordered by distance (closest first).
            Includes '_distance' key if include_distance=True.

        Examples:
            >>> async for row in table.find_similar('embedding', [0.1, 0.2, 0.3]):
            ...     print(row['name'], row['_distance'])
        """
        table = await self._get_table()

        # Normalize query vector to list
        if HAS_NUMPY and isinstance(query_vector, np.ndarray):
            query_vector = query_vector.tolist()

        dialect_name = self._dialect
        dist_expr, order_dir = vector_distance_sql(column, query_vector, metric, dialect_name)

        if dist_expr == "__python_distance__":
            # SQLite fallback: fetch all rows and compute distance in Python
            stmt = select(table)
            where_clause = FilterBuilder.build(table, filters) if filters else None
            if where_clause is not None:
                stmt = stmt.where(where_clause)

            async with self._pool.connect() as conn:
                result = await conn.execute(stmt)
                rows_with_distance = []
                for row in result:
                    row_dict = dict(row._mapping)
                    vec_value = deserialize_vector(row_dict.get(column))
                    if vec_value is None:
                        continue
                    dist = compute_distance(query_vector, vec_value, metric)
                    if threshold is not None and dist > threshold:
                        continue
                    if include_distance:
                        row_dict['_distance'] = dist
                    rows_with_distance.append((dist, row_dict))

                rows_with_distance.sort(key=lambda x: x[0])
                for _, row_dict in rows_with_distance[:limit]:
                    yield row_dict
        else:
            # PostgreSQL/MySQL: use native vector distance
            distance_col = literal_column(dist_expr).label('_distance')
            stmt = select(table, distance_col)

            where_clause = FilterBuilder.build(table, filters) if filters else None
            if where_clause is not None:
                stmt = stmt.where(where_clause)

            if threshold is not None:
                stmt = stmt.where(text(f"{dist_expr} <= :threshold").bindparams(threshold=threshold))

            order_clause = asc(text(dist_expr)) if order_dir == "ASC" else desc(text(dist_expr))
            stmt = stmt.order_by(order_clause).limit(limit)

            async with self._pool.connect() as conn:
                result = await conn.execute(stmt)
                for row in result:
                    row_dict = dict(row._mapping)
                    if not include_distance:
                        row_dict.pop('_distance', None)
                    yield row_dict

    async def create_vector_index(
        self,
        column: str,
        method: str = 'hnsw',
        metric: str = DistanceMetric.COSINE,
        **params,
    ) -> str:
        """
        Create vector similarity search index (PostgreSQL only with pgvector).

        Args:
            column: Vector column name
            method: Index method ('hnsw' or 'ivfflat')
            metric: Distance metric for index ops class
            **params: Additional index parameters (e.g., m=16, ef_construction=64)

        Returns:
            Index name

        Raises:
            VectorError: If dialect does not support vector indexes
        """
        if self._dialect != 'postgresql':
            raise VectorError(
                f"Vector indexes are only supported on PostgreSQL with pgvector, "
                f"current dialect: {self._dialect}"
            )

        ops_map = {
            DistanceMetric.COSINE: 'vector_cosine_ops',
            DistanceMetric.L2: 'vector_l2_ops',
            DistanceMetric.INNER_PRODUCT: 'vector_ip_ops',
        }
        ops_class = ops_map.get(metric, 'vector_cosine_ops')

        index_name = f"idx_{self._name}_{column}_{method}_{metric}"

        table = await self._get_table()
        preparer = self._db._engine.dialect.identifier_preparer
        quoted_table = preparer.quote(table.name)
        quoted_column = preparer.quote(column)

        with_clause = ""
        if params:
            with_parts = [f"{k} = {v}" for k, v in params.items()]
            with_clause = f" WITH ({', '.join(with_parts)})"

        create_sql = (
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {quoted_table} "
            f"USING {method} ({quoted_column} {ops_class}){with_clause}"
        )

        async with self._pool.acquire() as conn:
            from sqlalchemy import DDL
            await conn.execute(DDL(create_sql))

        return index_name
