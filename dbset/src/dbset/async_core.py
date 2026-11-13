"""Async core API - AsyncDatabase and AsyncTable classes built on SQLAlchemy async."""
from __future__ import annotations

from contextlib import aclosing, asynccontextmanager
from typing import Any, AsyncIterator

from sqlalchemy import MetaData, asc, delete, desc, insert, literal_column, select, text, update, func
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .connection import AsyncConnectionPool, create_pool_config, resolve_postgres_driver
from .exceptions import ReadOnlyError, QueryError, TableNotFoundError, VectorError
from .query import FilterBuilder
from .schema import AsyncSchemaManager
from .bulk import existing_keys_select, is_existing, key_batch_size, split_upsert_rows, update_by_keys, update_params
from .types import (
    PrimaryKeyConfig,
    PrimaryKeyType,
    TypeInference,
    infer_types_from_rows,
    insert_batches,
    prepare_insert_rows,
    strip_uncreatable_nulls,
)
from .validators import ReadOnlyValidator
from .vector import (
    LIBSQL,
    LIBSQL_ANN_OVERSAMPLE,
    LIBSQL_INDEX_METRICS,
    LIBSQL_TABLE_INDEXES_SQL,
    DistanceMetric,
    Vector,
    compute_distance,
    deserialize_vector,
    find_libsql_vector_index,
    LIBSQL_ROWID_KEY,
    libsql_candidates_query,
    libsql_create_index_sql,
    libsql_indexed_count_query,
    libsql_rows_query,
    pick_libsql_candidates,
    serialize_vector,
    vector_dialect,
    vector_distance_sql,
)
from .fts import FTSManager
from .hybrid import fuse_results, FusionMethod

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
        skip_null_columns: bool = True,
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
        self._skip_null_columns = skip_null_columns
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
        skip_null_columns: bool = True,
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

        # Create async engine (a postgresql:// URL without a driver gets an installed async one)
        engine = create_async_engine(resolve_postgres_driver(url, is_async=True), **engine_config)

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
            skip_null_columns=skip_null_columns,
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
            skip_null_columns=self._skip_null_columns,
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

        The result set is fetched in full before the first row is yielded, so
        the connection is released before the caller starts consuming.

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

        # Fetch with the connection held, yield after it is released (see find()).
        async with self._pool.connect() as conn:
            if isinstance(sql, str):
                # Raw SQL string
                result = await conn.execute(sql, params)
            else:
                # SQLAlchemy statement
                result = await conn.execute(sql)

            rows = [dict(row._mapping) for row in result]

        for row in rows:
            yield row

    @asynccontextmanager
    async def transaction(self):
        """
        Start explicit transaction context.

        Everything inside the block runs on one connection and commits together,
        or rolls back if the block raises - statements executed on the yielded
        connection and table methods called inside it alike. Work started in a
        task spawned inside the block joins the same connection, so keep those
        statements sequential: a connection runs one statement at a time and
        drivers differ in how they react to an overlapping one.

        A statement that fails inside a nested operation undoes only that
        operation where the backend has real savepoints (PostgreSQL, MySQL), so
        the rest of the block can carry on. sqlite has no usable savepoints -
        pysqlite releases one as a commit - so there the operation joins the
        transaction, and the driver's own behaviour after the error decides.

        Examples:
            >>> async with db.transaction():
            ...     await db['users'].insert({'name': 'Alice'})
            ...     await db['orders'].insert({'user_id': 1, 'total': 100})
            ...     # Both committed together
        """
        if self._read_only:
            raise ReadOnlyError("Transactions not allowed in read-only mode")

        # pool.acquire() already opens the transaction; ambient=True publishes
        # the connection so table operations inside the block join it instead of
        # opening their own checkout and committing separately.
        async with self._pool.acquire(ambient=True) as conn:
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
        skip_null_columns: bool = True,
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
        self._skip_null_columns = skip_null_columns
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

    @property
    def _vector_dialect(self) -> str:
        """Dialect name for vector storage and search: 'libsql' for libSQL, else _dialect."""
        return vector_dialect(self._db._engine.dialect)

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

        # Strip None-valued columns that don't exist (skip_null_columns enabled)
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)

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

        # Get or create table (with the configured primary key, as insert() does)
        table = await self._schema.get_table(
            self._name,
            ensure_exists=ensure,
            pk_config=self._db._pk_config if ensure else None
        )

        # Generated primary keys; None-only new columns dropped (skip_null_columns)
        rows = prepare_insert_rows(rows, table, self._db._pk_config, self._skip_null_columns)

        # Infer types from all rows and ensure columns
        if ensure:
            inferred_types = infer_types_from_rows(rows, dialect=self._dialect)
            await self._schema.ensure_columns(table, inferred_types)

            # Clear cached table and reload
            self._table = None
            table = await self._schema.get_table(self._name, ensure_exists=False)

        # Preprocess rows (serialize vectors)
        rows = [self._preprocess_row(r) for r in rows]

        # One transaction: all rows or none. One executemany per run of rows with the same keys.
        total = 0
        async with self._pool.acquire() as conn:
            for chunk in insert_batches(rows, chunk_size):
                await conn.execute(insert(table), chunk)
                total += len(chunk)

        return total

    async def find(
        self,
        _limit: int | None = None,
        _offset: int = 0,
        _order_by: str | list[str] | None = None,
        _stream: bool = False,
        **filters,
    ) -> AsyncIterator[dict]:
        """
        Find rows matching filters.

        The result set is fetched in full before the first row is yielded, so
        the connection is released before the caller starts consuming.

        Pass ``_stream=True`` to iterate the result set from the database
        instead: rows arrive as they are read and the connection stays checked
        out until the iteration finishes, which on a single-connection pool
        (sqlite ":memory:") also blocks every other caller until then. Close the
        generator (``aclosing``) if you abandon it early.

        Args:
            _limit: Maximum number of rows to return
            _offset: Number of rows to skip
            _order_by: Column(s) to order by (prefix with '-' for DESC)
            _stream: Iterate from the database instead of fetching everything
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
        where_clause = FilterBuilder.build(table, filters, dialect=self._dialect)

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

        if _stream:
            # Explicitly asked for: the connection is held for the whole
            # iteration, which is what streaming costs.
            # acquire(), not connect(): a server-side cursor needs a
            # transaction, and a non-transactional scope runs in AUTOCOMMIT.
            async with self._pool.acquire() as conn:
                result = await conn.stream(stmt)
                async for row in result:
                    yield dict(row._mapping)
            return

        # Rows are materialized while the connection is held, then yielded after
        # it is released: SQLAlchemy buffers the result set anyway, so keeping the
        # connection checked out across yields buys nothing and leaves it held for
        # as long as the caller takes to consume - or until the generator is
        # finalized, if the caller abandons it mid-iteration.
        async with self._pool.connect() as conn:
            result = await conn.execute(stmt)
            rows = [dict(row._mapping) for row in result]

        for row in rows:
            yield row

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
        # aclosing() closes the generator here instead of leaving it suspended
        # for the event loop to finalize in a separate task later. find() no
        # longer holds a connection across its yields, so this is hygiene rather
        # than a fix, but an abandoned generator is still a loose end.
        async with aclosing(self.find(_limit=1, **filters)) as rows:
            async for row in rows:
                return row
        return None

    async def all(self, _stream: bool = False) -> AsyncIterator[dict]:
        """
        Get all rows in table.

        The result set is fetched in full before the first row is yielded, so
        the connection is released before the caller starts consuming.

        Pass ``_stream=True`` to iterate the result set from the database
        instead: rows arrive as they are read and the connection stays checked
        out until the iteration finishes, which on a single-connection pool
        (sqlite ":memory:") also blocks every other caller until then. Close the
        generator (``aclosing``) if you abandon it early.

        Args:
            _stream: Iterate from the database instead of fetching everything

        Yields:
            All rows as dictionaries

        Examples:
            >>> async for user in table.all():
            ...     print(user)
        """
        async for row in self.find(_stream=_stream):
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
        where_clause = FilterBuilder.build(table, filters, dialect=self._dialect)

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

        # Strip None-valued columns that don't exist (skip_null_columns enabled)
        if self._skip_null_columns:
            row = strip_uncreatable_nulls(row, table)

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

        where_clause = FilterBuilder.build(table, filters, dialect=self._dialect)

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

            # Strip None-valued columns that don't exist (skip_null_columns enabled)
            if self._skip_null_columns:
                row = strip_uncreatable_nulls(row, table)

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

            # None-only new columns dropped (skip_null_columns); primary keys only for new rows, below
            rows = prepare_insert_rows(
                rows, table, self._db._pk_config, self._skip_null_columns, generate_keys=False
            )

            # Infer types from all rows and ensure columns exist
            inferred_types = infer_types_from_rows(rows, dialect=self._dialect)
            if types:
                inferred_types.update(types)
            await self._schema.ensure_columns(table, inferred_types)

            # Filter keys to only include columns that exist in the table (for index creation)
            # After ensure_columns, columns from the rows + original table columns all exist
            table_cols = {col.name for col in table.columns} | set(inferred_types)
            index_keys = [k for k in keys if k in table_cols]

            # Get fresh table from schema (metadata is up-to-date after ensure_columns)
            fresh_table = await self._schema.get_table(self._name, ensure_exists=False)

            # Auto-create index on valid keys for performance (once for batch), bypassing stale cache
            if index_keys:
                await self.create_index(index_keys, table=fresh_table)

            # Clear cached table
            self._table = None

        try:
            table = await self._schema.get_table(self._name, ensure_exists=False)
        except TableNotFoundError:
            table = None  # no table and ensure=False: every row goes row by row, as before
        bulk, rowwise = split_upsert_rows(rows, keys, table) if table is not None else ({}, list(rows))

        # One transaction; row-by-row upserts inside join it (ambient connection)
        async with self._pool.acquire(ambient=True) as conn:
            if bulk:
                values = list(bulk)
                found: set[tuple] = set()
                found_str: set[tuple] = set()
                step = key_batch_size(keys)
                for i in range(0, len(values), step):
                    result = await conn.execute(existing_keys_select(table, keys, values[i:i + step]))
                    for r in result:
                        found.add(tuple(r))
                        found_str.add(tuple(map(str, r)))
                new = [row for v, row in bulk.items() if not is_existing(v, found, found_str)]
                old = [row for v, row in bulk.items() if is_existing(v, found, found_str)]

                if new:
                    new = prepare_insert_rows(new, table, self._db._pk_config, skip_null_columns=False)
                    new = [self._preprocess_row(r) for r in new]
                    for chunk in insert_batches(new, chunk_size):
                        await conn.execute(insert(table), chunk)

                params = [p for p in (update_params(self._preprocess_row(r), keys) for r in old) if p]
                for chunk in insert_batches(params, chunk_size):
                    await conn.execute(update_by_keys(table, keys, chunk[0]), chunk)

            for row in rowwise:
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
        where_clause = FilterBuilder.build(table, filters, dialect=self._dialect)

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
        where_clause = FilterBuilder.build(table, filters, dialect=self._dialect)

        # Build SELECT DISTINCT statement
        selected_columns = [table.c[col] for col in columns]
        stmt = select(*selected_columns).distinct()

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Fetch with the connection held, yield after it is released (see find()).
        async with self._pool.connect() as conn:
            result = await conn.execute(stmt)
            rows = [dict(row._mapping) for row in result]

        for row in rows:
            yield row

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
                result[key] = serialize_vector(value, self._vector_dialect)
            elif isinstance(value, list) and TypeInference._is_vector_list(value):
                result[key] = serialize_vector(value, self._vector_dialect)
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

        The result set is fetched in full before the first row is yielded, so
        the connection is released before the caller starts consuming.

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

        dialect_name = self._vector_dialect
        column_sql = column
        if dialect_name == LIBSQL:
            preparer = self._db._engine.dialect.identifier_preparer
            column_sql = preparer.quote(column)
            rows = await self._libsql_ann_search(
                table, column, query_vector, metric, limit, threshold, filters, preparer
            )
            if rows is not None:
                for row_dict in rows:
                    if not include_distance:
                        row_dict.pop('_distance', None)
                    yield row_dict
                return

        dist_expr, order_dir = vector_distance_sql(column_sql, query_vector, metric, dialect_name)

        if dist_expr == "__python_distance__":
            # SQLite fallback: fetch all rows and compute distance in Python
            stmt = select(table)
            where_clause = FilterBuilder.build(table, filters, dialect=self._dialect) if filters else None
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

            where_clause = FilterBuilder.build(table, filters, dialect=self._dialect) if filters else None
            if where_clause is not None:
                stmt = stmt.where(where_clause)

            if dialect_name == LIBSQL:
                # libSQL's vector_distance_* raise on NULL; rows without a vector are skipped
                # (as the Python fallback skips them).
                stmt = stmt.where(literal_column(column_sql).isnot(None))

            if threshold is not None:
                stmt = stmt.where(text(f"{dist_expr} <= :threshold").bindparams(threshold=threshold))

            order_clause = asc(text(dist_expr)) if order_dir == "ASC" else desc(text(dist_expr))
            stmt = stmt.order_by(order_clause).limit(limit)

            # Fetch with the connection held, yield after it is released (see find()).
            async with self._pool.connect() as conn:
                result = await conn.execute(stmt)
                rows = []
                for row in result:
                    row_dict = dict(row._mapping)
                    if not include_distance:
                        row_dict.pop('_distance', None)
                    rows.append(row_dict)

            for row_dict in rows:
                yield row_dict

    async def create_vector_index(
        self,
        column: str,
        method: str | None = None,
        metric: str = DistanceMetric.COSINE,
        where: dict[str, Any] | None = None,
        **params,
    ) -> str:
        """
        Create vector similarity search index (PostgreSQL with pgvector, or libSQL).

        Args:
            column: Vector column name
            method: Index method. PostgreSQL: 'hnsw' (default) or 'ivfflat';
                    libSQL: 'diskann' (default, the only one)
            metric: Distance metric for index ops class. libSQL: 'cosine' or 'l2'
            where: libSQL only: a partial index over the rows where every
                   ``column == value`` holds, e.g. ``{'lang': 'en'}``. find_similar
                   uses it for searches whose equality filters include these.
            **params: Additional index parameters. PostgreSQL: e.g. m=16, ef_construction=64;
                      libSQL: e.g. max_neighbors=64, compress_neighbors='float8'

        Returns:
            Index name

        Raises:
            VectorError: If dialect does not support vector indexes
        """
        if self._vector_dialect == LIBSQL:
            return await self._create_libsql_vector_index(column, method, metric, params, where)

        if where:
            raise VectorError("Partial vector indexes (where=...) are supported on libSQL only")

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
        method = method or 'hnsw'

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

    async def _create_libsql_vector_index(
        self, column: str, method: str | None, metric: str, params: dict, where: dict | None = None,
    ) -> str:
        """CREATE INDEX ... (libsql_vector_idx(column, 'metric=...')) - a DiskANN index."""
        if method not in (None, 'diskann'):
            raise VectorError(f"libSQL vector indexes use DiskANN; method {method!r} is not supported")
        if metric not in LIBSQL_INDEX_METRICS:
            raise VectorError(
                f"libSQL vector indexes support metrics {sorted(LIBSQL_INDEX_METRICS)}, got {metric!r}"
            )
        table = await self._get_table()
        preparer = self._db._engine.dialect.identifier_preparer
        try:
            index_name, create_sql = libsql_create_index_sql(table, column, metric, params, preparer, where)
        except ValueError as e:
            raise VectorError(str(e)) from e
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(text(create_sql))
        except Exception as e:
            hint = ""
            if "unexpected vector column type" in str(e):
                hint = (
                    " The column must be declared F32_BLOB(dim): dbset declares it so for vectors it "
                    "creates on libSQL; a column created by plain SQLite (TEXT) cannot be indexed."
                )
            raise VectorError(f"Failed to create libSQL vector index on {table.name}.{column}: {e}.{hint}") from e
        return index_name

    async def _libsql_ann_search(self, table, column, query_vector, metric, limit, threshold, filters, preparer):
        """Nearest rows through a DiskANN index, ordered like the exact scan; None when no
        index serves the search or only an exact scan can give a complete answer.

        vector_top_k picks its candidates before any WHERE. The index whose conditions
        (partial index) cover the most equality filters is used; the other filters run on
        limit * 10, * 100, * 1000 candidates until `limit` rows pass. vector_top_k yields at
        most the index's search_l candidates (200 by default) whatever k is asked: fewer
        candidates than k means the cap was hit - unless they are every row the index holds,
        the caller then scans exactly. The same check guards searches without filters, so a
        `limit` above search_l is not cut short either.
        """
        if limit < 0:
            return None  # SQLite reads LIMIT -1 as "no limit": the exact scan does that
        if limit == 0:
            return []
        async with self._pool.connect() as conn:
            result = await conn.execute(LIBSQL_TABLE_INDEXES_SQL, {'table_name': table.name})
            index_rows = result.fetchall()
        plan = find_libsql_vector_index(index_rows, column, metric, filters)
        if plan is None:
            return None
        index_name, conditions, residual = plan
        where_clause = FilterBuilder.build(table, residual, dialect=self._dialect) if residual else None
        factors = LIBSQL_ANN_OVERSAMPLE if where_clause is not None else (1,) + LIBSQL_ANN_OVERSAMPLE
        indexed = None
        for factor in factors:
            k = limit * factor
            async with self._pool.connect() as conn:
                stmt = libsql_candidates_query(
                    table, column, query_vector, metric, index_name, k, where_clause, preparer
                )
                result = await conn.execute(stmt)
                candidates = result.fetchall()
                complete, chosen = pick_libsql_candidates(candidates, limit, threshold)
                if not complete and len(candidates) < k:
                    if indexed is None:
                        result = await conn.execute(libsql_indexed_count_query(table, column, conditions))
                        indexed = result.scalar()
                    if len(candidates) < indexed:
                        return None  # capped by search_l: only an exact scan is complete
                    complete = True  # every indexed row was a candidate
                if not complete:
                    continue
                if not chosen:
                    return []
                result = await conn.execute(libsql_rows_query(table, [rowid for rowid, _ in chosen], preparer))
                by_rowid = {}
                for row in result:
                    row_dict = dict(row._mapping)
                    by_rowid[row_dict.pop(LIBSQL_ROWID_KEY)] = row_dict
            rows = []
            for rowid, distance in chosen:
                row_dict = by_rowid.get(rowid)
                if row_dict is not None:  # deleted in between
                    row_dict['_distance'] = distance
                    rows.append(row_dict)
            return rows
        return None

    async def create_fts_index(
        self,
        columns: str | list[str],
        language: str = 'english',
    ) -> str:
        """
        Create full-text search index for BM25 text search.

        Args:
            columns: Column name(s) to index. Supports JSONB paths like 'metadata.field'
            language: Language for stemming (default: 'english')

        Returns:
            Index name

        Examples:
            >>> # Single column
            >>> await table.create_fts_index('title')

            >>> # Multiple columns including JSONB
            >>> await table.create_fts_index(['title', 'metadata.description'])
        """
        if isinstance(columns, str):
            columns = [columns]

        pk_col = self._db._pk_config.column_name

        async with self._pool.acquire() as conn:
            return await FTSManager.create_fts_index_async(
                conn,
                self._name,
                columns,
                self._dialect,
                language=language,
                pk_column=pk_col,
            )

    async def has_fts_index(self, columns: str | list[str]) -> bool:
        """
        Check if FTS index exists for specified columns.

        Args:
            columns: Column name(s) to check

        Returns:
            True if FTS index exists

        Examples:
            >>> if not await table.has_fts_index('title'):
            ...     await table.create_fts_index('title')
        """
        if isinstance(columns, str):
            columns = [columns]

        async with self._pool.connect() as conn:
            return await FTSManager.has_fts_index_async(
                conn,
                self._name,
                columns,
                self._dialect,
            )

    async def hybrid_search(
        self,
        vector_column: str,
        text_column: str | list[str],
        query_vector: Any,
        query_text: str,
        *,
        limit: int = 10,
        offset: int = 0,
        vector_weight: float = 0.5,
        fusion: str = 'rrf',
        rrf_k: int = 60,
        ensure: bool = False,
        language: str = 'english',
        vector_metric: str = DistanceMetric.COSINE,
        include_scores: bool = True,
        text_query_mode: str = 'and',
        vector_max_distance: float | None = None,
        **filters,
    ) -> AsyncIterator[dict]:
        """
        Hybrid search combining BM25 text search and vector similarity.

        Uses Reciprocal Rank Fusion (RRF) or Linear weighted combination
        to merge results from both search methods.

        Args:
            vector_column: Name of the vector embedding column
            text_column: Text column(s) for BM25 search. Supports JSONB paths.
            query_vector: Query embedding vector (list or numpy array)
            query_text: Text query for BM25 search
            limit: Maximum number of results (default: 10)
            offset: Number of results to skip for pagination (default: 0)
            vector_weight: Weight for vector results in linear fusion (0.0-1.0)
            fusion: Fusion method - 'rrf' (default) or 'linear'
            rrf_k: K parameter for RRF (default: 60)
            ensure: If True, create FTS index if not exists
            language: Language for text stemming (default: 'english')
            vector_metric: Distance metric for vector search
            include_scores: Include '_score' field in results (default: True)
            vector_max_distance: drop vector candidates farther than this
                (same metric as vector_metric, e.g. cosine distance 0..2).
                Without it the vector branch ALWAYS returns its K nearest
                rows — pure noise when nothing in the (filtered) corpus is
                actually similar. None (default) keeps the old behaviour.
            text_query_mode: BM25 word semantics — 'and' (default): a document
                must contain ALL query words; 'or': ANY word matches and
                fuller matches simply rank higher. Use 'or' for multilingual
                corpora where extra query words should broaden, not narrow,
                the text branch. (postgresql/sqlite; no-op on mysql)
            **filters: Additional column filters (supports JSONB dot notation)

        Yields:
            Rows as dictionaries with '_score' field (if include_scores=True)

        Examples:
            >>> # Simple hybrid search
            >>> async for row in table.hybrid_search(
            ...     'embedding', 'title',
            ...     query_vector=[0.1, 0.2, 0.3],
            ...     query_text='machine learning',
            ...     limit=10
            ... ):
            ...     print(row['title'], row['_score'])

            >>> # With JSONB columns and filters
            >>> async for row in table.hybrid_search(
            ...     'embedding',
            ...     ['title', 'metadata.description'],
            ...     query_vector=[0.1, 0.2, 0.3],
            ...     query_text='python tutorial',
            ...     ensure=True,
            ...     **{'metadata.category': 'programming'}
            ... ):
            ...     print(row)
        """
        # Normalize text_column to list
        if isinstance(text_column, str):
            text_columns = [text_column]
        else:
            text_columns = list(text_column)

        # Normalize query vector
        if HAS_NUMPY and isinstance(query_vector, np.ndarray):
            query_vector = query_vector.tolist()

        pk_col = self._db._pk_config.column_name

        # Ensure FTS index exists if requested
        if ensure:
            has_fts = await self.has_fts_index(text_columns)
            if not has_fts:
                await self.create_fts_index(text_columns, language=language)

        # Fetch more results than needed for fusion (we need overlap)
        search_limit = (offset + limit) * 3

        # Execute BM25 search
        async with self._pool.connect() as conn:
            # Filters go INSIDE the FTS query too — otherwise the BM25 branch
            # floods the fusion with rows the row-fetch below will drop, and
            # filtered searches come back near-empty / vector-only.
            bm25_results = await FTSManager.fts_search_async(
                conn,
                self._name,
                text_columns,
                query_text,
                self._dialect,
                limit=search_limit,
                language=language,
                pk_column=pk_col,
                query_mode=text_query_mode,
                filters=filters,
            )

        # Execute vector search - collect results
        vector_results = []
        async for row in self.find_similar(
            vector_column,
            query_vector,
            metric=vector_metric,
            limit=search_limit,
            include_distance=True,
            **filters,
        ):
            doc_id = row.get(pk_col)
            distance = row.get('_distance', 0.0)
            if vector_max_distance is not None and distance > vector_max_distance:
                continue  # too dissimilar — noise, not a match
            vector_results.append((doc_id, distance))

        # Fuse results
        fused = fuse_results(
            vector_results,
            bm25_results,
            method=fusion,
            vector_weight=vector_weight,
            rrf_k=rrf_k,
        )

        # Fetch full rows for top results (with offset for pagination)
        top_ids = [doc_id for doc_id, _ in fused[offset:offset + limit]]
        scores = {doc_id: score for doc_id, score in fused[offset:offset + limit]}

        if not top_ids:
            return

        # Fetch rows by IDs (applying filters to ensure consistency)
        yielded_count = 0

        for doc_id in top_ids:
            if yielded_count >= limit:
                break
            # Apply filters when fetching to ensure consistency
            row = await self.find_one(**{pk_col: doc_id, **filters})
            if row:
                if include_scores:
                    row['_score'] = scores.get(doc_id, 0.0)
                yield row
                yielded_count += 1
