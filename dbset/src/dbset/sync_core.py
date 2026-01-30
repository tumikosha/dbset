"""Sync core API - Database and Table classes built on SQLAlchemy sync."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import Engine, MetaData, create_engine, delete, func, insert, select, update

from .connection import SyncConnectionPool, create_pool_config
from .exceptions import QueryError, ReadOnlyError, TableNotFoundError
from .query import FilterBuilder
from .schema import SyncSchemaManager
from .types import PrimaryKeyConfig, PrimaryKeyType, TypeInference
from .validators import ReadOnlyValidator


class Database:
    """
    Sync database connection - thin wrapper over SQLAlchemy Engine.

    Provides Pythonic dict-based API for database operations with
    automatic schema management and read-only mode support.

    Examples:
        >>> db = Database.connect('postgresql://localhost/mydb')
        >>> table = db['users']
        >>> table.insert({'name': 'John', 'age': 30})
        >>> for user in table.find(age={'>=': 18}):
        ...     print(user)
        >>> db.close()
    """

    def __init__(
        self,
        engine: Engine,
        metadata: MetaData,
        schema_manager: SyncSchemaManager,
        pool: SyncConnectionPool,
        read_only: bool = False,
        ensure_schema: bool = True,
        pk_config: PrimaryKeyConfig | None = None,
        text_index_prefix: int = 255,
    ):
        """
        Initialize database connection.

        Args:
            engine: SQLAlchemy Engine
            metadata: SQLAlchemy MetaData
            schema_manager: SyncSchemaManager for DDL operations
            pool: SyncConnectionPool for connection management
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
        self._tables: dict[str, 'Table'] = {}

    @classmethod
    def connect(
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
    ) -> 'Database':
        """
        Create sync database connection.

        Args:
            url: Database URL (e.g., 'postgresql://user:pass@host/db')
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
            **engine_kwargs: Additional arguments for create_engine

        Returns:
            Database instance

        Examples:
            >>> # Full access with default Integer PK
            >>> db = Database.connect('postgresql://localhost/mydb')

            >>> # UUID primary keys
            >>> db = Database.connect(
            ...     'postgresql://localhost/mydb',
            ...     primary_key_type='uuid'
            ... )

            >>> # Custom PK column name
            >>> db = Database.connect(
            ...     'postgresql://localhost/mydb',
            ...     primary_key_type='uuid',
            ...     primary_key_column='user_id'
            ... )

            >>> # Read-only mode for safety
            >>> db = Database.connect(
            ...     'postgresql://localhost/mydb',
            ...     read_only=True
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

        # Create engine
        engine = create_engine(url, **engine_config)

        # Create metadata
        metadata = MetaData(schema=schema)

        # Create schema manager
        schema_manager = SyncSchemaManager(engine, metadata, schema)

        # Create connection pool
        pool = SyncConnectionPool(engine)

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

    def __getitem__(self, table_name: str) -> 'Table':
        """
        Get table by name (dict-like access).

        Args:
            table_name: Name of table

        Returns:
            Table instance

        Examples:
            >>> users = db['users']
            >>> users.insert({'name': 'John'})
        """
        # Return cached table if exists
        if table_name in self._tables:
            return self._tables[table_name]

        # Create new table wrapper
        table = Table(
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

    def query(
        self,
        sql: str | Any,
        **params,
    ) -> Iterator[dict]:
        """
        Execute raw SQL query or SQLAlchemy statement.

        Args:
            sql: SQL string or SQLAlchemy statement
            **params: Query parameters for raw SQL

        Yields:
            Rows as dictionaries

        Raises:
            ReadOnlyError: If write query in read-only mode

        Examples:
            >>> # Raw SQL
            >>> for row in db.query("SELECT * FROM users WHERE age >= :age", age=18):
            ...     print(row)

            >>> # SQLAlchemy statement
            >>> from sqlalchemy import select
            >>> stmt = select(users_table).where(users_table.c.age >= 18)
            >>> for row in db.query(stmt):
            ...     print(row)
        """
        # Validate read-only mode for raw SQL
        if self._read_only and isinstance(sql, str):
            ReadOnlyValidator.validate_sql(sql)

        with self._pool.connect() as conn:
            if isinstance(sql, str):
                # Raw SQL string
                result = conn.execute(sql, params)
            else:
                # SQLAlchemy statement
                result = conn.execute(sql)

            # Yield rows as dicts
            for row in result:
                yield dict(row._mapping)

    @contextmanager
    def transaction(self):
        """
        Start explicit transaction context.

        Examples:
            >>> with db.transaction():
            ...     db['users'].insert({'name': 'Alice'})
            ...     db['orders'].insert({'user_id': 1, 'total': 100})
        """
        if self._read_only:
            raise ReadOnlyError("Transactions not allowed in read-only mode")

        with self._pool.acquire() as conn:
            with conn.begin():
                yield conn

    @property
    def tables(self) -> list[str]:
        """
        Get list of all table names in database.

        Returns:
            List of table names

        Examples:
            >>> tables = db.tables
            >>> print(tables)
            ['users', 'orders', 'products']
        """
        return self._schema.get_table_names()

    def close(self):
        """Close database connection and dispose engine."""
        self._pool.close()

    @property
    def read_only(self) -> bool:
        """Check if database is in read-only mode."""
        return self._read_only

    @property
    def engine(self) -> Engine:
        """Get underlying SQLAlchemy Engine."""
        return self._engine

    @property
    def metadata(self) -> MetaData:
        """Get SQLAlchemy MetaData."""
        return self._metadata


class Table:
    """
    Sync table wrapper - provides Pythonic dict-based API for table operations.

    Examples:
        >>> table = db['users']
        >>> pk = table.insert({'name': 'John', 'age': 30})
        >>> for user in table.find(age={'>=': 18}):
        ...     print(user)
        >>> table.update({'age': 31}, keys=['name'])
        >>> table.delete(name='John')
    """

    def __init__(
        self,
        db: Database,
        name: str,
        schema_manager: SyncSchemaManager,
        pool: SyncConnectionPool,
        read_only: bool = False,
        ensure_schema: bool = True,
        text_index_prefix: int = 255,
    ):
        """Initialize table wrapper."""
        self._db = db
        self._name = name
        self._schema = schema_manager
        self._pool = pool
        self._read_only = read_only
        self._ensure_schema = ensure_schema
        self._text_index_prefix = text_index_prefix
        self._table = None  # SQLAlchemy Table (lazy loaded)

    def _get_table(self):
        """Lazy load SQLAlchemy Table object."""
        if self._table is None:
            self._table = self._schema.get_table(
                self._name,
                ensure_exists=self._ensure_schema,
            )
        return self._table

    @property
    def table(self):
        """
        Get underlying SQLAlchemy Table object.

        Returns:
            SQLAlchemy Table object

        Examples:
            >>> sqla_table = table.table
            >>> stmt = select(sqla_table).where(sqla_table.c.age > 18)
            >>> for row in db.query(stmt):
            ...     print(row)
        """
        return self._get_table()

    @property
    def name(self) -> str:
        """Get table name."""
        return self._name

    @property
    def _dialect(self) -> str:
        """Get database dialect name (e.g., 'postgresql', 'sqlite')."""
        return self._db._engine.dialect.name

    def insert(
        self,
        row: dict[str, Any],
        ensure: bool | None = None,
        types: dict[str, Any] | None = None,
    ) -> Any:
        """
        Insert single row into table.

        Args:
            row: Dictionary of column_name -> value
            ensure: If True, auto-create table/columns
            types: Optional dict of column_name -> SQLAlchemy type

        Returns:
            Primary key of inserted row

        Raises:
            ReadOnlyError: If in read-only mode

        Examples:
            >>> pk = table.insert({'name': 'John', 'age': 30})
            >>> print(pk)  # 1 (Integer) or UUID string

            >>> # With custom UUID value
            >>> from uuid import uuid4
            >>> pk = table.insert({'id': uuid4(), 'name': 'John'})
        """
        if self._read_only:
            raise ReadOnlyError("INSERT not allowed in read-only mode")

        ensure = ensure if ensure is not None else self._ensure_schema

        # Get or create table
        table = self._schema.get_table(
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
            if types:
                inferred_types.update(types)
            self._schema.ensure_columns(table, inferred_types)

            # Clear cached table and reload
            self._table = None
            table = self._schema.get_table(self._name, ensure_exists=False)

        # Insert row
        stmt = insert(table).values(**row)

        with self._pool.acquire() as conn:
            result = conn.execute(stmt)
            # For UUID/CUSTOM, return the generated value from row
            # For Integer, return from inserted_primary_key
            if self._db._pk_config.pk_type != PrimaryKeyType.INTEGER:
                return row.get(pk_col)
            else:
                return result.inserted_primary_key[0]

    def insert_many(
        self,
        rows: list[dict[str, Any]],
        ensure: bool | None = None,
        chunk_size: int = 1000,
    ) -> int:
        """
        Insert multiple rows (batch operation).

        Args:
            rows: List of dictionaries
            ensure: If True, auto-create table/columns
            chunk_size: Number of rows per batch

        Returns:
            Number of rows inserted

        Examples:
            >>> rows = [
            ...     {'name': 'John', 'age': 30},
            ...     {'name': 'Jane', 'age': 25},
            ... ]
            >>> count = table.insert_many(rows)
        """
        if self._read_only:
            raise ReadOnlyError("INSERT not allowed in read-only mode")

        if not rows:
            return 0

        ensure = ensure if ensure is not None else self._ensure_schema

        # Get or create table
        table = self._schema.get_table(self._name, ensure_exists=ensure)

        # Infer types and ensure columns
        if ensure:
            inferred_types = TypeInference.infer_types_from_row(rows[0], dialect=self._dialect)
            self._schema.ensure_columns(table, inferred_types)

            # Clear cached table and reload
            self._table = None
            table = self._schema.get_table(self._name, ensure_exists=False)

        # Insert in chunks
        total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            with self._pool.acquire() as conn:
                conn.execute(insert(table), chunk)
                total += len(chunk)

        return total

    def find(
        self,
        _limit: int | None = None,
        _offset: int = 0,
        _order_by: str | list[str] | None = None,
        **filters,
    ) -> Iterator[dict]:
        """
        Find rows matching filters.

        Args:
            _limit: Maximum number of rows
            _offset: Number of rows to skip
            _order_by: Column(s) to order by
            **filters: Column filters

        Yields:
            Rows as dictionaries

        Examples:
            >>> for user in table.find(age={'>=': 18}, status='active'):
            ...     print(user)
        """
        table = self._get_table()

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
        with self._pool.connect() as conn:
            result = conn.execute(stmt)
            for row in result:
                yield dict(row._mapping)

    def find_one(self, **filters) -> dict | None:
        """
        Find single row matching filters.

        Args:
            **filters: Column filters

        Returns:
            Row as dictionary or None

        Examples:
            >>> user = table.find_one(name='John')
        """
        for row in self.find(_limit=1, **filters):
            return row
        return None

    def all(self) -> Iterator[dict]:
        """
        Get all rows in table.

        Yields:
            All rows as dictionaries

        Examples:
            >>> for user in table.all():
            ...     print(user)
        """
        for row in self.find():
            yield row

    def count(self, **filters) -> int:
        """
        Count rows matching filters.

        Args:
            **filters: Column filters

        Returns:
            Number of matching rows

        Examples:
            >>> total = table.count()
            >>> adults = table.count(age={'>=': 18})
        """
        table = self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build COUNT statement
        stmt = select(func.count()).select_from(table)

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Execute
        with self._pool.connect() as conn:
            result = conn.execute(stmt)
            return result.scalar()

    def update(
        self,
        row: dict[str, Any],
        keys: list[str] | None = None,
        **filters,
    ) -> int:
        """
        Update rows matching filters.

        Args:
            row: Dictionary of column_name -> new_value
            keys: List of key columns for WHERE clause
            **filters: Column filters

        Returns:
            Number of rows updated

        Examples:
            >>> count = table.update({'age': 31}, name='John')
        """
        if self._read_only:
            raise ReadOnlyError("UPDATE not allowed in read-only mode")

        table = self._get_table()

        # Build WHERE clause
        if keys:
            # Filter keys to only include columns that exist in the table
            # This matches dataset behavior: non-existent keys are ignored
            table_cols = {col.name for col in table.columns}
            valid_keys = [k for k in keys if k in table_cols and k in row]

            filters = {k: row[k] for k in valid_keys}
            row = {k: v for k, v in row.items() if k not in keys}

            # If row is empty after removing keys, nothing to update
            if not row:
                return 0

        where_clause = FilterBuilder.build(table, filters)

        if where_clause is None:
            raise QueryError("UPDATE requires WHERE clause (filters or keys)")

        # Build UPDATE statement
        stmt = update(table).where(where_clause).values(**row)

        # Execute
        with self._pool.acquire() as conn:
            result = conn.execute(stmt)
            return result.rowcount

    def upsert(
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
            >>> pk = table.upsert({'name': 'John', 'age': 31}, keys=['name'])

            >>> # With ensure=True (auto-creates table, columns, and index on keys)
            >>> pk = table.upsert(
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
            table = self._schema.get_table(
                self._name,
                ensure_exists=True,
                pk_config=self._db._pk_config
            )

            # Infer types and ensure columns exist
            inferred_types = TypeInference.infer_types_from_row(row, dialect=self._dialect)
            if types:
                inferred_types.update(types)
            self._schema.ensure_columns(table, inferred_types)

            # Filter keys to only include columns that exist in the table (for index creation)
            # After ensure_columns, columns from row + original table columns all exist
            table_cols = {col.name for col in table.columns} | set(row.keys())
            index_keys = [k for k in keys if k in table_cols]

            # Auto-create index on valid keys for performance
            if index_keys:
                self.create_index(index_keys)

            # Clear cached table
            self._table = None

        # Check if row exists using original keys (with .get() for missing keys)
        # This matches dataset behavior: non-existent keys cause query to fail/return None
        filters = {k: row.get(k) for k in original_keys}
        try:
            existing = self.find_one(**filters)
        except QueryError:
            # Table/columns don't exist yet - insert
            existing = None

        if existing:
            # Update existing
            self.update(row, keys=keys, ensure=ensure)
            return existing.get('id')
        else:
            # Insert new
            return self.insert(row, ensure=ensure, types=types)

    def upsert_many(
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
            >>> count = table.upsert_many(rows, keys=['email'], ensure=True)
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
            table = self._schema.get_table(
                self._name,
                ensure_exists=True,
                pk_config=self._db._pk_config
            )

            # Infer types from first row and ensure columns exist
            inferred_types = TypeInference.infer_types_from_row(rows[0], dialect=self._dialect)
            if types:
                inferred_types.update(types)
            self._schema.ensure_columns(table, inferred_types)

            # Filter keys to only include columns that exist in the table (for index creation)
            # After ensure_columns, columns from row + original table columns all exist
            table_cols = {col.name for col in table.columns} | set(rows[0].keys())
            index_keys = [k for k in keys if k in table_cols]

            # Auto-create index on valid keys for performance (once for batch)
            if index_keys:
                self.create_index(index_keys)

            # Clear cached table
            self._table = None

        # Process each row (upsert handles non-existent keys gracefully)
        for row in rows:
            self.upsert(row, keys=keys, ensure=False, types=types)

        return len(rows)

    def delete(self, **filters) -> int:
        """
        Delete rows matching filters.

        Args:
            **filters: Column filters

        Returns:
            Number of rows deleted

        Examples:
            >>> count = table.delete(name='John')
        """
        if self._read_only:
            raise ReadOnlyError("DELETE not allowed in read-only mode")

        if not filters:
            raise QueryError("DELETE requires WHERE clause")

        table = self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build DELETE statement
        stmt = delete(table).where(where_clause)

        # Execute
        with self._pool.acquire() as conn:
            result = conn.execute(stmt)
            return result.rowcount

    def distinct(
        self,
        *columns: str,
        **filters,
    ) -> Iterator[dict]:
        """
        Get distinct values for specified columns.

        Args:
            *columns: Column names
            **filters: Column filters

        Yields:
            Distinct rows

        Examples:
            >>> for row in table.distinct('status'):
            ...     print(row['status'])
        """
        if not columns:
            raise QueryError("DISTINCT requires at least one column")

        table = self._get_table()

        # Build WHERE clause
        where_clause = FilterBuilder.build(table, filters)

        # Build SELECT DISTINCT statement
        selected_columns = [table.c[col] for col in columns]
        stmt = select(*selected_columns).distinct()

        if where_clause is not None:
            stmt = stmt.where(where_clause)

        # Execute and yield rows
        with self._pool.connect() as conn:
            result = conn.execute(stmt)
            for row in result:
                yield dict(row._mapping)

    def create_index(
        self,
        columns: str | list[str],
        name: str | None = None,
        unique: bool = False,
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
            >>> idx_name = table.create_index('email')
            >>> # Returns: 'idx_tablename_email'

            >>> # Compound index on multiple columns
            >>> idx_name = table.create_index(['country', 'city'])
            >>> # Returns: 'idx_tablename_country_city'

            >>> # Unique index with custom name
            >>> idx_name = table.create_index(
            ...     'username',
            ...     name='unique_username',
            ...     unique=True
            ... )

            >>> # Idempotent - creating again returns same name
            >>> idx_name_again = table.create_index('email')
            >>> assert idx_name == idx_name_again
        """
        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]

        table = self._get_table()
        return self._schema.create_index(
            table, columns, name, unique,
            text_index_prefix=self._text_index_prefix,
            **kw
        )

    def has_index(self, columns: str | list[str]) -> bool:
        """
        Check if index exists on specified column(s).

        Useful for conditional index creation to avoid unnecessary operations.

        Args:
            columns: Column name (string) or list of column names

        Returns:
            True if matching index exists on these columns, False otherwise

        Examples:
            >>> # Check single column index
            >>> if not table.has_index('email'):
            ...     table.create_index('email')

            >>> # Check compound index
            >>> has_compound = table.has_index(['country', 'city'])

            >>> # Verify index creation
            >>> table.create_index('email')
            >>> assert table.has_index('email') is True
        """
        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]

        table = self._get_table()
        return self._schema.index_exists(table, columns)
