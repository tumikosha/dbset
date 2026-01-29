"""Schema management - DDL operations for auto-creating tables and columns."""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from sqlalchemy import (
    Column,
    DDL,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    inspect,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.types import TypeEngine

from .exceptions import ColumnNotFoundError, SchemaError, TableNotFoundError
from .types import PrimaryKeyConfig, TypeInference


class AsyncSchemaManager:
    """
    Async schema manager for DDL operations.

    Handles automatic table/column creation, schema reflection,
    and DDL operations using SQLAlchemy async API.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        metadata: MetaData,
        schema: str | None = None,
    ):
        """
        Initialize schema manager.

        Args:
            engine: SQLAlchemy AsyncEngine
            metadata: SQLAlchemy MetaData for schema reflection
            schema: Database schema name (optional)
        """
        self._engine = engine
        self._metadata = metadata
        self._schema = schema

    async def get_table(
        self,
        table_name: str,
        ensure_exists: bool = False,
        pk_config: PrimaryKeyConfig | None = None,
    ) -> Table:
        """
        Get SQLAlchemy Table object, optionally creating if not exists.

        Args:
            table_name: Name of table
            ensure_exists: If True, create table if it doesn't exist
            pk_config: Primary key configuration (used when creating new table)

        Returns:
            SQLAlchemy Table object

        Raises:
            TableNotFoundError: If table doesn't exist and ensure_exists=False
        """
        # Reflect metadata to get current schema
        await self.reflect()

        # Check if table exists in metadata
        full_name = f"{self._schema}.{table_name}" if self._schema else table_name

        if full_name in self._metadata.tables:
            return self._metadata.tables[full_name]

        if table_name in self._metadata.tables:
            return self._metadata.tables[table_name]

        # Table not found
        if not ensure_exists:
            raise TableNotFoundError(table_name)

        # Create table with configured primary key
        return await self.create_table(table_name, pk_config=pk_config)

    async def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.

        Args:
            table_name: Name of table

        Returns:
            True if table exists, False otherwise
        """
        await self.reflect()

        full_name = f"{self._schema}.{table_name}" if self._schema else table_name

        return (
            full_name in self._metadata.tables
            or table_name in self._metadata.tables
        )

    async def get_table_names(self) -> list[str]:
        """
        Get list of all table names in database.

        Returns:
            List of table names
        """
        await self.reflect()
        return list(self._metadata.tables.keys())

    async def create_table(
        self,
        table_name: str,
        columns: dict[str, TypeEngine] | None = None,
        pk_config: PrimaryKeyConfig | None = None,
    ) -> Table:
        """
        Create new table with specified columns.

        Args:
            table_name: Name of table to create
            columns: Dict of column_name -> SQLAlchemy type (optional)
            pk_config: Primary key configuration (default: Integer 'id' column)

        Returns:
            SQLAlchemy Table object

        Raises:
            SchemaError: If table creation fails

        Examples:
            >>> # Create table with default id column
            >>> table = await schema.create_table('users')

            >>> # Create table with specific columns
            >>> table = await schema.create_table('users', {
            ...     'name': String(255),
            ...     'age': Integer(),
            ... })

            >>> # Create table with UUID primary key
            >>> pk_config = PrimaryKeyConfig(pk_type='uuid', column_name='user_id')
            >>> table = await schema.create_table('users', pk_config=pk_config)
        """
        try:
            # Use provided pk_config or default to Integer 'id' column
            if pk_config is None:
                pk_config = PrimaryKeyConfig()

            # Define table with configured primary key
            table_columns = [pk_config.get_column()]

            # Add additional columns if provided
            if columns:
                for col_name, col_type in columns.items():
                    # Skip primary key column if already provided
                    if col_name != pk_config.column_name:
                        table_columns.append(Column(col_name, col_type))

            # Create Table object
            table = Table(
                table_name,
                self._metadata,
                *table_columns,
                schema=self._schema,
            )

            # Create table in database
            async with self._engine.begin() as conn:
                await conn.run_sync(table.create, checkfirst=True)

            # Refresh metadata
            await self.reflect()

            return table

        except Exception as e:
            raise SchemaError(
                f"Failed to create table '{table_name}': {e}",
                table_name=table_name,
            )

    async def ensure_columns(
        self,
        table: Table,
        columns: dict[str, TypeEngine],
    ) -> None:
        """
        Ensure columns exist in table, creating missing ones.

        Args:
            table: SQLAlchemy Table object
            columns: Dict of column_name -> SQLAlchemy type

        Raises:
            SchemaError: If column creation fails
        """
        existing_columns = {col.name for col in table.columns}
        missing_columns = set(columns.keys()) - existing_columns

        for col_name in missing_columns:
            await self.add_column(table, col_name, columns[col_name])

    async def add_column(
        self,
        table: Table,
        column_name: str,
        column_type: TypeEngine,
    ) -> None:
        """
        Add new column to existing table.

        Args:
            table: SQLAlchemy Table object
            column_name: Name of column to add
            column_type: SQLAlchemy type for column

        Raises:
            SchemaError: If column addition fails
        """
        try:
            # Generate column definition SQL
            col = Column(column_name, column_type)
            col_type_sql = col.type.compile(dialect=self._engine.dialect)

            # Quote identifiers properly for SQL safety (handles special chars like dashes)
            preparer = self._engine.dialect.identifier_preparer
            quoted_table = preparer.quote(table.name)
            quoted_column = preparer.quote(column_name)

            # Generate ALTER TABLE statement
            alter_sql = f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_column} {col_type_sql}"

            async with self._engine.begin() as conn:
                await conn.execute(DDL(alter_sql))

            # Refresh metadata
            await self.reflect()

        except Exception as e:
            raise SchemaError(
                f"Failed to add column '{column_name}' to table '{table.name}': {e}",
                table_name=table.name,
            )

    async def drop_table(self, table_name: str) -> None:
        """
        Drop table from database.

        Args:
            table_name: Name of table to drop

        Raises:
            TableNotFoundError: If table doesn't exist
            SchemaError: If drop operation fails
        """
        if not await self.table_exists(table_name):
            raise TableNotFoundError(table_name)

        try:
            table = await self.get_table(table_name)
            async with self._engine.begin() as conn:
                await conn.run_sync(table.drop)

            # Refresh metadata
            self._metadata.remove(table)

        except Exception as e:
            raise SchemaError(
                f"Failed to drop table '{table_name}': {e}",
                table_name=table_name,
            )

    async def reflect(self) -> None:
        """
        Reflect database schema into metadata.

        Loads current table/column definitions from database.
        """
        try:
            # Clear existing metadata to force fresh reflection
            self._metadata.clear()

            async with self._engine.connect() as conn:
                await conn.run_sync(
                    self._metadata.reflect,
                    schema=self._schema,
                )
        except Exception as e:
            raise SchemaError(f"Failed to reflect schema: {e}")

    @staticmethod
    def _generate_index_name(table_name: str, columns: list[str]) -> str:
        """
        Generate index name following convention: idx_{table}_{col1}_{col2}.

        Args:
            table_name: Name of table
            columns: List of column names

        Returns:
            Generated index name (truncated to 63 chars if necessary)
        """
        # Build base name
        col_part = "_".join(columns)
        base_name = f"idx_{table_name}_{col_part}"

        # PostgreSQL has 63 char limit for identifiers
        if len(base_name) <= 63:
            return base_name

        # Truncate and add hash suffix for uniqueness
        hash_suffix = hashlib.md5(base_name.encode()).hexdigest()[:8]
        max_prefix_len = 63 - len(hash_suffix) - 1  # -1 for underscore
        return f"{base_name[:max_prefix_len]}_{hash_suffix}"

    async def create_index(
        self,
        table: Table,
        columns: list[str],
        name: str | None = None,
        unique: bool = False,
        text_index_prefix: int = 255,
        **kw: Any,
    ) -> str:
        """
        Create index on table columns.

        Args:
            table: SQLAlchemy Table object
            columns: List of column names to index
            name: Custom index name (auto-generated if None)
            unique: Create unique index
            text_index_prefix: Prefix length for TEXT columns (MySQL/MariaDB only)
            **kw: Additional SQLAlchemy Index kwargs (e.g., postgresql_where)

        Returns:
            Index name

        Raises:
            ColumnNotFoundError: If column doesn't exist in table
            SchemaError: If index creation fails
            ValueError: If columns list is empty

        Examples:
            >>> # Single column index
            >>> idx_name = await schema.create_index(table, ['email'])

            >>> # Compound index
            >>> idx_name = await schema.create_index(table, ['country', 'city'])

            >>> # Unique index with custom name
            >>> idx_name = await schema.create_index(
            ...     table, ['username'], name='unique_username', unique=True
            ... )
        """
        if not columns:
            raise ValueError("columns list cannot be empty")

        try:
            # Validate all columns exist
            table_cols = {col.name for col in table.columns}
            for col_name in columns:
                if col_name not in table_cols:
                    raise ColumnNotFoundError(col_name, table.name)

            # Generate index name if not provided
            index_name = name or self._generate_index_name(table.name, columns)

            # Check if index already exists (idempotent)
            if await self.index_exists(table, columns):
                return index_name

            # Create Index object
            index_columns = [table.c[col_name] for col_name in columns]

            # For MySQL/MariaDB: TEXT columns require prefix length for indexing
            dialect_name = self._engine.dialect.name
            if dialect_name in ('mysql', 'mariadb'):
                mysql_length = {}
                for col_name in columns:
                    col = table.c[col_name]
                    if isinstance(col.type, Text):
                        mysql_length[col_name] = text_index_prefix
                if mysql_length:
                    kw['mysql_length'] = mysql_length

            index = Index(index_name, *index_columns, unique=unique, **kw)

            # Create index in database
            async with self._engine.begin() as conn:
                await conn.run_sync(index.create, checkfirst=True)

            # Refresh metadata to include new index
            await self.reflect()

            return index_name

        except ColumnNotFoundError:
            raise
        except Exception as e:
            raise SchemaError(
                f"Failed to create index on table '{table.name}': {e}",
                table_name=table.name,
            )

    async def index_exists(self, table: Table, columns: list[str]) -> bool:
        """
        Check if index exists on specified columns.

        Args:
            table: SQLAlchemy Table object
            columns: List of column names

        Returns:
            True if matching index exists, False otherwise

        Examples:
            >>> exists = await schema.index_exists(table, ['email'])
            >>> exists = await schema.index_exists(table, ['country', 'city'])
        """
        # Reflect metadata to get fresh schema
        await self.reflect()

        # Get updated table object from metadata
        full_name = f"{self._schema}.{table.name}" if self._schema else table.name
        if full_name in self._metadata.tables:
            fresh_table = self._metadata.tables[full_name]
        elif table.name in self._metadata.tables:
            fresh_table = self._metadata.tables[table.name]
        else:
            return False

        # Convert columns to set for comparison
        columns_set = set(columns)

        # Check each index
        for index in fresh_table.indexes:
            index_cols = {col.name for col in index.columns}

            # For single column, order doesn't matter
            if len(columns) == 1:
                if columns_set == index_cols:
                    return True
            # For compound indexes, check exact match (order-independent for simplicity)
            else:
                if columns_set == index_cols:
                    return True

        return False


class SyncSchemaManager:
    """
    Sync schema manager for DDL operations.

    Identical API to AsyncSchemaManager but using sync SQLAlchemy.
    """

    def __init__(
        self,
        engine: Engine,
        metadata: MetaData,
        schema: str | None = None,
    ):
        """
        Initialize schema manager.

        Args:
            engine: SQLAlchemy Engine
            metadata: SQLAlchemy MetaData for schema reflection
            schema: Database schema name (optional)
        """
        self._engine = engine
        self._metadata = metadata
        self._schema = schema

    def get_table(
        self,
        table_name: str,
        ensure_exists: bool = False,
        pk_config: PrimaryKeyConfig | None = None,
    ) -> Table:
        """Get SQLAlchemy Table object (sync version)."""
        # Reflect metadata
        self.reflect()

        # Check if table exists
        full_name = f"{self._schema}.{table_name}" if self._schema else table_name

        if full_name in self._metadata.tables:
            return self._metadata.tables[full_name]

        if table_name in self._metadata.tables:
            return self._metadata.tables[table_name]

        # Table not found
        if not ensure_exists:
            raise TableNotFoundError(table_name)

        # Create table
        return self.create_table(table_name, pk_config=pk_config)

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists (sync version)."""
        self.reflect()

        full_name = f"{self._schema}.{table_name}" if self._schema else table_name

        return (
            full_name in self._metadata.tables
            or table_name in self._metadata.tables
        )

    def get_table_names(self) -> list[str]:
        """Get list of all table names (sync version)."""
        self.reflect()
        return list(self._metadata.tables.keys())

    def create_table(
        self,
        table_name: str,
        columns: dict[str, TypeEngine] | None = None,
        pk_config: PrimaryKeyConfig | None = None,
    ) -> Table:
        """Create new table (sync version)."""
        try:
            # Use provided pk_config or default to Integer 'id' column
            if pk_config is None:
                pk_config = PrimaryKeyConfig()

            # Define table with configured primary key
            table_columns = [pk_config.get_column()]

            # Add additional columns
            if columns:
                for col_name, col_type in columns.items():
                    # Skip primary key column if already provided
                    if col_name != pk_config.column_name:
                        table_columns.append(Column(col_name, col_type))

            # Create Table object
            table = Table(
                table_name,
                self._metadata,
                *table_columns,
                schema=self._schema,
            )

            # Create table in database
            with self._engine.begin() as conn:
                table.create(conn, checkfirst=True)

            # Refresh metadata
            self.reflect()

            return table

        except Exception as e:
            raise SchemaError(
                f"Failed to create table '{table_name}': {e}",
                table_name=table_name,
            )

    def ensure_columns(
        self,
        table: Table,
        columns: dict[str, TypeEngine],
    ) -> None:
        """Ensure columns exist (sync version)."""
        existing_columns = {col.name for col in table.columns}
        missing_columns = set(columns.keys()) - existing_columns

        for col_name in missing_columns:
            self.add_column(table, col_name, columns[col_name])

    def add_column(
        self,
        table: Table,
        column_name: str,
        column_type: TypeEngine,
    ) -> None:
        """Add column to table (sync version)."""
        try:
            # Generate column definition SQL
            col = Column(column_name, column_type)
            col_type_sql = col.type.compile(dialect=self._engine.dialect)

            # Quote identifiers properly for SQL safety (handles special chars like dashes)
            preparer = self._engine.dialect.identifier_preparer
            quoted_table = preparer.quote(table.name)
            quoted_column = preparer.quote(column_name)

            # Generate ALTER TABLE statement
            alter_sql = f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_column} {col_type_sql}"

            with self._engine.begin() as conn:
                conn.execute(DDL(alter_sql))

            # Refresh metadata
            self.reflect()

        except Exception as e:
            raise SchemaError(
                f"Failed to add column '{column_name}' to table '{table.name}': {e}",
                table_name=table.name,
            )

    def drop_table(self, table_name: str) -> None:
        """Drop table (sync version)."""
        if not self.table_exists(table_name):
            raise TableNotFoundError(table_name)

        try:
            table = self.get_table(table_name)
            with self._engine.begin() as conn:
                table.drop(conn)

            # Refresh metadata
            self._metadata.remove(table)

        except Exception as e:
            raise SchemaError(
                f"Failed to drop table '{table_name}': {e}",
                table_name=table_name,
            )

    def reflect(self) -> None:
        """Reflect database schema (sync version)."""
        try:
            # Clear existing metadata to force fresh reflection
            self._metadata.clear()

            with self._engine.connect() as conn:
                self._metadata.reflect(
                    bind=conn,
                    schema=self._schema,
                )
        except Exception as e:
            raise SchemaError(f"Failed to reflect schema: {e}")

    @staticmethod
    def _generate_index_name(table_name: str, columns: list[str]) -> str:
        """
        Generate index name following convention: idx_{table}_{col1}_{col2}.

        Args:
            table_name: Name of table
            columns: List of column names

        Returns:
            Generated index name (truncated to 63 chars if necessary)
        """
        # Build base name
        col_part = "_".join(columns)
        base_name = f"idx_{table_name}_{col_part}"

        # PostgreSQL has 63 char limit for identifiers
        if len(base_name) <= 63:
            return base_name

        # Truncate and add hash suffix for uniqueness
        hash_suffix = hashlib.md5(base_name.encode()).hexdigest()[:8]
        max_prefix_len = 63 - len(hash_suffix) - 1  # -1 for underscore
        return f"{base_name[:max_prefix_len]}_{hash_suffix}"

    def create_index(
        self,
        table: Table,
        columns: list[str],
        name: str | None = None,
        unique: bool = False,
        text_index_prefix: int = 255,
        **kw: Any,
    ) -> str:
        """
        Create index on table columns (sync version).

        Args:
            table: SQLAlchemy Table object
            columns: List of column names to index
            name: Custom index name (auto-generated if None)
            unique: Create unique index
            text_index_prefix: Prefix length for TEXT columns (MySQL/MariaDB only)
            **kw: Additional SQLAlchemy Index kwargs

        Returns:
            Index name

        Raises:
            ColumnNotFoundError: If column doesn't exist in table
            SchemaError: If index creation fails
            ValueError: If columns list is empty
        """
        if not columns:
            raise ValueError("columns list cannot be empty")

        try:
            # Validate all columns exist
            table_cols = {col.name for col in table.columns}
            for col_name in columns:
                if col_name not in table_cols:
                    raise ColumnNotFoundError(col_name, table.name)

            # Generate index name if not provided
            index_name = name or self._generate_index_name(table.name, columns)

            # Check if index already exists (idempotent)
            if self.index_exists(table, columns):
                return index_name

            # Create Index object
            index_columns = [table.c[col_name] for col_name in columns]

            # For MySQL/MariaDB: TEXT columns require prefix length for indexing
            dialect_name = self._engine.dialect.name
            if dialect_name in ('mysql', 'mariadb'):
                mysql_length = {}
                for col_name in columns:
                    col = table.c[col_name]
                    if isinstance(col.type, Text):
                        mysql_length[col_name] = text_index_prefix
                if mysql_length:
                    kw['mysql_length'] = mysql_length

            index = Index(index_name, *index_columns, unique=unique, **kw)

            # Create index in database
            with self._engine.begin() as conn:
                index.create(conn, checkfirst=True)

            # Refresh metadata to include new index
            self.reflect()

            return index_name

        except ColumnNotFoundError:
            raise
        except Exception as e:
            raise SchemaError(
                f"Failed to create index on table '{table.name}': {e}",
                table_name=table.name,
            )

    def index_exists(self, table: Table, columns: list[str]) -> bool:
        """
        Check if index exists on specified columns (sync version).

        Args:
            table: SQLAlchemy Table object
            columns: List of column names

        Returns:
            True if matching index exists, False otherwise
        """
        # Reflect metadata to get fresh schema
        self.reflect()

        # Get updated table object from metadata
        full_name = f"{self._schema}.{table.name}" if self._schema else table.name
        if full_name in self._metadata.tables:
            fresh_table = self._metadata.tables[full_name]
        elif table.name in self._metadata.tables:
            fresh_table = self._metadata.tables[table.name]
        else:
            return False

        # Convert columns to set for comparison
        columns_set = set(columns)

        # Check each index
        for index in fresh_table.indexes:
            index_cols = {col.name for col in index.columns}

            # For single column, order doesn't matter
            if len(columns) == 1:
                if columns_set == index_cols:
                    return True
            # For compound indexes, check exact match (order-independent for simplicity)
            else:
                if columns_set == index_cols:
                    return True

        return False
