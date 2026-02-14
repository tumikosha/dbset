"""Full-text search support for all database dialects (SQLite FTS5, PostgreSQL tsvector, MySQL FULLTEXT)."""
from __future__ import annotations

from typing import Any, Sequence

from sqlalchemy import text, func
from sqlalchemy.engine import Connection


class FTSConfig:
    """Configuration for FTS index."""

    def __init__(
        self,
        columns: list[str],
        language: str = 'english',
        index_name: str | None = None,
    ):
        """
        Initialize FTS configuration.

        Args:
            columns: List of columns to index (supports JSONB paths like 'metadata.field')
            language: Language for stemming (default: 'english')
            index_name: Custom index name (auto-generated if None)
        """
        self.columns = columns
        self.language = language
        self.index_name = index_name


def _parse_column_path(column_name: str) -> tuple[str, str | None]:
    """
    Parse column path for JSONB access.

    Args:
        column_name: Column name, possibly with dot notation (e.g., 'metadata.field')

    Returns:
        Tuple of (base_column, json_path) where json_path is None for regular columns
    """
    if '.' in column_name:
        parts = column_name.split('.', 1)
        return parts[0], parts[1]
    return column_name, None


def _build_column_accessor(column_name: str, dialect: str) -> str:
    """
    Build SQL expression to access column value, handling JSONB paths.

    Args:
        column_name: Column name, possibly with dot notation
        dialect: Database dialect name

    Returns:
        SQL expression string
    """
    base_col, json_path = _parse_column_path(column_name)

    if json_path is None:
        return base_col

    if dialect == 'postgresql':
        return f"({base_col}->>'{json_path}')"
    elif dialect == 'sqlite':
        return f"json_extract({base_col}, '$.{json_path}')"
    else:  # mysql
        return f"JSON_UNQUOTE(JSON_EXTRACT({base_col}, '$.{json_path}'))"


def _build_concat_expression(columns: list[str], dialect: str) -> str:
    """
    Build SQL expression to concatenate multiple columns for FTS.

    Args:
        columns: List of column names (may include JSONB paths)
        dialect: Database dialect name

    Returns:
        SQL expression that concatenates all columns with spaces
    """
    accessors = [_build_column_accessor(col, dialect) for col in columns]

    if dialect == 'postgresql':
        # COALESCE each column, concatenate with ||
        parts = [f"COALESCE({acc}, '')" for acc in accessors]
        return " || ' ' || ".join(parts)
    elif dialect == 'sqlite':
        # SQLite uses || for concat
        parts = [f"COALESCE({acc}, '')" for acc in accessors]
        return " || ' ' || ".join(parts)
    else:  # mysql
        # MySQL uses CONCAT_WS
        parts = [f"COALESCE({acc}, '')" for acc in accessors]
        return f"CONCAT_WS(' ', {', '.join(parts)})"


class FTSManager:
    """Full-text search manager for all database dialects."""

    # FTS table suffix for SQLite
    FTS_TABLE_SUFFIX = '_fts'

    # Generated column name for multi-column FTS
    FTS_CONTENT_COLUMN = '_fts_content'
    FTS_TSVECTOR_COLUMN = '_fts_tsv'

    @staticmethod
    def get_fts_index_name(table_name: str, columns: list[str]) -> str:
        """Generate standard FTS index name."""
        col_suffix = '_'.join(col.replace('.', '_') for col in columns)
        return f"idx_{table_name}_{col_suffix}_fts"

    @staticmethod
    def get_fts_table_name(table_name: str) -> str:
        """Get FTS virtual table name for SQLite."""
        return f"{table_name}{FTSManager.FTS_TABLE_SUFFIX}"

    @staticmethod
    async def create_fts_index_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        dialect: str,
        language: str = 'english',
        pk_column: str = 'id',
    ) -> str:
        """
        Create FTS index asynchronously.

        Args:
            conn: Database connection
            table_name: Name of the table
            columns: Columns to index (supports JSONB paths)
            dialect: Database dialect name
            language: Language for stemming
            pk_column: Primary key column name

        Returns:
            Index name
        """
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        if dialect == 'sqlite':
            await FTSManager._create_sqlite_fts_async(
                conn, table_name, columns, pk_column
            )
        elif dialect == 'postgresql':
            await FTSManager._create_postgresql_fts_async(
                conn, table_name, columns, language
            )
        elif dialect in ('mysql', 'mariadb'):
            await FTSManager._create_mysql_fts_async(
                conn, table_name, columns
            )
        else:
            raise ValueError(f"FTS not supported for dialect: {dialect}")

        return index_name

    @staticmethod
    def create_fts_index_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        dialect: str,
        language: str = 'english',
        pk_column: str = 'id',
    ) -> str:
        """
        Create FTS index synchronously.

        Args:
            conn: Database connection
            table_name: Name of the table
            columns: Columns to index (supports JSONB paths)
            dialect: Database dialect name
            language: Language for stemming
            pk_column: Primary key column name

        Returns:
            Index name
        """
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        if dialect == 'sqlite':
            FTSManager._create_sqlite_fts_sync(
                conn, table_name, columns, pk_column
            )
        elif dialect == 'postgresql':
            FTSManager._create_postgresql_fts_sync(
                conn, table_name, columns, language
            )
        elif dialect in ('mysql', 'mariadb'):
            FTSManager._create_mysql_fts_sync(
                conn, table_name, columns
            )
        else:
            raise ValueError(f"FTS not supported for dialect: {dialect}")

        return index_name

    @staticmethod
    async def _create_sqlite_fts_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        pk_column: str,
    ) -> None:
        """Create SQLite FTS5 virtual table and triggers."""
        fts_table = FTSManager.get_fts_table_name(table_name)
        concat_expr = _build_concat_expression(columns, 'sqlite')

        # Create FTS5 virtual table
        create_fts_sql = f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table} USING fts5(
                content,
                content={table_name},
                content_rowid={pk_column}
            )
        """
        await conn.execute(text(create_fts_sql))

        # Populate FTS table with existing data
        populate_sql = f"""
            INSERT OR REPLACE INTO {fts_table}(rowid, content)
            SELECT {pk_column}, {concat_expr}
            FROM {table_name}
        """
        await conn.execute(text(populate_sql))

        # Build trigger expressions with 'new.' prefix for columns
        trigger_columns = []
        for c in columns:
            if '.' not in c:
                trigger_columns.append(f'new.{c}')
            else:
                # For JSONB paths like 'metadata.field', prefix the base column
                base, path = c.split('.', 1)
                trigger_columns.append(f'new.{base}.{path}')

        trigger_concat_expr = _build_concat_expression(trigger_columns, 'sqlite')

        # Create triggers for keeping FTS in sync
        insert_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_insert
            AFTER INSERT ON {table_name} BEGIN
                INSERT INTO {fts_table}(rowid, content)
                SELECT new.{pk_column}, {trigger_concat_expr};
            END
        """

        update_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_update
            AFTER UPDATE ON {table_name} BEGIN
                DELETE FROM {fts_table} WHERE rowid = old.{pk_column};
                INSERT INTO {fts_table}(rowid, content)
                SELECT new.{pk_column}, {trigger_concat_expr};
            END
        """

        delete_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_delete
            AFTER DELETE ON {table_name} BEGIN
                DELETE FROM {fts_table} WHERE rowid = old.{pk_column};
            END
        """

        try:
            await conn.execute(text(insert_trigger))
            await conn.execute(text(update_trigger))
            await conn.execute(text(delete_trigger))
        except Exception:
            # Triggers may already exist
            pass

    @staticmethod
    def _create_sqlite_fts_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        pk_column: str,
    ) -> None:
        """Create SQLite FTS5 virtual table and triggers (sync version)."""
        fts_table = FTSManager.get_fts_table_name(table_name)
        concat_expr = _build_concat_expression(columns, 'sqlite')

        # Create FTS5 virtual table
        create_fts_sql = f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {fts_table} USING fts5(
                content,
                content={table_name},
                content_rowid={pk_column}
            )
        """
        conn.execute(text(create_fts_sql))

        # Populate FTS table with existing data
        populate_sql = f"""
            INSERT OR REPLACE INTO {fts_table}(rowid, content)
            SELECT {pk_column}, {concat_expr}
            FROM {table_name}
        """
        conn.execute(text(populate_sql))

        # Build trigger expressions with 'new.' prefix for columns
        trigger_columns = []
        for c in columns:
            if '.' not in c:
                trigger_columns.append(f'new.{c}')
            else:
                base, path = c.split('.', 1)
                trigger_columns.append(f'new.{base}.{path}')

        trigger_concat_expr = _build_concat_expression(trigger_columns, 'sqlite')

        # Create triggers
        insert_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_insert
            AFTER INSERT ON {table_name} BEGIN
                INSERT INTO {fts_table}(rowid, content)
                SELECT new.{pk_column}, {trigger_concat_expr};
            END
        """

        update_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_update
            AFTER UPDATE ON {table_name} BEGIN
                DELETE FROM {fts_table} WHERE rowid = old.{pk_column};
                INSERT INTO {fts_table}(rowid, content)
                SELECT new.{pk_column}, {trigger_concat_expr};
            END
        """

        delete_trigger = f"""
            CREATE TRIGGER IF NOT EXISTS {table_name}_fts_delete
            AFTER DELETE ON {table_name} BEGIN
                DELETE FROM {fts_table} WHERE rowid = old.{pk_column};
            END
        """

        try:
            conn.execute(text(insert_trigger))
            conn.execute(text(update_trigger))
            conn.execute(text(delete_trigger))
        except Exception:
            pass

    @staticmethod
    async def _create_postgresql_fts_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        language: str,
    ) -> None:
        """Create PostgreSQL tsvector column and GIN index."""
        concat_expr = _build_concat_expression(columns, 'postgresql')
        tsv_col = FTSManager.FTS_TSVECTOR_COLUMN
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        # Check if tsvector column exists
        check_sql = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_name}' AND column_name = '{tsv_col}'
        """
        result = await conn.execute(text(check_sql))
        exists = result.fetchone() is not None

        if not exists:
            # Add tsvector column directly (single generated column)
            # Use COALESCE on each part of concat_expr to handle NULLs
            add_tsv_sql = f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS {tsv_col} tsvector
                GENERATED ALWAYS AS (to_tsvector('{language}', {concat_expr})) STORED
            """
            await conn.execute(text(add_tsv_sql))

        # Create GIN index
        create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name} USING GIN({tsv_col})
        """
        await conn.execute(text(create_index_sql))

    @staticmethod
    def _create_postgresql_fts_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        language: str,
    ) -> None:
        """Create PostgreSQL tsvector column and GIN index (sync version)."""
        concat_expr = _build_concat_expression(columns, 'postgresql')
        tsv_col = FTSManager.FTS_TSVECTOR_COLUMN
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        # Check if tsvector column exists
        check_sql = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_name}' AND column_name = '{tsv_col}'
        """
        result = conn.execute(text(check_sql))
        exists = result.fetchone() is not None

        if not exists:
            # Add tsvector column directly (single generated column)
            add_tsv_sql = f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS {tsv_col} tsvector
                GENERATED ALWAYS AS (to_tsvector('{language}', {concat_expr})) STORED
            """
            conn.execute(text(add_tsv_sql))

        # Create GIN index
        create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name} USING GIN({tsv_col})
        """
        conn.execute(text(create_index_sql))

    @staticmethod
    async def _create_mysql_fts_async(
        conn: Any,
        table_name: str,
        columns: list[str],
    ) -> None:
        """Create MySQL FULLTEXT index."""
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        # For MySQL, we need actual columns (not JSONB paths)
        # Filter to only direct columns
        direct_columns = [c for c in columns if '.' not in c]

        if not direct_columns:
            raise ValueError("MySQL FULLTEXT requires at least one non-JSONB column")

        col_list = ', '.join(direct_columns)

        # Check if index exists
        check_sql = f"""
            SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_NAME = '{table_name}' AND INDEX_NAME = '{index_name}'
        """
        result = await conn.execute(text(check_sql))
        exists = result.fetchone() is not None

        if not exists:
            create_sql = f"""
                ALTER TABLE {table_name}
                ADD FULLTEXT INDEX {index_name} ({col_list})
            """
            await conn.execute(text(create_sql))

    @staticmethod
    def _create_mysql_fts_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
    ) -> None:
        """Create MySQL FULLTEXT index (sync version)."""
        index_name = FTSManager.get_fts_index_name(table_name, columns)

        direct_columns = [c for c in columns if '.' not in c]

        if not direct_columns:
            raise ValueError("MySQL FULLTEXT requires at least one non-JSONB column")

        col_list = ', '.join(direct_columns)

        check_sql = f"""
            SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_NAME = '{table_name}' AND INDEX_NAME = '{index_name}'
        """
        result = conn.execute(text(check_sql))
        exists = result.fetchone() is not None

        if not exists:
            create_sql = f"""
                ALTER TABLE {table_name}
                ADD FULLTEXT INDEX {index_name} ({col_list})
            """
            conn.execute(text(create_sql))

    @staticmethod
    async def has_fts_index_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        dialect: str,
    ) -> bool:
        """Check if FTS index exists asynchronously."""
        if dialect == 'sqlite':
            fts_table = FTSManager.get_fts_table_name(table_name)
            check_sql = f"""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='{fts_table}'
            """
            result = await conn.execute(text(check_sql))
            return result.fetchone() is not None

        elif dialect == 'postgresql':
            index_name = FTSManager.get_fts_index_name(table_name, columns)
            check_sql = f"""
                SELECT indexname FROM pg_indexes
                WHERE indexname = '{index_name}'
            """
            result = await conn.execute(text(check_sql))
            return result.fetchone() is not None

        elif dialect in ('mysql', 'mariadb'):
            index_name = FTSManager.get_fts_index_name(table_name, columns)
            check_sql = f"""
                SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_NAME = '{table_name}' AND INDEX_NAME = '{index_name}'
            """
            result = await conn.execute(text(check_sql))
            return result.fetchone() is not None

        return False

    @staticmethod
    def has_fts_index_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        dialect: str,
    ) -> bool:
        """Check if FTS index exists synchronously."""
        if dialect == 'sqlite':
            fts_table = FTSManager.get_fts_table_name(table_name)
            check_sql = f"""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='{fts_table}'
            """
            result = conn.execute(text(check_sql))
            return result.fetchone() is not None

        elif dialect == 'postgresql':
            index_name = FTSManager.get_fts_index_name(table_name, columns)
            check_sql = f"""
                SELECT indexname FROM pg_indexes
                WHERE indexname = '{index_name}'
            """
            result = conn.execute(text(check_sql))
            return result.fetchone() is not None

        elif dialect in ('mysql', 'mariadb'):
            index_name = FTSManager.get_fts_index_name(table_name, columns)
            check_sql = f"""
                SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_NAME = '{table_name}' AND INDEX_NAME = '{index_name}'
            """
            result = conn.execute(text(check_sql))
            return result.fetchone() is not None

        return False

    @staticmethod
    async def fts_search_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        query: str,
        dialect: str,
        limit: int = 100,
        language: str = 'english',
        pk_column: str = 'id',
    ) -> list[tuple[Any, float]]:
        """
        Execute FTS search and return (pk, score) pairs.

        Args:
            conn: Database connection
            table_name: Table name
            columns: Indexed columns
            query: Search query text
            dialect: Database dialect
            limit: Maximum results
            language: Language for stemming
            pk_column: Primary key column

        Returns:
            List of (primary_key, bm25_score) tuples, sorted by relevance
        """
        if dialect == 'sqlite':
            return await FTSManager._sqlite_search_async(
                conn, table_name, query, limit, pk_column
            )
        elif dialect == 'postgresql':
            return await FTSManager._postgresql_search_async(
                conn, table_name, query, limit, language, pk_column
            )
        elif dialect in ('mysql', 'mariadb'):
            return await FTSManager._mysql_search_async(
                conn, table_name, columns, query, limit, pk_column
            )
        else:
            raise ValueError(f"FTS not supported for dialect: {dialect}")

    @staticmethod
    def fts_search_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        query: str,
        dialect: str,
        limit: int = 100,
        language: str = 'english',
        pk_column: str = 'id',
    ) -> list[tuple[Any, float]]:
        """Execute FTS search synchronously."""
        if dialect == 'sqlite':
            return FTSManager._sqlite_search_sync(
                conn, table_name, query, limit, pk_column
            )
        elif dialect == 'postgresql':
            return FTSManager._postgresql_search_sync(
                conn, table_name, query, limit, language, pk_column
            )
        elif dialect in ('mysql', 'mariadb'):
            return FTSManager._mysql_search_sync(
                conn, table_name, columns, query, limit, pk_column
            )
        else:
            raise ValueError(f"FTS not supported for dialect: {dialect}")

    @staticmethod
    async def _sqlite_search_async(
        conn: Any,
        table_name: str,
        query: str,
        limit: int,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """SQLite FTS5 BM25 search."""
        fts_table = FTSManager.get_fts_table_name(table_name)

        # FTS5 bm25() returns negative scores (more negative = better match)
        # We negate to get positive scores where higher = better
        search_sql = f"""
            SELECT rowid, -bm25({fts_table}) as score
            FROM {fts_table}
            WHERE {fts_table} MATCH :query
            ORDER BY score DESC
            LIMIT :limit
        """
        result = await conn.execute(text(search_sql), {'query': query, 'limit': limit})
        return [(row[0], row[1]) for row in result.fetchall()]

    @staticmethod
    def _sqlite_search_sync(
        conn: Connection,
        table_name: str,
        query: str,
        limit: int,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """SQLite FTS5 BM25 search (sync)."""
        fts_table = FTSManager.get_fts_table_name(table_name)

        search_sql = f"""
            SELECT rowid, -bm25({fts_table}) as score
            FROM {fts_table}
            WHERE {fts_table} MATCH :query
            ORDER BY score DESC
            LIMIT :limit
        """
        result = conn.execute(text(search_sql), {'query': query, 'limit': limit})
        return [(row[0], row[1]) for row in result.fetchall()]

    @staticmethod
    async def _postgresql_search_async(
        conn: Any,
        table_name: str,
        query: str,
        limit: int,
        language: str,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """PostgreSQL ts_rank search."""
        tsv_col = FTSManager.FTS_TSVECTOR_COLUMN

        search_sql = f"""
            SELECT {pk_column}, ts_rank_cd({tsv_col}, plainto_tsquery(:lang, :query)) as score
            FROM {table_name}
            WHERE {tsv_col} @@ plainto_tsquery(:lang, :query)
            ORDER BY score DESC
            LIMIT :limit
        """
        result = await conn.execute(
            text(search_sql),
            {'lang': language, 'query': query, 'limit': limit}
        )
        return [(row[0], row[1]) for row in result.fetchall()]

    @staticmethod
    def _postgresql_search_sync(
        conn: Connection,
        table_name: str,
        query: str,
        limit: int,
        language: str,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """PostgreSQL ts_rank search (sync)."""
        tsv_col = FTSManager.FTS_TSVECTOR_COLUMN

        search_sql = f"""
            SELECT {pk_column}, ts_rank_cd({tsv_col}, plainto_tsquery(:lang, :query)) as score
            FROM {table_name}
            WHERE {tsv_col} @@ plainto_tsquery(:lang, :query)
            ORDER BY score DESC
            LIMIT :limit
        """
        result = conn.execute(
            text(search_sql),
            {'lang': language, 'query': query, 'limit': limit}
        )
        return [(row[0], row[1]) for row in result.fetchall()]

    @staticmethod
    async def _mysql_search_async(
        conn: Any,
        table_name: str,
        columns: list[str],
        query: str,
        limit: int,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """MySQL FULLTEXT search."""
        direct_columns = [c for c in columns if '.' not in c]
        col_list = ', '.join(direct_columns)

        search_sql = f"""
            SELECT {pk_column}, MATCH({col_list}) AGAINST(:query IN NATURAL LANGUAGE MODE) as score
            FROM {table_name}
            WHERE MATCH({col_list}) AGAINST(:query IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
            LIMIT :limit
        """
        result = await conn.execute(text(search_sql), {'query': query, 'limit': limit})
        return [(row[0], row[1]) for row in result.fetchall()]

    @staticmethod
    def _mysql_search_sync(
        conn: Connection,
        table_name: str,
        columns: list[str],
        query: str,
        limit: int,
        pk_column: str,
    ) -> list[tuple[Any, float]]:
        """MySQL FULLTEXT search (sync)."""
        direct_columns = [c for c in columns if '.' not in c]
        col_list = ', '.join(direct_columns)

        search_sql = f"""
            SELECT {pk_column}, MATCH({col_list}) AGAINST(:query IN NATURAL LANGUAGE MODE) as score
            FROM {table_name}
            WHERE MATCH({col_list}) AGAINST(:query IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
            LIMIT :limit
        """
        result = conn.execute(text(search_sql), {'query': query, 'limit': limit})
        return [(row[0], row[1]) for row in result.fetchall()]
