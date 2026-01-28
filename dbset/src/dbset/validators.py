"""SQL validation for read-only mode - integrates with TriggerAI sql_validator."""
from __future__ import annotations

from dbset.sql_validator import (
    SQLValidationError,
    extract_table_names,
    validate_readonly,
    validate_tables_exist,
)

from .exceptions import ReadOnlyError, ValidationError


class ReadOnlyValidator:
    """
    Validates SQL queries for read-only safety.

    Integrates with TriggerAI's existing sql_validator.py to ensure
    consistent validation logic across the application.
    """

    @staticmethod
    def validate_sql(sql: str) -> None:
        """
        Validate that SQL query is read-only (SELECT only).

        Args:
            sql: SQL query to validate

        Raises:
            ReadOnlyError: If query contains write operations

        Examples:
            >>> ReadOnlyValidator.validate_sql("SELECT * FROM users")
            # No error

            >>> ReadOnlyValidator.validate_sql("DELETE FROM users")
            ReadOnlyError: Forbidden keyword detected: DELETE
        """
        try:
            validate_readonly(sql)
        except SQLValidationError as e:
            raise ReadOnlyError(str(e))

    @staticmethod
    def validate_operation(operation: str) -> None:
        """
        Validate that operation is allowed in read-only mode.

        Args:
            operation: Operation name (e.g., 'INSERT', 'UPDATE', 'DELETE')

        Raises:
            ReadOnlyError: If operation is not allowed

        Examples:
            >>> ReadOnlyValidator.validate_operation('SELECT')
            # No error

            >>> ReadOnlyValidator.validate_operation('INSERT')
            ReadOnlyError: Operation 'INSERT' not allowed in read-only mode
        """
        forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'TRUNCATE']
        if operation.upper() in forbidden:
            raise ReadOnlyError(f"Operation '{operation}' not allowed in read-only mode")

    @staticmethod
    def extract_table_names(sql: str) -> list[str]:
        """
        Extract table names from SQL query.

        Uses TriggerAI's existing extraction logic.

        Args:
            sql: SQL query

        Returns:
            List of table names referenced in the query

        Examples:
            >>> ReadOnlyValidator.extract_table_names("SELECT * FROM users")
            ['users']

            >>> ReadOnlyValidator.extract_table_names("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
            ['users', 'orders']
        """
        return extract_table_names(sql)

    @staticmethod
    def validate_tables_exist(sql: str, existing_tables: list[str]) -> None:
        """
        Validate that all tables in SQL query exist in schema.

        Args:
            sql: SQL query
            existing_tables: List of table names that exist in schema

        Raises:
            ValidationError: If query references non-existent tables

        Examples:
            >>> ReadOnlyValidator.validate_tables_exist(
            ...     "SELECT * FROM users",
            ...     ['users', 'orders']
            ... )
            # No error

            >>> ReadOnlyValidator.validate_tables_exist(
            ...     "SELECT * FROM unknown_table",
            ...     ['users', 'orders']
            ... )
            ValidationError: Tables not found: unknown_table
        """
        missing = validate_tables_exist(sql, existing_tables)
        if missing:
            raise ValidationError(f"Tables not found: {', '.join(missing)}")
