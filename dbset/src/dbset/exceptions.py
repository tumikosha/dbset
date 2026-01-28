"""Exception hierarchy for AsyncDataset library."""
from __future__ import annotations


class DatasetError(Exception):
    """Base exception for all dataset-related errors."""
    pass


class ConnectionError(DatasetError):
    """Raised when database connection fails."""
    pass


class TableNotFoundError(DatasetError):
    """Raised when attempting to access a table that doesn't exist."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f"Table '{table_name}' does not exist")


class ColumnNotFoundError(DatasetError):
    """Raised when attempting to access a column that doesn't exist."""

    def __init__(self, column_name: str, table_name: str):
        self.column_name = column_name
        self.table_name = table_name
        super().__init__(
            f"Column '{column_name}' does not exist in table '{table_name}'"
        )


class ReadOnlyError(DatasetError):
    """Raised when attempting write operations in read-only mode."""

    def __init__(self, operation: str):
        self.operation = operation
        super().__init__(
            f"Operation '{operation}' not allowed in read-only mode"
        )


class TransactionError(DatasetError):
    """Raised when transaction operation fails."""
    pass


class ValidationError(DatasetError):
    """Raised when data validation fails."""
    pass


class SchemaError(DatasetError):
    """Raised when DDL operation fails."""

    def __init__(self, message: str, table_name: str | None = None):
        self.table_name = table_name
        super().__init__(message)


class QueryError(DatasetError):
    """Raised when query execution fails."""
    pass


class TypeInferenceError(DatasetError):
    """Raised when type inference fails."""
    pass
