"""Type inference system - Python types to SQLAlchemy types mapping."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import TypeEngine

from .exceptions import TypeInferenceError


class TypeInference:
    """
    Infers SQLAlchemy column types from Python values.

    Used for automatic schema creation when inserting data.
    """

    # Default prefix length for TEXT column indexes (used by MySQL/MariaDB)
    TEXT_INDEX_PREFIX_LENGTH = 255

    @staticmethod
    def infer_type(
        value: Any,
        max_string_length: int | None = None,
        dialect: str | None = None,
    ) -> TypeEngine:
        """
        Infer SQLAlchemy type from Python value.

        Args:
            value: Python value to infer type from
            max_string_length: Deprecated, ignored. Kept for backward compatibility.
            dialect: Database dialect name (e.g., 'postgresql', 'sqlite')
                     Used to select optimal type (JSONB for PostgreSQL)

        Returns:
            SQLAlchemy type instance

        Raises:
            TypeInferenceError: If type cannot be inferred

        Examples:
            >>> TypeInference.infer_type(42)
            Integer()
            >>> TypeInference.infer_type("hello")
            Text()
            >>> TypeInference.infer_type(True)
            Boolean()
            >>> TypeInference.infer_type({'key': 'value'}, dialect='postgresql')
            JSONB()
            >>> TypeInference.infer_type([1, 2, 3], dialect='sqlite')
            JSON()
        """
        # None values default to Text (nullable)
        if value is None:
            return Text()

        # Boolean must be checked before int (bool is subclass of int in Python)
        if isinstance(value, bool):
            return Boolean()

        # Numeric types
        if isinstance(value, int):
            return Integer()

        if isinstance(value, Decimal):
            precision, scale = TypeInference._calculate_decimal_precision(value)
            if precision is None:  # Infinity/NaN
                return Float()
            return Numeric(precision=precision, scale=scale)

        if isinstance(value, float):
            return Float()

        # Date/time types
        if isinstance(value, datetime):
            return DateTime()

        if isinstance(value, date):
            return Date()

        # String types - always use Text for maximum flexibility
        if isinstance(value, str):
            return Text()

        # Bytes - store as Text (can be enhanced later for binary types)
        if isinstance(value, bytes):
            return Text()

        # JSON types (dict, list) - use JSONB for PostgreSQL, JSON for others
        if isinstance(value, (dict, list)):
            if dialect == 'postgresql':
                return JSONB()
            return JSON()

        # Unknown type
        raise TypeInferenceError(
            f"Cannot infer SQLAlchemy type for Python type: {type(value).__name__}"
        )

    @staticmethod
    def _calculate_decimal_precision(value: Decimal) -> tuple[int | None, int | None]:
        """
        Calculate precision and scale from Decimal value.

        Args:
            value: Decimal value to analyze

        Returns:
            Tuple of (precision, scale) or (None, None) for special values

        Examples:
            >>> TypeInference._calculate_decimal_precision(Decimal('123.45'))
            (5, 2)
            >>> TypeInference._calculate_decimal_precision(Decimal('0.000001'))
            (1, 6)
            >>> TypeInference._calculate_decimal_precision(Decimal('Infinity'))
            (None, None)
        """
        tuple_repr = value.as_tuple()

        # Handle special values (Infinity, NaN)
        if tuple_repr.exponent in ('F', 'n'):
            return None, None

        num_digits = len(tuple_repr.digits)
        exponent = tuple_repr.exponent

        # Handle zero
        if num_digits == 1 and tuple_repr.digits[0] == 0:
            return 1, 0

        # Calculate precision and scale
        if exponent >= 0:
            scale = 0
            precision = num_digits + exponent
        else:
            scale = abs(exponent)
            precision = num_digits

        # Apply SQL standard max (38)
        MAX_PRECISION = 38
        if precision > MAX_PRECISION:
            precision = MAX_PRECISION
            scale = min(scale, MAX_PRECISION)

        return precision, scale

    @staticmethod
    def infer_types_from_row(
        row: dict[str, Any],
        max_string_length: int | None = None,
        dialect: str | None = None,
    ) -> dict[str, TypeEngine]:
        """
        Infer types for all columns in a row.

        Args:
            row: Dictionary of column_name -> value
            max_string_length: Maximum string length before using Text
            dialect: Database dialect name (e.g., 'postgresql', 'sqlite')

        Returns:
            Dictionary of column_name -> SQLAlchemy type

        Examples:
            >>> TypeInference.infer_types_from_row({'name': 'John', 'age': 30})
            {'name': String(255), 'age': Integer()}
            >>> TypeInference.infer_types_from_row(
            ...     {'data': {'nested': 'value'}},
            ...     dialect='postgresql'
            ... )
            {'data': JSONB()}
        """
        types = {}
        for column_name, value in row.items():
            types[column_name] = TypeInference.infer_type(
                value, max_string_length=max_string_length, dialect=dialect
            )
        return types

    @staticmethod
    def merge_types(type1: TypeEngine, type2: TypeEngine) -> TypeEngine:
        """
        Merge two SQLAlchemy types, choosing the more general one.

        Used when inferring types from multiple rows - ensures compatibility.

        Args:
            type1: First type
            type2: Second type

        Returns:
            More general type that can accommodate both

        Examples:
            >>> TypeInference.merge_types(Integer(), Float())
            Float()
            >>> TypeInference.merge_types(String(50), String(100))
            String(100)
        """
        # Same type - return first (with special handling for parametrized types)
        if type(type1) == type(type2):
            # For String, take the larger length
            if isinstance(type1, String) and isinstance(type2, String):
                if type1.length is None or type2.length is None:
                    return String(None)
                return String(max(type1.length, type2.length))
            # For Numeric, merge precision and scale
            if isinstance(type1, Numeric) and isinstance(type2, Numeric):
                return TypeInference._merge_numeric_types(type1, type2)
            return type1

        # Integer + Float = Float
        if isinstance(type1, Integer) and isinstance(type2, Float):
            return Float()
        if isinstance(type1, Float) and isinstance(type2, Integer):
            return Float()

        # Numeric + Integer = Numeric
        if isinstance(type1, Numeric) and isinstance(type2, Integer):
            return type1
        if isinstance(type1, Integer) and isinstance(type2, Numeric):
            return type2

        # Numeric + Float = Float (Float is more general)
        if isinstance(type1, Numeric) and isinstance(type2, Float):
            return Float()
        if isinstance(type1, Float) and isinstance(type2, Numeric):
            return Float()

        # Date + DateTime = DateTime
        if isinstance(type1, Date) and isinstance(type2, DateTime):
            return DateTime()
        if isinstance(type1, DateTime) and isinstance(type2, Date):
            return DateTime()

        # String + Text = Text
        if isinstance(type1, (String, Text)) and isinstance(type2, (String, Text)):
            return Text()

        # JSON + JSONB = prefer JSONB (more efficient on PostgreSQL)
        if isinstance(type1, (JSON, JSONB)) and isinstance(type2, (JSON, JSONB)):
            # Prefer JSONB if either is JSONB
            if isinstance(type1, JSONB) or isinstance(type2, JSONB):
                return JSONB()
            return JSON()

        # Default: use Text as most general string type
        return Text()

    @staticmethod
    def _merge_numeric_types(type1: Numeric, type2: Numeric) -> Numeric:
        """
        Merge two Numeric types - take max precision and scale.

        Args:
            type1: First Numeric type
            type2: Second Numeric type

        Returns:
            Numeric type with maximum precision and scale

        Examples:
            >>> TypeInference._merge_numeric_types(Numeric(10, 2), Numeric(8, 3))
            Numeric(10, 3)
        """
        p1, s1 = type1.precision, type1.scale
        p2, s2 = type2.precision, type2.scale

        # If unbounded, return unbounded
        if p1 is None or p2 is None or s1 is None or s2 is None:
            return Numeric()

        max_precision = max(p1, p2)
        max_scale = max(s1, s2)

        # Ensure precision >= scale (SQL requirement)
        if max_precision < max_scale:
            max_precision = max_scale

        return Numeric(precision=max_precision, scale=max_scale)


class PrimaryKeyType(Enum):
    """Supported primary key types for auto-created tables."""

    INTEGER = 'integer'
    UUID = 'uuid'
    CUSTOM = 'custom'


class PrimaryKeyConfig:
    """
    Configuration for primary key generation in auto-created tables.

    Allows customization of primary key type, column name, and value generation.
    Used to support UUID primary keys and custom naming conventions.

    Examples:
        >>> # Default: Integer autoincrement
        >>> pk_config = PrimaryKeyConfig()

        >>> # UUID primary keys
        >>> pk_config = PrimaryKeyConfig(pk_type='uuid')

        >>> # Custom column name
        >>> pk_config = PrimaryKeyConfig(
        ...     pk_type='uuid',
        ...     column_name='user_id'
        ... )

        >>> # Custom generator with uppercase UUIDs
        >>> from uuid import uuid4
        >>> pk_config = PrimaryKeyConfig(
        ...     pk_type='uuid',
        ...     generator=lambda: str(uuid4()).upper()
        ... )

        >>> # Fully custom primary key
        >>> pk_config = PrimaryKeyConfig(
        ...     pk_type='custom',
        ...     column_name='custom_id',
        ...     generator=lambda: f"USER_{uuid4()}",
        ...     sqlalchemy_type=String(50)
        ... )
    """

    def __init__(
        self,
        pk_type: PrimaryKeyType | str = PrimaryKeyType.INTEGER,
        column_name: str = 'id',
        generator: Callable[[], Any] | None = None,
        sqlalchemy_type: TypeEngine | None = None,
    ):
        """
        Initialize primary key configuration.

        Args:
            pk_type: Type of primary key (INTEGER, UUID, CUSTOM)
            column_name: Name of primary key column (default: 'id')
            generator: Function to generate PK values (required for CUSTOM)
            sqlalchemy_type: SQLAlchemy type for column (required for CUSTOM)

        Raises:
            ValueError: If CUSTOM pk_type missing generator or sqlalchemy_type
        """
        # Convert string to enum
        if isinstance(pk_type, str):
            pk_type = PrimaryKeyType(pk_type.lower())

        self.pk_type = pk_type
        self.column_name = column_name

        # Setup generator and SQLAlchemy type based on pk_type
        if pk_type == PrimaryKeyType.INTEGER:
            self.generator = None  # Auto-increment handled by DB
            self.sqlalchemy_type = Integer
            self.autoincrement = True

        elif pk_type == PrimaryKeyType.UUID:
            # Default UUID generator: uuid4 returning string
            self.generator = generator or (lambda: str(uuid4()))
            # Use String(36) for UUID storage (compatible with all DBs)
            self.sqlalchemy_type = sqlalchemy_type or String(36)
            self.autoincrement = False

        elif pk_type == PrimaryKeyType.CUSTOM:
            if not generator:
                raise ValueError("CUSTOM pk_type requires generator function")
            if not sqlalchemy_type:
                raise ValueError("CUSTOM pk_type requires sqlalchemy_type")
            self.generator = generator
            self.sqlalchemy_type = sqlalchemy_type
            self.autoincrement = False

        else:
            raise ValueError(f"Unknown pk_type: {pk_type}")

    def get_column(self) -> Column:
        """
        Create SQLAlchemy Column for primary key.

        Returns:
            SQLAlchemy Column configured as primary key

        Examples:
            >>> pk_config = PrimaryKeyConfig(pk_type='uuid')
            >>> col = pk_config.get_column()
            >>> print(col.name)  # 'id'
            >>> print(col.primary_key)  # True
        """
        return Column(
            self.column_name,
            self.sqlalchemy_type,
            primary_key=True,
            autoincrement=self.autoincrement,
        )

    def generate_value(self) -> Any:
        """
        Generate primary key value for UUID/CUSTOM types.

        Returns:
            Generated primary key value, or None for INTEGER (DB auto-increment)

        Examples:
            >>> pk_config = PrimaryKeyConfig(pk_type='uuid')
            >>> value = pk_config.generate_value()
            >>> print(type(value))  # <class 'str'>
        """
        if self.generator:
            return self.generator()
        return None  # Integer uses DB autoincrement
