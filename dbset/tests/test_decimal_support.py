"""Comprehensive tests for Decimal → Numeric type support."""

from decimal import Decimal

import pytest
from sqlalchemy import Float, Integer, Numeric

from dbset.types import TypeInference


class TestDecimalInference:
    """Test basic Decimal → Numeric inference."""

    def test_infer_decimal_basic(self):
        """Test basic decimal with standard precision."""
        result = TypeInference.infer_type(Decimal('123.45'))
        assert isinstance(result, Numeric)
        assert result.precision == 5
        assert result.scale == 2

    def test_infer_decimal_high_precision(self):
        """Test decimal with high precision after decimal point."""
        result = TypeInference.infer_type(Decimal('0.000001'))
        assert isinstance(result, Numeric)
        assert result.precision == 1
        assert result.scale == 6

    def test_infer_decimal_integer_like(self):
        """Test decimal that looks like integer (no fractional part)."""
        result = TypeInference.infer_type(Decimal('100'))
        assert isinstance(result, Numeric)
        assert result.precision == 3
        assert result.scale == 0

    def test_infer_decimal_zero(self):
        """Test decimal zero."""
        result = TypeInference.infer_type(Decimal('0'))
        assert isinstance(result, Numeric)
        assert result.precision == 1
        assert result.scale == 0

    def test_infer_decimal_negative(self):
        """Test negative decimal."""
        result = TypeInference.infer_type(Decimal('-999.99'))
        assert isinstance(result, Numeric)
        assert result.precision == 5
        assert result.scale == 2

    def test_infer_decimal_large_integer(self):
        """Test large integer decimal."""
        result = TypeInference.infer_type(Decimal('123456789'))
        assert isinstance(result, Numeric)
        assert result.precision == 9
        assert result.scale == 0

    def test_infer_decimal_scientific_notation(self):
        """Test decimal from scientific notation."""
        result = TypeInference.infer_type(Decimal('1.23E+5'))
        assert isinstance(result, Numeric)
        # 1.23E+5 = 123000
        assert result.scale == 0

    def test_infer_decimal_infinity(self):
        """Test Decimal('Infinity') falls back to Float."""
        result = TypeInference.infer_type(Decimal('Infinity'))
        assert isinstance(result, Float)

    def test_infer_decimal_nan(self):
        """Test Decimal('NaN') falls back to Float."""
        result = TypeInference.infer_type(Decimal('NaN'))
        assert isinstance(result, Float)

    def test_infer_decimal_very_large_precision(self):
        """Test decimal with precision > 38 (SQL max) gets capped."""
        # Create decimal with 50 digits
        large_decimal = Decimal('1' * 50)
        result = TypeInference.infer_type(large_decimal)
        assert isinstance(result, Numeric)
        assert result.precision == 38  # Capped at SQL standard max

    def test_infer_decimal_trailing_zeros(self):
        """Test decimal with trailing zeros."""
        result = TypeInference.infer_type(Decimal('100.00'))
        assert isinstance(result, Numeric)
        assert result.precision == 5
        assert result.scale == 2


class TestFloatInference:
    """Test that float inference remains unchanged (backward compatibility)."""

    def test_infer_float_basic(self):
        """Test basic float inference."""
        result = TypeInference.infer_type(3.14)
        assert isinstance(result, Float)

    def test_infer_float_zero(self):
        """Test float zero."""
        result = TypeInference.infer_type(0.0)
        assert isinstance(result, Float)

    def test_infer_float_negative(self):
        """Test negative float."""
        result = TypeInference.infer_type(-99.9)
        assert isinstance(result, Float)

    def test_infer_float_scientific(self):
        """Test float scientific notation."""
        result = TypeInference.infer_type(1.23e5)
        assert isinstance(result, Float)


class TestNumericMerging:
    """Test merging logic for Numeric types."""

    def test_merge_numeric_same_dimensions(self):
        """Test merging two Numeric with same precision/scale."""
        type1 = Numeric(precision=10, scale=2)
        type2 = Numeric(precision=10, scale=2)

        result = TypeInference.merge_types(type1, type2)
        assert isinstance(result, Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_merge_numeric_different_precision(self):
        """Test merging Numeric with different precisions."""
        type1 = Numeric(precision=10, scale=2)
        type2 = Numeric(precision=8, scale=3)

        result = TypeInference.merge_types(type1, type2)
        assert isinstance(result, Numeric)
        assert result.precision == 10  # max(10, 8)
        assert result.scale == 3       # max(2, 3)

    def test_merge_numeric_scale_exceeds_precision(self):
        """Test merging where result scale > precision gets adjusted."""
        type1 = Numeric(precision=5, scale=2)
        type2 = Numeric(precision=4, scale=4)

        result = TypeInference.merge_types(type1, type2)
        assert isinstance(result, Numeric)
        assert result.precision == 5  # max(5, 4)
        assert result.scale == 4       # max(2, 4)

    def test_merge_numeric_unbounded(self):
        """Test merging with unbounded Numeric (no precision/scale)."""
        type1 = Numeric()
        type2 = Numeric(precision=10, scale=2)

        result = TypeInference.merge_types(type1, type2)
        assert isinstance(result, Numeric)
        assert result.precision is None
        assert result.scale is None

    def test_merge_numeric_integer(self):
        """Test merging Numeric with Integer."""
        numeric_type = Numeric(precision=10, scale=2)
        integer_type = Integer()

        result = TypeInference.merge_types(numeric_type, integer_type)
        assert isinstance(result, Numeric)

        # Reverse order
        result = TypeInference.merge_types(integer_type, numeric_type)
        assert isinstance(result, Numeric)

    def test_merge_numeric_float(self):
        """Test merging Numeric with Float (Float is more general)."""
        numeric_type = Numeric(precision=10, scale=2)
        float_type = Float()

        result = TypeInference.merge_types(numeric_type, float_type)
        assert isinstance(result, Float)

        # Reverse order
        result = TypeInference.merge_types(float_type, numeric_type)
        assert isinstance(result, Float)


class TestMixedRowInference:
    """Test type inference on rows with mixed numeric types."""

    def test_row_with_decimal_and_float(self):
        """Test row with both Decimal and float values."""
        row = {
            'price': Decimal('99.99'),
            'discount': 0.15,
            'quantity': 5,
        }

        types = TypeInference.infer_types_from_row(row)
        assert isinstance(types['price'], Numeric)
        assert isinstance(types['discount'], Float)
        assert isinstance(types['quantity'], Integer)

    def test_row_with_multiple_decimals(self):
        """Test row with multiple Decimal values."""
        row = {
            'amount': Decimal('1000.00'),
            'tax': Decimal('85.00'),
            'total': Decimal('1085.00'),
        }

        types = TypeInference.infer_types_from_row(row)
        assert all(isinstance(t, Numeric) for t in types.values())

    def test_row_with_decimal_zero_and_nonzero(self):
        """Test row with zero and non-zero decimals."""
        row = {
            'value1': Decimal('0'),
            'value2': Decimal('123.45'),
        }

        types = TypeInference.infer_types_from_row(row)
        assert isinstance(types['value1'], Numeric)
        assert isinstance(types['value2'], Numeric)


class TestDecimalPrecisionCalculation:
    """Test the _calculate_decimal_precision helper method."""

    def test_calculate_precision_standard(self):
        """Test standard decimal precision calculation."""
        precision, scale = TypeInference._calculate_decimal_precision(
            Decimal('123.45')
        )
        assert precision == 5
        assert scale == 2

    def test_calculate_precision_no_fractional(self):
        """Test precision calculation for integer-like decimal."""
        precision, scale = TypeInference._calculate_decimal_precision(
            Decimal('12345')
        )
        assert precision == 5
        assert scale == 0

    def test_calculate_precision_only_fractional(self):
        """Test precision calculation for 0.xxx decimal."""
        precision, scale = TypeInference._calculate_decimal_precision(
            Decimal('0.123')
        )
        assert precision == 3
        assert scale == 3

    def test_calculate_precision_special_values(self):
        """Test that Infinity/NaN return None."""
        precision, scale = TypeInference._calculate_decimal_precision(
            Decimal('Infinity')
        )
        assert precision is None
        assert scale is None

        precision, scale = TypeInference._calculate_decimal_precision(
            Decimal('NaN')
        )
        assert precision is None
        assert scale is None

    def test_calculate_precision_very_large(self):
        """Test precision capping at 38 (SQL standard)."""
        # 50 digit number
        large_decimal = Decimal('1' * 50)
        precision, scale = TypeInference._calculate_decimal_precision(large_decimal)
        assert precision == 38
        assert scale == 0
