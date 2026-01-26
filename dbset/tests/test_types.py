"""Unit tests for types.py - TypeInference system."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import Boolean, Date, DateTime, Float, Integer, Numeric, String, Text

from dbset.types import TypeInference, TypeInferenceError


def test_infer_boolean():
    """Test boolean type inference."""
    result = TypeInference.infer_type(True)
    assert isinstance(result, Boolean)

    result = TypeInference.infer_type(False)
    assert isinstance(result, Boolean)


def test_infer_integer():
    """Test integer type inference."""
    result = TypeInference.infer_type(42)
    assert isinstance(result, Integer)

    result = TypeInference.infer_type(0)
    assert isinstance(result, Integer)

    result = TypeInference.infer_type(-100)
    assert isinstance(result, Integer)


def test_infer_float():
    """Test float type inference."""
    result = TypeInference.infer_type(3.14)
    assert isinstance(result, Float)

    # Decimal now infers to Numeric (not Float)
    result = TypeInference.infer_type(Decimal('99.99'))
    assert isinstance(result, Numeric)
    assert result.precision == 4
    assert result.scale == 2


def test_infer_string():
    """Test string type inference."""
    # Short string -> String(255)
    result = TypeInference.infer_type('hello')
    assert isinstance(result, String)
    assert result.length == 255

    # Long string -> Text
    long_string = 'x' * 300
    result = TypeInference.infer_type(long_string)
    assert isinstance(result, Text)


def test_infer_datetime():
    """Test datetime type inference."""
    result = TypeInference.infer_type(datetime.now())
    assert isinstance(result, DateTime)


def test_infer_date():
    """Test date type inference."""
    result = TypeInference.infer_type(date.today())
    assert isinstance(result, Date)


def test_infer_none():
    """Test None type inference (defaults to String)."""
    result = TypeInference.infer_type(None)
    assert isinstance(result, String)


def test_infer_bytes():
    """Test bytes type inference."""
    result = TypeInference.infer_type(b'hello')
    assert isinstance(result, Text)


def test_infer_unknown_type():
    """Test inference of unsupported type."""
    with pytest.raises(TypeInferenceError):
        TypeInference.infer_type(object())


def test_infer_types_from_row():
    """Test inferring types for entire row."""
    row = {
        'name': 'John',
        'age': 30,
        'active': True,
        'balance': 99.99,
        'created_at': datetime.now(),
    }

    types = TypeInference.infer_types_from_row(row)

    assert isinstance(types['name'], String)
    assert isinstance(types['age'], Integer)
    assert isinstance(types['active'], Boolean)
    assert isinstance(types['balance'], Float)
    assert isinstance(types['created_at'], DateTime)


def test_merge_same_types():
    """Test merging identical types."""
    type1 = Integer()
    type2 = Integer()

    result = TypeInference.merge_types(type1, type2)
    assert isinstance(result, Integer)


def test_merge_integer_float():
    """Test merging Integer and Float (should prefer Float)."""
    type1 = Integer()
    type2 = Float()

    result = TypeInference.merge_types(type1, type2)
    assert isinstance(result, Float)

    # Reverse order should also give Float
    result = TypeInference.merge_types(type2, type1)
    assert isinstance(result, Float)


def test_merge_date_datetime():
    """Test merging Date and DateTime (should prefer DateTime)."""
    type1 = Date()
    type2 = DateTime()

    result = TypeInference.merge_types(type1, type2)
    assert isinstance(result, DateTime)


def test_merge_string_lengths():
    """Test merging String with different lengths (should take max)."""
    type1 = String(50)
    type2 = String(100)

    result = TypeInference.merge_types(type1, type2)
    assert isinstance(result, String)
    assert result.length == 100


def test_merge_string_text():
    """Test merging String and Text (should prefer Text)."""
    type1 = String(255)
    type2 = Text()

    result = TypeInference.merge_types(type1, type2)
    assert isinstance(result, Text)


def test_custom_max_string_length():
    """Test custom max string length threshold."""
    short_string = 'x' * 50

    # Default threshold (255)
    result = TypeInference.infer_type(short_string)
    assert isinstance(result, String)

    # Custom threshold (30) - should use Text
    result = TypeInference.infer_type(short_string, max_string_length=30)
    assert isinstance(result, Text)
