"""Unit tests for scripts/utils_date.py — to_date, to_datetime_col, yyyymm, date_to_str."""
import sys
import os
from datetime import date

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(_SCRIPTS_DIR))

from utils_date import to_date, to_datetime_col, yyyymm, date_to_str  # noqa: E402


# ---------------------------------------------------------------------------
# to_date
# ---------------------------------------------------------------------------

def test_to_date_valid_string():
    """'2024-01-15' must parse to date(2024, 1, 15)."""
    result = to_date('2024-01-15')
    assert result == date(2024, 1, 15)


def test_to_date_invalid_string_returns_none():
    """'not-a-date' must return None."""
    result = to_date('not-a-date')
    assert result is None


def test_to_date_empty_string_returns_none():
    """Empty string must return None."""
    result = to_date('')
    assert result is None


def test_to_date_wrong_format_returns_none():
    """Date in wrong format (MM/DD/YYYY) must return None."""
    result = to_date('01/15/2024')
    assert result is None


def test_to_date_partial_date_returns_none():
    """Partial date '2024-01' without day component must return None."""
    result = to_date('2024-01')
    assert result is None


def test_to_date_returns_date_type():
    """Return type must be datetime.date, not datetime.datetime."""
    result = to_date('2024-06-30')
    assert isinstance(result, date)


def test_to_date_leading_trailing_whitespace():
    """String with surrounding whitespace must still parse correctly."""
    result = to_date('  2024-01-15  ')
    assert result == date(2024, 1, 15)


def test_to_date_end_of_year():
    """'2023-12-31' must parse correctly."""
    result = to_date('2023-12-31')
    assert result == date(2023, 12, 31)


# ---------------------------------------------------------------------------
# to_datetime_col
# ---------------------------------------------------------------------------

def test_to_datetime_col_valid_series():
    """Series of valid date strings must be converted to datetime64."""
    s = pd.Series(['2024-01-01', '2024-06-15', '2024-12-31'])
    result = to_datetime_col(s)
    assert result.dtype == 'datetime64[ns]'
    assert result.iloc[0] == pd.Timestamp('2024-01-01')
    assert result.iloc[1] == pd.Timestamp('2024-06-15')
    assert result.iloc[2] == pd.Timestamp('2024-12-31')


def test_to_datetime_col_invalid_entries_become_nat():
    """Unparseable entries must become NaT instead of raising an exception."""
    s = pd.Series(['2024-01-01', 'bad-date', None, '2024-03-01'])
    result = to_datetime_col(s)
    assert pd.isna(result.iloc[1]), "Invalid date string must produce NaT"
    assert pd.isna(result.iloc[2]), "None entry must produce NaT"
    assert result.iloc[0] == pd.Timestamp('2024-01-01')


def test_to_datetime_col_returns_series():
    """Return type must be a pandas Series."""
    s = pd.Series(['2024-01-01'])
    result = to_datetime_col(s)
    assert isinstance(result, pd.Series)


def test_to_datetime_col_empty_series():
    """Empty Series must return an empty Series without error."""
    s = pd.Series([], dtype=str)
    result = to_datetime_col(s)
    assert isinstance(result, pd.Series)
    assert len(result) == 0


def test_to_datetime_col_preserves_length():
    """Output length must equal input length."""
    s = pd.Series(['2024-01-01', '2024-02-01', '2024-03-01'])
    result = to_datetime_col(s)
    assert len(result) == 3


# ---------------------------------------------------------------------------
# yyyymm
# ---------------------------------------------------------------------------

def test_yyyymm_march():
    """date(2024, 3, 15) must produce '202403'."""
    result = yyyymm(date(2024, 3, 15))
    assert result == '202403'


def test_yyyymm_december():
    """December (month 12) must produce a two-digit month: '202312'."""
    result = yyyymm(date(2023, 12, 1))
    assert result == '202312'


def test_yyyymm_january():
    """January (month 1) must be zero-padded: '202401'."""
    result = yyyymm(date(2024, 1, 1))
    assert result == '202401'


def test_yyyymm_returns_string():
    """Return type must be str."""
    result = yyyymm(date(2024, 3, 15))
    assert isinstance(result, str)


def test_yyyymm_length_six():
    """Result must always be exactly 6 characters."""
    result = yyyymm(date(2024, 3, 15))
    assert len(result) == 6


# ---------------------------------------------------------------------------
# date_to_str
# ---------------------------------------------------------------------------

def test_date_to_str_basic():
    """date(2024, 3, 15) must produce '2024-03-15'."""
    result = date_to_str(date(2024, 3, 15))
    assert result == '2024-03-15'


def test_date_to_str_january():
    """Single-digit month must be zero-padded: '2024-01-05'."""
    result = date_to_str(date(2024, 1, 5))
    assert result == '2024-01-05'


def test_date_to_str_returns_string():
    """Return type must be str."""
    result = date_to_str(date(2024, 3, 15))
    assert isinstance(result, str)


def test_date_to_str_format():
    """Result must match the pattern YYYY-MM-DD (10 characters with hyphens at positions 4 and 7)."""
    result = date_to_str(date(2024, 3, 15))
    assert len(result) == 10
    assert result[4] == '-'
    assert result[7] == '-'


def test_date_to_str_roundtrip_with_to_date():
    """date_to_str followed by to_date must recover the original date."""
    original = date(2024, 7, 22)
    s = date_to_str(original)
    recovered = to_date(s)
    assert recovered == original
