#!/usr/bin/env python3
"""Canonical date parsing utilities for MACRO-DATA pipeline.

All scripts should prefer these helpers over ad-hoc datetime.strptime or
pd.to_datetime calls to ensure consistent behaviour across the codebase.
"""
from datetime import datetime, date
import pandas as pd


def to_date(s: str) -> date | None:
    """Parse a 'YYYY-MM-DD' string to datetime.date. Returns None on failure."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip(), '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def to_datetime_col(series: pd.Series) -> pd.Series:
    """Parse a pandas Series of date strings to datetime64.

    Tries the strict '%Y-%m-%d' format first; falls back to pandas inference
    with errors='coerce' so that unparseable values become NaT instead of
    raising an exception.
    """
    try:
        return pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')
    except Exception:
        return pd.to_datetime(series, errors='coerce')


def yyyymm(d: date) -> str:
    """Format a date as 'YYYYMM' for API date-range parameters."""
    return f"{d.year}{d.month:02d}"


def date_to_str(d: date) -> str:
    """Format a date as 'YYYY-MM-DD'."""
    return d.strftime('%Y-%m-%d')
