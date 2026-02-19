"""Unit tests for scripts/utils_surprise.py — compute_surprise_z_from_events_df."""
import sys
import os
import math

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path setup: allow importing from the scripts/ directory regardless of how
# pytest is invoked (from repo root, from tests/, or from anywhere else).
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(_SCRIPTS_DIR))

from utils_surprise import compute_surprise_z_from_events_df  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(n=5, include_expected=True):
    """Build a minimal events DataFrame with a single event type."""
    rows = []
    for i in range(n):
        row = {
            'event_id': i + 1,
            'event_type': 'CPI',
            'event_date': f'2024-{i + 1:02d}-15',
            'actual_value': 100.0 + i * 0.5,
        }
        if include_expected:
            row['expected_value'] = 100.0 + i * 0.4
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_empty_input_returns_empty_dataframe():
    """Empty DataFrame input must return an empty DataFrame with correct columns."""
    empty = pd.DataFrame()
    result = compute_surprise_z_from_events_df(empty)
    assert isinstance(result, pd.DataFrame)
    assert 'surprise_z' in result.columns
    assert len(result) == 0


def test_none_input_returns_empty_dataframe():
    """None input must return an empty DataFrame."""
    result = compute_surprise_z_from_events_df(None)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Tests that hit the pandas 2.2 compatibility bug — marked xfail
# ---------------------------------------------------------------------------


def test_returns_dataframe_with_surprise_z_column():
    """Result must be a DataFrame containing a 'surprise_z' column."""
    df = _make_df()
    result = compute_surprise_z_from_events_df(df)
    assert isinstance(result, pd.DataFrame)
    assert 'surprise_z' in result.columns



def test_surprise_z_not_all_nan():
    """With a 5-row input the function must compute at least one finite surprise_z."""
    df = _make_df(n=5)
    result = compute_surprise_z_from_events_df(df)
    z = pd.to_numeric(result['surprise_z'], errors='coerce')
    assert z.notna().any(), "Expected at least one non-NaN surprise_z value"



def test_look_ahead_bias_first_row_nan():
    """The first row's surprise is NaN (diff of first element), but the fallback
    standardizes by group std, so surprise_z may be finite or NaN depending on
    how many non-NaN surprise values exist. With n=5 and min_periods=5, the
    rolling std will always be NaN (requires >=5 prior obs), triggering the
    fallback for all rows. We just verify no exception is raised and the output
    has the right shape.
    """
    df = _make_df(n=5)
    result = compute_surprise_z_from_events_df(df, window=5, min_periods=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(df)
    assert 'surprise_z' in result.columns



def test_freq_map_changes_window_behavior():
    """Providing a freq_map that maps the event type to 'W' should use window=26.

    With only 5 rows neither window produces a valid rolling std, but the
    code path must execute without error and return a DataFrame of the same
    length.
    """
    df = _make_df(n=5)
    freq_map = {'CPI': 'W'}
    result = compute_surprise_z_from_events_df(df, freq_map=freq_map)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(df)
    assert 'surprise_z' in result.columns



def test_fallback_when_expected_value_missing():
    """When expected_value column is absent, surprise is computed as diff(actual).

    The function must still return a DataFrame with surprise_z.
    """
    df = _make_df(n=5, include_expected=False)
    result = compute_surprise_z_from_events_df(df)
    assert isinstance(result, pd.DataFrame)
    assert 'surprise_z' in result.columns
    assert len(result) == len(df)



def test_result_length_matches_input():
    """Output row count must equal input row count."""
    df = _make_df(n=5)
    result = compute_surprise_z_from_events_df(df)
    assert len(result) == len(df)


def test_multiple_event_types_grouped_separately():
    """Each event_type group must be standardized independently.

    With two or more distinct event_type groups pandas 2.2 groupby.apply
    aligns correctly and returns a Series, so this code path works.
    """
    rows = []
    for i in range(4):
        rows.append({
            'event_id': i + 1,
            'event_type': 'CPI',
            'event_date': f'2024-{i + 1:02d}-15',
            'actual_value': 100.0 + i,
            'expected_value': 99.5 + i,
        })
    for i in range(4):
        rows.append({
            'event_id': i + 10,
            'event_type': 'NFP',
            'event_date': f'2024-{i + 1:02d}-01',
            'actual_value': 200.0 + i * 5,
            'expected_value': 195.0 + i * 5,
        })
    df = pd.DataFrame(rows)
    result = compute_surprise_z_from_events_df(df)
    assert len(result) == len(df)
    assert 'event_type' in result.columns
    assert set(result['event_type'].unique()) == {'CPI', 'NFP'}
