#!/usr/bin/env python3
"""Unified I/O helpers for MACRO-DATA pipeline (L1: CSV → Parquet migration path).

Provides read/write functions that support both CSV (current default) and
Parquet (future default for 50%+ storage savings and faster I/O).

Usage:
    from utils_io import read_df, write_df

    # reads .parquet if it exists, falls back to .csv
    df = read_df('out/us/reaction_long')

    # writes both .parquet and .csv by default (safe migration period)
    write_df(df, 'out/us/reaction_long')

Set USE_PARQUET=1 environment variable (or pass fmt='parquet') to opt into
Parquet-only mode once all consumers are updated.
"""
import os
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Opt-in: set env var USE_PARQUET=1 to write Parquet instead of CSV.
# During the migration period, write_df writes BOTH formats by default.
_USE_PARQUET = os.environ.get('USE_PARQUET', '0').strip() == '1'


def _parquet_available() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return True
        except ImportError:
            return False


def read_df(path_stem: str, fmt: str = 'auto', **kwargs) -> pd.DataFrame:
    """Read a DataFrame from Parquet or CSV.

    Args:
        path_stem: File path without extension (e.g. 'out/us/reaction_long').
                   Extension is inferred automatically.
        fmt: 'auto' (prefer .parquet if exists), 'parquet', or 'csv'.
        **kwargs: Passed through to pd.read_parquet or pd.read_csv.

    Returns:
        DataFrame, or empty DataFrame on error/missing file.
    """
    parquet_path = path_stem + '.parquet'
    csv_path = path_stem + '.csv'

    if fmt == 'parquet' or (fmt == 'auto' and os.path.exists(parquet_path) and _parquet_available()):
        try:
            df = pd.read_parquet(parquet_path, **kwargs)
            logger.debug('Read parquet: %s (%d rows)', parquet_path, len(df))
            return df
        except Exception as e:
            logger.warning('Parquet read failed for %s: %s — falling back to CSV', parquet_path, e)

    # CSV fallback
    if not os.path.exists(csv_path):
        logger.warning('File not found: %s(.csv/.parquet)', path_stem)
        return pd.DataFrame()
    try:
        csv_kwargs = {'encoding': 'utf-8-sig', 'engine': 'python', 'on_bad_lines': 'skip'}
        csv_kwargs.update(kwargs)
        df = pd.read_csv(csv_path, **csv_kwargs)
        logger.debug('Read csv: %s (%d rows)', csv_path, len(df))
        return df
    except Exception as e:
        logger.error('CSV read failed for %s: %s', csv_path, e)
        return pd.DataFrame()


def write_df(df: pd.DataFrame, path_stem: str, fmt: str = 'auto',
             also_csv: bool = True, index: bool = False, **kwargs) -> None:
    """Write a DataFrame to Parquet and/or CSV.

    Args:
        df: DataFrame to write.
        path_stem: File path without extension.
        fmt: 'auto' (write Parquet when available + CSV), 'parquet', or 'csv'.
        also_csv: When fmt='parquet', also write CSV for backwards compatibility.
        index: Include DataFrame index in output (default False).
        **kwargs: Passed through to write functions.
    """
    if df is None or df.empty:
        logger.warning('write_df: empty DataFrame, skipping write to %s', path_stem)
        return

    os.makedirs(os.path.dirname(os.path.abspath(path_stem)), exist_ok=True)
    wrote_parquet = False

    if (fmt in ('auto', 'parquet')) and _USE_PARQUET and _parquet_available():
        parquet_path = path_stem + '.parquet'
        try:
            df.to_parquet(parquet_path, index=index, **kwargs)
            logger.info('Wrote parquet: %s (%d rows)', parquet_path, len(df))
            wrote_parquet = True
        except Exception as e:
            logger.warning('Parquet write failed for %s: %s — falling back to CSV', parquet_path, e)

    if fmt == 'csv' or not wrote_parquet or also_csv:
        csv_path = path_stem + '.csv'
        try:
            df.to_csv(csv_path, index=index, encoding='utf-8', **kwargs)
            logger.debug('Wrote csv: %s (%d rows)', csv_path, len(df))
        except Exception as e:
            logger.error('CSV write failed for %s: %s', csv_path, e)
