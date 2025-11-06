#!/usr/bin/env python3
import os
import pandas as pd


def _to_datetime_ymd(series):
    try:
        return pd.to_datetime(series, format='%Y-%m-%d', errors='coerce')
    except Exception:
        return pd.to_datetime(series, errors='coerce')


def compute_surprise_z_from_events_df(ev: pd.DataFrame, window: int = 12, min_periods: int = 3) -> pd.DataFrame:
    """Compute surprise_z from an events DataFrame.
    Expects columns: event_id, event_date, [event_type or event_name], expected_value, actual_value.
    Returns a DataFrame with ['event_id','surprise_z'] (and event_type if available).
    """
    if ev is None or ev.empty:
        return pd.DataFrame(columns=['event_id', 'surprise_z'])
    df = ev.copy()
    for c in ['expected_value', 'actual_value']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    if 'event_date' in df.columns:
        df['event_date'] = _to_datetime_ymd(df['event_date'])

    # Compute surprise (actual - expected; fallback to diff of actual by group)
    if 'actual_value' in df.columns:
        df = df.sort_values(['event_type' if 'event_type' in df.columns else 'event_name', 'event_date'])
        if 'expected_value' in df.columns and df['expected_value'].notna().any():
            df['surprise'] = df['actual_value'] - df['expected_value']
            # fill rows where expected is NaN with diff(actual)
            grp = 'event_type' if 'event_type' in df.columns else 'event_name'
            diff_actual = df.groupby(grp)['actual_value'].diff()
            df['surprise'] = df['surprise'].where(df['expected_value'].notna(), diff_actual)
        else:
            grp = 'event_type' if 'event_type' in df.columns else 'event_name'
            df['surprise'] = df.groupby(grp)['actual_value'].diff()
    else:
        return pd.DataFrame(columns=['event_id', 'surprise_z'])

    def _z(g: pd.DataFrame):
        g = g.sort_values('event_date')
        roll = g['surprise'].rolling(window=window, min_periods=min_periods).std()
        z = g['surprise'] / roll
        # robust fallback if rolling std is NaN/0
        mask = z.isna() | (roll == 0)
        if mask.any():
            grp_std = g['surprise'].std()
            eps = 1e-9
            denom = grp_std if (grp_std is not None and not pd.isna(grp_std) and grp_std != 0) else eps
            z = z.where(~mask, g['surprise'] / denom)
        return z

    grp_key = None
    if 'event_type' in df.columns and df['event_type'].notna().any():
        grp_key = 'event_type'
    elif 'event_name' in df.columns and df['event_name'].notna().any():
        grp_key = 'event_name'

    if grp_key:
        gb = df.groupby(grp_key, group_keys=False)
        try:
            df['surprise_z'] = gb.apply(_z, include_groups=False)
        except TypeError:
            df['surprise_z'] = gb.apply(_z)
    else:
        df['surprise_z'] = pd.NA

    cols = ['event_id', 'surprise_z']
    if 'event_type' in df.columns:
        cols.insert(1, 'event_type')
    return df[cols]


def ensure_surprise_z_on_frame(df: pd.DataFrame, market: str, data_dir: str | None = None, na_threshold: float = 0.8) -> pd.DataFrame:
    """Ensure a DataFrame has surprise_z. If missing/mostly NaN, compute from macro_events.csv and merge by event_id.
    Fallback: if still missing and df has 'surprise', standardize by event_name.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    has_z = 'surprise_z' in out.columns
    if has_z:
        z = pd.to_numeric(out.get('surprise_z'), errors='coerce')
        if z.isna().mean() <= na_threshold:
            return out

    base = data_dir or os.path.join('data', market)
    ev_path = os.path.join(base, 'macro_events.csv')
    try:
        ev = pd.read_csv(ev_path, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    except Exception:
        try:
            ev = pd.read_csv(ev_path)
        except Exception:
            ev = None
    if ev is not None and not ev.empty:
        join = compute_surprise_z_from_events_df(ev)
        if not join.empty:
            out = out.merge(join[['event_id', 'surprise_z']], on='event_id', how='left')
    # final fallback: standardize existing 'surprise' per event_name
    if 'surprise_z' not in out.columns or pd.to_numeric(out.get('surprise_z'), errors='coerce').isna().mean() > na_threshold:
        if 'surprise' in out.columns:
            s = pd.to_numeric(out['surprise'], errors='coerce')
            key = out['event_type'] if 'event_type' in out.columns else out.get('event_name')
            if key is not None:
                out['surprise_z'] = (s - s.groupby(key).transform('mean')) / s.groupby(key).transform('std')
    return out
