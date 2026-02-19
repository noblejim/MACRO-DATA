#!/usr/bin/env python3
"""Validate KR macro data quality.

Checks:
- Expected value ranges per event_type
- NULL rate per event_type
- Duplicate detection (EMPLOYMENT vs INFL_EXPECT correlation)
- CPI_YOY extreme value detection
- Overall data completeness report

Usage:
    python scripts/validate_kr_data.py --data-dir data/kr
"""
import os
import sys
import argparse
import math

try:
    import pandas as pd
except ImportError:
    print('ERROR: pandas required. Install: pip install pandas')
    sys.exit(1)

EXPECTED_RANGES = {
    'CPI_YOY':      (-5, 20),
    'PPI_YOY':      (-10, 30),
    'UNEMP_RATE':   (0, 15),
    'POLICY_RATE':  (0, 10),
    'CCSI':         (50, 150),
    'EMPLOYMENT':   (-500, 500),
    'INFL_EXPECT':  (0, 10),
    'IP_YOY':       (-30, 50),
    'PMI_MFG':      (30, 100),
    'GDP_QOQ':      (-10, 15),
    'TRADE_BAL':    (-50, 50),
    'M2_YOY':       (-5, 30),
    'LEI':          (80, 120),
    'USDKRW':       (800, 2000),
    'EQUITY_INDEX': (1000, 5000),
}

ALL_EVENT_TYPES = sorted(EXPECTED_RANGES.keys())


def read_csv_safe(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        try:
            return pd.read_csv(path)
        except Exception:
            return None


def check_ranges(df, issues):
    """Check actual_value ranges per event_type."""
    print('\n--- Range Validation ---')
    for et in ALL_EVENT_TYPES:
        subset = df[df['event_type'] == et].copy()
        vals = pd.to_numeric(subset['actual_value'], errors='coerce').dropna()
        if vals.empty:
            continue
        lo, hi = EXPECTED_RANGES[et]
        out_of_range = vals[(vals < lo) | (vals > hi)]
        if len(out_of_range) > 0:
            msg = f'FAIL: {et} has {len(out_of_range)}/{len(vals)} values outside [{lo}, {hi}]'
            msg += f' (min={vals.min():.2f}, max={vals.max():.2f})'
            issues.append(msg)
            print(msg)
        else:
            print(f'OK: {et} all {len(vals)} values in [{lo}, {hi}] (min={vals.min():.2f}, max={vals.max():.2f})')


def check_null_rates(df, issues):
    """Report NULL rate per event_type."""
    print('\n--- NULL Rate Report ---')
    for et in ALL_EVENT_TYPES:
        subset = df[df['event_type'] == et]
        total = len(subset)
        if total == 0:
            msg = f'WARN: {et} has 0 rows in events'
            issues.append(msg)
            print(msg)
            continue
        filled = subset['actual_value'].apply(lambda x: str(x).strip() != '' and str(x).strip() != 'nan').sum()
        null_rate = 1.0 - filled / total
        status = 'OK' if null_rate < 0.3 else ('WARN' if null_rate < 0.7 else 'FAIL')
        msg = f'{status}: {et} fill={filled}/{total} (null_rate={null_rate:.1%})'
        if status != 'OK':
            issues.append(msg)
        print(msg)


def check_duplicate_series(df, issues):
    """Check if EMPLOYMENT and INFL_EXPECT return identical data (known bug)."""
    print('\n--- Duplicate Series Check ---')
    emp = df[df['event_type'] == 'EMPLOYMENT'].copy()
    inf = df[df['event_type'] == 'INFL_EXPECT'].copy()

    if emp.empty or inf.empty:
        print('SKIP: EMPLOYMENT or INFL_EXPECT has no data')
        return

    emp_vals = pd.to_numeric(emp.set_index('event_date')['actual_value'], errors='coerce').dropna()
    inf_vals = pd.to_numeric(inf.set_index('event_date')['actual_value'], errors='coerce').dropna()

    common_dates = emp_vals.index.intersection(inf_vals.index)
    if len(common_dates) < 5:
        print(f'SKIP: only {len(common_dates)} common dates')
        return

    emp_common = emp_vals.loc[common_dates]
    inf_common = inf_vals.loc[common_dates]

    if emp_common.std() == 0 or inf_common.std() == 0:
        print('WARN: zero variance in one series')
        return

    corr = emp_common.corr(inf_common)
    if abs(corr) > 0.95:
        msg = f'FAIL: EMPLOYMENT vs INFL_EXPECT correlation = {corr:.4f} (likely duplicate data)'
        issues.append(msg)
        print(msg)
    else:
        print(f'OK: EMPLOYMENT vs INFL_EXPECT correlation = {corr:.4f}')


def check_cpi_extremes(df, issues):
    """Check for CPI_YOY extreme values (known bug: raw index stored instead of %)."""
    print('\n--- CPI_YOY Extreme Check ---')
    cpi = df[df['event_type'] == 'CPI_YOY']
    vals = pd.to_numeric(cpi['actual_value'], errors='coerce').dropna()
    if vals.empty:
        print('SKIP: no CPI_YOY data')
        return
    extreme = vals[vals.abs() > 20]
    if len(extreme) > 0:
        msg = f'FAIL: CPI_YOY has {len(extreme)} values with |val| > 20 (max={vals.max():.0f}, likely raw index not YoY%)'
        issues.append(msg)
        print(msg)
    else:
        print(f'OK: CPI_YOY all {len(vals)} values look like proper YoY%')


def main():
    ap = argparse.ArgumentParser(description='Validate KR macro data quality')
    ap.add_argument('--data-dir', default=os.path.join('data', 'kr'))
    ap.add_argument('--strict', action='store_true', help='Exit with error code on failures')
    args = ap.parse_args()

    issues = []

    # Check files exist
    events_path = os.path.join(args.data_dir, 'macro_events.csv')
    actuals_path = os.path.join(args.data_dir, 'macro_actuals.csv')
    sources_path = os.path.join(args.data_dir, 'macro_sources.csv')

    for p in [events_path, actuals_path, sources_path]:
        if os.path.exists(p):
            print(f'FOUND: {p}')
        else:
            msg = f'MISSING: {p}'
            issues.append(msg)
            print(msg)

    # Use actuals if available, else events
    df = read_csv_safe(actuals_path)
    if df is None or df.empty:
        df = read_csv_safe(events_path)
    if df is None or df.empty:
        print('ERROR: No data to validate')
        sys.exit(1)

    print(f'\nLoaded {len(df)} rows, {df["event_type"].nunique()} event types')

    check_null_rates(df, issues)
    check_ranges(df, issues)
    check_cpi_extremes(df, issues)
    check_duplicate_series(df, issues)

    # Summary
    print('\n' + '=' * 60)
    if issues:
        print(f'VALIDATION: {len(issues)} issue(s) found:')
        for i, iss in enumerate(issues, 1):
            print(f'  {i}. {iss}')
    else:
        print('VALIDATION: All checks passed.')
    print('=' * 60)

    if args.strict and issues:
        sys.exit(1)


if __name__ == '__main__':
    main()
