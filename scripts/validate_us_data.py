#!/usr/bin/env python3
import os
import argparse
import sys
import pandas as pd


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


def check_exists(path, strict, issues):
    if not os.path.exists(path):
        msg = f"MISSING: {path}"
        issues.append(msg)
        print(msg)
        return None
    print(f"FOUND: {path}")
    return path


def check_columns(df, name, required, issues):
    if df is None or df.empty:
        issues.append(f"EMPTY: {name}")
        print(f"EMPTY: {name}")
        return
    missing = [c for c in required if c not in df.columns]
    if missing:
        issues.append(f"{name}: missing cols {missing}")
        print(f"WARN: {name} missing cols {missing}")
    else:
        print(f"OK: {name} required cols present")


def check_date_parse(df, name, col='event_date', issues=None):
    if df is None or col not in df.columns:
        return
    s = pd.to_datetime(df[col], errors='coerce')
    rate = s.notna().mean()
    if rate < 0.9:
        msg = f"WARN: {name} {col} parse rate {rate:.1%}"
        if issues is not None:
            issues.append(msg)
        print(msg)
    else:
        print(f"OK: {name} {col} parse")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out-dir', default='out/us')
    ap.add_argument('--strict', action='store_true')
    args = ap.parse_args()

    issues = []

    # Core files to validate if present
    macro_imp = check_exists(os.path.join(args.out_dir, 'macro_impact.csv'), args.strict, issues)
    rbq = check_exists(os.path.join(args.out_dir, 'reaction_by_surprise_quantile.csv'), args.strict, issues)
    pimp = check_exists(os.path.join(args.out_dir, 'partial_impact.csv'), args.strict, issues)

    # Optional file often present
    rwc = os.path.join(args.out_dir, 'reaction_with_cycle.csv')
    if os.path.exists(rwc):
        print(f"FOUND: {rwc}")
    else:
        print(f"INFO: not found (optional): {rwc}")

    # Column checks
    if macro_imp:
        df = read_csv_safe(macro_imp)
        check_columns(df, 'macro_impact', ['metric', 'n'], issues)
        check_date_parse(df, 'macro_impact', col='event_date', issues=issues)
    if rbq:
        df = read_csv_safe(rbq)
        check_columns(df, 'reaction_by_surprise_quantile', ['surprise_quantile'], issues)
        check_date_parse(df, 'reaction_by_surprise_quantile', col='event_date', issues=issues)
    if pimp:
        df = read_csv_safe(pimp)
        # accept either t0_return_avg or rank_21 presence
        if 't0_return_avg' not in df.columns and 'rank_21' not in df.columns:
            issues.append('partial_impact: expected columns not found (t0_return_avg or rank_21)')
            print('WARN: partial_impact expected cols missing (t0_return_avg or rank_21)')
        else:
            print('OK: partial_impact key cols present')
        check_date_parse(df, 'partial_impact', col='event_date', issues=issues)

    if args.strict and any(msg.startswith('MISSING') for msg in issues):
        print('Strict mode: failures detected')
        sys.exit(1)

    print('Validation completed.')


if __name__ == '__main__':
    main()
