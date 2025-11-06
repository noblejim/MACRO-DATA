#!/usr/bin/env python3
import os
import argparse
import pandas as pd
try:
    from scripts.utils_surprise import ensure_surprise_z_on_frame
except Exception:
    from utils_surprise import ensure_surprise_z_on_frame


def read_csv_robust(path, usecols=None):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, usecols=usecols, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    except Exception:
        try:
            return pd.read_csv(path, usecols=usecols)
        except Exception:
            return None


def main():
    ap = argparse.ArgumentParser(description='Compute reactions by surprise quantile (t0, +/-1, +/-3)')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--out-dir', default=None)
    ap.add_argument('--quantiles', type=int, default=5, help='Number of quantile buckets (default 5)')
    ap.add_argument('--last-days', type=int, default=180, help='Use only events within the last N days (default 180). 0 disables filtering')
    ap.add_argument('--last-events', type=int, default=100, help='Use only the last N distinct event dates (default 100). 0 disables')
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    rl_path = os.path.join(out_dir, 'reaction_long.csv')
    df = read_csv_robust(rl_path)
    if df is None or df.empty:
        raise SystemExit(f'Missing or empty {rl_path}')

    # optional date filters
    if 'event_date' in df.columns and (args.last_days or args.last_events):
        try:
            df['event_date'] = pd.to_datetime(df['event_date'], format='%Y-%m-%d', errors='coerce')
            if args.last_days and args.last_days > 0 and df['event_date'].notna().any():
                maxd = df['event_date'].dropna().max()
                mind = maxd - pd.Timedelta(days=args.last_days)
                df = df[(df['event_date'] >= mind) & (df['event_date'] <= maxd)]
            if args.last_events and args.last_events > 0 and df['event_date'].notna().any():
                dates = df['event_date'].dropna().sort_values().unique()
                if len(dates) > args.last_events:
                    keep = set(dates[-args.last_events:])
                    df = df[df['event_date'].isin(keep)]
        except Exception:
            pass

    # ensure numeric and required columns
    for c in ['t0_return_avg','win1_cum_avg','win3_cum_avg','surprise_z']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    if 'surprise_z' not in df.columns or pd.to_numeric(df.get('surprise_z'), errors='coerce').isna().mean() > 0.8:
        df = ensure_surprise_z_on_frame(df, args.market, data_dir=os.path.join('data', args.market))
        if 'surprise_z' not in df.columns:
            raise SystemExit('reaction_long.csv missing surprise_z and fallback failed')
    # quantile labels 1..Q per event_type to preserve cross-type scaling
    rows = []
    for (et, sec), g in df.groupby(['event_type','sector']):
        g = g.copy()
        if g['surprise_z'].notna().sum() < args.quantiles:
            continue
        q = pd.qcut(g['surprise_z'], args.quantiles, labels=False, duplicates='drop')
        g['q'] = (q + 1) if q is not None else None
        agg = g.groupby('q', as_index=False).agg(
            t0=('t0_return_avg','mean'),
            win1=('win1_cum_avg','mean'),
            win3=('win3_cum_avg','mean'),
            n=('surprise_z','count')
        )
        for _, r in agg.iterrows():
            rows.append({'event_type': et, 'sector': sec, 'quantile': int(r['q']), 't0_return_avg': r['t0'], 'win1_cum_avg': r['win1'], 'win3_cum_avg': r['win3'], 'n': int(r['n'])})

    if not rows:
        print('No quantile rows computed')
        return
    out = pd.DataFrame(rows)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, 'reaction_by_surprise_quantile.csv')
    out.to_csv(path, index=False)
    print(f'Wrote {path}')


if __name__ == '__main__':
    main()
