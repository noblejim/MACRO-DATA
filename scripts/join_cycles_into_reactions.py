#!/usr/bin/env python3
import os
import argparse
import pandas as pd


def main():
    ap = argparse.ArgumentParser(description='Join sector cycle momentum/ranks into reaction_long to produce reaction_with_cycle.csv')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--out-dir', default=None)
    ap.add_argument('--windows', default='21,63,126', help='Comma-separated windows to include (e.g., 21,63,126)')
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    rlong = os.path.join(out_dir, 'reaction_long.csv')
    ranks = os.path.join(out_dir, 'sector_cycle_rank.csv')

    if not os.path.exists(rlong):
        raise SystemExit(f'Missing {rlong}')
    if not os.path.exists(ranks):
        raise SystemExit(f'Missing {ranks}')

    rl = pd.read_csv(rlong, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    rk = pd.read_csv(ranks, encoding='utf-8-sig', engine='python', on_bad_lines='skip')

    # types
    if 't0_date' in rl.columns:
        rl['t0_date'] = pd.to_datetime(rl['t0_date'], errors='coerce')
    if 'date' in rk.columns:
        rk['date'] = pd.to_datetime(rk['date'], errors='coerce')

    # select windows
    wins = [w.strip() for w in (args.windows or '21,63,126').split(',') if w.strip()]
    moms = [f'mom_{w}' for w in wins]
    ranks_cols = [f'rank_{w}' for w in wins]

    keep_cols = ['date','sector'] + moms + ranks_cols
    rk2 = rk[keep_cols].copy()
    rk2 = rk2.rename(columns={'date':'t0_date'})

    joined = rl.merge(rk2, on=['t0_date','sector'], how='left')

    outp = os.path.join(out_dir, 'reaction_with_cycle.csv')
    joined.to_csv(outp, index=False)
    print(f'Wrote {outp} (rows={len(joined)})')


if __name__ == '__main__':
    main()
