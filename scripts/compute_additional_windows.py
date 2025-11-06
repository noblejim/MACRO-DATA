#!/usr/bin/env python3
import os
import csv
import argparse
from collections import defaultdict
from datetime import datetime


def parse_date(s):
    return datetime.strptime(s, '%Y-%m-%d').date()


def read_tickers(path):
    mapping = {}
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get('ticker') or '').strip()
            s = (row.get('sector') or '').strip()
            if t:
                mapping[t] = s or 'UNKNOWN'
    return mapping


def read_prices(path):
    prices = defaultdict(dict)
    dates_set = set()
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                d = parse_date((row.get('date') or '').strip())
            except Exception:
                continue
            t = (row.get('ticker') or '').strip()
            try:
                px = float(row.get('adj_close'))
            except Exception:
                continue
            prices[t][d] = px
            dates_set.add(d)
    dates = sorted(dates_set)
    return dates, prices


def read_events(path):
    evs = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                dt = parse_date((row.get('event_date') or '').strip())
            except Exception:
                continue
            evs.append({
                'event_id': (row.get('event_id') or '').strip(),
                'event_name': (row.get('event_name') or '').strip(),
                'event_date': dt,
            })
    evs.sort(key=lambda x: (x['event_date'], x['event_name']))
    return evs


def build_index(dates):
    return {d: i for i, d in enumerate(dates)}


def nearest_on_or_after(dates, target):
    # binary search
    lo, hi = 0, len(dates) - 1
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] >= target:
            ans = dates[mid]
            hi = mid - 1
        else:
            lo = mid + 1
    return ans


def compute_returns(dates, prices_by_ticker):
    rets = defaultdict(dict)
    for t, series in prices_by_ticker.items():
        last_px = None
        for d in dates:
            px = series.get(d)
            if px is None:
                last_px = None if last_px is None else last_px
                continue
            if last_px is not None and last_px != 0:
                rets[t][d] = (px / last_px) - 1.0
            last_px = px
    return rets


def cumprod(vals):
    p = 1.0
    c = 0
    for v in vals:
        if v is None:
            continue
        p *= (1.0 + v)
        c += 1
    return (p - 1.0) if c > 0 else None

def build_cumprod(dates, rets_by_ticker):
    """Precompute prefix cumulative products for each ticker across all dates.
    cp[t][i] = product_{k<=i} (1 + r_k) where r_k may be missing; missing steps keep cp unchanged.
    """
    cp = {}
    for t, rmap in rets_by_ticker.items():
        pref = []
        p = 1.0
        for d in dates:
            r = rmap.get(d)
            if r is not None:
                p *= (1.0 + r)
            pref.append(p)
        cp[t] = pref
    return cp


def main():
    ap = argparse.ArgumentParser(description='Compute additional reaction windows (±N) and merge into reaction_long.csv')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--data-dir', default=None)
    ap.add_argument('--out-dir', default=None)
    ap.add_argument('--windows', default='5,10,21', help='Comma-separated half-windows (days), e.g., 5,10,21')
    args = ap.parse_args()

    base_data = args.data_dir or os.path.join('data', args.market)
    base_out = args.out_dir or os.path.join('out', args.market)
    os.makedirs(base_out, exist_ok=True)

    tick_map = read_tickers(os.path.join(base_data, 'tickers.csv'))
    dates, prices = read_prices(os.path.join(base_data, 'prices.csv'))
    if not dates:
        raise SystemExit('No price dates found')
    rets = compute_returns(dates, prices)
    # prefix cumulative products for fast window queries
    cp = build_cumprod(dates, rets)
    idx = build_index(dates)
    evs = read_events(os.path.join(base_data, 'macro_events.csv'))

    # pre-build per-sector membership
    sector_members = defaultdict(list)
    for t, s in tick_map.items():
        sector_members[s].append(t)

    wins = [int(x.strip()) for x in args.windows.split(',') if x.strip()]
    # compute sector cumulative returns around events
    # results[win][(event_id, sector)] -> cumret
    results = {w: {} for w in wins}
    for ev in evs:
        d0 = nearest_on_or_after(dates, ev['event_date'])
        if d0 is None:
            continue
        i0 = idx[d0]
        for s, members in sector_members.items():
            # collect per-ticker cumulative via prefix products
            for w in wins:
                lo = max(0, i0 - w)
                hi = min(len(dates) - 1, i0 + w)
                vals = []
                for t in members:
                    pref = cp.get(t)
                    if not pref:
                        continue
                    c_hi = pref[hi]
                    c_lo = pref[lo]
                    if c_lo is None or c_lo == 0:
                        continue
                    cr = (c_hi / c_lo) - 1.0
                    vals.append(cr)
                avg = (sum(vals)/len(vals)) if vals else None
                results[w][(ev['event_id'], s, ev['event_name'], d0)] = avg

    # write heatmaps and merge into reaction_long
    for w in wins:
        outp = os.path.join(base_out, f'reaction_heatmap_win{w}.csv')
        with open(outp, 'w', newline='', encoding='utf-8') as f:
            wtr = csv.writer(f)
            wtr.writerow(['event_date','event_name','sector',f'win{w}_cum_avg'])
            for (eid, sect, name, d0), val in results[w].items():
                wtr.writerow([d0.strftime('%Y-%m-%d'), name, sect, f"{val:.6f}" if val is not None else ''])

    # merge into reaction_long
    rl_path = os.path.join(base_out, 'reaction_long.csv')
    if os.path.exists(rl_path):
        import pandas as pd
        rl = pd.read_csv(rl_path, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
        # clean up any previously merged duplicate window columns (_x/_y) and existing targets
        drop_cols = []
        for w in wins:
            base = f'win{w}_cum_avg'
            for suf in ['', '_x', '_y']:
                col = f'{base}{suf}'
                if col in rl.columns:
                    drop_cols.append(col)
        if drop_cols:
            rl = rl.drop(columns=drop_cols, errors='ignore')
        # ensure keys
        if 'event_id' in rl.columns and 'sector' in rl.columns:
            for w in wins:
                rows = []
                for (eid, sect, name, d0), val in results[w].items():
                    rows.append({'event_id': eid, 'sector': sect, f'win{w}_cum_avg': val})
                if rows:
                    dfw = pd.DataFrame(rows)
                    rl = rl.merge(dfw, on=['event_id','sector'], how='left')
            rl.to_csv(rl_path, index=False)


if __name__ == '__main__':
    main()
