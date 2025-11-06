#!/usr/bin/env python3
import os
import argparse
import pandas as pd
try:
    from scripts.utils_surprise import ensure_surprise_z_on_frame
except Exception:
    from utils_surprise import ensure_surprise_z_on_frame
from datetime import timedelta


FOCUS_GROUPS = {
    'CPI': lambda et: isinstance(et, str) and et.startswith('CPI'),
    'PCE': lambda et: isinstance(et, str) and et.startswith('PCE'),
    'NFP': lambda et: et == 'NFP',
    'FOMC': lambda et: et == 'FOMC',
}


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


def ensure_surprise_z(df: pd.DataFrame, market: str) -> pd.DataFrame:
    return ensure_surprise_z_on_frame(df, market, data_dir=os.path.join('data', market))


def event_to_focus(et: str):
    for k, fn in FOCUS_GROUPS.items():
        try:
            if fn(et):
                return k
        except Exception:
            continue
    return None


def detect_metrics(df: pd.DataFrame):
    base = []
    if 't0_return_avg' in df.columns:
        base.append('t0_return_avg')
    wins = [c for c in df.columns if c.startswith('win') and c.endswith('_cum_avg')]
    wins = sorted(set(wins), key=lambda x: (len(x), x))
    return base + wins


def build_top_bottom(rl: pd.DataFrame) -> pd.DataFrame:
    df = rl.copy()
    # detect and coerce metrics
    metrics = detect_metrics(df)
    for c in metrics:
        df[c] = pd.to_numeric(df.get(c), errors='coerce')
    df['focus'] = df['event_type'].apply(event_to_focus)
    df = df.dropna(subset=['focus','sector'])
    rows = []
    for f, g in df.groupby('focus'):
        for m in metrics:
            agg = g.groupby('sector', as_index=False)[m].mean().dropna(subset=[m])
            if agg.empty:
                continue
            top3 = agg.sort_values(m, ascending=False).head(3)
            bot3 = agg.sort_values(m, ascending=True).head(3)
            for _, r in top3.iterrows():
                rows.append({'focus': f, 'metric': m, 'rank_type': 'Top', 'sector': r['sector'], 'value': r[m]})
            for _, r in bot3.iterrows():
                rows.append({'focus': f, 'metric': m, 'rank_type': 'Bottom', 'sector': r['sector'], 'value': r[m]})
    return pd.DataFrame(rows)


def build_quantile_focus(rl: pd.DataFrame, qn: int, market: str) -> pd.DataFrame:
    # compute quantiles by event_type×sector on surprise_z
    df = rl.copy()
    df = ensure_surprise_z(df, market)
    metrics = detect_metrics(df) + ['surprise_z']
    for c in metrics:
        df[c] = pd.to_numeric(df.get(c), errors='coerce')
    df['focus'] = df['event_type'].apply(event_to_focus)
    df = df.dropna(subset=['focus','sector','surprise_z'])
    rows = []
    for (f, sec), g in df.groupby(['focus','sector']):
        if g['surprise_z'].notna().sum() < qn:
            continue
        q = pd.qcut(g['surprise_z'], qn, labels=False, duplicates='drop')
        g = g.copy(); g['quantile'] = (q + 1)
        # aggregate dynamically across available metrics
        agg_dict = {m: ('%s' % m, 'mean') for m in detect_metrics(g)}
        agg_dict['n'] = ('surprise_z','count')
        agg = g.groupby('quantile', as_index=False).agg(**agg_dict)
        for _, r in agg.iterrows():
            row = {'focus': f, 'sector': sec, 'quantile': int(r['quantile']), 'n': int(r['n'])}
            for m in detect_metrics(g):
                row[m] = r[m]
            rows.append(row)
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description='Focus report for CPI/PCE/NFP/FOMC: Top/Bottom sectors and Quantile breakdown')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--out-dir', default=None)
    ap.add_argument('--last-days', type=int, default=180, help='Use only events within the last N days (default 180). 0 disables filtering')
    ap.add_argument('--last-events', type=int, default=100, help='Use only the last N distinct event dates (default 100). 0 disables')
    ap.add_argument('--regime-current', action='store_true', help='Also compute Top/Bottom limited to the current regime window')
    ap.add_argument('--quantiles', type=int, default=5)
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    rl_path = os.path.join(out_dir, 'reaction_long.csv')
    rl = read_csv_robust(rl_path)
    if rl is None or rl.empty:
        raise SystemExit('Missing reaction_long.csv')

    # date filter – last-days
    if 'event_date' in rl.columns and args.last_days and args.last_days > 0:
        rl['event_date'] = pd.to_datetime(rl['event_date'], format='%Y-%m-%d', errors='coerce')
        if rl['event_date'].notna().any():
            maxd = rl['event_date'].dropna().max()
            mind = maxd - timedelta(days=args.last_days)
            rl = rl[(rl['event_date'] >= mind) & (rl['event_date'] <= maxd)]

    # event count filter – last-events
    if 'event_date' in rl.columns and args.last_events and args.last_events > 0:
        rl = rl.copy()
        rl['event_date'] = pd.to_datetime(rl['event_date'], format='%Y-%m-%d', errors='coerce')
        dates = rl['event_date'].dropna().sort_values().unique()
        if len(dates) > args.last_events:
            keep = set(dates[-args.last_events:])
            rl = rl[rl['event_date'].isin(keep)]

    # Top/Bottom
    topbot = build_top_bottom(rl)
    if not topbot.empty:
        topbot.to_csv(os.path.join(out_dir, 'focus_top_bottom.csv'), index=False)
        print('Wrote focus_top_bottom.csv')

    # Quantile breakdown (computed directly from filtered rl)
    qf = build_quantile_focus(rl, args.quantiles, args.market)
    if not qf.empty:
        qf.to_csv(os.path.join(out_dir, 'focus_by_quantile.csv'), index=False)
        print('Wrote focus_by_quantile.csv')

    # Regime-limited Top/Bottom & Quantile → current regime window only
    if args.regime_current:
        regp = os.path.join('data', args.market, 'macro_regimes.csv')
        reg = read_csv_robust(regp)
        if reg is not None and not reg.empty and 'event_date' in rl.columns:
            try:
                rl2 = rl.copy()
                rl2['event_date'] = pd.to_datetime(rl2['event_date'], format='%Y-%m-%d', errors='coerce')
                reg['start_date'] = pd.to_datetime(reg['start_date'], format='%Y-%m-%d', errors='coerce')
                reg['end_date'] = pd.to_datetime(reg['end_date'], format='%Y-%m-%d', errors='coerce')
                ref = rl2['event_date'].dropna().max()
                hit = reg[(reg['start_date'] <= ref) & (reg['end_date'] >= ref)]
                if not hit.empty:
                    sd, ed = hit.iloc[0]['start_date'], hit.iloc[0]['end_date']
                    rlf = rl2[(rl2['event_date'] >= sd) & (rl2['event_date'] <= ed)]
                    tb_reg = build_top_bottom(rlf)
                    if not tb_reg.empty:
                        tb_reg.to_csv(os.path.join(out_dir, 'focus_top_bottom_regime.csv'), index=False)
                        print('Wrote focus_top_bottom_regime.csv')
                    # Quantile within regime
                    global market
                    market = args.market
                    qf_reg = build_quantile_focus(rlf, args.quantiles, args.market)
                    if not qf_reg.empty:
                        qf_reg.to_csv(os.path.join(out_dir, 'focus_by_quantile_regime.csv'), index=False)
                        print('Wrote focus_by_quantile_regime.csv')
            except Exception:
                pass


if __name__ == '__main__':
    main()
