#!/usr/bin/env python3
import os
import argparse
import math
import pandas as pd
try:
    from scripts.utils_surprise import ensure_surprise_z_on_frame, compute_surprise_z_from_events_df
except Exception:
    from utils_surprise import ensure_surprise_z_on_frame, compute_surprise_z_from_events_df

def read_csv_robust(path, usecols=None):
    if not os.path.exists(path):
        return None
    # try utf-8-sig, python engine, skip bad lines
    try:
        return pd.read_csv(path, usecols=usecols, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    except Exception:
        try:
            return pd.read_csv(path, usecols=usecols, engine='python', on_bad_lines='skip')
        except Exception:
            try:
                return pd.read_csv(path, usecols=usecols)
            except Exception:
                return None


def compute_surprise_z_from_events(events_path):
    if not os.path.exists(events_path):
        return None
    ev = read_csv_robust(events_path)
    if ev is None or ev.empty:
        return None
    return compute_surprise_z_from_events_df(ev)


def ols_slope_t(x, y):
    # returns slope, t_stat, n
    df = pd.DataFrame({'x': x, 'y': y}).dropna()
    n = len(df)
    if n < 3:
        return None, None, n
    x = df['x'].astype(float)
    y = df['y'].astype(float)
    vx = x.var(ddof=1)
    if vx == 0 or pd.isna(vx):
        return None, None, n
    cov = ((x - x.mean()) * (y - y.mean())).sum() / (n - 1)
    beta = cov / vx
    resid = y - (beta * x + (y.mean() - beta * x.mean()))
    s2 = (resid ** 2).sum() / (n - 2)
    se = math.sqrt(s2 / ((x - x.mean()) ** 2).sum()) if ((x - x.mean()) ** 2).sum() != 0 else None
    t = (beta / se) if (se not in (None, 0)) else None
    return beta, t, n


def compute_impact(df, value_cols=None, event_key='event_type'):
    rows = []
    # auto-detect metrics
    if value_cols is None:
        cand = [c for c in df.columns if (c == 't0_return_avg') or (c.startswith('win') and c.endswith('_cum_avg'))]
        value_cols = sorted(set(cand), key=lambda x: (x!='t0_return_avg', x))
    # fallback to event_name if event_type not available
    key = event_key if event_key in df.columns else ('event_name' if 'event_name' in df.columns else None)
    if key is None:
        return pd.DataFrame(rows)
    for (evt_type, sector), g in df.groupby([key, 'sector']):
        x = pd.to_numeric(g['surprise_z'], errors='coerce')
        for col in value_cols:
            y = pd.to_numeric(g[col], errors='coerce')
            beta, t, n = ols_slope_t(x, y)
            # up/down asymmetry
            beta_pos, t_pos, n_pos = ols_slope_t(x[x > 0], y[x > 0])
            beta_neg, t_neg, n_neg = ols_slope_t(x[x < 0], y[x < 0])
            rows.append({
                'event_type': evt_type,
                'sector': sector,
                'metric': col,
                'n': n,
                'beta': beta,
                't_stat': t,
                'beta_pos': beta_pos,
                't_pos': t_pos,
                'n_pos': n_pos,
                'beta_neg': beta_neg,
                't_neg': t_neg,
                'n_neg': n_neg,
            })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description='Compute macro impact (sector sensitivity to surprise z)')
    ap.add_argument('--market', choices=['us','kr'], default='kr')
    ap.add_argument('--out-dir', default=None, help='Directory with reaction_long.csv (defaults to out/<market>)')
    ap.add_argument('--output', default=None, help='Path to write macro_impact.csv (defaults to out/<market>/macro_impact.csv)')
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    inp = os.path.join(out_dir, 'reaction_long.csv')
    if not os.path.exists(inp):
        raise SystemExit(f'Missing input: {inp}')
    df = pd.read_csv(inp)
    # ensure grouping key present
    if 'event_type' not in df.columns and 'event_name' not in df.columns:
        df['event_name'] = ''
    # attach/ensure surprise_z using shared util
    data_dir = os.path.join('data', args.market)
    df = ensure_surprise_z_on_frame(df, args.market, data_dir=data_dir)

    # choose grouping key by availability
    group_key = 'event_type' if ('event_type' in df.columns and df['event_type'].astype(str).str.len().gt(0).any()) else 'event_name'
    res = compute_impact(df, value_cols=None, event_key=group_key)
    out_path = args.output or os.path.join(out_dir, 'macro_impact.csv')
    res.to_csv(out_path, index=False)
    print(f'Wrote {out_path}')


if __name__ == '__main__':
    main()
