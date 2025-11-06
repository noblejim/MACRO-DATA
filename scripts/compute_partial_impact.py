#!/usr/bin/env python3
import os
import argparse
import math
import pandas as pd
try:
    from scripts.utils_surprise import ensure_surprise_z_on_frame
except Exception:
    from utils_surprise import ensure_surprise_z_on_frame


def ols_beta_t(X, y, col):
    # X: DataFrame with intercept and regressors, y: Series, col: target coef name
    # Returns (beta, t, n)
    df = pd.concat([X, y.rename('y')], axis=1).dropna()
    n = len(df)
    if n < 3:
        return None, None, n
    # build design matrix
    cols = X.columns.tolist()
    try:
        import numpy as np
    except Exception:
        # fallback without numpy: compute via normal equations using pandas
        # X'X and X'y
        XtX = df[cols].T.dot(df[cols])
        try:
            XtX_inv = XtX.astype(float).values
            # matrix inverse via pandas/numpy not available; abort
            return None, None, n
        except Exception:
            return None, None, n
    # use numpy if available
    import numpy as np
    Xmat = df[cols].to_numpy(dtype=float)
    yv = df['y'].to_numpy(dtype=float)
    try:
        beta = np.linalg.lstsq(Xmat, yv, rcond=None)[0]
    except Exception:
        return None, None, n
    yhat = Xmat.dot(beta)
    resid = yv - yhat
    k = Xmat.shape[1]
    if n <= k:
        return None, None, n
    s2 = (resid**2).sum() / (n - k)
    try:
        XtX_inv = np.linalg.inv(Xmat.T.dot(Xmat))
    except Exception:
        return None, None, n
    # find index of target col
    try:
        j = cols.index(col)
    except ValueError:
        return None, None, n
    se = math.sqrt(s2 * XtX_inv[j, j]) if XtX_inv[j, j] > 0 else None
    b = float(beta[j])
    t = (b / se) if (se not in (None, 0)) else None
    return b, t, n


def main():
    ap = argparse.ArgumentParser(description='Partial impact: t0_return_avg ~ surprise_z + rank_21 (controls) by event_type×sector')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--out-dir', default=None)
    ap.add_argument('--by-regime', action='store_true', help='Also compute partial impact within each regime window and write partial_impact_by_regime.csv')
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    path = os.path.join(out_dir, 'reaction_with_cycle.csv')
    if not os.path.exists(path):
        raise SystemExit(f'Missing {path}')
    df = pd.read_csv(path, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    if 'surprise_z' not in df.columns or pd.to_numeric(df.get('surprise_z'), errors='coerce').isna().mean() > 0.8:
        df = ensure_surprise_z_on_frame(df, args.market, data_dir=os.path.join('data', args.market))
    for c in ['t0_return_avg','surprise_z','rank_21','t0_date']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce') if c != 't0_date' else pd.to_datetime(df[c], errors='coerce')
    rows = []
    def compute_partial(g, tag=None):
        if g[['t0_return_avg','surprise_z']].dropna().shape[0] < 5:
            return None
        X = pd.DataFrame({'const': 1.0, 'surprise_z': g['surprise_z'], 'rank_21': g.get('rank_21')})
        y = g['t0_return_avg']
        b, t, n = ols_beta_t(X, y, 'surprise_z')
        return b, t, n
    # overall
    for (et, sec), g in df.groupby(['event_type','sector']):
        res = compute_partial(g)
        if res:
            b, t, n = res
            rows.append({'scope': 'overall', 'event_type': et, 'sector': sec, 'metric': 't0_return_avg', 'beta_partial': b, 't_stat': t, 'n': n})
    if args.by_regime:
        regp = os.path.join('data', args.market, 'macro_regimes.csv')
        if os.path.exists(regp):
            try:
                reg = pd.read_csv(regp, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
                reg['start_date'] = pd.to_datetime(reg['start_date'], errors='coerce')
                reg['end_date'] = pd.to_datetime(reg['end_date'], errors='coerce')
                for _, rr in reg.iterrows():
                    sd, ed, name = rr['start_date'], rr['end_date'], rr.get('regime','')
                    if pd.isna(sd) or pd.isna(ed):
                        continue
                    mask = (df.get('t0_date') >= sd) & (df.get('t0_date') <= ed)
                    sub = df[mask]
                    for (et, sec), g in sub.groupby(['event_type','sector']):
                        res = compute_partial(g)
                        if res:
                            b, t, n = res
                            rows.append({'scope': f'regime:{name}', 'event_type': et, 'sector': sec, 'metric': 't0_return_avg', 'beta_partial': b, 't_stat': t, 'n': n})
            except Exception:
                pass
    if not rows:
        print('No partial impact rows computed')
        return
    out = pd.DataFrame(rows)
    out_path = os.path.join(out_dir, 'partial_impact.csv')
    out.to_csv(out_path, index=False)
    print(f'Wrote {out_path}')
    # by-regime extract
    if args.by_regime:
        out_reg = out[out['scope'].str.startswith('regime:', na=False)].copy()
        if not out_reg.empty:
            out_reg.to_csv(os.path.join(out_dir, 'partial_impact_by_regime.csv'), index=False)
            print('Wrote partial_impact_by_regime.csv')

if __name__ == '__main__':
    main()
