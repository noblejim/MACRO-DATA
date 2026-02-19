#!/usr/bin/env python3
"""build_summary.py - 파이프라인 결과 요약 JSON 생성.

Claude가 매 세션마다 CSV 원본을 다시 분석하지 않도록,
핵심 결과를 out/{market}/summary.json 으로 압축한다.

Usage:
    python scripts/build_summary.py --market us
    python scripts/build_summary.py --market kr
    python scripts/build_summary.py              # both
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(market, folder, fname):
    """Read CSV from data/ or out/ with silent failure."""
    path = os.path.join(BASE, folder, market, fname)
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, encoding='utf-8-sig')
    except Exception:
        return None


def _read_json(market, folder, fname):
    path = os.path.join(BASE, folder, market, fname)
    if not os.path.exists(path):
        return None
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def _safe_val(v):
    """Convert numpy/pandas types to JSON-serializable Python types."""
    if pd.isna(v):
        return None
    if hasattr(v, 'item'):
        return v.item()
    return v

# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def build_meta(market):
    meta = _read_json(market, 'out', 'macro_impact_meta.json')
    if meta:
        return {
            'generated_at': meta.get('generated_at'),
            'n_events': meta.get('n_events'),
            'n_sectors': meta.get('n_sectors'),
            'n_tests': meta.get('n_tests_total'),
            'n_significant_fdr05': meta.get('n_significant_fdr05'),
        }
    return {}


def build_significant_impacts(market, top_n=15):
    """Top N significant macro-sector impacts by |t_stat|."""
    df = _read(market, 'out', 'macro_impact.csv')
    if df is None or df.empty:
        return []
    # Filter significant only
    sig = df[df['significant_bh'] == True].copy()
    if sig.empty:
        # Fallback: top by |t_stat| even if not significant
        sig = df.dropna(subset=['t_stat']).copy()
        sig['_abs_t'] = sig['t_stat'].abs()
        sig = sig.nlargest(top_n, '_abs_t')
        return [
            {
                'event': row['event_type'],
                'sector': row['sector'],
                'metric': row['metric'],
                'beta': _safe_val(round(row['beta'], 5)) if pd.notna(row.get('beta')) else None,
                't_stat': _safe_val(round(row['t_stat'], 3)),
                'p_adj': _safe_val(round(row['p_adj_bh'], 4)) if pd.notna(row.get('p_adj_bh')) else None,
                'significant': False,
                'n': _safe_val(row['n']),
            }
            for _, row in sig.iterrows()
        ]
    sig['_abs_t'] = sig['t_stat'].abs()
    sig = sig.nlargest(top_n, '_abs_t')
    return [
        {
            'event': row['event_type'],
            'sector': row['sector'],
            'metric': row['metric'],
            'beta': _safe_val(round(row['beta'], 5)),
            't_stat': _safe_val(round(row['t_stat'], 3)),
            'p_adj': _safe_val(round(row['p_adj_bh'], 4)) if pd.notna(row.get('p_adj_bh')) else None,
            'significant': True,
            'n': _safe_val(row['n']),
        }
        for _, row in sig.iterrows()
    ]


def build_regime_sectors(market, top_n=5):
    """Current regime top/bottom sectors by avg daily return."""
    regimes = _read(market, 'data', 'macro_regimes.csv')
    regime_avg = _read(market, 'out', 'sector_cycle_regime_avg.csv')
    if regimes is None or regime_avg is None:
        return {}

    # Find current regime (last row by end_date)
    regimes['end_date'] = pd.to_datetime(regimes['end_date'])
    current = regimes.sort_values('end_date').iloc[-1]
    regime_name = current['regime']
    start = str(current.get('start_date', ''))
    end = str(current['end_date'].date())

    # Filter regime_avg to current regime period
    mask = (
        (regime_avg['regime'] == regime_name) &
        (regime_avg['start_date'] == current.get('start_date', current['end_date']))
    )
    sub = regime_avg[mask].copy() if mask.any() else regime_avg[regime_avg['regime'] == regime_name].copy()

    if sub.empty:
        # Fallback: latest regime block
        regime_avg['end_date_dt'] = pd.to_datetime(regime_avg['end_date'])
        latest_end = regime_avg['end_date_dt'].max()
        sub = regime_avg[regime_avg['end_date_dt'] == latest_end].copy()

    if sub.empty:
        return {'regime': regime_name, 'period': f'{start} ~ {end}'}

    sub = sub.sort_values('avg_daily_return', ascending=False)
    top = sub.head(top_n)
    bottom = sub.tail(top_n).sort_values('avg_daily_return')

    return {
        'regime': regime_name,
        'period': f'{start} ~ {end}',
        'top_sectors': [
            {'sector': r['sector'], 'avg_daily_return': _safe_val(round(r['avg_daily_return'], 6))}
            for _, r in top.iterrows()
        ],
        'bottom_sectors': [
            {'sector': r['sector'], 'avg_daily_return': _safe_val(round(r['avg_daily_return'], 6))}
            for _, r in bottom.iterrows()
        ],
    }


def build_focus_summary(market):
    """Focus event top/bottom sectors (CPI, PCE, NFP, FOMC)."""
    df = _read(market, 'out', 'focus_top_bottom.csv')
    if df is None or df.empty:
        return []
    results = []
    for focus in df['focus'].unique():
        sub = df[df['focus'] == focus]
        tops = sub[sub['rank_type'] == 'Top'][['sector', 'metric', 'value']].to_dict('records')
        bots = sub[sub['rank_type'] == 'Bottom'][['sector', 'metric', 'value']].to_dict('records')
        results.append({
            'focus': focus,
            'top_sectors': [
                {'sector': r['sector'], 'metric': r['metric'], 'value': _safe_val(round(r['value'], 6)) if r.get('value') else None}
                for r in tops[:3]
            ],
            'bottom_sectors': [
                {'sector': r['sector'], 'metric': r['metric'], 'value': _safe_val(round(r['value'], 6)) if r.get('value') else None}
                for r in bots[:3]
            ],
        })
    return results


def build_momentum_snapshot(market, windows=(21, 63, 126)):
    """Latest sector momentum ranks."""
    df = _read(market, 'out', 'sector_cycle_rank.csv')
    if df is None or df.empty:
        return {}
    df['date'] = pd.to_datetime(df['date'])
    latest_date = df['date'].max()
    latest = df[df['date'] == latest_date].copy()

    snapshot = {'date': str(latest_date.date()), 'sectors': {}}
    for _, row in latest.iterrows():
        sector = row['sector']
        if sector not in snapshot['sectors']:
            snapshot['sectors'][sector] = {}
        for w in windows:
            mom_col = f'mom_{w}'
            rank_col = f'rank_{w}'
            if mom_col in row.index and rank_col in row.index:
                snapshot['sectors'][sector][f'mom_{w}'] = _safe_val(round(row[mom_col], 5)) if pd.notna(row[mom_col]) else None
                snapshot['sectors'][sector][f'rank_{w}'] = _safe_val(row[rank_col])
    return snapshot


def build_data_quality(market):
    """Data quality metrics."""
    events = _read(market, 'data', 'macro_events.csv')
    if events is None:
        return {}

    total = len(events)
    has_actual = events['actual_value'].notna().sum()
    has_expected = events['expected_value'].notna().sum()

    # Date range
    events['event_date'] = pd.to_datetime(events['event_date'], errors='coerce')
    min_date = str(events['event_date'].min().date()) if events['event_date'].notna().any() else None
    max_date = str(events['event_date'].max().date()) if events['event_date'].notna().any() else None

    # Event types with no actual data
    by_type = events.groupby('event_type')['actual_value'].apply(lambda s: s.notna().sum())
    empty_types = by_type[by_type == 0].index.tolist()

    # Recent 30d fill rate
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
    recent = events[events['event_date'] >= cutoff]
    recent_fill = recent['actual_value'].notna().mean() if len(recent) > 0 else None

    return {
        'total_events': int(total),
        'actual_fill_rate': round(int(has_actual) / total, 3) if total else 0,
        'expected_fill_rate': round(int(has_expected) / total, 3) if total else 0,
        'date_range': f'{min_date} ~ {max_date}',
        'event_types_no_data': empty_types,
        'recent_30d_fill_rate': round(float(recent_fill), 3) if recent_fill is not None else None,
    }


def build_recent_events(market, n=10):
    """Most recent N events with actual values."""
    events = _read(market, 'data', 'macro_events.csv')
    if events is None:
        return []
    events['event_date'] = pd.to_datetime(events['event_date'], errors='coerce')
    filled = events.dropna(subset=['actual_value']).sort_values('event_date', ascending=False).head(n)
    return [
        {
            'event_type': row['event_type'],
            'date': str(row['event_date'].date()),
            'actual': _safe_val(row['actual_value']),
            'expected': _safe_val(row['expected_value']) if pd.notna(row.get('expected_value')) else None,
        }
        for _, row in filled.iterrows()
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_summary(market):
    summary = {
        'market': market,
        'summary_generated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'meta': build_meta(market),
        'data_quality': build_data_quality(market),
        'significant_impacts': build_significant_impacts(market),
        'current_regime': build_regime_sectors(market),
        'focus_events': build_focus_summary(market),
        'momentum_snapshot': build_momentum_snapshot(market),
        'recent_events': build_recent_events(market),
    }

    out_path = os.path.join(BASE, 'out', market, 'summary.json')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'[{market.upper()}] summary.json written → {out_path}')
    return summary


def main():
    parser = argparse.ArgumentParser(description='Build pipeline summary JSON')
    parser.add_argument('--market', choices=['us', 'kr'], default=None,
                        help='Market to summarize (default: both)')
    args = parser.parse_args()

    markets = [args.market] if args.market else ['us', 'kr']
    for m in markets:
        build_summary(m)
    print('Done.')


if __name__ == '__main__':
    main()
