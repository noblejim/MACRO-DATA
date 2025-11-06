#!/usr/bin/env python3
import os
import argparse
import pandas as pd


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def read_csv_safe(path, dtype=None):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=dtype)
    except Exception:
        return None


def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def pivot_heatmap(df, index_cols, column_col, value_col, agg='mean'):
    if df is None or df.empty:
        return None
    df = df.copy()
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    pv = pd.pivot_table(df, index=index_cols, columns=column_col, values=value_col, aggfunc=agg)
    pv = pv.sort_index()
    # flatten index to columns for writing convenience
    if isinstance(pv.index, pd.MultiIndex):
        pv = pv.reset_index()
    else:
        pv = pv.reset_index()
    return pv


def write_df(ws, start_row, start_col, df):
    # returns written range bottom-right (row, col)
    r, c = start_row, start_col
    # headers
    for j, col in enumerate(df.columns):
        ws.write(r, c + j, col)
    # rows
    for i in range(len(df)):
        for j, col in enumerate(df.columns):
            ws.write(r + 1 + i, c + j, df.iloc[i, j])
    return r + 1 + len(df) - 1, c + len(df.columns) - 1


def add_colorscale(workbook, worksheet, first_row, first_col, last_row, last_col, reverse=False, is_percentage=False, palette='orange'):
    # Apply a 3-color scale conditional format
    # reverse=True → lower is better (e.g., ranks)
    cf_range = (first_row, first_col, last_row, last_col)
    # Orange-forward default palette
    if palette == 'orange':
        if reverse:
            # lower values better (ranks): stronger orange at min
            min_color = '#FFA500'  # orange
            mid_color = '#FFE0B2'  # light orange
            max_color = '#FFFFFF'  # white
        else:
            # higher values better (returns): stronger orange at max
            min_color = '#FFFFFF'
            mid_color = '#FFE0B2'
            max_color = '#FFA500'
    else:
        # fallback blue-red
        min_color = '#5A8DEE' if not reverse else '#FF6961'
        mid_color = '#FFFFFF'
        max_color = '#FF6961' if not reverse else '#5A8DEE'
    worksheet.conditional_format(first_row, first_col, last_row, last_col, {
        'type': '3_color_scale',
        'min_color': min_color,
        'mid_color': mid_color,
        'max_color': max_color,
    })
    if is_percentage:
        percent_fmt = workbook.add_format({'num_format': '0.00%'})
        worksheet.set_column(first_col, last_col, 12, percent_fmt)
    else:
        worksheet.set_column(first_col, last_col, 12)


def build_dashboard(market='kr', out_dir=None, dashboard_path=None):
    base_out = out_dir or os.path.join('out', market)
    ensure_dir(os.path.dirname(dashboard_path))

    # Load CSVs
    t0 = read_csv_safe(os.path.join(base_out, 'reaction_heatmap_t0.csv'))
    w1 = read_csv_safe(os.path.join(base_out, 'reaction_heatmap_win1.csv'))
    w3 = read_csv_safe(os.path.join(base_out, 'reaction_heatmap_win3.csv'))
    ranks = read_csv_safe(os.path.join(base_out, 'sector_cycle_rank.csv'))
    regimes = read_csv_safe(os.path.join(base_out, 'sector_cycle_regime_avg.csv'))
    with_cycle = read_csv_safe(os.path.join(base_out, 'reaction_with_cycle.csv'))

    # Pivot into wide matrices
    t0_pv = pivot_heatmap(t0, ['event_date', 'event_name'], 'sector', 't0_return_avg')
    w1_pv = pivot_heatmap(w1, ['event_date', 'event_name'], 'sector', 'win1_cum_avg')
    w3_pv = pivot_heatmap(w3, ['event_date', 'event_name'], 'sector', 'win3_cum_avg')

    rank21_pv = None
    if ranks is not None and not ranks.empty:
        # choose rank_21 when available
        if 'rank_21' in ranks.columns:
            ranks['rank_21'] = pd.to_numeric(ranks['rank_21'], errors='coerce')
            rank21_pv = pd.pivot_table(ranks, index=['date'], columns='sector', values='rank_21', aggfunc='mean').reset_index()
        else:
            # fallback to first rank_* column
            rank_cols = [c for c in ranks.columns if c.startswith('rank_')]
            if rank_cols:
                col = rank_cols[0]
                ranks[col] = pd.to_numeric(ranks[col], errors='coerce')
                rank21_pv = pd.pivot_table(ranks, index=['date'], columns='sector', values=col, aggfunc='mean').reset_index()
        # also build rank_63 and rank_126 if present
        rank63_pv = None
        rank126_pv = None
        if 'rank_63' in ranks.columns:
            ranks['rank_63'] = pd.to_numeric(ranks['rank_63'], errors='coerce')
            rank63_pv = pd.pivot_table(ranks, index=['date'], columns='sector', values='rank_63', aggfunc='mean').reset_index()
        if 'rank_126' in ranks.columns:
            ranks['rank_126'] = pd.to_numeric(ra