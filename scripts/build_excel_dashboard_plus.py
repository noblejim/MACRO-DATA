#!/usr/bin/env python3
import os
import argparse
import pandas as pd

# ------- small helpers -------

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def read_csv_safe(path, dtype=None):
    if not os.path.exists(path):
        return None
    try:
            return pd.read_csv(path, dtype=dtype, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
    except Exception:
        try:
            return pd.read_csv(path, dtype=dtype)
        except Exception:
            return None


def to_num(df, cols):
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
    pv = pv.reset_index()
    return pv


def add_colorscale(workbook, worksheet, first_row, first_col, last_row, last_col, reverse=False, is_percentage=False, palette='orange'):
    # Simple 3-color scale
    if palette == 'orange':
        if reverse:
            min_color = '#FFA500'; mid_color = '#FFE0B2'; max_color = '#FFFFFF'
        else:
            min_color = '#FFFFFF'; mid_color = '#FFE0B2'; max_color = '#FFA500'
    else:
        min_color = '#5A8DEE' if not reverse else '#FF6961'
        mid_color = '#FFFFFF'
        max_color = '#FF6961' if not reverse else '#5A8DEE'
    worksheet.conditional_format(first_row, first_col, last_row, last_col, {
        'type': '3_color_scale', 'min_color': min_color, 'mid_color': mid_color, 'max_color': max_color,
    })
    if is_percentage:
        pct = workbook.add_format({'num_format': '0.00%'})
        worksheet.set_column(first_col, last_col, 12, pct)
    else:
        worksheet.set_column(first_col, last_col, 12)


# ------- worksheet fetch helper -------

def get_ws(writer, name: str):
    ws = writer.sheets.get(name)
    if ws is not None:
        return ws
    # fallback to last created worksheet
    try:
        wss = writer.book.worksheets()
        if wss:
            return wss[-1]
    except Exception:
        pass
    # as a last resort create a generic sheet
    return writer.book.add_worksheet('Sheet')


# ------- workbook builders -------

def write_parameters_sheet(writer, market, last_days, last_events):
    ws = writer.book.add_worksheet('Parameters')
    ws.write(0, 0, 'Market'); ws.write(0, 1, market)
    ws.write(1, 0, 'Last Days'); ws.write(1, 1, last_days if last_days else '')
    ws.write(2, 0, 'Last Events'); ws.write(2, 1, last_events if last_events else '')
    return ws


def write_controls_sheet(writer):
    ws = writer.book.add_worksheet('Controls')
    ws.write(0, 0, 'Note')
    ws.write(0, 1, 'Use Excel slicers/filters on heatmap sheets. Parameters sheet shows filters used to build this workbook.')
    return ws


def write_overview(writer, out_dir):
    wb = writer.book
    ws = wb.add_worksheet('Overview')
    kpi_label = wb.add_format({'bold': True, 'align': 'center'})
    kpi_val = wb.add_format({'align': 'center'})
    kpi_pct = wb.add_format({'align': 'center', 'num_format': '0.00%'})

    # Load inputs for KPIs
    t0 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_t0.csv'))
    imp = read_csv_safe(os.path.join(out_dir, 'macro_impact.csv'))

    # Reference date and recent counts
    ref_date = None
    if t0 is not None and not t0.empty and 'event_date' in t0.columns:
        try:
            t0['event_date'] = pd.to_datetime(t0['event_date'], errors='coerce')
            ref_date = t0['event_date'].dropna().max()
        except Exception:
            ref_date = None

    ws.write(0, 0, 'Latest Event Date', kpi_label)
    ws.write(1, 0, ref_date.strftime('%Y-%m-%d') if ref_date is not None else '-', kpi_val)

    # Simple counts
    events_30 = ''
    if ref_date is not None and t0 is not None and not t0.empty:
        try:
            m = t0[(t0['event_date'] >= ref_date - pd.Timedelta(days=30)) & (t0['event_date'] <= ref_date)]
            events_30 = int(m[['event_name','event_date']].drop_duplicates().shape[0])
        except Exception:
            events_30 = ''
    ws.write(0, 2, 'Events (30d)', kpi_label)
    ws.write(1, 2, events_30 if events_30 != '' else '-', kpi_val)

    # Significant impacts (|t|>=1.96, n>=10) on t0
    sig_val = '-'
    try:
        if imp is not None and not imp.empty:
            df = imp.copy()
            df = df[df.get('metric') == 't0_return_avg']
            df['t_stat'] = pd.to_numeric(df.get('t_stat'), errors='coerce')
            df['n'] = pd.to_numeric(df.get('n'), errors='coerce')
            sig = df[(df['n'] >= 10) & (df['t_stat'].abs() >= 1.96)]
            sig_val = int(len(sig))
    except Exception:
        pass
    ws.write(0, 4, 'Sig Impacts (t0)', kpi_label)
    try:
        ws.write_number(1, 4, float(sig_val), kpi_val)
    except Exception:
        ws.write(1, 4, str(sig_val), kpi_val)

    # Quick guide
    ws.write(3, 0, 'Sheets', kpi_label)
    ws.write(4, 0, '• Reactions (t0/±1/±3/±5/±10/±21)')
    ws.write(5, 0, '• By Surprise Quantile (raw/t0)')
    ws.write(6, 0, '• Partial Impact (t0|rank) and Regime')
    ws.write(7, 0, '• Focus Top/Bottom, Focus Quantile (raw/regime)')
    ws.write(8, 0, '• Parameters, Controls')
    return ws


def write_heatmap_sheet(writer, name, df, is_pct=True):
    if df is None or df.empty:
        return
    df.to_excel(writer, sheet_name=name, index=False)
    ws = get_ws(writer, name)
    nrows, ncols = df.shape
    add_colorscale(writer.book, ws, 1, 2, nrows, ncols - 1, reverse=False, is_percentage=is_pct, palette='orange')
    ws.freeze_panes(1, 2)


# ------- main -------

def main():
    ap = argparse.ArgumentParser(description='Build advanced Excel dashboard workbook (US/KR) with parameters & controls')
    ap.add_argument('--market', choices=['us','kr'], default='us')
    ap.add_argument('--data-dir', default=None, help='Data dir (unused, for symmetry)')
    ap.add_argument('--out-dir', default=None, help='Source CSV directory (defaults to out/<market>)')
    ap.add_argument('--dashboard-path', default=None, help='Output .xlsx path (defaults to dashboards/<market>_dashboard.xlsx)')
    ap.add_argument('--last-days', type=int, default=365)
    ap.add_argument('--last-events', type=int, default=180)
    args = ap.parse_args()

    out_dir = args.out_dir or os.path.join('out', args.market)
    dash_dir = os.path.join('dashboards')
    ensure_dir(dash_dir)
    dashboard_path = args.dashboard_path or os.path.join(dash_dir, f'{args.market}_dashboard.xlsx')

    # Load inputs
    t0 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_t0.csv'))
    w1 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_win1.csv'))
    w3 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_win3.csv'))
    w5 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_win5.csv'))
    w10 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_win10.csv'))
    w21 = read_csv_safe(os.path.join(out_dir, 'reaction_heatmap_win21.csv'))
    rq = read_csv_safe(os.path.join(out_dir, 'reaction_by_surprise_quantile.csv'))
    imp = read_csv_safe(os.path.join(out_dir, 'macro_impact.csv'))
    pimp = read_csv_safe(os.path.join(out_dir, 'partial_impact.csv'))
    pimp_reg = read_csv_safe(os.path.join(out_dir, 'partial_impact_by_regime.csv'))
    ftop = read_csv_safe(os.path.join(out_dir, 'focus_top_bottom.csv'))
    ftop_reg = read_csv_safe(os.path.join(out_dir, 'focus_top_bottom_regime.csv'))
    fquant = read_csv_safe(os.path.join(out_dir, 'focus_by_quantile.csv'))
    fquant_reg = read_csv_safe(os.path.join(out_dir, 'focus_by_quantile_regime.csv'))
    react_long = read_csv_safe(os.path.join(out_dir, 'reaction_long.csv'))

    # Build workbook
    with pd.ExcelWriter(dashboard_path, engine='xlsxwriter') as writer:
        # --- Patch add_worksheet to sanitize sheet names globally ---
        created = set()
        orig_add = writer.book.add_worksheet
        invalid = set('[]:*?/\\')

        def _sanitize(name: str) -> str:
            if not name:
                return name
            s = ''.join(('-' if ch in invalid else ch) for ch in name)
            if len(s) > 31:
                s = s[:31]
            base = s
            i = 1
            while s in created:
                suff = f" ({i+1})"
                s = (base[: max(0, 31 - len(suff))] + suff)
                i += 1
            created.add(s)
            return s

        def _patched_add_worksheet(name=None):
            sname = _sanitize(name) if name is not None else name
            return orig_add(sname)

        writer.book.add_worksheet = _patched_add_worksheet
        # -------------------------------------------------------------

        # Meta sheets
        write_parameters_sheet(writer, args.market, args.last_days, args.last_events)
        write_controls_sheet(writer)
        write_overview(writer, out_dir)

        # Reactions
        t0_pv = pivot_heatmap(t0, ['event_date','event_name'], 'sector', 't0_return_avg')
        w1_pv = pivot_heatmap(w1, ['event_date','event_name'], 'sector', 'win1_cum_avg')
        w3_pv = pivot_heatmap(w3, ['event_date','event_name'], 'sector', 'win3_cum_avg')
        w5_pv = pivot_heatmap(w5, ['event_date','event_name'], 'sector', 'win5_cum_avg') if w5 is not None else None
        w10_pv = pivot_heatmap(w10, ['event_date','event_name'], 'sector', 'win10_cum_avg') if w10 is not None else None
        w21_pv = pivot_heatmap(w21, ['event_date','event_name'], 'sector', 'win21_cum_avg') if w21 is not None else None

        write_heatmap_sheet(writer, 'Reactions (t0)', t0_pv, True)
        write_heatmap_sheet(writer, 'Reactions (±1)', w1_pv, True)
        write_heatmap_sheet(writer, 'Reactions (±3)', w3_pv, True)
        write_heatmap_sheet(writer, 'Reactions (+/-5)', w5_pv, True)
        write_heatmap_sheet(writer, 'Reactions (+/-10)', w10_pv, True)
        write_heatmap_sheet(writer, 'Reactions (+/-21)', w21_pv, True)

        # Macro impact (heatmap by metric)
        if imp is not None and not imp.empty:
            imp = imp.copy()
            imp['t_stat'] = pd.to_numeric(imp.get('t_stat'), errors='coerce')
            for m in sorted(imp['metric'].dropna().unique().tolist()):
                sub = imp[imp['metric'] == m]
                pv = sub.pivot_table(index='event_type', columns='sector', values='t_stat', aggfunc='mean').reset_index()
                sheet = f'Macro Impact ({m})'
                pv.to_excel(writer, sheet_name=sheet, index=False)
                ws = get_ws(writer, sheet)
                r, c = pv.shape
                add_colorscale(writer.book, ws, 1, 1, r, c - 1, reverse=False, is_percentage=False, palette='orange')
                ws.freeze_panes(1, 1)

        # Reactions by Surprise Quantile
        if rq is not None and not rq.empty:
            rq.to_excel(writer, sheet_name='By Surprise Quantile (raw)', index=False)
            rq_t0 = rq.pivot_table(index=['event_type','quantile'], columns='sector', values='t0_return_avg', aggfunc='mean').reset_index()
            rq_t0.to_excel(writer, sheet_name='By Surprise Quantile (t0)', index=False)
            wsq = get_ws(writer, 'By Surprise Quantile (t0)')
            r, c = rq_t0.shape
            add_colorscale(writer.book, wsq, 1, 2, r, c - 1, reverse=False, is_percentage=True, palette='orange')
            wsq.freeze_panes(1, 2)

        # Partial Impact (overall)
        if pimp is not None and not pimp.empty:
            pv = pimp.pivot_table(index='event_type', columns='sector', values='beta_partial', aggfunc='mean').reset_index()
            pv.to_excel(writer, sheet_name='Partial Impact (t0|rank)', index=False)
            ws = get_ws(writer, 'Partial Impact (t0|rank)')
            r, c = pv.shape
            add_colorscale(writer.book, ws, 1, 1, r, c - 1, reverse=False, is_percentage=False, palette='orange')
            ws.freeze_panes(1, 1)

        # Partial Impact (by Regime)
        if pimp_reg is not None and not pimp_reg.empty and 'beta_partial' in pimp_reg.columns:
            pv = pimp_reg.pivot_table(index=['scope','event_type'], columns='sector', values='beta_partial', aggfunc='mean').reset_index()
            pv.to_excel(writer, sheet_name='Partial Impact (Regime)', index=False)

        # Focus Top/Bottom
        if ftop is not None and not ftop.empty:
            ftop.to_excel(writer, sheet_name='Focus Top/Bottom (raw)', index=False)
            for m in sorted(ftop['metric'].dropna().unique().tolist()):
                sub = ftop[ftop['metric'] == m].copy()
                pv = sub.pivot_table(index=['focus','rank_type'], columns='sector', values='value', aggfunc='mean').reset_index()
                sheet = f'Focus Top/Bottom ({m})'
                pv.to_excel(writer, sheet_name=sheet, index=False)
                ws = get_ws(writer, sheet)
                r, c = pv.shape
                add_colorscale(writer.book, ws, 1, 2, r, c - 1, reverse=False, is_percentage=True, palette='orange')
                ws.freeze_panes(1, 2)

        # Focus By Quantile (raw + t0 heatmap)
        if fquant is not None and not fquant.empty:
            fquant.to_excel(writer, sheet_name='Focus Quantile (raw)', index=False)
            if 't0_return_avg' in fquant.columns:
                pv = fquant.pivot_table(index=['focus','quantile'], columns='sector', values='t0_return_avg', aggfunc='mean').reset_index()
                sheet = 'Focus Quantile (t0)'
                pv.to_excel(writer, sheet_name=sheet, index=False)
                ws = get_ws(writer, sheet)
                r, c = pv.shape
                add_colorscale(writer.book, ws, 1, 2, r, c - 1, reverse=False, is_percentage=True, palette='orange')
                ws.freeze_panes(1, 2)

        # Focus (Regime) raw + t0 heatmap
        if ftop_reg is not None and not ftop_reg.empty:
            ftop_reg.to_excel(writer, sheet_name='Focus Top/Bottom (Regime)', index=False)
        if fquant_reg is not None and not fquant_reg.empty and 't0_return_avg' in fquant_reg.columns:
            pv = fquant_reg.pivot_table(index=['focus','quantile'], columns='sector', values='t0_return_avg', aggfunc='mean').reset_index()
            sheet = 'Focus Quantile (Regime t0)'
            pv.to_excel(writer, sheet_name=sheet, index=False)
            ws = get_ws(writer, sheet)
            r, c = pv.shape
            add_colorscale(writer.book, ws, 1, 2, r, c - 1, reverse=False, is_percentage=True, palette='orange')
            ws.freeze_panes(1, 2)

    print(f'Wrote: {dashboard_path}')


if __name__ == '__main__':
    main()
