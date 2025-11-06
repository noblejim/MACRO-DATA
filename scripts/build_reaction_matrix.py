#!/usr/bin/env python3
import csv
from collections import defaultdict
from datetime import datetime
import os
import argparse


# Paths
DEFAULT_DATA_DIR = os.path.join(os.getcwd(), 'data')
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), 'out')


def parse_date(s):
    return datetime.strptime(s, '%Y-%m-%d').date()


def read_tickers(path):
    mapping = {}
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            t = row['ticker'].strip()
            s = row['sector'].strip()
            if t:
                mapping[t] = s or 'UNKNOWN'
    return mapping


def read_prices(path):
    # returns: dates (sorted list), prices[ticker][date] = adj_close (float)
    prices = defaultdict(dict)
    dates_set = set()
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            dt = parse_date(row['date'].strip())
            t = row['ticker'].strip()
            try:
                px = float(row['adj_close'])
            except Exception:
                continue
            prices[t][dt] = px
            dates_set.add(dt)
    dates = sorted(dates_set)
    return dates, prices


def read_events(path):
    events = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                evt_date = parse_date(row['event_date'].strip())
            except Exception:
                continue
            # parse optional expected/actual
            def _to_float(x):
                try:
                    if x is None:
                        return None
                    x = x.strip()
                    if x == '':
                        return None
                    return float(x)
                except Exception:
                    return None
            expected_val = _to_float(row.get('expected_value'))
            actual_val = _to_float(row.get('actual_value'))
            events.append({
                'event_id': row.get('event_id', '').strip() or row.get('id', '').strip() or '',
                'event_name': row.get('event_name', '').strip() or row.get('name', '').strip() or '',
                'event_date': evt_date,
                'event_type': row.get('event_type', '').strip(),
                'importance': row.get('importance', '').strip(),
                'expected_value': expected_val,
                'actual_value': actual_val,
            })
    # sort by date for stable output
    events.sort(key=lambda x: (x['event_date'], x['event_name']))
    return events


def build_trading_index(dates):
    # Map date -> index and index -> date for quick window lookups
    idx = {d: i for i, d in enumerate(dates)}
    return idx


def nearest_trading_on_or_after(dates, date):
    # Binary search for date >= target
    lo, hi = 0, len(dates) - 1
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] >= date:
            ans = dates[mid]
            hi = mid - 1
        else:
            lo = mid + 1
    return ans


def compute_returns(dates, prices_by_ticker):
    # returns_by_ticker[ticker][date] = simple return (P_t/P_{t-1} - 1)
    returns = defaultdict(dict)
    for t, series in prices_by_ticker.items():
        # Build ordered list aligned to global dates; skip if missing P_{t-1}
        last_px = None
        last_dt = None
        # Iterate in global date order; if a date is missing for ticker, skip it (no return for that date)
        for d in dates:
            px = series.get(d)
            if px is None:
                last_px = px
                last_dt = d
                continue
            if last_px is not None:
                try:
                    r = (px / last_px) - 1.0
                except ZeroDivisionError:
                    r = None
                if r is not None:
                    returns[t][d] = r
            last_px = px
            last_dt = d
    return returns


def adjust_returns_for_benchmark(returns_by_ticker, benchmark_ticker):
    if not benchmark_ticker:
        return returns_by_ticker
    bench = returns_by_ticker.get(benchmark_ticker)
    if not bench:
        return returns_by_ticker
    # Subtract benchmark return where available
    for t, series in list(returns_by_ticker.items()):
        if t == benchmark_ticker:
            continue
        for d, r in list(series.items()):
            b = bench.get(d)
            if b is not None:
                series[d] = r - b
    return returns_by_ticker


def average(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def cumulative(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    # simple cumulative: product(1+r) - 1
    p = 1.0
    for v in values:
        p *= (1.0 + v)
    return p - 1.0


def window_indices(center_idx, w):
    return list(range(center_idx - w, center_idx + w + 1))


def compute_sector_reactions(dates, tick2sector, returns_by_ticker, events, win1=1, win3=3, sector_bench_map=None):
    date_index = build_trading_index(dates)

    # Pre-compute sector -> tickers
    sector_to_tickers = defaultdict(list)
    for t, s in tick2sector.items():
        sector_to_tickers[s].append(t)

    sectors = sorted(sector_to_tickers.keys())

    rows = []  # long-format rows

    for evt in events:
        evt_date = evt['event_date']
        t0_date = nearest_trading_on_or_after(dates, evt_date)
        if t0_date is None:
            # all data earlier than first trading day
            continue
        t0_idx = date_index[t0_date]

        # window indices
        win1_idx = window_indices(t0_idx, win1)
        win3_idx = window_indices(t0_idx, win3)

        for sector in sectors:
            tickers = sector_to_tickers[sector]

            # sector-specific benchmark returns (if provided)
            bench_ticker = None
            bench_returns = None
            if sector_bench_map:
                bench_ticker = sector_bench_map.get(sector)
                if bench_ticker:
                    bench_returns = returns_by_ticker.get(bench_ticker)

            # Collect event-day returns by ticker
            t0_rets = []
            win1_rets_by_ticker = []
            win3_rets_by_ticker = []

            for t in tickers:
                # event-day return
                r_t0 = returns_by_ticker[t].get(t0_date)
                if bench_returns is not None:
                    b = bench_returns.get(t0_date)
                    if b is not None and r_t0 is not None:
                        r_t0 = r_t0 - b
                if r_t0 is not None:
                    t0_rets.append(r_t0)

                # windows
                # build per-ticker list of returns over window if all dates present for ticker
                # partial windows allowed; cumulative will handle missing values by skipping them
                w1 = []
                for i in win1_idx:
                    if 0 <= i < len(dates):
                        rd = returns_by_ticker[t].get(dates[i])
                        if bench_returns is not None:
                            b = bench_returns.get(dates[i])
                            if b is not None and rd is not None:
                                rd = rd - b
                        if rd is not None:
                            w1.append(rd)
                if w1:
                    win1_rets_by_ticker.append(w1)

                w3 = []
                for i in win3_idx:
                    if 0 <= i < len(dates):
                        rd = returns_by_ticker[t].get(dates[i])
                        if bench_returns is not None:
                            b = bench_returns.get(dates[i])
                            if b is not None and rd is not None:
                                rd = rd - b
                        if rd is not None:
                            w3.append(rd)
                if w3:
                    win3_rets_by_ticker.append(w3)

            # Aggregate across tickers
            t0_avg = average(t0_rets)

            # For windows, use cumulative per ticker then average across tickers
            win1_cums = [cumulative(w) for w in win1_rets_by_ticker]
            win1_avg = average(win1_cums)

            win3_cums = [cumulative(w) for w in win3_rets_by_ticker]
            win3_avg = average(win3_cums)

            # surprise fields (repeat per sector row for Excel convenience)
            expected_val = evt.get('expected_value')
            actual_val = evt.get('actual_value')
            surprise = None
            surprise_pct = None
            if actual_val is not None and expected_val is not None:
                surprise = actual_val - expected_val
                if expected_val not in (0, None):
                    try:
                        surprise_pct = surprise / abs(expected_val)
                    except Exception:
                        surprise_pct = None

            rows.append({
                'event_id': evt.get('event_id', ''),
                'event_name': evt.get('event_name', ''),
                'event_date': evt.get('event_date', '').isoformat() if evt.get('event_date') else '',
                't0_date': t0_date.isoformat(),
                'event_type': evt.get('event_type', ''),
                'importance': evt.get('importance', ''),
                'sector': sector,
                't0_return_avg': f"{t0_avg:.6f}" if t0_avg is not None else '',
                'win1_cum_avg': f"{win1_avg:.6f}" if win1_avg is not None else '',
                'win3_cum_avg': f"{win3_avg:.6f}" if win3_avg is not None else '',
                'expected_value': f"{expected_val:.6f}" if isinstance(expected_val, (int, float)) and expected_val is not None else '',
                'actual_value': f"{actual_val:.6f}" if isinstance(actual_val, (int, float)) and actual_val is not None else '',
                'surprise': f"{surprise:.6f}" if isinstance(surprise, (int, float)) and surprise is not None else '',
                'surprise_pct': f"{surprise_pct:.6f}" if isinstance(surprise_pct, (int, float)) and surprise_pct is not None else '',
            })

    return rows


def write_long_csv(path, rows):
    fieldnames = ['event_id', 'event_name', 'event_date', 't0_date', 'event_type', 'importance', 'sector',
                  't0_return_avg', 'win1_cum_avg', 'win3_cum_avg',
                  'expected_value', 'actual_value', 'surprise', 'surprise_pct']
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def pivot_heatmap(rows, value_key):
    # Returns list of rows: event_id,event_name,event_date,importance,sector,value
    out = []
    for r in rows:
        v = r.get(value_key, '')
        out.append({
            'event_id': r['event_id'],
            'event_name': r['event_name'],
            'event_date': r['event_date'],
            'event_type': r.get('event_type', ''),
            'importance': r['importance'],
            'sector': r['sector'],
            value_key: v,
        })
    return out


def write_heatmap_csv(path, rows, value_key):
    fieldnames = ['event_id', 'event_name', 'event_date', 'event_type', 'importance', 'sector', value_key]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def wide_from_long(rows, metrics=('t0_return_avg', 'win1_cum_avg', 'win3_cum_avg')):
    # Build one row per event, columns per sector/metric
    # key: (event_id,event_name,event_date,importance)
    events = {}
    sectors = set()
    for r in rows:
        key = (r['event_id'], r['event_name'], r['event_date'], r['importance'])
        if key not in events:
            events[key] = {}
        s = r['sector']
        sectors.add(s)
        for m in metrics:
            events[key][(s, m)] = r.get(m, '')

    sectors = sorted(sectors)
    header = ['event_id', 'event_name', 'event_date', 't0_date', 'event_type', 'importance', 'expected_value', 'actual_value', 'surprise', 'surprise_pct']
    for m in metrics:
        for s in sectors:
            header.append(f"{m}__{s}")

    out_rows = []
    for (eid, ename, edate, imp), vals in events.items():
        row = {
            'event_id': eid,
            'event_name': ename,
            'event_date': edate,
            'importance': imp,
        }
        rep = None
        for r in rows:
            if (r['event_id'], r['event_name'], r['event_date'], r['importance']) == (eid, ename, edate, imp):
                rep = r
                break
        if rep:
            row['expected_value'] = rep.get('expected_value', '')
            row['actual_value'] = rep.get('actual_value', '')
            row['surprise'] = rep.get('surprise', '')
            row['surprise_pct'] = rep.get('surprise_pct', '')
            row['t0_date'] = rep.get('t0_date', '')
            row['event_type'] = rep.get('event_type', '')
        for m in metrics:
            for s in sectors:
                row[f"{m}__{s}"] = vals.get((s, m), '')
        out_rows.append(row)

    return header, out_rows


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def read_macro_regimes(path):
    # CSV columns: start_date,end_date,regime
    regimes = []
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                sd = parse_date(row['start_date'].strip())
                ed = parse_date(row['end_date'].strip())
                name = row.get('regime', '').strip()
            except Exception:
                continue
            if not name:
                continue
            if ed < sd:
                sd, ed = ed, sd
            regimes.append({'start': sd, 'end': ed, 'name': name})
    regimes.sort(key=lambda x: (x['start'], x['end']))
    return regimes


def build_sector_series_from_benchmarks(dates, returns_by_ticker, sector_bench_map):
    # returns dict: sector -> {date: return}
    if not sector_bench_map:
        return {}
    out = defaultdict(dict)
    for sector, bt in sector_bench_map.items():
        series = returns_by_ticker.get(bt, {})
        for d in dates:
            r = series.get(d)
            if r is not None:
                out[sector][d] = r
    return out


def trailing_cum_return(series_by_date, dates, end_idx, window):
    # product(1+r) - 1 over last `window` days ending at end_idx
    if window <= 0:
        return None
    start = end_idx - window + 1
    if start < 0:
        return None
    p = 1.0
    have = False
    for i in range(start, end_idx + 1):
        d = dates[i]
        r = series_by_date.get(d)
        if r is None:
            continue
        p *= (1.0 + r)
        have = True
    if not have:
        return None
    return p - 1.0


def compute_sector_cycle_momentum(dates, sector_series, windows=(21, 63, 126)):
    # returns list rows: date,sector,mom_21,mom_63,mom_126 (subset depending on windows)
    rows = []
    for idx, d in enumerate(dates):
        for sector, ser in sector_series.items():
            row = {'date': d.isoformat(), 'sector': sector}
            for w in windows:
                key = f'mom_{w}'
                row[key] = None
                val = trailing_cum_return(ser, dates, idx, w)
                if val is not None:
                    row[key] = f"{val:.6f}"
            rows.append(row)
    return rows


def compute_sector_cycle_ranks(momentum_rows, windows=(21, 63, 126)):
    # For each date, rank sectors by momentum descending (1=best)
    by_date = defaultdict(list)
    for r in momentum_rows:
        by_date[r['date']].append(r)
    out = []
    for d, rows in by_date.items():
        sectors = rows
        for w in windows:
            key = f'mom_{w}'
            # collect numeric values
            vals = []
            for r in sectors:
                v = r.get(key)
                try:
                    vnum = float(v) if v not in (None, '') else None
                except Exception:
                    vnum = None
                vals.append(vnum)
            # rank
            sorted_pairs = sorted([(i, v) for i, v in enumerate(vals) if v is not None], key=lambda x: x[1], reverse=True)
            rank = {}
            for rk, (i, _) in enumerate(sorted_pairs, start=1):
                rank[i] = rk
            # emit
        for i, r in enumerate(sectors):
            out_row = {'date': d, 'sector': r['sector']}
            for w in windows:
                key = f'mom_{w}'
                try:
                    v = float(r.get(key)) if r.get(key) not in (None, '') else None
                except Exception:
                    v = None
                out_row[key] = r.get(key)
                # recompute ranks for this date to fill out_row
            for w in windows:
                key = f'mom_{w}'
                # Build ranks once per date for simplicity (small N)
            out.append(out_row)
    # Second pass to actually compute ranks per date properly
    out = []
    for d, rows in by_date.items():
        windows_list = list(windows)
        # precompute ranks per window
        ranks_per_window = {}
        for w in windows_list:
            key = f'mom_{w}'
            vals = []
            for r in rows:
                try:
                    vals.append(float(r.get(key)) if r.get(key) not in (None, '') else None)
                except Exception:
                    vals.append(None)
            sorted_pairs = sorted([(i, v) for i, v in enumerate(vals) if v is not None], key=lambda x: x[1], reverse=True)
            rank_map = {}
            for rk, (i, _) in enumerate(sorted_pairs, start=1):
                rank_map[i] = rk
            ranks_per_window[w] = rank_map
        # emit with ranks
        for i, r in enumerate(rows):
            row = {'date': d, 'sector': r['sector']}
            for w in windows_list:
                key = f'mom_{w}'
                row[key] = r.get(key)
                rk_key = f'rank_{w}'
                row[rk_key] = ranks_per_window[w].get(i, '')
            out.append(row)
    return out


def build_cycle_lookup(rank_rows, windows):
    # Map (date, sector) -> {mom_w: val, rank_w: val}
    lookup = {}
    for r in rank_rows:
        key = (r['date'], r['sector'])
        d = lookup.setdefault(key, {})
        for w in windows:
            mk = f'mom_{w}'
            rk = f'rank_{w}'
            d[mk] = r.get(mk, '')
            d[rk] = r.get(rk, '')
    return lookup


def join_reaction_with_cycle(rows, cycle_lookup, windows):
    out = []
    for r in rows:
        key = (r.get('t0_date', r.get('event_date')), r['sector'])
        extra = cycle_lookup.get(key, {})
        new_r = dict(r)
        for w in windows:
            mk = f'mom_{w}'
            rk = f'rank_{w}'
            new_r[mk] = extra.get(mk, '')
            new_r[rk] = extra.get(rk, '')
        out.append(new_r)
    return out


def main():
    parser = argparse.ArgumentParser(description='Build sector reactions around macro events')
    parser.add_argument('--market', choices=['us','kr'], help='Convenience flag to set data/out dirs to data/<market>, out/<market> unless overridden')
    parser.add_argument('--data-dir', default=None, help='Input data directory (defaults to ./data or ./data/<market>)')
    parser.add_argument('--out-dir', default=None, help='Output directory (defaults to ./out or ./out/<market>)')
    parser.add_argument('--benchmark-ticker', default=None, help='Optional benchmark ticker for excess returns (must be present in prices.csv)')
    parser.add_argument('--write-wide', action='store_true', help='Also write wide feature matrix CSV')
    parser.add_argument('--win1', type=int, default=1, help='Half-window for short window (default 1 => [-1,+1])')
    parser.add_argument('--win3', type=int, default=3, help='Half-window for medium window (default 3 => [-3,+3])')
    parser.add_argument('--sector-benchmark-csv', default=None, help='CSV with columns sector,benchmark_ticker for sector-specific benchmarks (overrides global benchmark)')
    parser.add_argument('--compute-cycles', action='store_true', help='Compute sector cycle momentum and ranks using sector benchmark tickers')
    parser.add_argument('--cycle-windows', default='21,63,126', help='Comma-separated lookbacks for momentum/ranks (defaults to 21,63,126)')
    parser.add_argument('--regimes-csv', default=None, help='Optional regimes CSV with start_date,end_date,regime to average sector returns by regime')
    parser.add_argument('--join-cycles-into-reactions', action='store_true', help='Join cycle momentum/rank at event t0 date into reaction_long output')
    args = parser.parse_args()

    if args.market and not args.data_dir:
        data_dir = os.path.join('data', args.market)
    else:
        data_dir = args.data_dir or DEFAULT_DATA_DIR
    if args.market and not args.out_dir:
        out_dir = os.path.join('out', args.market)
    else:
        out_dir = args.out_dir or DEFAULT_OUT_DIR

    os.makedirs(out_dir, exist_ok=True)

    # Read inputs
    tickers_path = os.path.join(data_dir, 'tickers.csv')
    prices_path = os.path.join(data_dir, 'prices.csv')
    events_path = os.path.join(data_dir, 'macro_events.csv')
    tick2sector = read_tickers(tickers_path)
    dates, prices = read_prices(prices_path)
    events = read_events(events_path)
    returns_by_ticker = compute_returns(dates, prices)

    # Optional benchmark adjustment
    if args.benchmark_ticker:
        returns_by_ticker = adjust_returns_for_benchmark(returns_by_ticker, args.benchmark_ticker)

    # Load sector-specific benchmark tickers mapping (if provided)
    sector_bench_map = {}
    if args.sector_benchmark_csv and os.path.exists(args.sector_benchmark_csv):
        with open(args.sector_benchmark_csv, newline='', encoding='utf-8-sig') as f:
            r = csv.DictReader(f)
            for row in r:
                s = (row.get('sector') or '').strip()
                bt = (row.get('benchmark_ticker') or row.get('benchmark_symbol') or row.get('benchmark_code') or '').strip()
                if s and bt:
                    sector_bench_map[s] = bt

    # Compute long-format reactions
    rows = compute_sector_reactions(dates, tick2sector, returns_by_ticker, events, win1=args.win1, win3=args.win3, sector_bench_map=sector_bench_map)

    # Write long
    long_path = os.path.join(out_dir, 'reaction_long.csv')
    write_long_csv(long_path, rows)

    # Write heatmaps
    t0_rows = pivot_heatmap(rows, 't0_return_avg')
    write_heatmap_csv(os.path.join(out_dir, 'reaction_heatmap_t0.csv'), t0_rows, 't0_return_avg')
    w1_rows = pivot_heatmap(rows, 'win1_cum_avg')
    write_heatmap_csv(os.path.join(out_dir, 'reaction_heatmap_win1.csv'), w1_rows, 'win1_cum_avg')
    w3_rows = pivot_heatmap(rows, 'win3_cum_avg')
    write_heatmap_csv(os.path.join(out_dir, 'reaction_heatmap_win3.csv'), w3_rows, 'win3_cum_avg')

    # Wide matrix (optional)
    if args.write_wide:
        header, out_rows = wide_from_long(rows)
        write_csv(os.path.join(out_dir, 'reaction_wide.csv'), header, out_rows)

    # Sector cycle momentum and ranks (optional)
    if args.compute_cycles:
        # Build sector series from benchmark tickers
        sector_series = {}
        if sector_bench_map:
            for s, bt in sector_bench_map.items():
                series = returns_by_ticker.get(bt, {})
                if series:
                    sector_series[s] = series
        if sector_series:
            windows = [int(x) for x in (args.cycle_windows or '21,63,126').split(',') if x]
            mom_rows = compute_sector_cycle_momentum(dates, sector_series, windows=windows)
            ranks = compute_sector_cycle_ranks(mom_rows, windows=windows)
            # write CSVs
            write_csv(os.path.join(out_dir, 'sector_cycle_momentum.csv'), ['date','sector'] + [f'mom_{w}' for w in windows], mom_rows)
            write_csv(os.path.join(out_dir, 'sector_cycle_rank.csv'), ['date','sector'] + [f'mom_{w}' for w in windows] + [f'rank_{w}' for w in windows], ranks)
            # Regime averages if provided
            if args.regimes_csv and os.path.exists(args.regimes_csv):
                regimes = read_macro_regimes(args.regimes_csv)
                # Build sector daily return series from benchmark tickers
                sector_series = {}
                for s, bt in sector_bench_map.items():
                    series = returns_by_ticker.get(bt, {})
                    if series:
                        sector_series[s] = series
                # Compute regime averages
                ra = []
                for reg in regimes:
                    sd, ed, name = reg['start'], reg['end'], reg['name']
                    dr = [d for d in dates if sd <= d <= ed]
                    if not dr:
                        continue
                    for sector, ser in sector_series.items():
                        vals = [ser.get(d) for d in dr if ser.get(d) is not None]
                        if not vals:
                            avg = ''
                        else:
                            avg = sum(vals) / len(vals)
                        ra.append({'regime': name, 'start_date': sd.isoformat(), 'end_date': ed.isoformat(), 'sector': sector,
                                   'avg_daily_return': f"{avg:.6f}" if avg != '' else ''})
                write_csv(os.path.join(out_dir, 'sector_cycle_regime_avg.csv'), ['regime','start_date','end_date','sector','avg_daily_return'], ra)

            # join cycle info into reactions (on t0_date, sector)
            if args.join_cycles_into_reactions:
                # Build lookup for quick joins
                windows = [int(x) for x in (args.cycle_windows or '21,63,126').split(',') if x]
                lookup = build_cycle_lookup(ranks, windows)
                joined = join_reaction_with_cycle(rows, lookup, windows)
                # Write as reaction_with_cycle.csv
                write_long_csv(os.path.join(out_dir, 'reaction_with_cycle.csv'), joined)
        else:
            # No sector benchmark map available; still emit a passthrough file for downstream scripts
            write_long_csv(os.path.join(out_dir, 'reaction_with_cycle.csv'), rows)


if __name__ == '__main__':
    main()
