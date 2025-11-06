#!/usr/bin/env python3
import os
import sys
import csv
import argparse
from datetime import datetime
import json
import time
from urllib.request import urlopen
from urllib.parse import quote
from urllib.error import HTTPError, URLError

import pandas as pd


def read_existing_prices(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    try:
        df = pd.read_csv(path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
        return df
    except Exception:
        return pd.DataFrame(columns=['date','ticker','adj_close'])


def read_tickers(path: str) -> list:
    if not os.path.exists(path):
        return []
    try:
        df = pd.read_csv(path)
        return sorted([t for t in df.get('ticker', pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if t])
    except Exception:
        return []


def read_sector_benchmarks(path: str) -> list:
    syms = set()
    if not os.path.exists(path):
        return []
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            sym = (row.get('benchmark_symbol') or row.get('benchmark_code') or row.get('benchmark_ticker') or '').strip()
            if sym:
                syms.add(sym)
    return sorted(syms)


def cache_dir_for(data_dir: str) -> str:
    cdir = os.path.join(data_dir, '.cache_fmp')
    os.makedirs(cdir, exist_ok=True)
    return cdir


def load_cached_prices(data_dir: str, symbol: str) -> pd.DataFrame:
    cpath = os.path.join(cache_dir_for(data_dir), f'{symbol}.csv')
    if not os.path.exists(cpath):
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    try:
        df = pd.read_csv(cpath)
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
        df['ticker'] = symbol
        return df[['date','ticker','adj_close']]
    except Exception:
        return pd.DataFrame(columns=['date','ticker','adj_close'])


def save_cached_prices(data_dir: str, symbol: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    cpath = os.path.join(cache_dir_for(data_dir), f'{symbol}.csv')
    out = df.copy()
    out = out.sort_values('date')
    out['date'] = out['date'].dt.strftime('%Y-%m-%d')
    out[['date','adj_close']].to_csv(cpath, index=False)


def fmp_historical(symbol: str, api_key: str, start: str = '2000-01-01', end: str | None = None, data_dir: str | None = None, refresh: bool = False) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    # Load cache unless refresh is requested; use it to compute incremental start
    cached = None
    cached_last_date = None
    if data_dir and not refresh:
        cached = load_cached_prices(data_dir, symbol)
        if not cached.empty:
            cached_last_date = cached['date'].max()

    # compute fetch window (incremental resume beyond cache)
    req_start = start
    if cached_last_date is not None:
        next_day = (cached_last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        # Only move start forward; never move it backwards
        req_start = max(start, next_day)

    base = 'https://financialmodelingprep.com/api/v3/historical-price-full'
    params = f"?from={quote(req_start)}"
    if end:
        params += f"&to={quote(end)}"
    url = f"{base}/{quote(symbol)}{params}&apikey={quote(api_key)}"

    # robust retry with backoff (handle 429/5xx and network errors)
    last_err = None
    attempt = 0
    while attempt < 5:
        try:
            with urlopen(url, timeout=25) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            break
        except HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                delay = min(30, 2 ** attempt) + (attempt * 0.1)
                time.sleep(delay)
                attempt += 1
                continue
            else:
                return pd.DataFrame(columns=['date','ticker','adj_close'])
        except URLError as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            time.sleep(delay)
            attempt += 1
            continue
        except Exception as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            time.sleep(delay)
            attempt += 1
            continue
    else:
        # all retries failed
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    hist = data.get('historical') or []
    if not isinstance(hist, list) or not hist:
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    rows = []
    for it in hist:
        d = it.get('date')
        if not d:
            continue
        # prefer adjusted close if present
        adj = it.get('adjClose', it.get('close', None))
        try:
            adj = float(adj)
        except Exception:
            continue
        rows.append({'date': d, 'ticker': symbol, 'adj_close': adj})
    if not rows:
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
    df = df.dropna(subset=['date','adj_close'])
    out = df[['date','ticker','adj_close']]
    # merge with cache if present
    if cached is not None and not cached.empty:
        out = pd.concat([cached[['date','ticker','adj_close']], out], ignore_index=True)
        out = out.drop_duplicates(subset=['date','ticker'], keep='last')
    # save/update cache
    if data_dir:
        try:
            save_cached_prices(data_dir, symbol, out[['date','adj_close']])
        except Exception:
            pass
    return out


def merge_prices(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if new is None or new.empty:
        return existing
    if existing is None or existing.empty:
        combined = new.copy()
    else:
        combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.dropna(subset=['date','ticker'])
    combined['date'] = pd.to_datetime(combined['date'], errors='coerce')
    combined = combined.sort_values(['ticker','date'])
    combined = combined.drop_duplicates(subset=['ticker','date'], keep='last')
    return combined


def main():
    ap = argparse.ArgumentParser(description='Fetch historical prices via FMP and merge into data/us/prices.csv')
    ap.add_argument('--data-dir', default=os.path.join('data','us'))
    ap.add_argument('--start', default='2000-01-01')
    ap.add_argument('--end', default=None)
    ap.add_argument('--tickers', default=None, help='Optional comma-separated symbols; defaults to tickers.csv + sector benchmarks + core ETFs')
    ap.add_argument('--refresh', action='store_true', help='Ignore disk cache and refetch from FMP')
    ap.add_argument('--slice-years', action='store_true', help='Fetch per year slices between --start and --end to reduce payloads/rate limits')
    args = ap.parse_args()

    data_dir = args.data_dir
    prices_csv = os.path.join(data_dir, 'prices.csv')
    tickers_csv = os.path.join(data_dir, 'tickers.csv')
    bench_csv = os.path.join(data_dir, 'sector_benchmarks.csv')

    api_key = os.environ.get('FMP_API_KEY')
    if not api_key:
        print('Missing FMP_API_KEY; will try FinanceDataReader fallback...')

    # assemble symbols
    symbols = []
    if args.tickers:
        symbols = [s.strip() for s in args.tickers.split(',') if s.strip()]
    else:
        symbols = read_tickers(tickers_csv)
        symbols += read_sector_benchmarks(bench_csv)
        # core market ETFs for richer coverage
        core = ['SPY','QQQ','DIA','IWM','MDY']
        for s in core:
            if s not in symbols:
                symbols.append(s)
    # unique preserve order
    seen = set(); uniq = []
    for s in symbols:
        if s not in seen:
            seen.add(s); uniq.append(s)
    symbols = uniq

    # logging setup
    lower_dir = data_dir.replace('/', '\\').lower()
    logs_dir = os.path.join('out', 'us', 'logs') if ('\\data\\us' in lower_dir or '/data/us' in data_dir.replace('\\','/')) else os.path.join(data_dir, '..', 'out', 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    log_rows = []
    fail_rows = []

    existing = read_existing_prices(prices_csv)
    total = existing.copy()
    fetched_rows = 0
    # First: try FMP
    for sym in symbols:
        sym_total = 0
        try:
            if api_key and args.slice_years and args.start:
                syear = int(args.start[:4])
                eyear = int((args.end[:4] if args.end else datetime.now().strftime('%Y')))
                for y in range(syear, eyear + 1):
                    ys = f"{y}-01-01"; ye = f"{y}-12-31"
                    df = fmp_historical(sym, api_key, ys, ye, data_dir=data_dir, refresh=args.refresh)
                    if df is None or df.empty:
                        continue
                    total = merge_prices(total, df)
                    cnt = len(df); sym_total += cnt; fetched_rows += cnt
                    print(f"FMP: {sym} [{ys}..{ye}] rows={cnt}")
                    time.sleep(0.4)
            else:
                df = fmp_historical(sym, api_key, args.start, args.end, data_dir=data_dir, refresh=args.refresh) if api_key else pd.DataFrame()
                if df is not None and not df.empty:
                    total = merge_prices(total, df)
                    cnt = len(df); sym_total += cnt; fetched_rows += cnt
                    print(f"FMP: {sym} rows={cnt}")
                    time.sleep(0.2)
        except Exception as e:
            fail_rows.append({'symbol': sym, 'error': f'FMP:{str(e)}'})
        finally:
            log_rows.append({'symbol': sym, 'rows_fetched': sym_total})

    # Fallback: FinanceDataReader if no rows fetched
    if fetched_rows == 0:
        print('FMP returned 0 rows for all symbols; trying FinanceDataReader fallback...')
        try:
            import FinanceDataReader as fdr
        except Exception:
            fdr = None
        if fdr is not None:
            fb_rows = 0
            for sym in symbols:
                try:
                    df = fdr.DataReader(sym, args.start, args.end)
                    if df is None or df.empty:
                        continue
                    out = df[['Close']].rename(columns={'Close':'adj_close'}).copy()
                    out['ticker'] = sym
                    out['date'] = out.index
                    out = out[['date','ticker','adj_close']]
                    out['date'] = pd.to_datetime(out['date'], errors='coerce')
                    out['adj_close'] = pd.to_numeric(out['adj_close'], errors='coerce')
                    out = out.dropna(subset=['date','adj_close'])
                    if not out.empty:
                        total = merge_prices(total, out)
                        fb_rows += len(out)
                        print(f"FDR: {sym} rows={len(out)}")
                        time.sleep(0.1)
                except Exception as e:
                    fail_rows.append({'symbol': sym, 'error': f'FDR:{str(e)}'})
            fetched_rows = fb_rows
            if fetched_rows > 0:
                print(f'FinanceDataReader fallback fetched total rows={fetched_rows}')

    # write logs
    try:
        if log_rows:
            pd.DataFrame(log_rows).to_csv(os.path.join(logs_dir, 'fmp_fetch_log.csv'), index=False)
        if fail_rows:
            pd.DataFrame(fail_rows).to_csv(os.path.join(logs_dir, 'fmp_failed_symbols.csv'), index=False)
    except Exception:
        pass

    if (fetched_rows == 0) and (total is None or total.empty):
        print('No price data fetched from FMP or fallback; creating no prices.csv. Downstream steps may skip.')
        sys.exit(0)

    # Always write prices.csv if we have any data (from cache or fresh)
    total = total.sort_values(['date','ticker'])
    total['date'] = total['date'].dt.strftime('%Y-%m-%d')
    total.to_csv(prices_csv, index=False)
    print(f"Wrote merged prices: {prices_csv} (rows={len(total)})")


if __name__ == '__main__':
    main()
