#!/usr/bin/env python3
import os
import sys
import csv
import argparse
from datetime import datetime, date

import pandas as pd
import FinanceDataReader as fdr

DATA_DIR_DEFAULT = os.path.join('data','kr')


def read_sector_benchmarks(path):
    tickers = set()
    with open(path, newline='', encoding='utf-8-sig') as f:
        r = csv.DictReader(f)
        for row in r:
            sym = (row.get('benchmark_symbol') or row.get('benchmark_code') or row.get('benchmark_ticker') or '').strip()
            if sym:
                tickers.add(sym)
    return sorted(tickers)


def read_existing_prices(path):
    if not os.path.exists(path):
        return pd.DataFrame(columns=['date','ticker','adj_close'])
    try:
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        return df
    except Exception:
        return pd.DataFrame(columns=['date','ticker','adj_close'])


def fetch_one(symbol, start=None, end=None):
    try:
        df = fdr.DataReader(symbol, start, end)
        if df is None or df.empty:
            return None
        out = df[['Close']].copy()
        out = out.rename(columns={'Close':'adj_close'})
        out['ticker'] = symbol
        out['date'] = out.index
        out = out[['date','ticker','adj_close']]
        out['date'] = pd.to_datetime(out['date'], errors='coerce')
        out['adj_close'] = pd.to_numeric(out['adj_close'], errors='coerce')
        out = out.dropna(subset=['date','adj_close'])
        return out
    except Exception:
        return None


def merge_prices(existing, new):
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
    ap = argparse.ArgumentParser(description='Fetch historical prices via FinanceDataReader and merge into data/kr/prices.csv')
    ap.add_argument('--data-dir', default=DATA_DIR_DEFAULT)
    ap.add_argument('--start', default='2000-01-01')
    ap.add_argument('--end', default=None)
    args = ap.parse_args()

    data_dir = args.data_dir
    prices_csv = os.path.join(data_dir, 'prices.csv')
    bench_csv = os.path.join(data_dir, 'sector_benchmarks.csv')

    if not os.path.exists(bench_csv):
        print(f'Missing {bench_csv}')
        sys.exit(1)

    symbols = read_sector_benchmarks(bench_csv)
    if 'KS11' not in symbols:
        symbols.append('KS11')

    existing = read_existing_prices(prices_csv)
    total = existing.copy()

    fetched = 0
    for sym in symbols:
        df = fetch_one(sym, args.start, args.end)
        if df is None or df.empty:
            continue
        total = merge_prices(total, df)
        fetched += len(df)

    if fetched == 0:
        print('No new rows fetched')
        return

    total = total.sort_values(['date','ticker'])
    total['date'] = total['date'].dt.strftime('%Y-%m-%d')
    total.to_csv(prices_csv, index=False)
    print(f'Wrote merged prices: {prices_csv} (rows={len(total)})')


if __name__ == '__main__':
    main()
