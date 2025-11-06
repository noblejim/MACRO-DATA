#!/usr/bin/env python3
import os
import csv
import argparse
import json
import time
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote


def cache_dir_for(data_dir: str) -> str:
    cdir = os.path.join(data_dir, '.cache_fred')
    os.makedirs(cdir, exist_ok=True)
    return cdir


def save_cached_series(data_dir: str, series_id: str, series: dict):
    if not series:
        return
    path = os.path.join(cache_dir_for(data_dir), f'{series_id}.csv')
    rows = [{'date': d, 'value': v} for d, v in sorted(series.items())]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['date','value'])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def fred_observations(api_key: str, series_id: str, start_date='2000-01-01', end_date='2026-12-31') -> dict:
    base = 'https://api.stlouisfed.org/fred/series/observations'
    url = (
        f"{base}?series_id={quote(series_id)}&api_key={quote(api_key)}&file_type=json"
        f"&observation_start={quote(start_date)}&observation_end={quote(end_date)}"
    )
    data = None
    attempt = 0
    while attempt < 5:
        try:
            with urlopen(url, timeout=25) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            break
        except HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(30, 2 ** attempt))
                attempt += 1
                continue
            return {}
        except URLError:
            time.sleep(min(30, 2 ** attempt))
            attempt += 1
            continue
        except Exception:
            time.sleep(min(30, 2 ** attempt))
            attempt += 1
            continue
    if data is None:
        return {}
    out = {}
    for o in (data.get('observations') or []):
        dt = o.get('date')
        v = o.get('value')
        if not dt or v in (None, '.'):
            continue
        try:
            out[dt] = float(v)
        except Exception:
            continue
    return out


def main():
    ap = argparse.ArgumentParser(description='Prefetch FRED series into data/<market>/.cache_fred (date,value) CSVs')
    ap.add_argument('--data-dir', default=os.path.join('data','us'))
    ap.add_argument('--fred-key', default=None)
    ap.add_argument('--series', required=True, help='Comma-separated FRED series ids (e.g., A36SNO, DGORDER)')
    ap.add_argument('--start', default='2000-01-01')
    ap.add_argument('--end', default='2026-12-31')
    args = ap.parse_args()

    key = args.fred_key or os.environ.get('FRED_API_KEY')
    if not key:
        raise SystemExit('Missing FRED API key (set env FRED_API_KEY or pass --fred-key)')
    series_ids = [s.strip() for s in args.series.split(',') if s.strip()]
    if not series_ids:
        raise SystemExit('No series ids provided')

    cached = 0
    for sid in series_ids:
        ser = fred_observations(key, sid, start_date=args.start, end_date=args.end)
        if ser:
            save_cached_series(args.data_dir, sid, ser)
            cached += 1
            print(f'Cached {sid}: {len(ser)} observations')
        else:
            print(f'No observations fetched for {sid}')
    print(f'Prefetch complete. Series cached: {cached}/{len(series_ids)}')


if __name__ == '__main__':
    main()
