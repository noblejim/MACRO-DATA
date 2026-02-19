#!/usr/bin/env python3
"""Diagnose KR macro data sources by calling BOK/KOSIS/FDR APIs and printing sample values.

Usage:
    python scripts/diagnose_kr_sources.py --data-dir data/kr
"""
import os
import sys
import argparse
import logging

# Reuse helpers from the fetch script
sys.path.insert(0, os.path.dirname(__file__))
from fetch_macro_from_bok_kosis import (
    read_csv_normalized, fetch_json, fetch_bok_series,
    fetch_kosis_series_param, compute_yoy_from_series,
)

logger = logging.getLogger(__name__)

EXPECTED_RANGES = {
    'CPI_YOY':      (-5, 20),
    'PPI_YOY':      (-10, 30),
    'UNEMP_RATE':   (0, 15),
    'POLICY_RATE':  (0, 10),
    'CCSI':         (50, 150),
    'EMPLOYMENT':   (-500, 500),
    'INFL_EXPECT':  (0, 10),
    'IP_YOY':       (-30, 50),
    'PMI_MFG':      (30, 100),
    'GDP_QOQ':      (-10, 15),
    'TRADE_BAL':    (-50, 50),
    'M2_YOY':       (-5, 30),
    'LEI':          (80, 120),
    'USDKRW':       (800, 2000),
    'EQUITY_INDEX': (1000, 5000),
}


def diagnose_bok(sources, api_key):
    """Diagnose all BOK sources."""
    bok_sources = [s for s in sources if (s.get('source') or '').upper() == 'BOK']
    if not bok_sources:
        print('\n[BOK] No BOK sources configured.')
        return

    print('\n' + '=' * 70)
    print('BOK ECOS API Diagnosis')
    print('=' * 70)

    for src in bok_sources:
        et = src.get('event_type', '?')
        stat_code = (src.get('stat_code') or '').strip()
        item_code1 = (src.get('item_code1') or '').strip() or None
        cycle = (src.get('cycle') or 'M').strip() or 'M'
        need_yoy = (src.get('compute_yoy') or '').strip().upper() == 'Y'

        print(f'\n--- {et} ---')
        print(f'  stat_code={stat_code}  item_code1={item_code1}  cycle={cycle}  compute_yoy={need_yoy}')

        if not stat_code:
            print('  SKIP: no stat_code')
            continue

        try:
            # Fetch recent 24 months without item_code1 filter to see all items
            series_all = fetch_bok_series(api_key, stat_code, cycle, '202301', '202602')
            print(f'  Raw series (no filter): {len(series_all)} entries')
            # Show last 3
            sorted_keys = sorted(series_all.keys())[-3:]
            for k in sorted_keys:
                print(f'    {k} = {series_all[k]}')

            # Also fetch with item_code1 filter
            if item_code1:
                series_filtered = fetch_bok_series(api_key, stat_code, cycle, '202301', '202602', item_code1=item_code1)
                print(f'  Filtered (item_code1={item_code1}): {len(series_filtered)} entries')
                sorted_keys = sorted(series_filtered.keys())[-3:]
                for k in sorted_keys:
                    print(f'    {k} = {series_filtered[k]}')

                if need_yoy and sorted_keys:
                    yoy = compute_yoy_from_series(series_filtered, sorted_keys[-1])
                    print(f'  YoY for {sorted_keys[-1]}: {yoy}')

            # Also try listing available ITEM_CODE1 values
            from urllib.request import urlopen
            from urllib.parse import quote
            import json
            list_url = f"https://ecos.bok.or.kr/api/StatisticSearch/{quote(api_key)}/json/kr/1/20/{quote(stat_code)}/{quote(cycle)}/202501/202501"
            resp_data = fetch_json(list_url)
            rows = resp_data.get('StatisticSearch', {}).get('row', [])
            if rows:
                print(f'  Available ITEM_CODE1 values (sample month 202501):')
                seen = set()
                for r in rows[:20]:
                    ic1 = r.get('ITEM_CODE1', '')
                    nm = r.get('ITEM_NAME1', '')
                    val = r.get('DATA_VALUE', '')
                    key = (ic1, nm)
                    if key not in seen:
                        seen.add(key)
                        print(f'    ITEM_CODE1={ic1}  NAME={nm}  VALUE={val}')

            # Range check
            lo, hi = EXPECTED_RANGES.get(et, (None, None))
            if lo is not None and series_all:
                last_val = list(series_all.values())[-1]
                if need_yoy:
                    sk = sorted(series_all.keys())
                    last_val = compute_yoy_from_series(series_all, sk[-1]) if sk else None
                if last_val is not None:
                    in_range = lo <= last_val <= hi
                    status = 'OK' if in_range else 'OUT OF RANGE'
                    print(f'  Range check [{lo}, {hi}]: {last_val} -> {status}')

        except Exception as e:
            print(f'  ERROR: {e}')


def diagnose_kosis(sources, api_key):
    """Diagnose all KOSIS sources."""
    kosis_sources = [s for s in sources if (s.get('source') or '').upper() == 'KOSIS']
    if not kosis_sources:
        print('\n[KOSIS] No KOSIS sources configured.')
        return

    print('\n' + '=' * 70)
    print('KOSIS API Diagnosis')
    print('=' * 70)

    for src in kosis_sources:
        et = src.get('event_type', '?')
        org_id = (src.get('org_id') or '').strip()
        tbl_id = (src.get('tbl_id') or '').strip()
        itm_id = (src.get('itm_id') or '').strip()
        prd_se = (src.get('prd_se') or 'M').strip() or 'M'
        need_yoy = (src.get('compute_yoy') or '').strip().upper() == 'Y'
        param_mode = (src.get('param_mode') or '').strip().upper() == 'Y'

        obj_params = {f'objL{i}': (src.get(f'objL{i}') or '').strip() for i in range(1, 9)}
        obj_str = ', '.join(f'{k}={v}' for k, v in obj_params.items() if v)

        print(f'\n--- {et} ---')
        print(f'  org_id={org_id}  tbl_id={tbl_id}  itm_id={itm_id}  prd_se={prd_se}')
        print(f'  param_mode={param_mode}  compute_yoy={need_yoy}  obj_params: {obj_str}')

        if not all([org_id, tbl_id, itm_id]):
            print('  SKIP: incomplete parameters')
            continue

        try:
            if prd_se == 'Q':
                start, end = '2023Q1', '2025Q4'
            else:
                start, end = '202301', '202602'

            series = fetch_kosis_series_param(api_key, org_id, tbl_id, itm_id, prd_se, start, end, obj_params)
            print(f'  Series: {len(series)} entries')
            sorted_keys = sorted(series.keys())[-5:]
            for k in sorted_keys:
                print(f'    {k} = {series[k]}')

            if need_yoy and sorted_keys:
                yoy = compute_yoy_from_series(series, sorted_keys[-1])
                print(f'  YoY for {sorted_keys[-1]}: {yoy}')

            # Range check
            lo, hi = EXPECTED_RANGES.get(et, (None, None))
            if lo is not None and series:
                last_val = list(series.values())[-1] if sorted_keys else None
                if need_yoy:
                    last_val = compute_yoy_from_series(series, sorted_keys[-1]) if sorted_keys else None
                if last_val is not None:
                    in_range = lo <= last_val <= hi
                    status = 'OK' if in_range else 'OUT OF RANGE'
                    print(f'  Range check [{lo}, {hi}]: {last_val} -> {status}')

        except Exception as e:
            print(f'  ERROR: {e}')


def diagnose_market(sources):
    """Diagnose Market/FDR sources."""
    market_sources = [s for s in sources if (s.get('source') or '').upper() == 'MARKET']
    if not market_sources:
        print('\n[MARKET] No Market sources configured.')
        return

    print('\n' + '=' * 70)
    print('Market / FinanceDataReader Diagnosis')
    print('=' * 70)

    try:
        import FinanceDataReader as fdr
    except ImportError:
        print('  FinanceDataReader not installed. Install: pip install finance-datareader')
        return

    for src in market_sources:
        et = src.get('event_type', '?')
        custom_url = (src.get('custom_url') or '').strip()

        print(f'\n--- {et} ---')
        print(f'  custom_url={custom_url}')

        if not custom_url.startswith('fdr:'):
            print('  SKIP: not an fdr: URL')
            continue

        ticker = custom_url[4:]
        try:
            df = fdr.DataReader(ticker, '2024-01-01')
            print(f'  Fetched {len(df)} rows')
            if not df.empty:
                close_col = 'Close' if 'Close' in df.columns else df.columns[0]
                last_3 = df[close_col].tail(3)
                for idx, val in last_3.items():
                    print(f'    {idx.strftime("%Y-%m-%d")} = {val:.2f}')

                lo, hi = EXPECTED_RANGES.get(et, (None, None))
                if lo is not None:
                    last_val = float(df[close_col].iloc[-1])
                    in_range = lo <= last_val <= hi
                    status = 'OK' if in_range else 'OUT OF RANGE'
                    print(f'  Range check [{lo}, {hi}]: {last_val:.2f} -> {status}')
        except Exception as e:
            print(f'  ERROR: {e}')


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    ap = argparse.ArgumentParser(description='Diagnose KR macro data sources')
    ap.add_argument('--data-dir', default=os.path.join('data', 'kr'))
    ap.add_argument('--bok-key', default=None)
    ap.add_argument('--kosis-key', default=None)
    args = ap.parse_args()

    sources_path = os.path.join(args.data_dir, 'macro_sources.csv')
    if not os.path.exists(sources_path):
        print(f'ERROR: {sources_path} not found')
        sys.exit(1)

    sources = read_csv_normalized(sources_path)
    print(f'Loaded {len(sources)} sources from {sources_path}')

    bok_key = args.bok_key or os.environ.get('BOK_API_KEY')
    kosis_key = args.kosis_key or os.environ.get('KOSIS_API_KEY')

    if bok_key:
        diagnose_bok(sources, bok_key)
    else:
        print('\nNo BOK_API_KEY — skipping BOK diagnosis.')

    if kosis_key:
        diagnose_kosis(sources, kosis_key)
    else:
        print('\nNo KOSIS_API_KEY — skipping KOSIS diagnosis.')

    diagnose_market(sources)

    print('\n' + '=' * 70)
    print('Diagnosis complete.')
    print('=' * 70)


if __name__ == '__main__':
    main()
