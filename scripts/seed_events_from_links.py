#!/usr/bin/env python3
import os
import csv
from datetime import date

FALLBACK_URLS = {}


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def month_iter(end: date, months_back: int):
    y, m = end.year, end.month
    for _ in range(months_back):
        yield y, m
        m -= 1
        if m == 0:
            y -= 1
            m = 12


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Seed macro_events.csv for last N months for KR using standard event types')
    ap.add_argument('--data-dir', default=os.path.join('data', 'kr'))
    ap.add_argument('--months', type=int, default=36)
    ap.add_argument('--importance', default='Medium')
    args = ap.parse_args()

    ev_path = os.path.join(args.data_dir, 'macro_events.csv')
    rows = read_csv(ev_path) if os.path.exists(ev_path) else []
    existing_ids = set((r.get('event_id') or '').strip() for r in rows)

    targets = [
        ('UNEMP_RATE', 'Unemployment Rate (KR)'),
        ('EMPLOYMENT', 'Employment Change (KR)'),
        ('CCSI', 'Consumer Confidence Index (KR)'),
        ('INFL_EXPECT', 'Inflation Expectations (KR)'),
        ('PMI_MFG', 'Manufacturing BSI SA (KR)'),
        ('IP_YOY', 'Industrial Production YoY (KR)'),
        ('LEI', 'Leading Economic Index (KR)'),
        ('CPI_YOY', 'CPI YoY (KR)'),
        ('PPI_YOY', 'PPI YoY (KR)'),
        ('TRADE_BAL', 'Trade Balance (KR)'),
        ('POLICY_RATE', 'BOK Policy Rate (KR)'),
        ('M2_YOY', 'M2 Money Supply YoY (KR)'),
        ('USDKRW', 'USD/KRW Level (KR)'),
        ('EQUITY_INDEX', 'KOSPI Index (KR)'),
        ('GDP_QOQ', 'GDP QoQ (KR)'),
    ]

    today = date.today()
    added = 0
    for et, name in targets:
        for y, m in month_iter(today, args.months):
            eid = f"KR_{et}_{y}_{m:02d}"
            if eid in existing_ids:
                continue
            event_date = f"{y}-{m:02d}-01"
            rows.append({
                'event_id': eid,
                'event_name': name,
                'event_type': et,
                'event_date': event_date,
                'importance': args.importance,
                'expected_value': '',
                'actual_value': '',
            })
            existing_ids.add(eid)
            added += 1

    if added:
        write_csv(ev_path, ['event_id','event_name','event_type','event_date','importance','expected_value','actual_value'], rows)
        print(f'Added {added} events into {ev_path}')
    else:
        print('No new events added (already present)')


if __name__ == '__main__':
    main()
