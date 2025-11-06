#!/usr/bin/env python3
import os
import csv
import argparse
from datetime import datetime, date, timedelta
import json
from urllib.request import urlopen
from urllib.parse import quote


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def to_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except Exception:
        return None


def fred_observations(api_key, series_id, start_date, end_date):
    base = 'https://api.stlouisfed.org/fred/series/observations'
    url = (
        f"{base}?series_id={quote(series_id)}&api_key={quote(api_key)}&file_type=json"
        f"&observation_start={quote(start_date)}&observation_end={quote(end_date)}"
    )
    with urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    obs = data.get('observations', [])
    out = []
    for o in obs:
        dt = to_date(o.get('date') or '')
        v = o.get('value')
        if dt is None or v in (None, '.'):
            continue
        out.append({'date': dt, 'value': v})
    return out

def to_month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def to_quarter_key(d: date) -> str:
    q = (d.month - 1)//3 + 1
    return f"{d.year:04d}-Q{q}"


def slug_from_event_type(et: str) -> str:
    base = et
    if base.endswith('_MOM'):
        base = base[:-4]
    if base.endswith('_YOY'):
        base = base[:-4]
    return base


def main():
    ap = argparse.ArgumentParser(description='Backfill macro_events.csv with monthly events from FRED for last N years for specified event types')
    ap.add_argument('--data-dir', default=os.path.join('data','us'))
    ap.add_argument('--event-types', default='', help='Comma-separated event types. If omitted and --all-fred is set, uses all FRED-mapped types')
    ap.add_argument('--all-fred', action='store_true', help='Backfill all FRED-mapped event types from macro_sources.csv')
    ap.add_argument('--years', type=int, default=None, help='How many years back to backfill')
    ap.add_argument('--start', default='2000-01-01', help='Start date (YYYY-MM-DD). Overrides --years when provided')
    ap.add_argument('--end', default=None, help='End date (YYYY-MM-DD), defaults to today')
    ap.add_argument('--fred-key', default=None)
    args = ap.parse_args()

    data_dir = args.data_dir
    ev_path = os.path.join(data_dir, 'macro_events.csv')
    src_path = os.path.join(data_dir, 'macro_sources.csv')
    if not (os.path.exists(ev_path) and os.path.exists(src_path)):
        raise SystemExit('Missing macro_events.csv or macro_sources.csv')

    fred_key = args.fred_key or os.environ.get('FRED_API_KEY')
    if not fred_key:
        print('FRED_API_KEY not set; will create event rows without actuals (fill later).')

    events = read_csv(ev_path)
    sources = read_csv(src_path)
    src_map = { (s.get('event_type') or '').strip(): s for s in sources }

    have = set()
    for r in events:
        et = (r.get('event_type') or '').strip()
        d = (r.get('event_date') or '').strip()
        if et and d:
            have.add((et, d))

    # decide event types
    if args.all_fred and not args.event_types:
        etypes = [et for et, s in src_map.items() if (s.get('provider') or '').strip().upper() == 'FRED']
    else:
        etypes = [e.strip() for e in (args.event_types or '').split(',') if e.strip()]
    if not etypes:
        raise SystemExit('No event types specified. Use --event-types or --all-fred')

    # decide date range
    if args.start:
        start_date = to_date(args.start) or date(2000, 1, 1)
    elif args.years:
        start_date = (date.today().replace(day=1) - timedelta(days=365*args.years)).replace(day=1)
    else:
        start_date = date(2000, 1, 1)
    end_date = to_date(args.end) if args.end else date.today()

    new_rows = 0
    for et in etypes:
        s = src_map.get(et)
        if not s or (s.get('provider') or '').strip().upper() != 'FRED':
            print(f'Skip {et}: no FRED mapping in macro_sources.csv')
            continue
        sid = (s.get('series_id') or '').strip()
        if not sid:
            print(f'Skip {et}: empty series_id')
            continue
        observations = []
        try:
            if fred_key:
                observations = fred_observations(fred_key, sid, start_date.isoformat(), end_date.isoformat())
        except Exception as e:
            print(f'FRED fetch failed for {et}: {e}')
        # Downsample by declared frequency: D/W->monthly, Q->quarterly, M->monthly
        freq = (s.get('freq') or '').strip().upper() or 'M'
        if observations:
            if freq in ('D','W'):
                # pick earliest observation per month
                by_month = {}
                for ob in observations:
                    k = to_month_key(ob['date'])
                    if k not in by_month:
                        by_month[k] = ob
                observations = [{'date': datetime.strptime(k+'-01','%Y-%m-%d').date(), 'value': by_month[k]['value']} for k in sorted(by_month.keys())]
            elif freq == 'Q':
                by_q = {}
                for ob in observations:
                    k = to_quarter_key(ob['date'])
                    by_q[k] = ob  # keep last seen in quarter
                # map to quarter-end month as event_date (use first day of quarter-end month)
                _map = {'Q1': (3,1), 'Q2': (6,1), 'Q3': (9,1), 'Q4': (12,1)}
                new_obs = []
                for k in sorted(by_q.keys()):
                    y, q = k.split('-')
                    m, d = _map[q]
                    new_obs.append({'date': date(int(y), m, d), 'value': by_q[k]['value']})
                observations = new_obs
            else:
                # monthly: normalize to 1st of month
                observations = [{'date': ob['date'].replace(day=1), 'value': ob['value']} for ob in observations]
        else:
            # Fallback synthesize monthly dates
            cur = date(start_date.year, start_date.month, 1)
            while cur <= end_date:
                observations.append({'date': cur, 'value': ''})
                y = cur.year + (cur.month // 12)
                m = (cur.month % 12) + 1
                cur = date(y, m, 1)

        base_slug = slug_from_event_type(et)
        label = (s.get('notes') or base_slug).split('(')[0].strip()
        for ob in observations:
            d = ob['date'].isoformat()
            if (et, d) in have:
                continue
            # build event_id like US_RETAIL_CONTROL_YYYY_MM
            y = ob['date'].year
            mm = f"{ob['date'].month:02d}"
            event_id = f"US_{base_slug}_{y}_{mm}"
            events.append({
                'event_id': event_id,
                'event_name': f"{label} (US)",
                'event_date': d,
                'importance': 'Medium',
                'expected_value': '',
                'actual_value': '',
                'event_type': et,
            })
            have.add((et, d))
            new_rows += 1

    if new_rows:
        # sort by event_date then name for stability
        try:
            for r in events:
                try:
                    r['_dt'] = to_date(r.get('event_date') or '')
                except Exception:
                    r['_dt'] = None
            events.sort(key=lambda x: (x.get('_dt') or date(1900,1,1), x.get('event_name','')))
            for r in events:
                if '_dt' in r:
                    del r['_dt']
        except Exception:
            pass
        # preserve columns
        cols = ['event_id','event_name','event_date','importance','expected_value','actual_value','event_type']
        write_csv(ev_path, cols, events)
    print(f'Backfilled {new_rows} new event rows for: {", ".join(etypes)}')


if __name__ == '__main__':
    main()
