#!/usr/bin/env python3
import os
import csv
import argparse
from datetime import datetime, date, timedelta
import json
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote
import time


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def yyyymm(d: date):
    return f"{d.year}{d.month:02d}"


def to_date(s: str):
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except Exception:
        return None


def fred_observations(api_key, series_id, start_date='2010-01-01', end_date='2026-12-31'):
    base = 'https://api.stlouisfed.org/fred/series/observations'
    url = (
        f"{base}?series_id={quote(series_id)}&api_key={quote(api_key)}&file_type=json"
        f"&observation_start={quote(start_date)}&observation_end={quote(end_date)}"
    )
    # robust retry with backoff and HTTP status handling
    data = None
    last_err = None
    attempt = 0
    while attempt < 5:
        try:
            with urlopen(url, timeout=20) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            break
        except HTTPError as e:
            last_err = e
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(30, 2 ** attempt))
                attempt += 1
                continue
            else:
                return {}
        except URLError as e:
            last_err = e
            time.sleep(min(30, 2 ** attempt))
            attempt += 1
            continue
        except Exception as e:
            last_err = e
            time.sleep(min(30, 2 ** attempt))
            attempt += 1
            continue
    if data is None:
        return {}
    obs = data.get('observations', [])
    out = {}
    for o in obs:
        dt = to_date(o.get('date') or '')
        v = o.get('value')
        if dt is None or v in (None, '.'):
            continue
        try:
            out[dt] = float(v)
        except Exception:
            continue
    return out


def nearest_key(d: date, series: dict):
    # pick same day, else nearest past then nearest future
    if d in series:
        return d
    past = [k for k in series.keys() if k <= d]
    if past:
        return max(past)
    future = [k for k in series.keys() if k >= d]
    if future:
        return min(future)
    return None


def cache_dir_for(data_dir: str) -> str:
    cdir = os.path.join(data_dir, '.cache_fred')
    os.makedirs(cdir, exist_ok=True)
    return cdir


def load_cached_series(data_dir: str, series_id: str):
    path = os.path.join(cache_dir_for(data_dir), f'{series_id}.csv')
    if not os.path.exists(path):
        return {}
    out = {}
    try:
        with open(path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    d = datetime.strptime(row['date'], '%Y-%m-%d').date()
                    v = float(row['value'])
                    out[d] = v
                except Exception:
                    continue
    except Exception:
        return {}
    return out


def save_cached_series(data_dir: str, series_id: str, series: dict):
    if not series:
        return
    path = os.path.join(cache_dir_for(data_dir), f'{series_id}.csv')
    rows = [{'date': d.strftime('%Y-%m-%d'), 'value': v} for d, v in sorted(series.items())]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['date','value'])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def fill_actuals_us(data_dir, fred_key):
    ev_path = os.path.join(data_dir, 'macro_events.csv')
    ac_path = os.path.join(data_dir, 'macro_actuals.csv')
    src_path = os.path.join(data_dir, 'macro_sources.csv')

    events = read_csv(ev_path)
    sources = read_csv(src_path) if os.path.exists(src_path) else []
    src_map = { (s.get('event_type') or '').strip(): s for s in sources }

    if os.path.exists(ac_path):
        actuals = read_csv(ac_path)
    else:
        # init template
        actuals = []
        for ev in events:
            actuals.append({
                'event_id': ev.get('event_id',''),
                'event_date': ev.get('event_date',''),
                'event_type': ev.get('event_type',''),
                'event_name': ev.get('event_name',''),
                'expected_value': ev.get('expected_value',''),
                'actual_value': ev.get('actual_value',''),
            })

    ac_map = { (a.get('event_id') or '').strip(): a for a in actuals }

    # Determine needed date windows per series to minimize payload
    needed = {}
    for ev in events:
        et = (ev.get('event_type') or '').strip()
        d = to_date((ev.get('event_date') or '').strip())
        if not et or d is None:
            continue
        s = src_map.get(et)
        if not s:
            continue
        prov = (s.get('provider') or s.get('source') or '').strip().upper()
        if prov != 'FRED':
            continue
        sid = (s.get('series_id') or '').strip()
        if not sid:
            continue
        freq = (s.get('freq') or 'M').strip().upper()
        # extend window for YoY/MoM needs
        start_buf = 370 if (s.get('compute_yoy') or '').strip().lower() in ('y','yes','1','true') else 40
        start = d - timedelta(days=start_buf)
        end = d + timedelta(days=5)
        if sid not in needed:
            needed[sid] = {'start': start, 'end': end, 'freq': freq}
        else:
            if start < needed[sid]['start']:
                needed[sid]['start'] = start
            if end > needed[sid]['end']:
                needed[sid]['end'] = end
        # also include secondary series for derived metrics (e.g., DIFF: headline - transport)
        sid_b = (s.get('series_id_b') or '').strip()
        derive_method = (s.get('derive_method') or '').strip().upper()
        if derive_method and sid_b:
            if sid_b not in needed:
                needed[sid_b] = {'start': start, 'end': end, 'freq': freq}
            else:
                if start < needed[sid_b]['start']:
                    needed[sid_b]['start'] = start
                if end > needed[sid_b]['end']:
                    needed[sid_b]['end'] = end
        # include fallback alternative series if provided
        alt = (s.get('alt_series_ids') or '').strip()
        if alt:
            for aid in [x.strip() for x in alt.split(';') if x.strip()]:
                if aid not in needed:
                    needed[aid] = {'start': start, 'end': end, 'freq': freq}
                else:
                    if start < needed[aid]['start']:
                        needed[aid]['start'] = start
                    if end > needed[aid]['end']:
                        needed[aid]['end'] = end

    # Pre-fetch with caching and retries
    series_cache = {}
    failed = []
    for sid, meta in needed.items():
        if not fred_key:
            series_cache[sid] = {}
            continue
        # try load cache first
        cached = load_cached_series(data_dir, sid)
        ser = cached
        # fetch missing tails
        sstr = meta['start'].strftime('%Y-%m-%d')
        estr = meta['end'].strftime('%Y-%m-%d')
        # decide if download needed
        if not ser:
            ser = fred_observations(fred_key, sid, start_date=sstr, end_date=estr)
            if ser:
                save_cached_series(data_dir, sid, ser)
            else:
                failed.append({'series_id': sid, 'start': sstr, 'end': estr})
        else:
            # optional: skip incremental; keep cache as-is
            pass
        series_cache[sid] = ser

    changed = 0
    chosen_by_event_type = {}
    for ev in events:
        eid = (ev.get('event_id') or '').strip()
        et = (ev.get('event_type') or '').strip()
        dstr = (ev.get('event_date') or '').strip()
        if not eid or not et or not dstr:
            continue
        row = ac_map.get(eid)
        if row is None:
            row = {
                'event_id': eid,
                'event_date': dstr,
                'event_type': et,
                'event_name': ev.get('event_name',''),
                'expected_value': ev.get('expected_value',''),
                'actual_value': ev.get('actual_value',''),
            }
            ac_map[eid] = row
        if (row.get('actual_value') or '').strip() != '':
            continue
        smap = src_map.get(et)
        if not smap:
            continue
        if (smap.get('source','').upper() != 'FRED') and (smap.get('provider','').upper() != 'FRED'):
            # manual or other providers not implemented here
            continue
        sid = (smap.get('series_id') or '').strip()
        if not sid:
            continue
        freq = (smap.get('freq') or 'M').strip().upper()
        comp_yoy = (smap.get('compute_yoy') or '').strip().lower() in ('y','yes','1','true')
        comp_mom = (smap.get('compute_mom') or '').strip().lower() in ('y','yes','1','true')
        derive_method = (smap.get('derive_method') or '').strip().upper()
        sid_b = (smap.get('series_id_b') or '').strip()

        dt = to_date(dstr)
        if dt is None:
            continue
        alt = (smap.get('alt_series_ids') or '').strip()
        alt_ids = [x.strip() for x in alt.split(';') if x.strip()] if alt else []
        series = series_cache.get(sid) or {}
        series_b = series_cache.get(sid_b) if sid_b else {}
        sid_used = sid
        if not series:
            # try alternatives in order
            used = None
            for aid in alt_ids:
                ser_alt = series_cache.get(aid) or {}
                if ser_alt:
                    used = (aid, ser_alt)
                    break
            if used is None:
                continue
            sid_used, series = used[0], used[1]

        # align by frequency
        def align_key(base_date, ser):
            if not ser:
                return None
            if freq == 'W' or freq == 'D':
                return nearest_key(base_date, ser)
            elif freq == 'Q':
                q_month = ((base_date.month - 1)//3 + 1) * 3
                q_end = date(base_date.year, q_month, 1)
                return nearest_key(q_end, ser)
            else:
                month_start = date(base_date.year, base_date.month, 1)
                return nearest_key(month_start, ser)

        key = align_key(dt, series)
        key_b = align_key(dt, series_b) if series_b else None

        if key is None or key not in series:
            continue
        def derive_value(ser_a, ser_b, k_a, k_b):
            a = ser_a.get(k_a) if (ser_a and k_a in ser_a) else None
            b = ser_b.get(k_b) if (ser_b and k_b in ser_b) else None
            if derive_method == 'DIFF':
                if a is None or b is None:
                    return None
                return a - b
            elif derive_method == 'SUM':
                if a is None and b is None:
                    return None
                return (a or 0.0) + (b or 0.0)
            else:
                # default: no derivation; use primary
                return ser_a.get(k_a) if (ser_a and k_a in ser_a) else None

        cur_level = derive_value(series, series_b, key, key_b)
        val = None
        try:
            if comp_yoy:
                # previous year key(s)
                if freq == 'W' or freq == 'D':
                    prev_dates_a = [k for k in series.keys() if k < key]
                    prev_a = series.get(max(prev_dates_a)) if prev_dates_a else None
                    prev_dates_b = [k for k in series_b.keys() if k < key_b] if (series_b and key_b) else []
                    prev_b = series_b.get(max(prev_dates_b)) if prev_dates_b else None
                else:
                    prev_key_a = date(key.year - 1, key.month, 1)
                    prev_a = series.get(prev_key_a)
                    prev_b = None
                    if series_b and key_b:
                        prev_key_b = date(key_b.year - 1, key_b.month, 1)
                        prev_b = series_b.get(prev_key_b)
                prev_level = derive_value(series, series_b, prev_key_a if freq not in ('W','D') else (max(prev_dates_a) if prev_dates_a else None), prev_key_b if (series_b and key_b and freq not in ('W','D')) else (max(prev_dates_b) if prev_dates_b else None))
                if prev_level not in (None, 0) and cur_level is not None:
                    val = (cur_level/prev_level - 1.0) * 100.0
            elif comp_mom:
                # previous period
                if freq == 'W' or freq == 'D':
                    prev_dates_a = [k for k in series.keys() if k < key]
                    prev_a = series.get(max(prev_dates_a)) if prev_dates_a else None
                    prev_dates_b = [k for k in series_b.keys() if k < key_b] if (series_b and key_b) else []
                    prev_b = series_b.get(max(prev_dates_b)) if prev_dates_b else None
                    prev_level = derive_value(series, series_b, max(prev_dates_a) if prev_dates_a else None, max(prev_dates_b) if prev_dates_b else None)
                else:
                    prev_month = key.month - 1 or 12
                    prev_year = key.year - 1 if key.month == 1 else key.year
                    prev_key_a = date(prev_year, prev_month, 1)
                    prev_key_b = date(prev_year, prev_month, 1) if (series_b and key_b) else None
                    prev_level = derive_value(series, series_b, prev_key_a, prev_key_b)
                if prev_level not in (None, 0) and cur_level is not None:
                    val = (cur_level/prev_level - 1.0) * 100.0
            else:
                val = cur_level
        except Exception:
            val = None

        if val is not None:
            row['actual_value'] = f"{val:.4f}" if isinstance(val, float) else str(val)
            changed += 1
            # record chosen series for this event_type
            if et and et not in chosen_by_event_type:
                chosen_by_event_type[et] = sid_used

    # write logs for failures (best-effort)
    try:
        if failed:
            logs_dir = os.path.join('out', 'us', 'logs') if data_dir.replace('\\','/').endswith('/us') else os.path.join(data_dir, '..', 'out', 'logs')
            os.makedirs(logs_dir, exist_ok=True)
            with open(os.path.join(logs_dir, 'fred_failed_series.csv'), 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=['series_id','start','end'])
                w.writeheader()
                for r in failed:
                    w.writerow(r)
    except Exception:
        pass

    # write chosen series map (best-effort)
    try:
        if chosen_by_event_type:
            logs_dir = os.path.join('out', 'us', 'logs') if data_dir.replace('\\','/').endswith('/us') else os.path.join(data_dir, '..', 'out', 'logs')
            os.makedirs(logs_dir, exist_ok=True)
            with open(os.path.join(logs_dir, 'fred_chosen_series.csv'), 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=['event_type','chosen_series_id'])
                w.writeheader()
                for k, v in sorted(chosen_by_event_type.items()):
                    w.writerow({'event_type': k, 'chosen_series_id': v})
    except Exception:
        pass

    if changed:
        out_rows = []
        for ev in events:
            eid = (ev.get('event_id') or '').strip()
            out_rows.append(ac_map.get(eid))
        write_csv(ac_path, ['event_id','event_date','event_type','event_name','expected_value','actual_value'], out_rows)
        print(f'Filled {changed} rows into {ac_path}')
    else:
        print('No US macro actuals updated (FRED).')


def main():
    ap = argparse.ArgumentParser(description='Fill US macro actuals via FRED (optional) and write to data/us/macro_actuals.csv')
    ap.add_argument('--data-dir', default=os.path.join('data','us'))
    ap.add_argument('--fred-key', default=None)
    args = ap.parse_args()
    fred_key = args.fred_key or os.environ.get('FRED_API_KEY')
    fill_actuals_us(args.data_dir, fred_key)


if __name__ == '__main__':
    main()
