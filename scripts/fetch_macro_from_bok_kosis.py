#!/usr/bin/env python3
import os
import csv
import argparse
from datetime import datetime, date
from urllib.request import urlopen
from urllib.parse import quote
import json


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def read_csv_normalized(path):
    rows = read_csv(path)
    out = []
    for r in rows:
        nr = {}
        for k, v in r.items():
            nk = (k or '').strip().strip("'\"")
            nv = (v or '').strip()
            nr[nk] = nv
        out.append(nr)
    return out


def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def yyyymm(d: date):
    return f"{d.year}{d.month:02d}"


def fetch_json(url):
    with urlopen(url) as resp:
        return json.loads(resp.read().decode('utf-8'))


def fetch_bok_series(api_key, stat_code, cycle, start_yyyymm, end_yyyymm):
    base = 'https://ecos.bok.or.kr/api/StatisticSearch'
    url = f"{base}/{quote(api_key)}/json/kr/1/999/{quote(stat_code)}/{quote(cycle)}/{quote(start_yyyymm)}/{quote(end_yyyymm)}"
    data = fetch_json(url)
    items = data.get('StatisticSearch', {}).get('row', [])
    out = {}
    for it in items:
        time = it.get('TIME') or it.get('TIME_PERIOD')
        val = it.get('DATA_VALUE')
        if time is None or val is None:
            continue
        try:
            out[time] = float(str(val).replace(',', ''))
        except Exception:
            continue
    return out


def fetch_kosis_series_param(api_key, org_id, tbl_id, itm_id, prd_se, start_yyyymm, end_yyyymm, obj_params=None):
    base = 'https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList'
    url = (
        f"{base}&apiKey={quote(api_key)}&format=json&jsonVD=Y"
        f"&orgId={quote(org_id)}&tblId={quote(tbl_id)}&itmId={quote(itm_id)}"
        f"&prdSe={quote(prd_se)}&startPrdDe={quote(start_yyyymm)}&endPrdDe={quote(end_yyyymm)}"
    )
    if obj_params:
        for i in range(1,9):
            key = f'objL{i}'
            val = (obj_params.get(key) or '').strip()
            if val != '':
                url += f"&{key}={quote(val)}"
    data = fetch_json(url)
    out = {}
    if isinstance(data, list):
        for it in data:
            c1 = str(it.get('C1', '')).strip()
            c2 = str(it.get('C2', '')).strip()
            if c1 not in ('', '0', '00', 'ALL'):
                continue
            if c2 not in ('', '0', '00', 'ALL'):
                continue
            prd = it.get('PRD_DE')
            dt = it.get('DT')
            if not prd or dt is None:
                continue
            try:
                out[prd] = float(str(dt).replace(',', ''))
            except Exception:
                continue
    return out


def update_macro_actuals_from_bok(data_dir, api_key):
    sources = read_csv(os.path.join(data_dir, 'macro_sources.csv'))
    src_map = {s['event_type']: s for s in sources if (s.get('source') or '').upper() == 'BOK'}
    events = read_csv(os.path.join(data_dir, 'macro_events.csv'))
    actuals_path = os.path.join(data_dir, 'macro_actuals.csv')
    actuals = read_csv(actuals_path) if os.path.exists(actuals_path) else []
    ac_map = {(a.get('event_id') or '').strip(): a for a in actuals}

    changed = False
    for ev in events:
        eid = (ev.get('event_id') or '').strip()
        et = (ev.get('event_type') or '').strip()
        if et not in src_map:
            continue
        row = ac_map.get(eid) or {
            'event_id': eid, 'event_date': ev.get('event_date',''), 'event_type': et,
            'event_name': ev.get('event_name',''), 'expected_value': ev.get('expected_value',''), 'actual_value': ev.get('actual_value',''),
        }
        if (row.get('actual_value') or '').strip() != '':
            continue
        stat_code = (src_map[et].get('stat_code') or '').strip()
        cycle = (src_map[et].get('cycle') or 'M').strip() or 'M'
        if not stat_code:
            continue
        try:
            d = datetime.strptime(ev.get('event_date'), '%Y-%m-%d').date()
        except Exception:
            continue
        start = f"{d.year-2}{d.month:02d}"; end = yyyymm(d)
        try:
            series = fetch_bok_series(api_key, stat_code, cycle, start, end)
        except Exception:
            series = {}
        key = yyyymm(d)
        if key in series:
            row['actual_value'] = str(series[key])
            ac_map[eid] = row
            changed = True
    if changed:
        out_rows = []
        for ev in events:
            k = (ev.get('event_id') or '').strip()
            out_rows.append(ac_map.get(k) or {
                'event_id': k, 'event_date': ev.get('event_date',''), 'event_type': (ev.get('event_type') or '').strip(),
                'event_name': ev.get('event_name',''), 'expected_value': ev.get('expected_value',''), 'actual_value': ev.get('actual_value',''),
            })
        write_csv(actuals_path, ['event_id','event_date','event_type','event_name','expected_value','actual_value'], out_rows)
        print(f'Updated actuals from BOK into {actuals_path}')
    else:
        print('No BOK updates applied.')


def update_macro_actuals_from_kosis(data_dir, api_key):
    sources = read_csv_normalized(os.path.join(data_dir, 'macro_sources.csv'))
    src_map = {s.get('event_type'): s for s in sources if (s.get('source') or '').upper() == 'KOSIS'}
    events = read_csv(os.path.join(data_dir, 'macro_events.csv'))
    actuals_path = os.path.join(data_dir, 'macro_actuals.csv')
    actuals = read_csv(actuals_path) if os.path.exists(actuals_path) else []
    ac_map = {(a.get('event_id') or '').strip(): a for a in actuals}

    changed = False
    for ev in events:
        eid = (ev.get('event_id') or '').strip(); et = (ev.get('event_type') or '').strip()
        if et not in src_map:
            continue
        row = ac_map.get(eid) or {
            'event_id': eid, 'event_date': ev.get('event_date',''), 'event_type': et,
            'event_name': ev.get('event_name',''), 'expected_value': ev.get('expected_value',''), 'actual_value': ev.get('actual_value',''),
        }
        if (row.get('actual_value') or '').strip() != '':
            continue
        s = src_map[et]
        org_id, tbl_id, itm_id = s.get('org_id'), s.get('tbl_id'), s.get('itm_id')
        prd_se = (s.get('prd_se') or 'M').strip() or 'M'
        param_mode = (s.get('param_mode') or '').strip().upper() == 'Y'
        start = None; end = None
        try:
            d = datetime.strptime(ev.get('event_date'), '%Y-%m-%d').date()
            start = f"{d.year-2}{d.month:02d}"; end = yyyymm(d)
        except Exception:
            continue
        series = {}
        if param_mode and all([org_id, tbl_id, itm_id]):
            obj_params = {f'objL{i}': (s.get(f'objL{i}') or '').strip() for i in range(1,9)}
            try:
                series = fetch_kosis_series_param(api_key, org_id, tbl_id, itm_id, prd_se, start, end, obj_params)
            except Exception:
                series = {}
        elif (s.get('custom_url') or '').strip():
            try:
                data = fetch_json(s.get('custom_url').strip())
                if isinstance(data, list):
                    for it in data:
                        prd = it.get('PRD_DE'); dt = it.get('DT')
                        if not prd or dt is None:
                            continue
                        series[prd] = float(str(dt).replace(',', ''))
            except Exception:
                series = {}
        key = yyyymm(d)
        if key in series:
            row['actual_value'] = str(series[key])
            ac_map[eid] = row
            changed = True
    if changed:
        out_rows = []
        for ev in events:
            k = (ev.get('event_id') or '').strip()
            out_rows.append(ac_map.get(k) or {
                'event_id': k, 'event_date': ev.get('event_date',''), 'event_type': (ev.get('event_type') or '').strip(),
                'event_name': ev.get('event_name',''), 'expected_value': ev.get('expected_value',''), 'actual_value': ev.get('actual_value',''),
            })
        write_csv(actuals_path, ['event_id','event_date','event_type','event_name','expected_value','actual_value'], out_rows)
        print(f'Updated actuals from KOSIS into {actuals_path}')
    else:
        print('No KOSIS updates applied.')


def main():
    ap = argparse.ArgumentParser(description='Fetch KR macro actuals via BOK and KOSIS into data/kr/macro_actuals.csv')
    ap.add_argument('--data-dir', default=os.path.join('data','kr'))
    ap.add_argument('--bok-key', default=None)
    ap.add_argument('--kosis-key', default=None)
    args = ap.parse_args()

    data_dir = args.data_dir
    bok_key = args.bok_key or os.environ.get('BOK_API_KEY')
    kosis_key = args.kosis_key or os.environ.get('KOSIS_API_KEY')

    if not os.path.exists(os.path.join(data_dir, 'macro_events.csv')):
        print('Missing data/kr/macro_events.csv; seed events first.'); return
    if not os.path.exists(os.path.join(data_dir, 'macro_sources.csv')):
        print('Missing data/kr/macro_sources.csv'); return

    if bok_key:
        try:
            update_macro_actuals_from_bok(data_dir, bok_key)
        except Exception:
            print('BOK fetch encountered an error (continuing).')
    else:
        print('No BOK_API_KEY; skipping BOK.')

    if kosis_key:
        try:
            update_macro_actuals_from_kosis(data_dir, kosis_key)
        except Exception:
            print('KOSIS fetch encountered an error (continuing).')
    else:
        print('No KOSIS_API_KEY; skipping KOSIS.')


if __name__ == '__main__':
    main()
