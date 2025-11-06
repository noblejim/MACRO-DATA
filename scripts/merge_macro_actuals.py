#!/usr/bin/env python3
import os
import csv
import argparse


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    p = argparse.ArgumentParser(description='Merge expected/actual from macro_actuals.csv into macro_events.csv')
    p.add_argument('--data-dir', default=os.path.join('data', 'kr'))
    p.add_argument('--backup', action='store_true', help='Keep a backup macro_events.bak.csv before writing')
    args = p.parse_args()

    ev_path = os.path.join(args.data_dir, 'macro_events.csv')
    ac_path = os.path.join(args.data_dir, 'macro_actuals.csv')
    if not os.path.exists(ev_path):
        raise SystemExit(f'Missing {ev_path}')
    if not os.path.exists(ac_path):
        raise SystemExit(f'Missing {ac_path} — run generate_macro_actuals_template.py first')

    events = read_csv(ev_path)
    actuals = read_csv(ac_path)

    # index actuals by event_id
    ac_map = {}
    for a in actuals:
        eid = (a.get('event_id') or '').strip()
        if not eid:
            continue
        ac_map[eid] = a

    # merge into events in place
    out = []
    for e in events:
        eid = (e.get('event_id') or '').strip()
        a = ac_map.get(eid)
        if a:
            exp = (a.get('expected_value') or '').strip()
            act = (a.get('actual_value') or '').strip()
            if exp != '':
                e['expected_value'] = exp
            if act != '':
                e['actual_value'] = act
        out.append(e)

    if args.backup:
        bak = ev_path.replace('.csv', '.bak.csv')
        # compute robust fieldnames union and drop None keys
        fields = []
        seen = set()
        for e in events:
            for k in (e.keys() if isinstance(e, dict) else []):
                if k is None:
                    continue
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
        write_csv(bak, fields, events)

    write_csv(ev_path, out[0].keys(), out)
    print(f'Merged values into: {ev_path}')


if __name__ == '__main__':
    main()
