#!/usr/bin/env python3
"""Compare two result matrices (simulator and TLC per instance and property).

    python3 baseline/compare.py baseline/untyped-355c8b7.json baseline/typed.json

Each file maps "instance.property" to {"sim": ..., "tlc": ...}: the verdict
("holds" or "violated"), the number of states in the counterexample
("trace"; TLC's is a shortest one) and the seconds it took. TLC also gives
the number of distinct states and the depth of the state graph.

A report written by ``check.py --tier full --json`` (spec -> instance ->
cells, D, states) is read as TLC results only:

    python3 baseline/compare.py before.json after.json
"""
import json
import sys


def load(path: str) -> dict:
    m = json.load(open(path))
    first = next(iter(m.values()))
    if 'sim' in first or 'tlc' in first:
        return m
    out = {}
    for spec in m.values():
        for inst, r in spec.items():
            for prop, v in r['cells'].items():
                out[f'{inst}.{prop}'] = {'tlc': {
                    'result': 'holds' if v == 'holds' else 'violated',
                    'trace': None if v == 'holds' else v,
                    'depth': r['D'], 'distinct': r['states'],
                    'secs': r['secs']}}
    return out


a, b = (load(p) for p in sys.argv[1:3])
assert a.keys() == b.keys()
verdicts = traces = depths = 0
for cell in sorted(a):
    for engine in ('sim', 'tlc'):
        if engine not in a[cell]:
            continue
        if a[cell][engine]['result'] != b[cell][engine]['result']:
            verdicts += 1
            print('verdict differs:', cell, engine, a[cell][engine]['result'],
                  b[cell][engine]['result'])
    if a[cell]['tlc'].get('trace') != b[cell]['tlc'].get('trace'):
        traces += 1
        print('shortest counterexample differs:', cell,
              a[cell]['tlc'].get('trace'), b[cell]['tlc'].get('trace'))
    if a[cell]['tlc'].get('depth') != b[cell]['tlc'].get('depth'):
        depths += 1
    if a[cell]['tlc'].get('distinct') != b[cell]['tlc'].get('distinct'):
        print('distinct states differ:', cell, a[cell]['tlc'].get('distinct'),
              b[cell]['tlc'].get('distinct'))
held = sum(v['tlc']['result'] == 'holds' for v in a.values())
print(f'{len(a)} cells ({held} hold, {len(a) - held} violated): '
      f'{verdicts} verdicts differ, {traces} shortest counterexamples '
      f'differ, {depths} cells differ in state-graph depth')
for name, m in (('first', a), ('second', b)):
    sim = sum(v['sim']['secs'] for v in m.values() if 'sim' in v)
    print(f'{name}: TLC {sum(v["tlc"]["secs"] for v in m.values()):.0f} s, '
          f'simulator {sim:.0f} s (summed over cells)')
