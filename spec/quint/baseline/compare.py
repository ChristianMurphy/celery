#!/usr/bin/env python3
"""Compare two result matrices (simulator and TLC per instance and property).

    python3 baseline/compare.py baseline/untyped-355c8b7.json baseline/typed.json

Each file maps "instance.property" to {"sim": ..., "tlc": ...}: the verdict
("holds" or "violated"), the number of states in the counterexample
("trace"; TLC's is a shortest one) and the seconds it took. TLC also gives
the number of distinct states and the depth of the state graph.
"""
import json
import sys

a, b = (json.load(open(p)) for p in sys.argv[1:3])
assert a.keys() == b.keys()
verdicts = traces = depths = 0
for cell in sorted(a):
    for engine in ('sim', 'tlc'):
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
held = sum(v['tlc']['result'] == 'holds' for v in a.values())
print(f'{len(a)} cells ({held} hold, {len(a) - held} violated): '
      f'{verdicts} verdicts differ, {traces} shortest counterexamples '
      f'differ, {depths} cells differ in state-graph depth')
for name, m in (('first', a), ('second', b)):
    print(f'{name}: TLC {sum(v["tlc"]["secs"] for v in m.values()):.0f} s, '
          f'simulator {sum(v["sim"]["secs"] for v in m.values()):.0f} s '
          '(summed over cells, three at a time)')
