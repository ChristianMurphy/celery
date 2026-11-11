#!/usr/bin/env python3
"""Print the worker results as one Markdown table per design, from
expected.json: a row per ack mode and fault class, a column per property,
`ok` or the number of states in a shortest counterexample.

    python3 tools/design_tables.py            # every design
    python3 tools/design_tables.py best pr    # some
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
PROPS = ['exactlyOnce', 'neverLost', 'quietDone', 'noAvoidableDup',
         'noUnavoidableDup', 'restoresOwnOnly', 'workerSurvives']


def main() -> None:
    expected = json.loads((HERE / 'expected.json').read_text())['worker.qnt']
    designs: dict[str, list[str]] = {}
    for inst in expected:
        designs.setdefault(inst.split('_')[0], []).append(inst)
    for design in sys.argv[1:] or designs:
        print(f'\n`{design}`\n')
        print('| ack, fault | D | ' + ' | '.join(PROPS) + ' |')
        print('|---' * (len(PROPS) + 2) + '|')
        for inst in designs[design]:
            e = expected[inst]
            _, ack, fault = inst.split('_')
            cells = [str(e['violated'].get(p, 'ok')) for p in PROPS]
            print(f'| {ack}, {fault} | {e["D"]} | ' + ' | '.join(cells) + ' |')


if __name__ == '__main__':
    main()
