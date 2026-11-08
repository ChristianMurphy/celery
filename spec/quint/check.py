#!/usr/bin/env python3
"""Run the simulator over every spec instance and property; print a matrix.

    python3 check.py                 # results + chords, simulator
    python3 check.py --verify        # also Apalache bounded model checking
    python3 check.py --only chord --samples 5000

Each cell is "ok" (no violation in the samples / up to the bound) or
"VIOLATED" (simulator, --seed 17 by default). Counterexamples are
written as ITF to traces/cex/<instance>.<property>.itf.json.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

RESULT_INSTANCES = ['rtw', 'mongo', 'sql', 'es']
RESULT_PROPS = ['successStable', 'readySticky']

FAULTS = ['none', 'dup', 'refuse', 'wlost_late', 'wlost_early', 'child_late',
          'child_early', 'child_reject']
CHORD_INSTANCES = ([f'redis_{f}' for f in FAULTS] + ['redis_lease']
                   + [f'kv_{f}' for f in FAULTS]
                   + [f'unlock_{f}' for f in FAULTS])
CHORD_PROPS = ['atMostOnce', 'noPremature', 'argsComplete',
               'noErrorAfterDispatch', 'noSpuriousError', 'quietDone',
               'neverDispatched']


def sim(spec: str, main: str, prop: str, samples: int, steps: int,
        seed: str) -> str:
    out = HERE / 'traces' / 'cex' / f'{main}.{prop}.itf.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = ['quint', 'run', f'--main={main}', spec, f'--invariant={prop}',
           f'--max-steps={steps}', f'--max-samples={samples}', f'--seed={seed}',
           f'--out-itf={out}']
    p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    text = p.stdout + p.stderr
    if '[violation]' in text:
        return 'VIOLATED'
    out.unlink(missing_ok=True)
    if '[ok]' in text:
        return 'ok'
    return 'ERROR: ' + text.strip().splitlines()[-1][:120]


def verify(spec: str, main: str, prop: str, steps: int) -> str:
    # apalache.json turns off deadlock checks: a run that used its whole
    # budget (writes, faults) stops, which is not a bug
    cmd = ['quint', 'verify', f'--main={main}', spec, f'--invariant={prop}',
           '--apalache-config=apalache.json',
           f'--max-steps={steps}']
    p = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    text = p.stdout + p.stderr
    if '[violation]' in text:
        return 'VIOLATED'
    if '[ok]' in text:
        return f'ok<={steps}'
    return 'ERROR: ' + text.strip().splitlines()[-1][:120]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', choices=['results', 'chord'])
    ap.add_argument('--samples', type=int, default=3000)
    ap.add_argument('--steps', type=int, default=40)
    ap.add_argument('--seed', default='17')
    ap.add_argument('--verify', action='store_true')
    ap.add_argument('--verify-steps', type=int, default=12)
    ap.add_argument('--jobs', type=int, default=4)
    args = ap.parse_args()

    jobs = []
    if args.only in (None, 'results'):
        jobs += [('resultstate.qnt', m, p) for m in RESULT_INSTANCES
                 for p in RESULT_PROPS]
    if args.only in (None, 'chord'):
        jobs += [('chord.qnt', m, p) for m in CHORD_INSTANCES
                 for p in CHORD_PROPS]

    with cf.ThreadPoolExecutor(args.jobs) as ex:
        futs = {ex.submit(sim, s, m, p, args.samples, args.steps, args.seed):
                (s, m, p) for s, m, p in jobs}
        res = {futs[f]: f.result() for f in cf.as_completed(futs)}
    # one at a time: quint verify talks to one shared Apalache server, and
    # parallel calls get each other's answers or a dropped connection
    vres = ({(s, m, p): verify(s, m, p, args.verify_steps) for s, m, p in jobs}
            if args.verify else {})

    for spec in ('resultstate.qnt', 'chord.qnt'):
        rows = sorted({m for s, m, _ in jobs if s == spec},
                      key=lambda m: [x[1] for x in jobs].index(m))
        if not rows:
            continue
        props = RESULT_PROPS if spec == 'resultstate.qnt' else CHORD_PROPS
        print(f'\n{spec}')
        print('| instance | ' + ' | '.join(props) + ' |')
        print('|---' * (len(props) + 1) + '|')
        for m in rows:
            cells = []
            for p in props:
                cell = res[(spec, m, p)]
                if args.verify:
                    cell += f' / {vres[(spec, m, p)]}'
                cells.append(cell)
            print(f'| {m} | ' + ' | '.join(cells) + ' |')


if __name__ == '__main__':
    main()
