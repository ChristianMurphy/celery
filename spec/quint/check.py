#!/usr/bin/env python3
"""Check every spec instance against every property, in three tiers.

    python3 check.py                  # fast: typecheck, type errors, simulator
    python3 check.py --all            # fast, on every instance
    python3 check.py --tier full      # exhaustive search (TLC), Apalache
    python3 check.py --tier long      # Apalache on the big instances too
    python3 check.py --only chord --instance redis_dup --tier full
    python3 check.py --tier full --update-expected   # rewrite expected.json
    python3 check.py --cex            # write counterexamples as ITF traces

fast   ``quint typecheck``; the mistakes in type_errors.py are rejected; one
       simulator run per instance: the properties expected to hold and
       ``steps <= D`` as invariants, the expected violations as witnesses,
       and a sample of traces to count which actions and outcomes ran.
       By default on the instances each spec lists as ``fast`` (about a
       minute); ``--all`` takes every instance.
full   one exhaustive search per instance with TLC (see tlc.py): every
       reachable state, the exact longest run D, every property that should
       hold, and that a run stops only in a quiet state; one more search per
       expected violation, for its shortest counterexample. Then Apalache
       on the small instances: ``steps <= D`` at depth D + 1, which makes
       its bounded check of each property at depth D complete.
long   the same Apalache checks for the other instances (each can take
       tens of minutes), and a larger simulator sample.

The outcome of every cell is compared with expected.json; a difference
fails the run.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import tlc

HERE = Path(__file__).resolve().parent
EXPECTED = HERE / 'expected.json'
#: Traces per instance read back to count actions and outcomes.
TRACES = 150
SCRATCH = tlc.SCRATCH


@dataclass
class Spec:
    file: str
    instances: list[str]
    # properties; each holds unless expected.json lists it as violated
    props: list[str]
    # "never X" invariants: violated means X is reachable
    witnesses: list[str]
    # instances small enough for Apalache in the full tier
    apalache: list[str] = field(default_factory=list)
    # instances the fast tier simulates unless --all is given
    fast: list[str] = field(default_factory=list)
    # the module whose ``type Action`` lists the actions and outcomes
    types: str = ''


FAULTS = ['none', 'dup', 'refuse', 'wlost_late', 'wlost_early', 'child_late',
          'child_early', 'child_reject']
SPECS = {
    'results': Spec(
        'resultstate.qnt', ['rtw', 'mongo', 'sql', 'es'],
        ['successStable', 'readySticky', 'progress'], ['noSuccessEver'],
        apalache=['rtw', 'mongo', 'sql', 'es'], types='resulttypes',
        fast=['rtw', 'mongo', 'sql', 'es']),
    'chord': Spec(
        'chord.qnt',
        [f'redis_{f}' for f in FAULTS] + ['redis_lease']
        + [f'kv_{f}' for f in FAULTS] + [f'unlock_{f}' for f in FAULTS],
        ['atMostOnce', 'noPremature', 'argsComplete', 'noErrorAfterDispatch',
         'noSpuriousError', 'quietDone', 'progress'],
        ['neverDispatched', 'neverFailed', 'neverQuiet'],
        types='chordtypes',
        fast=['redis_dup', 'redis_wlost_late', 'redis_child_late',
              'redis_lease', 'kv_dup', 'kv_refuse', 'kv_wlost_late',
              'unlock_dup', 'unlock_refuse', 'unlock_child_reject']),
}


def expected() -> dict:
    return json.loads(EXPECTED.read_text()) if EXPECTED.exists() else {}


def quint(*args: str, timeout: float | None = None) -> str:
    p = subprocess.run(['quint', *args], cwd=HERE, capture_output=True,
                       text=True, timeout=timeout)
    return p.stdout + p.stderr


# ---- fast tier -------------------------------------------------------------
def action_tags(state: dict) -> list[str]:
    """``Action`` and ``Action.Outcome`` of an ITF state's lastAction."""
    for name, value in state.items():
        if name.endswith('lastAction'):
            tags = [value['tag']]
            inner = value['value']
            if isinstance(inner, dict) and isinstance(inner.get('outcome'), dict):
                tags.append(f'{value["tag"]}.{inner["outcome"]["tag"]}')
            return tags
    return []


def simulate(spec: Spec, inst: str, exp: dict, samples: int,
             seed: str) -> dict:
    violated = exp.get('violated', {})
    holds = [p for p in spec.props if p not in violated]
    looked_for = [p for p in spec.props + spec.witnesses if p in violated]
    depth = exp.get('D', 60)
    out = Path(tempfile.mkdtemp(prefix='sim-', dir=SCRATCH))
    t0 = time.time()
    try:
        text = quint(
            'run', f'--main={inst}', str(tlc.instance_file(spec.file, inst)),
            f'--invariant={" and ".join(holds + [f"steps <= {depth}"])}',
            '--witnesses', *[f'not({p})' for p in looked_for],
            f'--max-samples={samples}', f'--max-steps={depth + 2}',
            f'--seed={seed}', f'--n-traces={TRACES}',
            f'--out-itf={out}/t_{{seq}}.itf.json')
        res: dict = {'secs': round(time.time() - t0, 1),
                     'ok': '[ok]' in text, 'witnessed': {}, 'actions': Counter()}
        for prop in looked_for:
            m = re.search(rf'not\({prop}\) was witnessed in (\d+) trace', text)
            res['witnessed'][prop] = int(m[1]) if m else 0
        m = re.search(r'Trace length statistics: max=(\d+)', text)
        # the simulator counts states; a run of n steps has n + 1
        res['longest'] = int(m[1]) - 1 if m else None
        if not res['ok']:
            res['why'] = [ln for ln in text.splitlines()
                          if 'rror' in ln or 'violation' in ln][:3]
            # name the invariant that failed
            for prop in holds + [f'steps <= {depth}']:
                one = quint('run', f'--main={inst}',
                            str(tlc.instance_file(spec.file, inst)),
                            f'--invariant={prop}', f'--max-samples={samples}',
                            f'--max-steps={depth + 2}', f'--seed={seed}')
                if '[violation]' in one:
                    res['why'].append(f'{prop} violated')
        for trace in out.glob('*.itf.json'):
            for state in json.loads(trace.read_text())['states']:
                res['actions'].update(action_tags(state))
        return res
    finally:
        shutil.rmtree(out, ignore_errors=True)


def declared_actions(spec: Spec) -> list[str]:
    """``Action`` and ``Action.Outcome`` for every variant of the spec's
    Action type, read from its types module."""
    text = (HERE / spec.file).read_text()
    text = text[text.index(f'module {spec.types} {{'):]
    sums = {m[1]: re.findall(r'\b([A-Z]\w*)\b(?:\([^|]*?\))?\s*(?:\||$)',
                             m[2].split('//')[0])
            for m in re.finditer(r'type (\w+) =((?:[^\n]|\n\s+\|)*)', text)}
    body = re.search(r'type Action =\n((?:\s+\|[^\n]*\n)+)', text)[1]
    tags = []
    for line in body.splitlines():
        m = re.match(r'\s+\| (\w+)(.*)', line)
        tags.append(m[1])
        outcome = re.search(r'outcome: (\w+)', m[2])
        if outcome:
            tags += [f'{m[1]}.{o}' for o in sums[outcome[1]]]
    return tags


def fast(names: list[str], only: str | None, samples: int, seed: str,
         jobs: int, every: bool = False) -> bool:
    ok = True
    t0 = time.time()
    for name in names:
        text = quint('typecheck', SPECS[name].file)
        if 'error' in text.lower():
            ok = False
            print(f'TYPECHECK FAILED {SPECS[name].file}\n{text}')
    p = subprocess.run([sys.executable, 'type_errors.py'], cwd=HERE,
                       capture_output=True, text=True)
    print(p.stdout.strip().splitlines()[-1])
    ok &= p.returncode == 0
    exp = expected()
    for name in names:
        spec = SPECS[name]
        insts = [i for i in spec.instances
                 if (i == only if only else every or i in spec.fast)]
        with cf.ThreadPoolExecutor(jobs) as ex:
            res = dict(zip(insts, ex.map(
                lambda i: simulate(spec, i, exp.get(spec.file, {}).get(i, {}),
                                   samples, seed), insts)))
        print(f'\n{spec.file} (simulator, {samples} samples per instance, '
              f'{len(insts)} of {len(spec.instances)} instances)')
        print('| instance | invariants and steps <= D | longest run | '
              'expected violations seen (traces) | time |')
        print('|---|---|---|---|---|')
        total: Counter = Counter()
        for inst in insts:
            r = res[inst]
            ok &= r['ok']
            total.update(r['actions'])
            seen = ', '.join(f'{p} {n}' for p, n in r['witnessed'].items())
            d = exp.get(spec.file, {}).get(inst, {}).get('D', '?')
            print(f'| {inst} | {"ok" if r["ok"] else "FAILED " + str(r["why"])}'
                  f' | {r["longest"]} of {d} | {seen or "-"} | {r["secs"]} s |')
        if only is None:
            # coverage: which actions and outcomes ran in the sampled traces
            declared = declared_actions(spec)
            missing = [t for t in declared if not total[t]]
            print(f'\nactions and outcomes in {TRACES} sampled traces per '
                  f'instance: {len(declared) - len(missing)} of {len(declared)}'
                  + (f'; not sampled: {", ".join(missing)}' if missing else ''))
            print(', '.join(f'{t} {total[t]}' for t in declared))
            if every:
                ok &= not missing
    print(f'\nfast tier: {time.time() - t0:.0f} s wall, '
          f'{"ok" if ok else "FAILED"}')
    return ok


# ---- full tier -------------------------------------------------------------
def search(spec: Spec, inst: str, tla: Path, exp: dict) -> dict:
    """Exhaustive search of one instance: the properties expected to hold in
    one pass (with the longest run and the quiet-stop check), then each
    expected violation on its own for its shortest counterexample."""
    violated = exp.get('violated', {})
    # a witness expected to hold (its state is unreachable here) is checked
    # in the same pass
    holds = [p for p in spec.props + spec.witnesses if p not in violated]
    res: dict = {'cells': {}}
    main = tlc.run_tlc(tla, holds, deadlock=True)
    res['secs'] = main['secs']
    res['states'] = main.get('distinct')
    if main['result'] == 'holds':
        res['D'] = main['depth'] - 1
        res['cells'] = {p: 'holds' for p in holds}
        res['stops_quiet'] = True
    else:
        # something unexpected: check each on its own to report all of them
        res['stops_quiet'] = main['result'] != 'deadlock'
        if main['result'] == 'deadlock':
            res['deadlock'] = main['trace']
        for prop in holds:
            r = tlc.run_tlc(tla, prop)
            res['secs'] += r['secs']
            res['cells'][prop] = (r['trace'] if r['result'] == 'violated'
                                  else r['result'])
        r = tlc.run_tlc(tla, [])
        res['D'] = r.get('depth', 0) - 1
        res['states'] = r.get('distinct')
    for prop in spec.props + spec.witnesses:
        if prop in violated:
            r = tlc.run_tlc(tla, prop)
            res['secs'] += r['secs']
            res['cells'][prop] = (r['trace'] if r['result'] == 'violated'
                                  else r['result'])
    res['secs'] = round(res['secs'], 1)
    return res


def apalache(spec: Spec, inst: str, depth: int, exp: dict,
             timeout: float | None) -> dict:
    """Bounded model checking at the proven depth. ``steps <= D`` holding
    at depth D + 1 shows no run has a step D + 1, so the bounded check of
    each property at depth D covers every run. One call at a time: the
    Apalache server gives wrong answers to parallel calls."""
    res: dict = {}
    violated = exp.get('violated', {})

    def verify(invariant: str, steps: int) -> tuple[str, float]:
        t0 = time.time()
        try:
            text = quint('verify', f'--main={inst}',
                         str(tlc.instance_file(spec.file, inst)),
                         f'--invariant={invariant}',
                         '--apalache-config=apalache.json',
                         f'--max-steps={steps}', timeout=timeout)
        except subprocess.TimeoutExpired:
            return 'timeout', round(time.time() - t0)
        verdict = ('violated' if '[violation]' in text
                   else 'holds' if '[ok]' in text
                   else 'error: ' + text.strip().splitlines()[-1][:100])
        return verdict, round(time.time() - t0)

    res[f'steps <= {depth}'] = verify(f'steps <= {depth}', depth + 1)
    for prop in spec.props:
        if prop == 'progress':
            # arithmetic over every process and budget: minutes per call in
            # Apalache, and the exhaustive search has checked it already
            continue
        res[prop] = verify(prop, depth)
        want = 'violated' if prop in violated else 'holds'
        if res[prop][0] not in (want, 'timeout'):
            res['MISMATCH ' + prop] = (f'expected {want}', 0)
    return res


def full(names: list[str], only: str | None, jobs: int, update: bool,
         long: bool, apalache_timeout: float, as_json: str | None) -> bool:
    ok = True
    t0 = time.time()
    exp = expected()
    report: dict = {}
    for name in names:
        spec = SPECS[name]
        insts = [i for i in spec.instances if only in (None, i)]
        tc = time.time()
        tlas = {i: tlc.compile_tla(spec.file, i, spec.props + spec.witnesses,
                                   step='stepOrQuiet') for i in insts}
        compile_secs = time.time() - tc
        with cf.ThreadPoolExecutor(jobs) as ex:
            res = dict(zip(insts, ex.map(
                lambda i: search(spec, i, tlas[i],
                                 exp.get(spec.file, {}).get(i, {})), insts)))
        report[spec.file] = res
        cols = spec.props + spec.witnesses
        print(f'\n{spec.file} (TLC, every reachable state; compile '
              f'{compile_secs:.0f} s)')
        print('| instance | D | states | ' + ' | '.join(cols)
              + ' | stops only when quiet | time |')
        print('|---' * (len(cols) + 5) + '|')
        for inst in insts:
            r = res[inst]
            e = exp.get(spec.file, {}).get(inst, {})
            cells = []
            for prop in cols:
                got = r['cells'].get(prop)
                want = e.get('violated', {}).get(prop, 'holds')
                cell = 'ok' if got == 'holds' else f'VIOLATED ({got})'
                if got != want and not update:
                    ok = False
                    cell += f' EXPECTED {want}'
                cells.append(cell)
            d = str(r['D'])
            if e.get('D') != r['D'] and not update:
                ok = False
                d += f' EXPECTED {e.get("D")}'
            quiet = 'yes' if r['stops_quiet'] else 'NO'
            ok &= r['stops_quiet']
            print(f'| {inst} | {d} | {r["states"]} | ' + ' | '.join(cells)
                  + f' | {quiet} | {r["secs"]} s |')
        if update:
            exp[spec.file] = exp.get(spec.file, {})
            for inst in insts:
                exp[spec.file][inst] = {
                    'D': res[inst]['D'],
                    'violated': {p: v for p, v in res[inst]['cells'].items()
                                 if v != 'holds'}}
    tlc_secs = time.time() - t0
    if update:
        EXPECTED.write_text(json.dumps(exp, indent=1, sort_keys=True) + '\n')
        print(f'wrote {EXPECTED.name}')
    # Apalache, one call at a time
    ta = time.time()
    for name in names:
        spec = SPECS[name]
        insts = [i for i in spec.instances if only in (None, i)
                 and (long or i in spec.apalache)]
        if not insts:
            continue
        print(f'\n{spec.file} (Apalache, bounded at the proven depth)')
        print('| instance | check | depth | result | time |')
        print('|---|---|---|---|---|')
        for inst in insts:
            e = exp.get(spec.file, {}).get(inst, {})
            depth = report[spec.file][inst]['D']
            r = apalache(spec, inst, depth, e, apalache_timeout)
            report[spec.file][inst]['apalache'] = r
            for check, (verdict, secs) in r.items():
                steps = depth + 1 if check.startswith('steps') else depth
                if check.startswith('MISMATCH') or 'error' in verdict:
                    ok = False
                print(f'| {inst} | {check} | {steps} | {verdict} | {secs} s |')
    if as_json:
        Path(as_json).write_text(json.dumps(report, indent=1, sort_keys=True))
    print(f'\n{"long" if long else "full"} tier: TLC {tlc_secs:.0f} s, '
          f'Apalache {time.time() - ta:.0f} s, '
          f'{time.time() - t0:.0f} s wall, {"ok" if ok else "FAILED"}')
    return ok


# ---- counterexamples as ITF traces (for the replay adapters) ---------------
def write_cex(names: list[str], samples: int, seed: str) -> None:
    exp = expected()
    out = HERE / 'traces' / 'cex'
    out.mkdir(parents=True, exist_ok=True)
    for name in names:
        spec = SPECS[name]
        for inst in spec.instances:
            e = exp.get(spec.file, {}).get(inst, {})
            for prop in e.get('violated', {}):
                path = out / f'{inst}.{prop}.itf.json'
                text = quint('run', f'--main={inst}',
                             str(tlc.instance_file(spec.file, inst)),
                             f'--invariant={prop}',
                             f'--max-steps={e["D"] + 2}',
                             f'--max-samples={samples}', f'--seed={seed}',
                             f'--out-itf={path}')
                if '[violation]' not in text:
                    path.unlink(missing_ok=True)
                    print(f'{inst}.{prop}: not found by the simulator')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--tier', choices=['fast', 'full', 'long'], default='fast')
    ap.add_argument('--only', choices=sorted(SPECS))
    ap.add_argument('--instance')
    ap.add_argument('--samples', type=int)
    ap.add_argument('--seed', default='17')
    ap.add_argument('--jobs', type=int, default=3)
    ap.add_argument('--update-expected', action='store_true')
    ap.add_argument('--apalache-timeout', type=float, default=3600)
    ap.add_argument('--json')
    ap.add_argument('--cex', action='store_true')
    ap.add_argument('--all', action='store_true',
                    help='fast tier: every instance')
    args = ap.parse_args()
    names = [args.only] if args.only else list(SPECS)
    if args.cex:
        write_cex(names, args.samples or 3000, args.seed)
        return 0
    if args.tier == 'fast':
        return 0 if fast(names, args.instance, args.samples or 400,
                         args.seed, args.jobs, args.all) else 1
    ok = full(names, args.instance, args.jobs, args.update_expected,
              args.tier == 'long', args.apalache_timeout, args.json)
    if args.tier == 'long':
        ok &= fast(names, args.instance, args.samples or 20000, args.seed,
                   args.jobs, True)
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
