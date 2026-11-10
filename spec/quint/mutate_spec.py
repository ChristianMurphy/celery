#!/usr/bin/env python3
"""Plant bugs in the spec and check that a property catches each one.

Each mutant is the spec with one rule changed back to what the code did
before a fix (or a plausible slip). The property named must be violated on
an instance where the unmutated spec satisfies it.

    python3 mutate_spec.py
"""
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

# (name, spec, instance, property, old, new)
MUTANTS = [
    ('mongo filter without $ne SUCCESS', 'resultstate.qnt', 'mongo', 'successStable',
     'if (curStatus == Success) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>',
     'if (false) (cur, Skipped)\n        else if (stickyRefuses) (cur, Skipped)\n        else (Present(w), Written)\n      | Sql =>'),
    ('sql without the locked re-read', 'resultstate.qnt', 'sql', 'successStable',
     'else if (curStatus == Success) (cur, Skipped) // :186-190',
     'else if (read == Success) (cur, Skipped) // :186-190'),
    ('es without the ready check', 'resultstate.qnt', 'es', 'readySticky',
     'else if (isReady(curStatus) and isUnready(w.status))\n          (cur, Skipped)',
     'else if (false)\n          (cur, Skipped)'),
    ('redis: no dedupe (before celery PR 10919)', 'chord.qnt', 'redis_dup', 'argsComplete',
     'val fresh = not(dset.contains(r.h))', 'val fresh = true'),
    ('redis: claim without NX (before celery PR 10998)', 'chord.qnt', 'redis_dup', 'atMostOnce',
     'val claim = complete and ckey == NoClaim', 'val claim = complete'),
    # The two owner checks below only matter when a claim can expire while
    # its holder still runs (leaseSafe: false), and then atMostOnce fails
    # without the mutation too: expected to be equivalent mutants.
    ('EQUIV redis: extend ignores the owner', 'chord.qnt', 'redis_wlost_late', 'atMostOnce',
     'val mine = ckey == Claimed(r.tok)\n    val reacquire = ckey == NoClaim and skey',
     'val mine = true\n    val reacquire = ckey == NoClaim and skey'),
    ('EQUIV redis: cleanup ignores the owner (celery issue 10997)', 'chord.qnt', 'redis_dup', 'atMostOnce',
     'val mine = ckey == Claimed(r.tok)\n    all {\n      CFG.proto == Redis, isHeader(e), r.pc == RedisPublished',
     'val mine = true\n    all {\n      CFG.proto == Redis, isHeader(e), r.pc == RedisPublished'),
    ('kv: count on >= size', 'chord.qnt', 'kv_dup', 'atMostOnce',
     '...r, pc: if (counter + 1 == N) KvComplete else HeaderDone',
     '...r, pc: if (counter + 1 >= N) KvComplete else HeaderDone'),
    ('unlock: retry refusal not failing the chord (before celery PR 10959)', 'chord.qnt', 'unlock_refuse', 'quietDone',
     "markError, uq' = uq,\n        used' = { ...used, refusals: used.refusals + 1 },",
     "keepObs, uq' = uq,\n        used' = { ...used, refusals: used.refusals + 1 },"),
    ('lost child counted again (before celery PR 10752)', 'chord.qnt', 'kv_child_late', 'noSpuriousError',
     'if (isHeader(e) and results.get(r.h) == Success) all {',
     'if (false) all {'),
]


def violated(spec_text: str, instance: str, prop: str, samples: int) -> bool:
    with tempfile.NamedTemporaryFile('w', suffix='.qnt', dir=HERE,
                                     prefix='.mutant_', delete=False) as f:
        f.write(spec_text)
        path = Path(f.name)
    try:
        p = subprocess.run(
            ['quint', 'run', f'--main={instance}', str(path),
             f'--invariant={prop}', '--max-steps=40',
             f'--max-samples={samples}', '--seed=23'],
            cwd=HERE, capture_output=True, text=True)
        return '[violation]' in p.stdout + p.stderr
    finally:
        path.unlink()


def main() -> None:
    caught = 0
    for name, spec, inst, prop, old, new in MUTANTS:
        text = (HERE / spec).read_text()
        assert text.count(old) == 1, f'{name}: pattern found {text.count(old)}x'
        base = violated(text, inst, prop, 3000)
        mut = violated(text.replace(old, new), inst, prop, 3000)
        ok = mut and not base
        if name.startswith('EQUIV'):
            ok = not ok
            label = 'equiv  ' if ok else 'NOT EQUIV'
        else:
            label = 'caught ' if ok else 'MISSED '
        caught += ok
        print(f'{label} {name}: {inst}.{prop} '
              f'(original {"violated" if base else "holds"}, mutant '
              f'{"violated" if mut else "holds"})')
    print(f'{caught}/{len(MUTANTS)} spec mutants behaved as expected')


if __name__ == '__main__':
    main()
