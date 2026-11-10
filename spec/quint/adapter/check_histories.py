"""Check recorded histories against resultstate.qnt with ``quint test``.

Each history becomes a Quint ``run``: the recorded actions in their order,
and after each one an ``expect`` that the spec's ``stored`` equals what was
read back from the backend. ``quint test`` passes a run only if every action
is enabled in the spec and every expectation holds, i.e. the spec allows the
recorded behavior. Histories in which a reader saw SUCCESS and the store
later held something else (before any forget) are listed: those are real
violations of ``successStable``.

    python -m adapter.check_histories histories/redis.jsonl
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
SPEC_STORE = {'redis': 'rtw', 'mongo': 'mongo', 'sql': 'sql', 'es': 'es'}
STICKY = {'es': 'true'}


def status(s: str) -> str:
    """'SUCCESS' -> the spec's constructor Success."""
    return s.capitalize()


def meta(m: dict) -> str:
    """What was read back, as the spec's Stored (a missing result reads as
    PENDING)."""
    if m['status'] == 'PENDING':
        return 'Absent'
    return f'Present({{ status: {status(m["status"])}, by: R({m["v"]}) }})'


def to_run(i: int, events: list) -> str:
    lines = [f'  run h{i} = init']
    for e in events:
        k = e['kind']
        if k == 'begin':
            act = f'begin(R({e["rn"]}), {status(e["status"])})'
        elif k == 'commit':
            act = f'commit(R({e["rn"]}))'
        elif k == 'forget':
            act = 'forget'
        elif k == 'observe':
            act = 'observe'
        else:
            raise ValueError(k)
        lines.append(f'    .then({act}).expect(stored == {meta(e["stored"])})')
    return '\n'.join(lines)


def success_lost(events: list) -> bool:
    seen = None
    for e in events:
        if e['kind'] == 'forget':
            seen = None
        elif e['kind'] == 'observe':
            seen = e['stored']
        if seen is not None and e['stored'] != seen:
            return True
    return False


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--as-store', choices=sorted(set(SPEC_STORE.values())),
                    help='check against another store model (sanity check: '
                         'redis histories must fail as mongo)')
    ap.add_argument('paths', nargs='+', type=Path)
    args = ap.parse_args()
    bad = 0
    for path in args.paths:
        hists = [json.loads(line) for line in path.read_text().splitlines()]
        backend = hists[0]['backend']
        store = args.as_store or SPEC_STORE[backend]
        mod = f'hist_{backend}' + (f'_as_{store}' if args.as_store else '')
        body = '\n\n'.join(to_run(i, h['events']) for i, h in enumerate(hists))
        src = (f'module {mod} {{\n'
               f'  import resulttypes.* from "../resultstate"\n'
               f'  import resultstate(STORE = {store.capitalize()}, '
               f'READY_STICKY = {STICKY.get(backend, "false")}).* '
               f'from "../resultstate"\n\n'
               f'{body}\n}}\n')
        out = HERE / 'histories' / f'{mod}.qnt'
        out.write_text(src)
        p = subprocess.run(['quint', 'test', '--backend=typescript', f'--main={mod}', str(out),
                            '--match=^h[0-9]+$'],
                           cwd=HERE, capture_output=True, text=True)
        text = p.stdout + p.stderr
        passed = text.count('ok h')
        failed = [ln.strip() for ln in text.splitlines()
                  if ln.strip().startswith(('1)', '2)', '3)', 'x ', '✗'))]
        lost = [i for i, h in enumerate(hists) if success_lost(h['events'])]
        print(f'{path}: {passed}/{len(hists)} histories allowed by '
              f'resultstate.qnt ({store}); quint test exit {p.returncode}')
        if p.returncode != 0:
            bad += 1
            print('\n'.join(text.splitlines()[-25:]))
        print(f'  histories where an observed SUCCESS was replaced: '
              f'{len(lost)} {lost[:10]}')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
