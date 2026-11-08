"""Replay resultstate.qnt traces against real result backends.

For each ITF trace, every step's ``lastAction`` is performed on the backend
with the real backend methods, in the trace's order, and the stored state
read back afterwards must equal the spec's ``stored``. A ``commit`` that the
spec says raises (outcome "error") must raise, and only then.

    python -m adapter.replay_results --backend redis traces/rtw/*.itf.json
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from . import itf
from .backends import (SPEC_STORE, finish_store, install_hooks, make_app,
                       observe, start_store, value_for)


class Mismatch(AssertionError):
    pass


def replay(name: str, path: Path, backend, reader) -> int:
    states = itf.load(path)
    task_id = f'quint-{uuid.uuid4().hex}'
    runs = {}
    for i, st in enumerate(states):
        act = st['lastAction']
        kind = act['kind']
        if kind == 'init':
            pass
        elif kind == 'begin':
            r = act['rn']
            runs[r] = start_store(backend, task_id,
                                  value_for(act['status'], act['v']),
                                  act['status'])
        elif kind == 'commit':
            r = act['rn']
            err = finish_store(runs.pop(r))
            want_err = act['outcome'] == 'error'
            if bool(err) != want_err:
                raise Mismatch(f'{path.name} step {i}: commit by run {r} '
                               f'raised {err!r}, spec outcome {act["outcome"]}')
        elif kind == 'forget':
            backend.forget(task_id)
        elif kind == 'observe':
            pass  # the comparison below is the observation
        else:
            raise ValueError(kind)
        try:
            got = observe(reader, task_id)
        except Exception as exc:  # noqa: BLE001 - an unreadable row is a mismatch
            raise Mismatch(f'{path.name} step {i} ({kind} {act}): reading the '
                           f'result raised {exc!r}') from exc
        want = st['stored']
        if got != want:
            raise Mismatch(f'{path.name} step {i} ({kind} {act}): backend has '
                           f'{got}, spec has {want}')
    for run in runs.values():  # runs left parked at the end of the trace
        run.gate.go.set()
        run.thread.join(5)
    return len(states)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backend', required=True, choices=sorted(SPEC_STORE))
    ap.add_argument('--mutate', action='append', default=[],
                    help='apply a code mutation from adapter.mutations')
    ap.add_argument('traces', nargs='+', type=Path)
    args = ap.parse_args(argv)

    if args.mutate:
        from . import mutations
        for m in args.mutate:
            mutations.apply(m)

    backend = make_app(args.backend).backend
    reader = make_app(args.backend).backend
    install_hooks(args.backend, backend)
    steps = failed = 0
    for path in args.traces:
        try:
            steps += replay(args.backend, path, backend, reader)
        except Mismatch as exc:
            failed += 1
            print(f'MISMATCH {exc}')
    print(f'{args.backend}: {len(args.traces) - failed}/{len(args.traces)} '
          f'traces matched ({steps} states)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
