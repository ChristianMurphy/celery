"""Print an ITF trace as one line per action.

    python -m adapter.show traces/cex/redis_lease.atMostOnce.itf.json
"""
from __future__ import annotations

import sys

from . import itf

CHORD_VARS = ('ckey', 'skey', 'jset', 'counter', 'groupSaved', 'results',
              'dispatched', 'chordError', 'queue', 'uq')


def fmt(state: dict, prev: dict | None) -> str:
    if 'stored' in state:
        return f"stored={state['stored']}"
    parts = []
    for k in CHORD_VARS:
        if k in state and (prev is None or state[k] != prev.get(k)):
            v = state[k]
            if k == 'jset':
                v = sorted((dict(d)['h'], dict(d)['res'], dict(d)['tok'])
                           for d in v)
            parts.append(f'{k}={v}')
    return ' '.join(parts)


def main() -> None:
    for path in sys.argv[1:]:
        print(f'== {path}')
        prev = None
        for i, st in enumerate(itf.load(path)):
            a = st['lastAction']
            label = ' '.join(f'{k}={v}' for k, v in sorted(a.items())
                             if v not in ('', 0))
            print(f'{i:3} {label:45} {fmt(st, prev)}')
            prev = st


if __name__ == '__main__':
    main()
