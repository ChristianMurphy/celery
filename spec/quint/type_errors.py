#!/usr/bin/env python3
"""Mistakes the types catch: each snippet must fail ``quint typecheck``.

Most cases pair a mistake written against the typed specs (``new``) with
the same mistake against the specs at 355c8b7, where states were strings and
ids were ints (``old``); the cases for what was specified since have none. The new one must be rejected. With ``--old`` the old
one is checked too, against the old specs from git: it must be accepted,
which shows the mistake used to pass.

    python3 type_errors.py
    python3 type_errors.py --old
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD_REV = '355c8b7'

NEW_CHORD = '''module t {
  import chordtypes.* from "./chord"
  import chord(CFG = lateAck(Redis)).* from "./chord"
  %s
}
'''
OLD_CHORD = '''module t {
  import chord(PROTO = "redis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false,
               LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0,
               MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).* from "./chord"
  %s
}
'''

# (what, new module, old module)
CASES = [
    ('a misspelled result state',
     NEW_CHORD % 'val bad = results.get(H(1)) == Sucess',
     OLD_CHORD % 'val bad = results.get(1) == "SUCESS"'),
    ('a claim token used as a header id',
     NEW_CHORD % 'val bad = queue.get(headerOf(execs.get(W(1))).tok)',
     OLD_CHORD % 'val bad = queue.get(execs.get(1).tok)'),
    ('a header id where an exec slot is expected (arguments swapped)',
     NEW_CHORD % 'run bad = init.then(deliver(H(1), W(2)))',
     OLD_CHORD % 'run bad = init.then(deliver(1, 2))'),
    ('a chord_unlock run with a claim token',
     NEW_CHORD % 'val bad: Exec = UnlockTask({ acked: false, pc: Polling, '
                 'tok: Tok(1) })',
     OLD_CHORD % 'val bad: Exec = { ...FREE, busy: true, kind: "unlock", '
                 'pc: "upoll", tok: 7 }'),
    ('a chord_unlock run at a header-only program counter',
     NEW_CHORD % 'val bad: Exec = UnlockTask({ acked: false, '
                 'pc: RedisClaimed })',
     OLD_CHORD % 'val bad: Exec = { ...FREE, busy: true, kind: "unlock", '
                 'pc: "claimed" }'),
    ('the claim key compared with an int sentinel',
     NEW_CHORD % 'val bad = ckey == -1',
     OLD_CHORD % 'val bad = ckey == -2'),
    ('the outcome of another action (a part-return outcome on a count)',
     NEW_CHORD % 'val bad = lastAction == Count({ w: W(1), h: H(1), '
                 'outcome: TookClaim })',
     OLD_CHORD % 'val bad = lastAction == { kind: "count", w: 1, h: 1, '
                 'outcome: "claimed" }'),
    ('a count compared with a result state',
     NEW_CHORD % 'val bad = results.get(H(1)) == dispatched',
     OLD_CHORD % 'val bad = queue.get(1) == dispatched'),
    ('a misspelled protocol (every guard false, every property holds)',
     '''module t {
  import chordtypes.* from "./chord"
  import chord(CFG = lateAck(Reddis)).* from "./chord"
}
''',
     '''module t {
  import chord(PROTO = "reddis", ACKS_LATE = true, REJECT_ON_WORKER_LOST = false,
               LEASE_SAFE = true, MAX_DUPS = 0, MAX_WORKER_LOST = 0,
               MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).* from "./chord"
}
'''),
    ('an unknown store (was silently checked as the last branch, "es")',
     '''module t {
  import resulttypes.* from "./resultstate"
  import resultstate(STORE = Redis, READY_STICKY = false).* from "./resultstate"
}
''',
     '''module t {
  import resultstate(STORE = "redis", READY_STICKY = false).* from "./resultstate"
}
'''),
    ('a run id used as a write count',
     '''module t {
  import resulttypes.* from "./resultstate"
  import resultstate(STORE = Rtw, READY_STICKY = false).* from "./resultstate"
  val bad = writes.get(R(1)) < R(2)
}
''',
     '''module t {
  import resultstate(STORE = "rtw", READY_STICKY = false).* from "./resultstate"
  val bad = writes.get(1) < 2
}
'''),
    ('a chord key that is not one (evicting a header id)',
     NEW_CHORD % 'run bad = init.then(evict(H(1)))', None),
    ('an interrupt handling that does not exist',
     '''module t {
  import chordtypes.* from "./chord"
  import chord(CFG = { ...lateAck(Redis), onInterrupt: FailSometimes }).* from "./chord"
}
''', None),
    ('a commit outcome that no store returns',
     '''module t {
  import resulttypes.* from "./resultstate"
  import resultstate(STORE = Rtw, READY_STICKY = false).* from "./resultstate"
  val bad = lastAction == Commit({ rn: R(1), status: Success, outcome: Sent })
}
''',
     '''module t {
  import resultstate(STORE = "rtw", READY_STICKY = false).* from "./resultstate"
  val bad = lastAction.outcome == "sent"
}
'''),
]


def typechecks(directory: Path, source: str) -> tuple[bool, str]:
    with tempfile.NamedTemporaryFile('w', suffix='.qnt', dir=directory,
                                     prefix='.typeerr_', delete=False) as f:
        f.write(source)
        path = Path(f.name)
    try:
        p = subprocess.run(['quint', 'typecheck', str(path)], cwd=directory,
                           capture_output=True, text=True)
        lines = [ln.strip() for ln in (p.stdout + p.stderr).splitlines()
                 if 'Error' in ln or ln.startswith('Trying')]
        return p.returncode == 0, ' '.join(lines[:2])[:150]
    finally:
        path.unlink()


def main() -> int:
    old_dir = None
    if '--old' in sys.argv:
        old_dir = Path(tempfile.mkdtemp(prefix='oldspec_'))
        for name in ('chord.qnt', 'resultstate.qnt'):
            (old_dir / name).write_text(subprocess.run(
                ['git', 'show', f'{OLD_REV}:spec/quint/{name}'], cwd=HERE,
                capture_output=True, text=True, check=True).stdout)
    bad = 0

    def check(case: tuple) -> tuple:
        what, new, old = case
        was_ok = typechecks(old_dir, old)[0] if old_dir and old else None
        return what, *typechecks(HERE, new), was_ok

    with ThreadPoolExecutor(4) as ex:
        results = list(ex.map(check, CASES))
    for what, ok, why, was_ok in results:
        line = f'{"MISSED  " if ok else "rejected"} {what}'
        bad += ok
        if was_ok is not None:
            line += f' (untyped spec: {"accepted" if was_ok else "REJECTED"})'
            bad += not was_ok
        print(line)
        if not ok:
            print(f'           {why}')
    print(f'{len(CASES) - bad}/{len(CASES)} mistakes are type errors now'
          + (' and typechecked before' if old_dir else ''))
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
