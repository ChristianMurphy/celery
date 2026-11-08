"""Record chord runs from a real worker and check them against chord.qnt.

Starts ``celery -A adapter.worker_app worker`` (prefork, 2 children, Redis
broker, acks_late, a short visibility timeout), applies chords whose header
tasks sometimes sleep past the visibility timeout so the broker redelivers
them while they run, and collects the JSON lines the worker logs. Each
chord's events become a Quint ``run`` over an instance of chord.qnt, with
``expect`` on every step's outcome; ``quint test`` then says whether the
spec allows what the worker did.

    python -m adapter.record_chords --proto redis --chords 20
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
N = 2


def record(proto: str, chords: int, vt: int, seed: int, log: Path) -> None:
    log.unlink(missing_ok=True)
    # a queue of its own, so nothing from an earlier run is redelivered
    env = dict(os.environ, QUINT_PROTO=proto, QUINT_LOG=str(log),
               QUINT_VT=str(vt), QUINT_QUEUE=f'quint-{uuid.uuid4().hex[:8]}')
    env['PYTHONPATH'] = f'{HERE}:{env.get("PYTHONPATH", "")}'
    worker = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'adapter.worker_app', 'worker',
         '-P', 'prefork', '-c', '2', '-l', 'warning', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=HERE, env=env, stdout=subprocess.DEVNULL,
        stderr=open(log.with_suffix('.worker.log'), 'w'))
    try:
        sys.path.insert(0, str(HERE))
        os.environ.update(env)
        from celery import chord

        from adapter.worker_app import body, errback, header
        time.sleep(3)
        rng = random.Random(seed)
        for _ in range(chords):
            sleeps = [rng.choice([0, 0.2, vt + 1.5]) for _ in range(N)]
            res = chord([header.s(i + 1, s) for i, s in enumerate(sleeps)])(
                body.s().on_error(errback.s()))
            try:
                res.get(timeout=4 * vt + 10, propagate=False)
            except Exception:  # noqa: BLE001 - the history says what happened
                pass
            time.sleep(vt + 2)   # let late redeliveries run too
    finally:
        worker.send_signal(signal.SIGTERM)
        worker.wait(60)


def split(log: Path) -> dict[str, list]:
    """Group events by chord: deliver/store/finish carry the group id; the
    others belong to whatever header the same process is running."""
    current: dict[int, str] = {}
    by_gid: dict[str, list] = defaultdict(list)
    for line in log.read_text().splitlines():
        e = json.loads(line)
        if e['kind'] == 'deliver':
            current[e['pid']] = e['gid']
        gid = e.get('gid') or current.get(e['pid'])
        if gid and e['kind'] != 'body_ran':
            by_gid[gid].append(e)
    return by_gid


def to_run(i: int, proto: str, events: list) -> str:
    steps: list[str] = []
    queued = {h: 1 for h in range(1, N + 1)}
    slot_of: dict[int, int] = {}
    pc: dict[int, str] = {}
    free = [1, 2, 3]
    published = 0

    def emit(action: str, outcome: str | None = None, ne: bool = False) -> None:
        if outcome is None:
            steps.append(f'    .then({action})')
        else:
            op = '!=' if ne else '=='
            steps.append(f'    .then({action}).expect(lastAction.outcome {op} "{outcome}")')

    for e in events:
        k = e['kind']
        if k == 'deliver':
            h = e['h']
            if queued[h] == 0:
                emit(f'dup({h})')
                queued[h] += 1
            w = free.pop(0)
            slot_of[e['pid']] = w
            pc[w] = 'run'
            queued[h] -= 1
            emit(f'deliver({w}, {h})')
            continue
        w = slot_of.get(e['pid'])
        if w is None:
            continue
        if k == 'store':
            emit(f'store({w})')
            pc[w] = 'stored'
        elif k == 'partReturn':
            ready, _, size, claimed = e['ret']
            out = ('done' if ready == -1 else 'claimed' if claimed
                   else 'complete' if size and int(ready) == int(size) else 'counted')
            emit(f'partReturn({w})', out)
            pc[w] = 'claimed' if claimed else 'finish'
        elif k == 'count':
            r = e['ret']
            out = 'complete' if r == N else 'over' if r > N else 'counted'
            emit(f'count({w})', out)
            pc[w] = 'kcomplete' if r == N else 'finish'
        elif k == 'extend':
            if pc[w] == 'claimed':
                emit(f'redisJoin({w})', 'ok')
            emit(f'extend({w})', 'lost', ne=bool(e['ret']))
            pc[w] = 'extended' if e['ret'] else 'finish'
        elif k == 'publish':
            if pc[w] == 'kcomplete':
                emit(f'kvJoin({w}, true)', 'ok')
            ok = e['outcome'] == 'ok'
            emit(f'publish({w}, {str(ok).lower()})', e['outcome'])
            published += ok
            pc[w] = 'published' if proto == 'redis' else 'kclean'
        elif k == 'cleanup':
            emit(f'cleanup({w})', 'cleaned' if e['ret'] else 'lost')
            pc[w] = 'finish'
        elif k == 'chord_error':
            if pc[w] == 'kcomplete':
                emit(f'kvJoin({w}, false)', 'error')
                pc[w] = 'kclean'
            elif pc[w] == 'claimed':
                emit(f'redisJoin({w})', 'error')
                pc[w] = 'finish'
            elif pc[w] == 'stored' and proto == 'kv':
                emit(f'count({w})', 'nogroup')
                pc[w] = 'finish'
        elif k == 'finish':
            if pc[w] == 'kclean':
                emit(f'kvClean({w})')
            emit(f'finish({w})')
            free.append(w)
            free.sort()
            del slot_of[e['pid']]
    steps.append(f'    .expect(dispatched == {published})')
    return f'  run c{i} = init\n' + '\n'.join(steps)


def natural_summary(log: Path) -> int:
    """Without backend hooks: count chords whose body ran and whose body
    errback was called too."""
    ev = [json.loads(line) for line in log.read_text().splitlines()]
    bodies = {e['pid'] for e in ev}  # noqa: F841 - kept for debugging
    gids = {e['gid'] for e in ev if e['kind'] == 'deliver'}
    redelivered = {g for g in gids
                   if sum(e['kind'] == 'deliver' and e['gid'] == g for e in ev) > N}
    errbacks = [e for e in ev if e['kind'] == 'errback']
    ran = sum(e['kind'] == 'body_ran' for e in ev)
    print(f'{log}: {len(gids)} chords, {len(redelivered)} with a redelivered '
          f'header, body ran {ran} times, body errback called {len(errbacks)} '
          f'times for {len({e["body"] for e in errbacks})} bodies')
    for e in errbacks[:3]:
        print('  errback:', e['exc'][:120])
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--proto', choices=['redis', 'kv'], default='redis')
    ap.add_argument('--chords', type=int, default=20)
    # body_ran lines also count broker redeliveries of the body message
    # itself (visibility timeout), which are not the chord's doing.
    ap.add_argument('--vt', type=int, default=2)
    ap.add_argument('--seed', type=int, default=0)
    ap.add_argument('--log', type=Path)
    ap.add_argument('--no-record', action='store_true',
                    help='only check an existing log')
    args = ap.parse_args(argv)
    log = args.log or HERE / 'histories' / f'chords_{args.proto}.jsonl'
    log.parent.mkdir(exist_ok=True)
    if not args.no_record:
        record(args.proto, args.chords, args.vt, args.seed, log)
    if os.environ.get('QUINT_NOHOOK'):
        return natural_summary(log)
    groups = split(log)
    mod = f'hist_chords_{args.proto}'
    runs = '\n\n'.join(to_run(i, args.proto, ev)
                       for i, ev in enumerate(groups.values()))
    src = (f'module {mod} {{\n'
           f'  import chord(PROTO = "{args.proto}", ACKS_LATE = true, '
           f'REJECT_ON_WORKER_LOST = false, LEASE_SAFE = true, MAX_DUPS = 20, '
           f'MAX_WORKER_LOST = 0, MAX_CHILD_LOST = 0, MAX_REFUSALS = 0).* '
           f'from "../chord"\n\n{runs}\n}}\n')
    out = HERE / 'histories' / f'{mod}.qnt'
    out.write_text(src)
    dups = sum(1 for ev in groups.values() for e in ev if e['kind'] == 'deliver') - N * len(groups)
    p = subprocess.run(['quint', 'test', '--backend=typescript', f'--main={mod}', str(out),
                        '--match=^c[0-9]+$'], cwd=HERE, capture_output=True,
                       text=True)
    text = p.stdout + p.stderr
    print(f'{log}: {len(groups)} chords, {dups} redelivered header runs; '
          f'{text.count("ok c")}/{len(groups)} allowed by chord.qnt '
          f'(quint test exit {p.returncode})')
    if p.returncode:
        print('\n'.join(text.splitlines()[-30:]))
    return p.returncode


if __name__ == '__main__':
    sys.exit(main())
