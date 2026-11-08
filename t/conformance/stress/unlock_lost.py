"""Finding chord-unlock-lost-child (database result backend, chord_unlock).

A backend without native chord support (here PostgreSQL through
SQLAlchemy) runs chord_unlock, which polls the header every
result_chord_retry_interval (1 s) and publishes the next poll with
self.retry() (builtins.py:50-130). If the pool child running chord_unlock is
lost before that retry is published, the main process acks the message
(documented for any task, docs/userguide/tasks.rst:32-47) and chord_unlock
has ignore_result, so nothing records an error: nobody polls the chord again
and the body is never sent nor failed.

  window    no fault: time from the main process handing chord_unlock to a
            child ("TaskPool: Apply", debug log) to the child logging
            "retry: Retry in" (after the next poll was published)
  targeted  SIGKILL the child running chord_unlock when "TaskPool: Apply"
            for it is logged (-c 2; the other child runs the only header)
  random    Poisson SIGKILLs of random pool children while chords stream
"""
from __future__ import annotations

import argparse
import os
import random
import re
import signal
import threading
import time
import uuid
from collections import Counter
from datetime import datetime

import common as C
from lost_child import build, outcome

APPLY = re.compile(r'^\[(?P<ts>[\d\- :,]+): DEBUG/MainProcess\] TaskPool: Apply '
                   r".*\('celery\.chord_unlock', '(?P<id>[0-9a-f-]+)'"
                   r""".*'argsrepr': "\('(?P<gid>[0-9a-f-]+)'""")
RETRY = re.compile(r'^\[(?P<ts>[\d\- :,]+): INFO/ForkPoolWorker-\d+\] Task '
                   r'celery\.chord_unlock\[(?P<id>[0-9a-f-]+)\] retry: Retry in')
DONE = re.compile(r'\] Task celery\.chord_unlock\[(?P<id>[0-9a-f-]+)\] '
                  r'(retry: Retry in|succeeded in|raised)')


def ts(s):
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S,%f').timestamp()


def parse(log):
    apply, retry, done, gid_of = [], [], {}, {}
    for line in log.read_text(errors='replace').splitlines():
        m = APPLY.match(line)
        if m:
            apply.append((ts(m['ts']), m['id'], m['gid']))
            gid_of[m['id']] = m['gid']
            continue
        m = RETRY.match(line)
        if m:
            retry.append((ts(m['ts']), m['id']))
        m = DONE.search(line)
        if m:
            done[m['id']] = done.get(m['id'], 0) + 1
    # a chord_unlock run handed to a child that never logged an outcome
    # (retry, success or error) was lost with its child
    applied = {}
    for _, i, _ in apply:
        applied[i] = applied.get(i, 0) + 1
    lost = {i for i, n in applied.items() if n > done.get(i, 0)}
    return apply, retry, lost, gid_of


def windows(apply, retry):
    """Pair each Apply with the next retry log line of the same id."""
    out = []
    by_id = {}
    for t, i in retry:
        by_id.setdefault(i, []).append(t)
    for t, i, _ in apply:
        nxt = [r for r in by_id.get(i, []) if r >= t]
        if nxt:
            out.append((min(nxt) - t) * 1000)
    return sorted(out)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=['window', 'targeted', 'random'])
    ap.add_argument('--chords', type=int, default=10)
    ap.add_argument('--headers', type=int, default=20)
    ap.add_argument('--base', type=float, default=0.2)
    ap.add_argument('--last-extra', type=float, default=0)
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--acks-late', type=int, default=0)
    ap.add_argument('--reject-lost', type=int, default=0)
    ap.add_argument('--kill-every', type=float, default=5.0)
    ap.add_argument('--inflight', type=int, default=2)
    ap.add_argument('--settle', type=float, default=20)
    ap.add_argument('--seed', type=int, default=1)
    a = ap.parse_args(argv)
    if a.mode == 'targeted':
        a.concurrency, a.headers, a.base, a.inflight = 2, 1, 6.0, 1

    run = f'unlock-{a.mode}-al{a.acks_late}-rj{a.reject_lost}-{uuid.uuid4().hex[:6]}'
    env = C.base_env(broker=f'{C.REDIS}/24', backend=C.POSTGRES, queue=run, ev=run,
                     acks_late=a.acks_late, reject_lost=a.reject_lost, join_timeout=3)
    C.apply_env(env)
    from celery import chord

    import stress_app as S
    print(C.proof())
    C.RESULTS.mkdir(exist_ok=True)
    rng = random.Random(a.seed)
    log = C.RESULTS / f'{run}.worker.log'
    load0 = C.load()
    w = C.Worker(env, log, concurrency=a.concurrency, extra=('-l', 'debug'))
    w.wait_ready()
    kills = []
    stop = threading.Event()

    def killer():
        while not stop.wait(rng.expovariate(1 / a.kill_every)):
            kids = w.children()
            if kids:
                pid = rng.choice(kids)
                try:
                    os.kill(pid, signal.SIGKILL)
                    kills.append(dict(t=time.time(), pid=pid))
                except ProcessLookupError:
                    pass

    header_pid = {}

    def tail_and_kill():
        """targeted: follow the log; on Apply of chord_unlock kill the child
        that isn't running the header."""
        f = open(log, errors='replace')
        f.seek(0, 2)
        while not stop.is_set():
            line = f.readline()
            if not line:
                time.sleep(0.0005)
                continue
            m = APPLY.match(line)
            if m and m['gid'] in header_pid and not header_pid[m['gid']].get('killed'):
                hp = header_pid[m['gid']]['pid']
                others = [p for p in w.children() if p != hp]
                if len(others) == 1:
                    os.kill(others[0], signal.SIGKILL)
                    header_pid[m['gid']]['killed'] = True
                    kills.append(dict(t=time.time(), pid=others[0], gid=m['gid'],
                                      after_ms=(time.time() - ts(m['ts'])) * 1000))

    chords = []
    t0 = time.time()
    try:
        if a.mode == 'random':
            threading.Thread(target=killer, daemon=True).start()
        if a.mode == 'targeted':
            threading.Thread(target=tail_and_kill, daemon=True).start()
        inflight = []
        for i in range(a.chords):
            tag = f'{run}-{i}'
            tids, sigs = build(S, tag, a.headers, a.last_extra, a.base, 0)
            res = chord(sigs)(S.body.s(tag).on_error(S.errback.s(tag=tag)))
            c = dict(tag=tag, body=res.id, gid=res.parent.id, tids=tids, t=time.time())
            chords.append(c)
            if a.mode == 'targeted':
                end = time.time() + 30
                while c['gid'] not in header_pid and time.time() < end:
                    for e in C.events(run):
                        if e['kind'] == 'start' and e['tid'] == tids[0]:
                            header_pid[c['gid']] = dict(pid=e['pid'])
                    time.sleep(0.01)
            inflight.append(res)
            if len(inflight) >= a.inflight:
                try:
                    inflight.pop(0).get(timeout=a.headers * a.base + 30, propagate=False)
                except Exception:  # noqa: BLE001 - outcome read below
                    pass
        time.sleep(a.settle)
    finally:
        stop.set()
        w.stop()
    load1 = C.load()
    evs = C.events(run)
    apply, retry, lost, gid_of = parse(log)
    lost_gids = {gid_of.get(i) for i in lost}
    for c in chords:
        c.update(outcome(S, evs, c))
        c['unlock_lost'] = c['gid'] in lost_gids
        c['unlock_runs'] = sum(1 for _, _, g in apply if g == c['gid'])
    v = Counter(c['verdict'] for c in chords)
    win = windows(apply, retry)

    def q(xs, p):
        return round(xs[min(len(xs) - 1, int(p * len(xs)))], 2) if xs else None
    summary = dict(
        run=run, mode=a.mode, chords=len(chords), headers=a.headers, base=a.base,
        concurrency=a.concurrency, acks_late=a.acks_late, reject_on_worker_lost=a.reject_lost,
        kill_every=a.kill_every if a.mode == 'random' else None,
        verdicts=dict(v), pending=C.fmt_rate(v['pending'], len(chords)),
        pending_with_unlock_lost=sum(1 for c in chords if c['verdict'] == 'pending' and c['unlock_lost']),
        unlock_lost_runs=len(lost), unlock_runs=len(apply), kills=len(kills),
        kill_after_apply_ms=[round(k['after_ms'], 2) for k in kills if 'after_ms' in k][:10],
        unlock_window_ms=dict(p50=q(win, .5), p90=q(win, .9), max=q(win, .999), n=len(win)),
        elapsed_s=round(time.time() - t0, 1), load_before=load0, load_after=load1,
        proof=C.proof())
    C.save(run, dict(summary=summary, chords=chords, kills=kills))
    for k, val in summary.items():
        print(f'{k}: {val}')


def reparse(path):
    """Recount lost chord_unlock runs of a finished run from its log."""
    import json
    from pathlib import Path
    d = json.loads(Path(path).read_text())
    apply, retry, lost, gid_of = parse(Path(path).with_suffix('.worker.log'))
    lost_gids = {gid_of.get(i) for i in lost}
    pend = [c for c in d['chords'] if c['verdict'] == 'pending']
    d['summary']['unlock_lost_runs'] = len(lost)
    d['summary']['pending_with_unlock_lost'] = sum(c['gid'] in lost_gids for c in pend)
    d['summary']['unlock_window_ms_all'] = windows(apply, retry)[:5]
    Path(path).write_text(json.dumps(d, indent=1, default=str))
    print(path, {k: d['summary'][k] for k in ('verdicts', 'pending', 'unlock_lost_runs',
                                               'pending_with_unlock_lost', 'unlock_runs')})


if __name__ == '__main__':
    import sys
    if sys.argv[1:2] == ['reparse']:
        for p in sys.argv[2:]:
            reparse(p)
    else:
        main()
