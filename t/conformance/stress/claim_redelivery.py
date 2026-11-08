"""Finding chord-claim-blocks-redelivery (Redis result backend).

The worker that took the chord's dispatch claim (<gid>.c, SET NX EX with a
TTL of max(5, 2 * result_chord_join_timeout) s, redis.py:963) is lost before
it publishes the body. With acks_late the last header's message goes back to
the queue. RabbitMQ redelivers it at once to another worker; that run's part
return finds the claim still held and returns (redis.py:129-143, 989). When
the claim expires nothing is left to run, so the body is never sent.

Two workers (A, B) consume one queue. A lost worker is SIGKILLed whole (node
loss, deploy) and replaced by a new one, as an orchestrator would.

  window    no fault: per chord, time the claim is held before cleanup
  targeted  kill the worker running the last header when the claim is SET
            (--at claim), or when its SUCCESS is SET (--at meta, control:
            before the claim)
  random    kill a random worker (Poisson) while chords stream

Controls: --broker redis (redelivery after visibility_timeout, set above the
claim TTL as the 1 h default is), and --at meta.
"""
from __future__ import annotations

import argparse
import os
import random
import signal
import threading
import time
import uuid
from collections import Counter

import common as C
from lost_child import build, outcome
from watch import KeyWatch

BACKEND_DB = 23


def ppid(pid):
    try:
        with open(f'/proc/{pid}/stat') as f:
            return int(f.read().rsplit(')', 1)[1].split()[1])
    except (OSError, IndexError, ValueError):
        return None


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=['window', 'targeted', 'random'])
    ap.add_argument('--at', choices=['claim', 'meta'], default='claim')
    ap.add_argument('--broker', choices=['rabbitmq', 'redis'], default='rabbitmq')
    ap.add_argument('--vt', type=float, default=10)
    ap.add_argument('--chords', type=int, default=10)
    ap.add_argument('--headers', type=int, default=4)
    ap.add_argument('--payload', type=int, default=0)
    ap.add_argument('--base', type=float, default=0.05)
    ap.add_argument('--last-extra', type=float, default=0.3)
    ap.add_argument('--concurrency', type=int, default=2)
    ap.add_argument('--kill-every', type=float, default=5.0)
    ap.add_argument('--inflight', type=int, default=2)
    ap.add_argument('--settle', type=float, default=25)
    ap.add_argument('--seed', type=int, default=1)
    a = ap.parse_args(argv)

    run = f'claim-{a.mode}-{a.broker}-{a.at}-{uuid.uuid4().hex[:6]}'
    broker = C.RABBIT if a.broker == 'rabbitmq' else f'{C.REDIS}/28'
    cfg = dict(broker=broker, backend=f'{C.REDIS}/{BACKEND_DB}', queue=run, ev=run,
               acks_late=1, join_timeout=3)
    if a.broker == 'redis':
        cfg['vt'] = a.vt
    env = C.base_env(**cfg)
    C.apply_env(env)
    from celery import chord

    import stress_app as S
    print(C.proof())
    C.RESULTS.mkdir(exist_ok=True)
    rng = random.Random(a.seed)
    lock = threading.Lock()
    workers = []
    kills = []

    def spawn():
        w = C.Worker(env, C.RESULTS / f'{run}.w{len(workers)}.log',
                     concurrency=a.concurrency)
        workers.append(w)
        return w

    def worker_of(pid):
        p = ppid(pid)
        for w in workers:
            if w.proc.poll() is None and (w.proc.pid == p or w.proc.pid == pid):
                return w
        return None

    def lose(w, why, t=None):
        with lock:
            if w.proc.poll() is not None:
                return
            os.killpg(w.proc.pid, signal.SIGKILL)
            kills.append(dict(t=t or time.time(), worker=w.name, why=why,
                              after=(time.time() - t) if t else None))
        # the orchestrator starts a replacement
        threading.Thread(target=lambda: spawn().wait_ready(), daemon=True).start()

    armed = {}

    def on_set(key, t):
        w = armed.pop(key, None)
        if w is not None:
            lose(w, key, t)

    watch = KeyWatch(C.REDIS, BACKEND_DB, on_set)
    watch.start()
    watch.ready.wait(5)
    load0 = C.load()
    for _ in range(2):
        spawn()
    for w in list(workers):
        w.wait_ready()
    stop = threading.Event()

    def killer():
        while not stop.wait(rng.expovariate(1 / a.kill_every)):
            alive = [w for w in workers if w.proc.poll() is None]
            if alive:
                lose(rng.choice(alive), 'random')

    chords = []
    t0 = time.time()
    try:
        if a.mode == 'random':
            threading.Thread(target=killer, daemon=True).start()
        inflight = []
        for i in range(a.chords):
            tag = f'{run}-{i}'
            tids, sigs = build(S, tag, a.headers, a.last_extra, a.base, a.payload)
            res = chord(sigs)(S.body.s(tag).on_error(S.errback.s(tag=tag)))
            c = dict(tag=tag, body=res.id, gid=res.parent.id, tids=tids, t=time.time())
            chords.append(c)
            if a.mode == 'targeted':
                pid = None
                end = time.time() + 30
                while pid is None and time.time() < end:
                    for e in C.events(run):
                        if e['kind'] == 'start' and e['tid'] == tids[-1]:
                            pid = e['pid']
                    time.sleep(0.005)
                w = worker_of(pid) if pid else None
                c['pid'], c['worker'] = pid, w.name if w else None
                if w:
                    key = (f'celery-taskset-meta-{c["gid"]}.c' if a.at == 'claim'
                           else f'celery-task-meta-{tids[-1]}')
                    armed[key] = w
            inflight.append(res)
            if len(inflight) >= a.inflight:
                try:
                    inflight.pop(0).get(timeout=a.base + a.last_extra + 10, propagate=False)
                except Exception:  # noqa: BLE001 - outcome read below
                    pass
            if a.mode == 'targeted':
                time.sleep(2)   # let the replacement come up
        time.sleep(a.settle if a.broker == 'rabbitmq' else max(a.settle, a.vt + 15))
    finally:
        stop.set()
        for w in workers:
            w.stop()
        watch.stop()
    load1 = C.load()
    evs = C.events(run)
    for c in chords:
        c.update(outcome(S, evs, c))
        ck = watch.all(f'celery-taskset-meta-{c["gid"]}.c')
        metas = [watch.first(f'celery-task-meta-{t}') for t in c['tids']]
        last = max((m for m in metas if m), default=None)
        c['claim_sets'] = len(ck)
        if ck and len(ck) > 1:
            c['ms_claim_held'] = (ck[1] - ck[0]) * 1000
        if last and ck:
            c['ms_meta_to_claim'] = (ck[0] - last) * 1000
        starts = Counter(e['tid'] for e in evs if e['kind'] == 'start' and e.get('gid') == c['gid'])
        c['last_header_runs'] = starts.get(c['tids'][-1], 0)
        rs = sorted(e['t'] for e in evs if e['kind'] == 'start' and e.get('tid') == c['tids'][-1])
        if ck and len(rs) > 1:
            # the redelivered run's start, relative to the claim (TTL 6 s)
            c['s_claim_to_redelivered_start'] = round(rs[1] - ck[0], 3)
    v = Counter(c['verdict'] for c in chords)
    held = sorted(c['ms_claim_held'] for c in chords if 'ms_claim_held' in c)

    def q(xs, p):
        return round(xs[min(len(xs) - 1, int(p * len(xs)))], 2) if xs else None
    summary = dict(run=run, mode=a.mode, at=a.at, broker=a.broker,
                   vt=a.vt if a.broker == 'redis' else None, chords=len(chords),
                   headers=a.headers, payload=a.payload, concurrency=a.concurrency,
                   kill_every=a.kill_every if a.mode == 'random' else None,
                   verdicts=dict(v), pending=C.fmt_rate(v['pending'], len(chords)),
                   kills=len(kills),
                   redelivered_start_after_claim_s=sorted(
                       c['s_claim_to_redelivered_start'] for c in chords
                       if 's_claim_to_redelivered_start' in c)[:20],
                   kill_latency_ms=[round((k['after'] or 0) * 1000, 2) for k in kills][:10],
                   claim_held_ms=dict(p50=q(held, .5), p90=q(held, .9), max=q(held, .999), n=len(held)),
                   elapsed_s=round(time.time() - t0, 1), load_before=load0, load_after=load1,
                   proof=C.proof())
    C.save(run, dict(summary=summary, chords=chords, kills=kills, events=evs))
    for k, val in summary.items():
        print(f'{k}: {val}')


if __name__ == '__main__':
    main()
