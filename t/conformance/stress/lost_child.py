"""Finding chord-lost-child-after-success, and the window it needs.

A chord header's pool child stores SUCCESS (mark_as_done -> store_result)
and is lost before on_chord_part_return finishes. Since celery PR 10752 the
main process sees SUCCESS stored and acks without a part return
(request.py:714-736), so the chord is never completed and nothing reports it.

Modes (Redis broker DB 20, Redis result backend DB 22, prefork):

  window    no fault; per chord, time from the last header's SUCCESS write
            (its celery-task-meta key is SET) to the dispatch claim (<gid>.c
            SET with a token) and to cleanup (<gid>.c SET to "done").
  targeted  SIGKILL the last header's pool child when its SUCCESS write is
            seen (--at meta) or when the claim is taken (--at claim); or,
            as a control, while it still runs (--at running).
  random    Poisson SIGKILLs of random pool children while chords stream.

Outcome per chord, read after a settle longer than the claim TTL: ok (body
ran), failed (body errback ran), pending (neither).
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
from watch import KeyWatch

BACKEND_DB = 22


def build(S, tag, n, last_extra, base, payload, time_limit=None):
    tids = [uuid.uuid4().hex for _ in range(n)]
    sigs = []
    for i, tid in enumerate(tids):
        d = base + (last_extra if i == n - 1 else 0)
        s = S.header.s(i + 1, d, payload).set(task_id=tid)
        if time_limit:
            s = s.set(time_limit=time_limit)
        sigs.append(s)
    return tids, sigs


def outcome(S, evs, c):
    ran = [e for e in evs if e['kind'] == 'body' and e.get('tag') == c['tag']]
    erb = [e for e in evs if e['kind'] == 'errback' and e.get('tag') == c['tag']]
    st = S.app.AsyncResult(c['body']).state
    if ran:
        v = 'ok'
    elif erb:
        v = 'failed'
    else:
        v = 'pending'
    hs = [S.app.AsyncResult(t).state for t in c['tids']]
    return dict(verdict=v, body_ran=len(ran), errbacks=len(erb),
                first_error=(min(erb, key=lambda e: e['t'])['exc'][:160] if erb else ''),
                body_state=st, header_states=dict(Counter(hs)))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=['window', 'targeted', 'random'])
    ap.add_argument('--at', choices=['meta', 'claim', 'running'], default='meta')
    ap.add_argument('--chords', type=int, default=20)
    ap.add_argument('--headers', type=int, default=4)
    ap.add_argument('--payload', type=int, default=0, help='bytes per header result')
    ap.add_argument('--base', type=float, default=0.05, help='header duration (s)')
    ap.add_argument('--last-extra', type=float, default=0.3)
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--acks-late', type=int, default=1)
    ap.add_argument('--reject-lost', type=int, default=1)
    ap.add_argument('--kill-every', type=float, default=2.0, help='random: mean s')
    ap.add_argument('--inflight', type=int, default=2)
    ap.add_argument('--time-limit', type=float, default=None)
    ap.add_argument('--backend', choices=['redis', 'memcached'], default='redis')
    ap.add_argument('--broker', choices=['redis', 'rabbitmq'], default='redis')
    ap.add_argument('--settle', type=float, default=20)
    ap.add_argument('--seed', type=int, default=1)
    ap.add_argument('--backend-db', type=int, default=BACKEND_DB)
    ap.add_argument('--broker-db', type=int, default=27)
    ap.add_argument('--docker-memory')
    ap.add_argument('--docker-cpus')
    a = ap.parse_args(argv)

    run = f'lost-{a.mode}-{a.at if a.mode == "targeted" else ""}-{uuid.uuid4().hex[:6]}'
    backend = f'{C.REDIS}/{a.backend_db}' if a.backend == 'redis' else C.MEMCACHED
    broker = f'{C.REDIS}/{a.broker_db}' if a.broker == 'redis' else C.RABBIT
    env = C.base_env(broker=broker, backend=backend, queue=run, ev=run,
                     acks_late=a.acks_late, reject_lost=a.reject_lost, join_timeout=3)
    C.apply_env(env)
    from celery import chord

    import stress_app as S
    print(C.proof())
    C.RESULTS.mkdir(exist_ok=True)
    rng = random.Random(a.seed)
    watch = KeyWatch(C.REDIS, a.backend_db) if a.backend == 'redis' else None
    armed = {}           # key -> pid to SIGKILL when the key is SET
    kills = []

    def on_set(key, t):
        pid = armed.pop(key, None)
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
                kills.append(dict(t=t, pid=pid, key=key, after=time.time() - t))
            except ProcessLookupError:
                kills.append(dict(t=t, pid=pid, key=key, missed=True))
    if watch:
        watch.callback = on_set
        watch.start()
        watch.ready.wait(5)

    docker = None
    if a.docker_memory or a.docker_cpus:
        docker = dict(memory=a.docker_memory, cpus=a.docker_cpus)
    load0 = C.load()
    w = C.Worker(env, C.RESULTS / f'{run}.worker.log', concurrency=a.concurrency,
                 docker=docker)
    w.wait_ready()
    stop_kills = threading.Event()

    def killer():
        while not stop_kills.wait(rng.expovariate(1 / a.kill_every)):
            kids = w.children()
            if kids:
                pid = rng.choice(kids)
                try:
                    os.kill(pid, signal.SIGKILL)
                    kills.append(dict(t=time.time(), pid=pid, random=True))
                except ProcessLookupError:
                    pass

    chords = []
    t_start = time.time()
    try:
        if a.mode == 'random':
            threading.Thread(target=killer, daemon=True).start()
        inflight = []
        for i in range(a.chords):
            tag = f'{run}-{i}'
            tids, sigs = build(S, tag, a.headers, a.last_extra, a.base, a.payload,
                               a.time_limit)
            res = chord(sigs)(S.body.s(tag).on_error(S.errback.s(tag=tag)))
            gid = res.parent.id
            c = dict(tag=tag, body=res.id, gid=gid, tids=tids, t=time.time())
            chords.append(c)
            if a.mode == 'targeted':
                last = tids[-1]
                # the pid comes from the task's own start record
                pid = None
                end = time.time() + 30
                while pid is None and time.time() < end:
                    for e in C.events(run):
                        if e['kind'] == 'start' and e['tid'] == last:
                            pid = e['pid']
                    time.sleep(0.005)
                c['pid'] = pid
                if a.at == 'meta':
                    armed[f'celery-task-meta-{last}'] = pid
                elif a.at == 'claim':
                    armed[f'celery-taskset-meta-{gid}.c'] = pid
                else:
                    time.sleep(a.last_extra / 2)
                    os.kill(pid, signal.SIGKILL)
                    kills.append(dict(t=time.time(), pid=pid, running=True))
            inflight.append(res)
            if len(inflight) >= a.inflight:
                r0 = inflight.pop(0)
                try:
                    r0.get(timeout=a.base + a.last_extra + 15, propagate=False)
                except Exception:  # noqa: BLE001 - outcome read below
                    pass
        time.sleep(a.settle)
    finally:
        stop_kills.set()
        w.stop()
        if watch:
            watch.stop()
    elapsed = time.time() - t_start
    load1 = C.load()

    evs = C.events(run)
    for c in chords:
        c.update(outcome(S, evs, c))
        if watch:
            meta = watch.first(f'celery-task-meta-{c["tids"][-1]}')
            metas = [watch.first(f'celery-task-meta-{t}') for t in c['tids']]
            ck = watch.all(f'celery-taskset-meta-{c["gid"]}.c')
            last_meta = max((m for m in metas if m), default=None)
            c['t_last_meta'] = last_meta
            c['t_claim'] = ck[0] if ck else None
            c['t_done'] = ck[1] if len(ck) > 1 else None
            if last_meta and ck:
                c['ms_meta_to_claim'] = (ck[0] - last_meta) * 1000
            if last_meta and len(ck) > 1:
                c['ms_meta_to_done'] = (ck[1] - last_meta) * 1000
    verdicts = Counter(c['verdict'] for c in chords)
    pend = verdicts['pending']
    win = sorted(c['ms_meta_to_done'] for c in chords if 'ms_meta_to_done' in c)
    claim = sorted(c['ms_meta_to_claim'] for c in chords if 'ms_meta_to_claim' in c)

    def q(xs, p):
        return round(xs[min(len(xs) - 1, int(p * len(xs)))], 2) if xs else None
    summary = dict(
        run=run, mode=a.mode, at=a.at, chords=len(chords), headers=a.headers,
        payload=a.payload, concurrency=a.concurrency, acks_late=a.acks_late,
        reject_on_worker_lost=a.reject_lost, backend=a.backend, broker=a.broker,
        time_limit=a.time_limit, docker=docker, kill_every=a.kill_every if a.mode == 'random' else None,
        verdicts=dict(verdicts), pending=C.fmt_rate(pend, len(chords)),
        kills=len(kills), kill_latency_ms=[round(k.get('after', 0) * 1000, 3) for k in kills][:10],
        window_meta_to_done_ms=dict(p50=q(win, .5), p90=q(win, .9), max=q(win, .999), n=len(win)),
        window_meta_to_claim_ms=dict(p50=q(claim, .5), p90=q(claim, .9), n=len(claim)),
        elapsed_s=round(elapsed, 1), load_before=load0, load_after=load1, proof=C.proof())
    C.save(run, dict(summary=summary, chords=chords, kills=kills, events=evs))
    for k, v in summary.items():
        print(f'{k}: {v}')


if __name__ == '__main__':
    main()
