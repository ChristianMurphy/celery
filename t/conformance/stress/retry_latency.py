"""Finding es-ready-states: can a stale RETRY replace the final FAILURE?

Task.retry() publishes the next attempt (app/task.py:959) before the worker
stores RETRY for the current one (app/trace.py:201). If the next attempt
fails and stores FAILURE before that RETRY write lands, the task ends as
RETRY. The plan expected a remote result backend (local broker) to open the
window. A remote backend is modelled by a Toxiproxy latency toxic on the
backend connection only (each response delayed by --latency ms, so each
round trip gains that much); the broker stays local.

Per task we record, from outside, when its celery-task-meta key is SET
(keyspace notifications on the real server, not through the proxy). The
gap between the two writes (RETRY, then FAILURE) is how far the run was
from the race; a stale RETRY shows as a final state of RETRY.

    python retry_latency.py --latency 0  --tasks 500
    python retry_latency.py --latency 20 --tasks 500
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
import uuid
from collections import Counter

import common as C
from watch import KeyWatch

TOXI = 'http://127.0.0.1:8474'
PROXY = 'stress-backend'
LISTEN = '127.0.0.1:26579'
DB = 26


def toxi(method, path, body=None):
    req = urllib.request.Request(TOXI + path, method=method,
                                 data=json.dumps(body).encode() if body else None,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read() or b'null')
    except urllib.error.HTTPError as e:
        if e.code in (404, 409):
            return None
        raise


def setup_proxy(latency):
    toxi('DELETE', f'/proxies/{PROXY}')
    toxi('POST', '/proxies', dict(name=PROXY, listen=LISTEN, upstream='127.0.0.1:16579'))
    if latency:
        toxi('POST', f'/proxies/{PROXY}/toxics',
             dict(name='rtt', type='latency', stream='downstream',
                  attributes=dict(latency=latency, jitter=0)))


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--latency', type=int, default=0, help='ms added per round trip')
    ap.add_argument('--tasks', type=int, default=500)
    ap.add_argument('--burst', type=int, default=20)
    ap.add_argument('--task', choices=['flaky', 'flaky_auto'], default='flaky')
    ap.add_argument('--concurrency', type=int, default=4)
    ap.add_argument('--pool', default='prefork')
    ap.add_argument('--backend', choices=['redis', 'elasticsearch'], default='redis',
                    help='elasticsearch: control, keeps ready states (celery PR 6115)')
    a = ap.parse_args(argv)
    setup_proxy(a.latency)
    run = f'retry-{a.task}-{a.backend}-lat{a.latency}-{a.pool}-{uuid.uuid4().hex[:6]}'
    backend = (f'redis://{LISTEN}/{DB}' if a.backend == 'redis'
               else 'elasticsearch://127.0.0.1:9200/stress_retry')
    env = C.base_env(broker=f'{C.REDIS}/25', backend=backend, queue=run, ev=run)
    C.apply_env(env)
    import stress_app as S
    print(C.proof())
    watch = KeyWatch(C.REDIS, DB)
    watch.start()
    watch.ready.wait(5)
    C.RESULTS.mkdir(exist_ok=True)
    load0 = C.load()
    w = C.Worker(env, C.RESULTS / f'{run}.worker.log', pool=a.pool,
                 concurrency=a.concurrency)
    w.wait_ready()
    ids = []
    t0 = time.time()
    try:
        for i in range(0, a.tasks, a.burst):
            task = getattr(S, a.task)
            batch = [task.delay(f'{run}-{i + k}') for k in range(min(a.burst, a.tasks - i))]
            ids += [r.id for r in batch]
            for r in batch:
                try:
                    r.get(timeout=30, propagate=False)
                except Exception:  # noqa: BLE001 - state read below
                    pass
        time.sleep(3)
    finally:
        w.stop()
        watch.stop()
    load1 = C.load()
    states = Counter()
    gaps = []
    for tid in ids:
        states[S.app.AsyncResult(tid).state] += 1
        sets = watch.all(f'celery-task-meta-{tid}')
        if len(sets) >= 2:
            gaps.append((sets[-1] - sets[-2]) * 1000)
    gaps.sort()

    def q(p):
        return round(gaps[min(len(gaps) - 1, int(p * len(gaps)))], 2) if gaps else None
    summary = dict(run=run, task=a.task, backend=a.backend, latency_ms=a.latency, pool=a.pool, concurrency=a.concurrency,
                   tasks=len(ids), states=dict(states),
                   stale_retry=C.fmt_rate(states['RETRY'], len(ids)),
                   gap_retry_to_failure_ms=dict(min=q(0), p10=q(.1), p50=q(.5), n=len(gaps)),
                   elapsed_s=round(time.time() - t0, 1), load_before=load0,
                   load_after=load1, proof=C.proof())
    C.save(run, dict(summary=summary, ids=ids))
    for k, v in summary.items():
        print(f'{k}: {v}')


if __name__ == '__main__':
    main()
