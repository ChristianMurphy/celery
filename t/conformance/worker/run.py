"""Run tasks on actual workers and count what the result backend shows.

Each scenario makes some task ids run more than once through a mechanism that
happens in production, with no delays injected and no Celery code patched:

``visibility``
    Redis broker, ``task_acks_late=True`` and a ``visibility_timeout`` shorter
    than some tasks' runtime, so the broker hands a running task to another
    worker process.
``eta``
    Redis broker with the default early ack, and a ``countdown`` longer than
    the ``visibility_timeout``. The worker holds the message unacked until
    its ETA, the broker restores it, and every worker process that received
    a copy runs it at the same ETA.
``same-id``
    The same task id is published twice, as a client publish retried after a
    lost reply does, or a client that reuses ids does.
``time-limit``
    ``task_time_limit`` close to the runtime: the worker stores FAILURE from
    the parent process while the pool process may be storing SUCCESS.

The driver watches the backend (polling, plus pub/sub on Redis) and reports:

``success_changed``
    A SUCCESS was seen and later replaced by another state or value.
``replaced_by_later_run``
    The final SUCCESS came from a run that stored it more than 50 ms after an
    earlier run stored its SUCCESS: the earlier one must have been replaced.
``final_not_success`` / ``stuck``
    The final state isn't SUCCESS / isn't a ready state at all.

Example::

    python run.py --backend redis --scenario visibility --tasks 600 --track-started
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import threading
import tempfile
import time
import uuid
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
READY = {'SUCCESS', 'FAILURE', 'REVOKED'}


def parse():
    p = argparse.ArgumentParser()
    p.add_argument('--backend', required=True)
    p.add_argument('--scenario', choices=['visibility', 'eta', 'same-id', 'time-limit'],
                   required=True)
    p.add_argument('--countdown', type=float, default=5.0, help='eta scenario: countdown (s)')
    p.add_argument('--tasks', type=int, default=200)
    p.add_argument('--track-started', action='store_true')
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--concurrency', type=int, default=8)
    p.add_argument('--pool', default='prefork')
    p.add_argument('--min', type=float, default=1.0, help='shortest task runtime (s)')
    p.add_argument('--max', type=float, default=4.0, help='longest task runtime (s)')
    p.add_argument('--visibility', type=float, default=2.0)
    p.add_argument('--time-limit', type=float, default=2.0)
    p.add_argument('--claim', action='store_true',
                   help='the run that loses a claim on the task id raises')
    p.add_argument('--prefetch', type=int, default=4, help='worker_prefetch_multiplier')
    p.add_argument('--load', type=float, default=0.8,
                   help='target share of worker processes kept busy')
    p.add_argument('--celery', default=str(HERE.parents[2]), help='Celery tree to run')
    p.add_argument('--kombu', default=os.environ.get('NATURAL_KOMBU', ''))
    p.add_argument('--broker', default='redis://127.0.0.1:16479/0')
    p.add_argument('--redis-backend', default='redis://127.0.0.1:16480/0')
    p.add_argument('--out', default='', help='append the summary as JSON to this file')
    p.add_argument('--label', default='')
    p.add_argument('--dump', default='', help='write events of the bad task ids here')
    p.add_argument('--logdir', default=tempfile.gettempdir(), help='worker logs')
    return p.parse_args()


def git_sha(path):
    try:
        return subprocess.check_output(['git', '-C', path, 'rev-parse', '--short', 'HEAD'],
                                       text=True).strip()
    except Exception:
        return '?'


def main():
    a = parse()
    run = f'{a.backend}_{a.scenario.replace("-", "")}_{uuid.uuid4().hex[:8]}'
    env = dict(os.environ)
    env.update(
        NATURAL_RUN=run, NATURAL_BACKEND=a.backend, NATURAL_BROKER=a.broker,
        NATURAL_LOG=a.broker.rsplit('/', 1)[0] + '/1',
        NATURAL_REDIS_BACKEND=a.redis_backend,
        NATURAL_TRACK_STARTED='1' if a.track_started else '0',
        NATURAL_ACKS_LATE='1' if a.scenario == 'visibility' else '0',
        NATURAL_PREFETCH=str(a.prefetch),
        NATURAL_CLAIM='1' if a.claim else '0',
        PYTHONPATH=':'.join(p for p in (a.celery, a.kombu, str(HERE)) if p),
    )
    if a.scenario in ('visibility', 'eta'):
        env['NATURAL_VISIBILITY'] = str(a.visibility)
    if a.scenario == 'time-limit':
        env['NATURAL_TIME_LIMIT'] = str(a.time_limit)
    os.environ.update(env)
    sys.path[:0] = [p for p in (a.celery, a.kombu, str(HERE)) if p]

    import celery
    import kombu
    import redis
    import natural_app
    from natural_app import app, work

    log = redis.Redis.from_url(env['NATURAL_LOG'])
    broker = redis.Redis.from_url(a.broker)
    broker.flushdb()
    backend = app.backend
    prepare_backend(a.backend, app)

    workers = []
    for i in range(a.workers):
        out = open(f'{a.logdir}/natural-{run}-w{i}.log', 'w')
        cmd = [sys.executable, '-m', 'celery', '-A', 'natural_app', 'worker',
               '-P', a.pool, '-c', str(a.concurrency), '-l', 'info', '-n', f'w{i}-{run}@%h',
               '--without-gossip', '--without-mingle', '--without-heartbeat']
        workers.append((subprocess.Popen(cmd, cwd=HERE, env=env, stdout=out,
                                         stderr=subprocess.STDOUT), out.name))
    for proc, name in workers:
        deadline = time.time() + 60
        while 'ready.' not in Path(name).read_text():
            if proc.poll() is not None or time.time() > deadline:
                raise SystemExit(Path(name).read_text()[-3000:])
            time.sleep(0.2)

    ids = [uuid.uuid4().hex for _ in range(a.tasks)]
    obs = defaultdict(list)     # id -> [(t, source, state, token)] on each change
    events = []                 # log events, in order
    stop = threading.Event()

    lock = threading.Lock()

    def read_log():
        with lock:
            for raw in log.lrange(natural_app.LOG_KEY, len(events), -1):
                events.append(json.loads(raw))

    def note(task_id, source, state, token, t0=None):
        # obs entries: (read started, read finished, source, state, token),
        # kept only when the value changes.
        t1 = time.time()
        seq = [o for o in obs[task_id] if o[2] == source]
        if not seq or seq[-1][3:] != (state, token):
            obs[task_id].append((t1 if t0 is None else t0, t1, source, state, token))

    def poller():
        last_event = {}
        last_event_seen = []
        while not stop.is_set():
            read_log()
            now = time.time()
            with lock:
                for e in events[len(last_event_seen):]:
                    last_event[e['id']] = e['t']
                    last_event_seen.append(1)
            for task_id in list(sent):
                seen = [o for o in obs[task_id] if o[2] == 'poll']
                settled = seen and seen[-1][3] in READY and now - last_event.get(task_id, now) > 5
                if settled:
                    continue
                t_read = time.time()
                state, token = read_meta(backend, task_id)
                note(task_id, 'poll', state, token, t_read)
            time.sleep(0.05)

    def subscriber():
        sub = redis.Redis.from_url(a.redis_backend).pubsub()
        sub.psubscribe('celery-task-meta-*')
        while not stop.is_set():
            m = sub.get_message(timeout=0.5)
            if m and m['type'] == 'pmessage':
                meta = json.loads(m['data'])
                result = meta.get('result')
                token = result.get('token') if isinstance(result, dict) else None
                note(meta['task_id'], 'pubsub', meta['status'], token, time.time())
        sub.close()

    sent = []
    threads = [threading.Thread(target=poller, daemon=True)]
    if a.backend == 'redis':
        threads.append(threading.Thread(target=subscriber, daemon=True))
    for t in threads:
        t.start()
    time.sleep(1)

    # Pace the publishing so about ``load`` of the worker processes are busy:
    # a long backlog would make messages wait in the prefetch buffer instead.
    capacity = a.workers * a.concurrency
    mean = (a.min + a.max) / 2
    copies = 2 if a.scenario == 'same-id' else 1
    gap = mean / (capacity * a.load) * copies
    rnd = random.Random(run)
    t0 = time.time()
    load_start = os.getloadavg()[0]
    for task_id in ids:
        duration = rnd.uniform(a.min, a.max)
        for _ in range(copies):
            if a.scenario == 'eta':
                work.apply_async((duration,), task_id=task_id, countdown=a.countdown)
            else:
                work.apply_async((duration,), task_id=task_id)
        sent.append(task_id)
        time.sleep(gap)

    # Wait until every task is ready, nothing is queued or unacked, and the
    # run log has been quiet longer than a visibility timeout and a sweep.
    quiet = (a.visibility + 3) if a.scenario in ('visibility', 'eta') else 3
    while True:
        read_log()
        last = events[-1]['t'] if events else t0
        states = [read_meta(backend, i)[0] for i in ids]
        pending = sum(s not in READY for s in states)
        busy = broker.llen('celery') + broker.hlen('unacked')
        if not pending and not busy and time.time() - last > quiet:
            break
        if time.time() - last > 120 + a.max:
            break           # stuck: report it
        time.sleep(1)
    stop.set()
    for proc, _ in workers:
        proc.send_signal(signal.SIGTERM)
    for proc, _ in workers:
        try:
            proc.wait(20)
        except subprocess.TimeoutExpired:
            proc.kill()
    read_log()

    final = {i: read_meta(backend, i) for i in ids}
    summary = classify(a, ids, events, obs, final)
    summary.update(
        run=run, label=a.label, backend=a.backend, scenario=a.scenario,
        track_started=a.track_started, claim=a.claim, acks_late=a.scenario == 'visibility',
        workers=a.workers, concurrency=a.concurrency, pool=a.pool, prefetch=a.prefetch,
        runtime=[a.min, a.max], elapsed=round(time.time() - t0, 1),
        loadavg=[round(load_start, 1), round(os.getloadavg()[0], 1)],
        visibility=a.visibility if a.scenario in ('visibility', 'eta') else None,
        countdown=a.countdown if a.scenario == 'eta' else None,
        time_limit=a.time_limit if a.scenario == 'time-limit' else None,
        celery=f'{celery.__version__} {git_sha(a.celery)} {celery.__file__}',
        kombu=f'{kombu.__version__} {git_sha(a.kombu) if a.kombu else ""} {kombu.__file__}',
    )
    print(json.dumps(summary))
    if a.dump:
        bad = {i for k, v in summary.items() if k.endswith('_ids') for i in v}
        with open(a.dump, 'a') as f:
            for i in sorted(bad):
                f.write(json.dumps({'run': run, 'id': i, 'final': final[i],
                                    'observed': obs[i],
                                    'events': [e for e in events if e['id'] == i]}) + '\n')
    if a.out:
        with open(a.out, 'a') as f:
            f.write(json.dumps(summary) + '\n')
    log.delete(natural_app.LOG_KEY)


def read_meta(backend, task_id):
    try:
        meta = backend._get_task_meta_for(task_id)
    except Exception as exc:     # a backend that errors on a missing key
        return f'ERROR {type(exc).__name__}', None
    result = meta.get('result')
    return meta['status'], result.get('token') if isinstance(result, dict) else None


def prepare_backend(name, app):
    if name == 'cassandra':
        app.backend._get_connection(write=True)
    if name == 'arangodb':
        db = app.backend.db
        if not db.hasCollection(app.backend.collection):
            db.createCollection(name=app.backend.collection)


def classify(a, ids, events, obs, final):
    def stored(v):
        # SUCCESS was stored before the run's own read-back started.
        p = v['post']
        return p['seen']['t0'] if p.get('seen') else p['t']

    runs = defaultdict(dict)        # id -> token -> {start, end, post, state}
    for e in events:
        runs[e['id']].setdefault(e['token'] or f"post-{e['pid']}-{e['t']}", {})[e['ev']] = e
        if e['ev'] == 'post':
            runs[e['id']][e['token'] or f"post-{e['pid']}-{e['t']}"]['state'] = e['state']
    out = defaultdict(list)
    near = {k: 0 for k in ('1ms', '5ms', '20ms', '100ms', '1s')}
    starts = 0
    for task_id in ids:
        r = {k: v for k, v in runs[task_id].items() if 'start' in v}
        starts += len(r)
        if len(r) > 1:
            out['duplicated'].append(task_id)
        stored_ok = any(v.get('post', {}).get('seen', {}).get('state') == 'SUCCESS'
                        for v in r.values())
        limit = [e for e in events if e['id'] == task_id and e['ev'] == 'failure'
                 and e.get('exc') == 'TimeLimitExceeded']
        if limit:
            out['time_limit_fired'].append(task_id)
            if stored_ok:
                out['failure_handlers_after_success'].append(task_id)
        state, token = final[task_id]
        if state not in READY:
            out['stuck'].append(task_id)
        elif state != 'SUCCESS':
            out['final_not_success'].append(task_id)
        # Merge what clients saw: the poller, Redis pub/sub, each run's own
        # read right after it stored, and the final read. A SUCCESS followed
        # by a read that started after it and shows anything else means the
        # SUCCESS was replaced.
        seen = list(obs[task_id])
        for v in r.values():
            p = v.get('post', {}).get('seen')
            if p:
                seen.append((p['t0'], p['t1'], 'self', p['state'], p['token']))
        seen.append((float('inf'), float('inf'), 'final', state, token))
        hits = set()
        for o in seen:
            if o[3] != 'SUCCESS':
                continue
            for x in seen:
                if x[0] > o[1] and x[3:] != o[3:] and x[2] != 'pubsub' and o[2] != 'pubsub':
                    hits.add('read')
        pub = [o for o in obs[task_id] if o[2] == 'pubsub']
        for i, o in enumerate(pub):
            if o[3] == 'SUCCESS' and any(x[3:] != o[3:] for x in pub[i + 1:]):
                hits.add('pubsub')
        for h in hits:
            out[f'success_changed_{h}'].append(task_id)
        ok = sorted((stored(v), k) for k, v in r.items()
                    if v.get('post', {}).get('state') == 'SUCCESS')
        if state == 'SUCCESS' and ok and token != ok[0][1]:
            when = dict((k, t) for t, k in ok).get(token)
            if when is not None and when - ok[0][0] > 0.05:
                out['replaced_by_later_run'].append(task_id)
        if len(r) > 1:
            # How close did one run's SUCCESS write come to another run's
            # write? STARTED is stored just before 'start' is logged, SUCCESS
            # just before 'post' is logged.
            best = None
            for k1, v1 in r.items():
                if 'post' not in v1:
                    continue
                for k2, v2 in r.items():
                    if k1 == k2:
                        continue
                    writes = [stored(v2)] if 'post' in v2 else []
                    if a.track_started:
                        writes.append(v2['start']['t'])
                    for w in writes:
                        d = abs(stored(v1) - w)
                        best = d if best is None else min(best, d)
            if best is not None:
                for k, lim in (('1ms', .001), ('5ms', .005), ('20ms', .02),
                               ('100ms', .1), ('1s', 1)):
                    near[k] += best <= lim
    bad = set()
    for k in ('stuck', 'final_not_success', 'success_changed_read',
              'success_changed_pubsub', 'replaced_by_later_run'):
        bad.update(out.get(k, []))
    summary = {
        'tasks': len(ids), 'runs': starts,
        'duplicated': len(out['duplicated']),
        'bad': len(bad),
        'near_misses': near,
    }
    for k, v in out.items():
        if k != 'duplicated':
            summary[k] = len(v)
            summary[k + '_ids'] = v[:10]
    return summary


if __name__ == '__main__':
    main()
