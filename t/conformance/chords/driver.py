"""Run chords against actual workers while a fault happens, and judge each one.

Example::

    python -m t.conformance.chords.driver --broker redis --backend redis \\
        --pool prefork --fault kill_random --chords 60

See README.rst for the faults, the verdicts and the history format.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..', '..', '..'))

BROKERS = {
    'redis': ('redis://127.0.0.1:16379/0', 'chords-broker-redis'),
    'rabbitmq': ('amqp://guest:guest@127.0.0.1:5673//', 'chords-rabbit'),
    'quorum': ('amqp://guest:guest@127.0.0.1:5673//', 'chords-rabbit'),
}

# name -> (url template, container we may restart or None, extra conf)
BACKENDS = {
    'redis': ('redis://127.0.0.1:16380/0', 'chords-backend-redis', {}),
    'postgresql': ('db+postgresql+psycopg2://postgres:pw@127.0.0.1:5434/celery',
                   'chords-pg', {}),
    'sqlite': ('db+sqlite:///{tmp}/chords-{run}.sqlite', None, {}),
    'mongodb': ('mongodb://127.0.0.1:27018/chords_{run}', 'chords-mongo', {}),
    'memcached': ('cache+memcached://127.0.0.1:11211/', None, {}),
    'cassandra': ('cassandra://', None, {
        'cassandra_servers': ['127.0.0.1'], 'cassandra_keyspace': 'celery',
        'cassandra_table': 'chords_{run}',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE'}),
    'couchdb': ('couchdb://admin:pw@127.0.0.1:5984/chords_{run}', None, {}),
    'arangodb': ('arangodb://root:pw@127.0.0.1:8529/_system/chords_{run}', None, {}),
    'elasticsearch': ('elasticsearch://127.0.0.1:9200/chords_{run}', None, {}),
}

FAULTS = ('none', 'kill_random', 'kill_child', 'kill_after_store',
          'broker_restart', 'backend_restart')


def fmt(value, **kw):
    if isinstance(value, str):
        return value.format(**kw)
    if isinstance(value, list):
        return [fmt(v, **kw) for v in value]
    if isinstance(value, dict):
        return {k: fmt(v, **kw) for k, v in value.items()}
    return value


class Run:
    def __init__(self, args):
        self.args = args
        self.run = args.run or f'{args.broker}-{args.backend}-{args.pool}-{args.fault}-{uuid.uuid4().hex[:6]}'
        self.tmp = tempfile.mkdtemp(prefix=f'chords-{self.run}-')
        url, self.backend_container, conf = BACKENDS[args.backend]
        self.env = dict(os.environ)
        self.env.update(
            PYTHONPATH=ROOT + os.pathsep + self.env.get('PYTHONPATH', ''),
            CHORDS_RUN=self.run,
            CHORDS_BROKER=BROKERS[args.broker][0],
            CHORDS_BACKEND=fmt(url, run=self.run.replace('-', '_'), tmp=self.tmp),
            CHORDS_ACKS_LATE='1' if args.acks_late else '0',
            CHORDS_REJECT_ON_LOST='1' if args.reject_on_lost else '0',
            CHORDS_PREFETCH=str(args.prefetch),
        )
        if args.broker == 'redis':
            self.env['CHORDS_VISIBILITY'] = str(args.visibility)
        if args.broker == 'quorum':
            self.env['CHORDS_QUEUE_TYPE'] = 'quorum'
        if conf:
            self.env['CHORDS_BACKEND_CONF'] = json.dumps(
                fmt(conf, run=self.run.replace('-', '_')))
        if args.fast_redelivery:
            self.env['CHORDS_FAST_REDELIVERY'] = '1'
        if args.fault == 'kill_after_store':
            self.env['CHORDS_NOTIFY'] = 'header_stored'
        self.broker_container = BROKERS[args.broker][1]
        self.workers = {}   # slot -> Popen
        self.gen = 0
        self.faults = []
        self.stop = threading.Event()
        self.lock = threading.Lock()

    # -- workers ---------------------------------------------------------
    def start_worker(self, slot):
        self.gen += 1
        log = open(os.path.join(self.tmp, f'worker-{slot}-{self.gen}.log'), 'w')
        cmd = [sys.executable, '-m', 'celery', '-A', 't.conformance.chords.app',
               'worker', '-P', self.args.pool, '-c', str(self.args.concurrency),
               '-n', f'w{slot}-{self.gen}@%h', '-l', 'info',
               '--without-gossip', '--without-mingle', '--without-heartbeat']
        proc = subprocess.Popen(cmd, env=self.env, cwd=ROOT, stdout=log,
                                stderr=subprocess.STDOUT, start_new_session=True)
        self.workers[slot] = proc
        return proc

    def kill_worker(self, slot, why):
        proc = self.workers.get(slot)
        if proc is None or proc.poll() is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        self.fault('kill_worker', slot=slot, pid=proc.pid, why=why)

    def children(self, pid):
        try:
            with open(f'/proc/{pid}/task/{pid}/children') as fh:
                return [int(p) for p in fh.read().split()]
        except OSError:
            return []

    def fault(self, kind, **fields):
        fields.update(kind=kind, ts=time.time())
        self.faults.append(fields)

    # -- fault loops -----------------------------------------------------
    def fault_loop(self):
        a = self.args
        rng = random.Random(a.seed)
        kind = a.fault
        if kind == 'kill_after_store':
            return self.kill_after_store_loop(rng)
        while not self.stop.wait(rng.uniform(*a.fault_every)):
            with self.lock:
                if kind == 'kill_random':
                    slot = rng.choice(list(self.workers))
                    self.kill_worker(slot, 'random')
                    time.sleep(a.restart_after)
                    self.start_worker(slot)
                elif kind == 'kill_child':
                    slot = rng.choice(list(self.workers))
                    kids = self.children(self.workers[slot].pid)
                    if kids:
                        pid = rng.choice(kids)
                        try:
                            os.kill(pid, signal.SIGKILL)
                            self.fault('kill_child', slot=slot, pid=pid)
                        except ProcessLookupError:
                            pass
                elif kind in ('broker_restart', 'backend_restart'):
                    name = (self.broker_container if kind == 'broker_restart'
                            else self.backend_container)
                    self.fault(kind + '_begin', container=name)
                    if a.hard_restart:
                        subprocess.run(['docker', 'kill', name], capture_output=True)
                        time.sleep(a.down_for)
                        subprocess.run(['docker', 'start', name], capture_output=True)
                    else:
                        subprocess.run(['docker', 'restart', '-t', '2', name],
                                       capture_output=True)
                    self.fault(kind + '_end', container=name)

    def kill_after_store_loop(self, rng):
        import redis
        r = redis.Redis.from_url(self.env.get('CHORDS_AUDIT', 'redis://127.0.0.1:16381/0'))
        key = f'chords:{self.run}:notify'
        last = 0.0
        while not self.stop.is_set():
            item = r.blpop([key], timeout=1)
            if not item:
                continue
            ev = json.loads(item[1])
            now = time.time()
            if now - last < self.args.fault_every[0] or rng.random() > self.args.kill_ratio:
                continue
            with self.lock:
                for slot, proc in self.workers.items():
                    if proc.poll() is None and proc.pid == ev['pgid']:
                        self.kill_worker(slot, f"after_store {ev['tid']}")
                        time.sleep(self.args.restart_after)
                        self.start_worker(slot)
                        last = time.time()
                        break

    # -- main ------------------------------------------------------------
    def main(self):
        a = self.args
        os.environ.update(self.env)
        sys.path.insert(0, ROOT)
        from celery import chord, group

        import celery
        import kombu

        from t.conformance.chords import app as appmod
        print(f'celery {celery.__file__}\nkombu {kombu.__file__}', file=sys.stderr)
        if a.backend == 'arangodb':
            import requests
            requests.post('http://127.0.0.1:8529/_db/_system/_api/collection',
                          auth=('root', 'pw'),
                          json={'name': f"chords_{self.run.replace('-', '_')}"})
        if a.backend == 'cassandra':
            appmod.app.backend._get_connection(write=True)

        audit = appmod.audit()
        audit.delete(appmod.EVENTS, appmod.NOTIFY)
        for slot in range(a.workers):
            self.start_worker(slot)
        deadline = time.time() + 60
        while audit.llen(appmod.EVENTS) < a.workers and time.time() < deadline:
            time.sleep(0.2)

        rng = random.Random(a.seed)
        chords = {}
        for _ in range(a.chords):
            cid = uuid.uuid4().hex[:12]
            hids = [uuid.uuid4().hex for _ in range(a.header)]
            bid = uuid.uuid4().hex
            sigs = [appmod.header.si(cid, i, round(rng.uniform(*a.sleep), 3)).set(task_id=hids[i])
                    for i in range(a.header)]
            body = appmod.body.s(chord_id=cid, header_ids=hids).set(task_id=bid)
            body.link_error(appmod.on_error.s(chord_id=cid))
            for attempt in range(30):
                try:
                    res = chord(group(sigs), body).apply_async()
                    break
                except Exception as exc:  # broker down while submitting
                    print('submit failed', exc, file=sys.stderr)
                    time.sleep(1)
            gid = res.parent.id if res.parent is not None else None
            chords[cid] = dict(chord=cid, group_id=gid, header_ids=hids, body_id=bid)
            appmod.emit('submit', chord=cid, group=gid, header_ids=hids, body_id=bid)
            if a.submit_gap:
                time.sleep(a.submit_gap)

        if a.fault != 'none':
            t = threading.Thread(target=self.fault_loop, daemon=True)
            t.start()
        started = time.time()

        def settled():
            done = set()
            for raw in audit.lrange(appmod.EVENTS, 0, -1):
                ev = json.loads(raw)
                if ev['ev'] in ('body_start', 'errback'):
                    done.add(ev.get('chord'))
            return done

        while time.time() - started < a.fault_for:
            if len(settled() & set(chords)) == len(chords):
                break
            time.sleep(1)
        self.stop.set()
        with self.lock:
            for slot, proc in list(self.workers.items()):
                if proc.poll() is not None:
                    self.start_worker(slot)
        settle_end = time.time() + a.settle
        while time.time() < settle_end:
            if len(settled() & set(chords)) == len(chords):
                break
            time.sleep(1)
        # Give late duplicates (redeliveries, restored messages) time to show.
        time.sleep(a.tail)
        for proc in self.workers.values():
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
        for proc in self.workers.values():
            try:
                proc.wait(15)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)

        events = [json.loads(r) for r in audit.lrange(appmod.EVENTS, 0, -1)]
        return self.judge(appmod.app, chords, events)

    # -- verdicts --------------------------------------------------------
    def judge(self, app, chords, events):
        a = self.args
        by_tid, by_group = {}, {}
        for c in chords.values():
            for t in c['header_ids'] + [c['body_id']]:
                by_tid[t] = c['chord']
            by_group[c['group_id']] = c['chord']
        hist = {cid: [] for cid in chords}
        for ev in events:
            cid = ev.get('chord') or by_tid.get(ev.get('tid')) or by_group.get(ev.get('group'))
            if cid in hist:
                hist[cid].append(ev)

        def state(tid):
            for attempt in range(5):
                try:
                    return app.AsyncResult(tid).state
                except Exception as exc:
                    err = repr(exc)
                    time.sleep(1)
            return f'error:{err}'

        records, counts = [], {}
        config = {k: v for k, v in vars(a).items() if k not in ('out',)}
        config.update(run=self.run, broker_url=self.env['CHORDS_BROKER'],
                      backend_url=self.env['CHORDS_BACKEND'])
        for cid, c in chords.items():
            evs = hist[cid]
            body_starts = [e for e in evs if e['ev'] == 'body_start']
            fresh = [e for e in body_starts if not e.get('redelivered')]
            publishes = [e for e in evs if e['ev'] == 'body_publish']
            errbacks = [e for e in evs if e['ev'] == 'errback']
            header_states = [state(t) for t in c['header_ids']]
            body_state = state(c['body_id'])
            expected = list(range(a.header))
            early = any(
                e['results'] != expected or any(s != 'SUCCESS' for s in e['header_states'])
                for e in body_starts)
            header_runs = {}
            for e in evs:
                if e['ev'] == 'header_start':
                    header_runs[e['idx']] = header_runs.get(e['idx'], 0) + 1
            all_ok = all(s == 'SUCCESS' for s in header_states)
            if len(fresh) > 1 or (len(body_starts) > 1 and len(publishes) > 1):
                outcome = 'duplicate_body'
            elif early:
                outcome = 'early_body'
            elif body_starts and errbacks:
                outcome = 'body_and_error'
            elif body_starts:
                outcome = 'ok'
            elif errbacks and all('Callback error' in e['exc'] for e in errbacks):
                # The body couldn't be published within the retry policy
                # (broker down): the chord fails, as documented.
                outcome = 'publish_error'
            elif errbacks or body_state == 'FAILURE':
                outcome = 'chord_error' if not all_ok else 'spurious_error'
            elif not all_ok:
                outcome = 'header_lost'
            elif publishes:
                outcome = 'body_not_run'
            else:
                outcome = 'stuck'
            counts[outcome] = counts.get(outcome, 0) + 1
            records.append(dict(
                run=self.run, **c, events=sorted(evs, key=lambda e: e['ts']),
                final=dict(header_states=header_states, body_state=body_state),
                verdict=dict(outcome=outcome, body_runs=len(body_starts),
                             body_fresh_runs=len(fresh),
                             body_publishes=len(publishes),
                             errbacks=len(errbacks), early=early,
                             header_runs=header_runs,
                             duplicate_headers=sum(1 for v in header_runs.values() if v > 1))))
        violations = sum(counts.get(k, 0) for k in
                         ('duplicate_body', 'early_body', 'body_and_error',
                          'stuck', 'spurious_error'))
        summary = dict(run=self.run, config=config, counts=counts,
                       violations=violations, faults=len(self.faults),
                       fault_log=self.faults,
                       duplicate_header_runs=sum(r['verdict']['duplicate_headers'] for r in records),
                       logs=self.tmp)
        os.makedirs(a.out, exist_ok=True)
        with open(os.path.join(a.out, f'{self.run}.jsonl'), 'w') as fh:
            for rec in records:
                fh.write(json.dumps(rec, default=repr) + '\n')
        with open(os.path.join(a.out, 'summary.jsonl'), 'a') as fh:
            fh.write(json.dumps(summary, default=repr) + '\n')
        return summary


def parse(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--broker', choices=BROKERS, default='redis')
    p.add_argument('--backend', choices=BACKENDS, default='redis')
    p.add_argument('--pool', choices=('prefork', 'solo', 'threads'), default='prefork')
    p.add_argument('--fault', choices=FAULTS, default='none')
    p.add_argument('--chords', type=int, default=60)
    p.add_argument('--header', type=int, default=4)
    p.add_argument('--sleep', type=float, nargs=2, default=(0.05, 1.0),
                   help='header task sleep range in seconds')
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--concurrency', type=int, default=2)
    p.add_argument('--prefetch', type=int, default=4)
    p.add_argument('--acks-late', type=int, default=1)
    p.add_argument('--reject-on-lost', type=int, default=1)
    p.add_argument('--visibility', type=int, default=15,
                   help='Redis broker visibility_timeout')
    p.add_argument('--fault-every', type=float, nargs=2, default=(2.0, 5.0))
    p.add_argument('--fault-for', type=float, default=120)
    p.add_argument('--kill-ratio', type=float, default=1.0,
                   help='kill_after_store: chance to act on a stored header')
    p.add_argument('--restart-after', type=float, default=0.5)
    p.add_argument('--hard-restart', type=int, default=0)
    p.add_argument('--down-for', type=float, default=2.0)
    p.add_argument('--settle', type=float, default=90)
    p.add_argument('--tail', type=float, default=10)
    p.add_argument('--submit-gap', type=float, default=0.0)
    p.add_argument('--fast-redelivery', type=int, default=0,
                   help='header tasks sleep only on their first delivery')
    p.add_argument('--seed', type=int, default=None)
    p.add_argument('--run', default=None)
    p.add_argument('--out', default=os.path.join(HERE, 'results'))
    return p.parse_args(argv)


if __name__ == '__main__':
    summary = Run(parse()).main()
    print(json.dumps({k: summary[k] for k in ('run', 'counts', 'violations',
                                              'faults', 'duplicate_header_runs')}))
