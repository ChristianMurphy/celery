"""Run one correctness test: a workload under faults against one configuration.

Usage (from the repository root)::

    python -m t.correctness.runner --broker redis --backend redis \\
        --acks-late --faults kill-worker --ops 300

Each run writes ``history.jsonl`` (canonical Jepsen-style operations),
``results.json`` (checker output) and worker/service logs to ``--out``.
"""
import argparse
import itertools
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time

from . import checker
from .proxy import Proxy
from .services import MySQLService, PostgresService, RabbitMQService, RedisService

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
QUEUE = 'correctness'
# Configurations whose documented promise differs; see README.
PROFILES = {
    'p0': ('defaults: early ack, prefetch x4', {'acks_late': False, 'reject_on_worker_lost': False, 'prefetch': 4}),
    'p1': ('at-least-once: acks_late, reject_on_worker_lost, prefetch x1 (+confirm_publish on RabbitMQ)',
           {'acks_late': True, 'reject_on_worker_lost': True, 'prefetch': 1}),
    'p2': ('p1 + worker_deduplicate_successful_tasks',
           {'acks_late': True, 'reject_on_worker_lost': True, 'prefetch': 1, 'dedup': True}),
    'p3': ('p1 + worker_cancel_long_running_tasks_on_connection_loss',
           {'acks_late': True, 'reject_on_worker_lost': True, 'prefetch': 1, 'cancel_on_connection_loss': True}),
}
FAULTS = ('kill-worker', 'pause-worker', 'partition-broker', 'blackhole-broker',
          'partition-backend', 'blackhole-backend', 'kill-broker', 'kill-backend',
          'pause-broker', 'pause-backend')


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--broker', choices=['redis', 'rabbitmq'], default='redis')
    p.add_argument('--backend', choices=['redis', 'postgres', 'mysql'], default='redis')
    p.add_argument('--queue-type', choices=['classic', 'quorum'], default='classic')
    p.add_argument('--confirm-publish', action='store_true')
    p.add_argument('--broker-persistence', default='aof-everysec',
                   choices=['none', 'rdb', 'aof-everysec', 'aof-always'])
    p.add_argument('--backend-persistence', default='aof-everysec',
                   choices=['none', 'rdb', 'aof-everysec', 'aof-always'])
    p.add_argument('--acks-late', action='store_true')
    p.add_argument('--reject-on-worker-lost', action='store_true')
    p.add_argument('--track-started', action='store_true')
    p.add_argument('--dedup', action='store_true', help='worker_deduplicate_successful_tasks')
    p.add_argument('--prefetch', type=int, default=1, help='worker_prefetch_multiplier')
    p.add_argument('--visibility-timeout', type=int, default=30)
    p.add_argument('--chord-unordered', action='store_true', help='result_chord_ordered=False')
    p.add_argument('--workers', type=int, default=3)
    p.add_argument('--concurrency', type=int, default=2)
    p.add_argument('--profile', choices=sorted(PROFILES),
                   help='preset ack/prefetch options: ' + '; '.join(f'{k}: {v[0]}' for k, v in sorted(PROFILES.items())))
    p.add_argument('--cancel-on-connection-loss', action='store_true',
                   help='worker_cancel_long_running_tasks_on_connection_loss')
    p.add_argument('--always-retry', action='store_true', help='result_backend_always_retry')
    p.add_argument('--workload', choices=['single', 'chord', 'retry', 'eta', 'chain'], default='single')
    p.add_argument('--retry-fails', type=int, default=2, help='retry workload: retries before success')
    p.add_argument('--retry-countdown', type=float, default=1.0)
    p.add_argument('--eta-range', default='1,15', help='eta workload: min,max countdown seconds')
    p.add_argument('--chain-length', type=int, default=3)
    p.add_argument('--chord-size', type=int, default=3)
    p.add_argument('--ops', type=int, default=300)
    p.add_argument('--rate', type=float, default=5.0, help='ops per second, all clients')
    p.add_argument('--clients', type=int, default=2)
    p.add_argument('--task-duration', default='0.1,2.0', help='min,max seconds')
    p.add_argument('--faults', default='kill-worker', help=f'comma separated: {",".join(FAULTS)} or none')
    p.add_argument('--fault-interval', type=float, default=10)
    p.add_argument('--fault-duration', type=float, default=3)
    p.add_argument('--crash-hook', action='append', default=[], metavar='POINT=P',
                   help='after_store_before_ack, before_execute or child_exit_after_store')
    p.add_argument('--read-rate', type=float, default=10)
    p.add_argument('--recovery-timeout', type=float, default=None)
    p.add_argument('--settle', type=float, default=5)
    p.add_argument('--port-base', type=int, default=6440)
    p.add_argument('--out', default=None)
    p.add_argument('--seed', type=int, default=None)
    p.add_argument('--jepsen-jar', default=os.environ.get('JEPSEN_BRIDGE_JAR'),
                   help='jepsen_bridge standalone jar: also check the queue with jepsen total-queue')
    args = p.parse_args(argv)
    if args.profile:
        for key, value in PROFILES[args.profile][1].items():
            setattr(args, key, value)
        if args.broker == 'rabbitmq' and args.acks_late:
            args.confirm_publish = True
    return args


class Test:

    def __init__(self, args):
        self.args = args
        self.seed = args.seed if args.seed is not None else random.randrange(1 << 30)
        self.rng = random.Random(self.seed)
        self.out = args.out or os.path.join(REPO, 'correctness-runs', time.strftime('%Y%m%d-%H%M%S'))
        os.makedirs(os.path.join(self.out, 'workers'), exist_ok=True)
        self.ops, self.lock = [], threading.Lock()
        self.known_tids = []
        self.stop = threading.Event()
        b = args.port_base
        data = os.path.join(self.out, 'data')
        self.recorder = RedisService('recorder', b + 2, data, persistence='none')
        if args.broker == 'redis':
            self.broker = RedisService('broker', b, data, persistence=args.broker_persistence)
        else:
            self.broker = RabbitMQService(f'cx-rabbit-{b}', b + 3, b + 4)
        if args.backend == 'redis':
            self.backend = RedisService('backend', b + 1, data, persistence=args.backend_persistence)
        elif args.backend == 'postgres':
            self.backend = PostgresService(f'cx-pg-{b}', b + 5)
        else:
            self.backend = MySQLService(f'cx-mysql-{b}', b + 6)
        self.workers = {}
        self.proxies = {}
        self.held = set()  # workers the nemesis is deliberately keeping down
        self.wlock = threading.RLock()
        self.finished = threading.Event()

    # -- history ---------------------------------------------------------

    def record(self, **op):
        op.setdefault('time', time.time())
        with self.lock:
            self.ops.append(op)
        return op

    def nemesis(self, f, **value):
        self.record(process='nemesis', type='info', f=f, value=value)

    # -- configuration ---------------------------------------------------

    def config(self, broker_url, backend_url):
        a = self.args
        hooks = dict(h.split('=') for h in a.crash_hook)
        return {
            'broker_url': broker_url, 'backend_url': backend_url,
            'recorder_url': self.recorder.url(), 'acks_late': a.acks_late,
            'reject_on_worker_lost': a.reject_on_worker_lost, 'track_started': a.track_started,
            'prefetch_multiplier': a.prefetch, 'deduplicate_successful_tasks': a.dedup,
            'visibility_timeout': a.visibility_timeout, 'confirm_publish': a.confirm_publish,
            'queue': QUEUE, 'queue_type': a.queue_type if a.queue_type == 'quorum' else None,
            'result_chord_ordered': not a.chord_unordered,
            'crash_hooks': {k: float(v) for k, v in hooks.items()},
            'cancel_on_connection_loss': a.cancel_on_connection_loss,
            'result_backend_always_retry': a.always_retry,
        }

    def service_url(self, service, port=None):
        return service.url(port) if service.kind != 'redis' else service.url(port, db=0)

    # -- workers ---------------------------------------------------------

    def start_worker(self, name):
        bproxy, rproxy = self.proxies[name]
        cfg = self.config(self.service_url(self.broker, bproxy.listen_port),
                          self.service_url(self.backend, rproxy.listen_port))
        env = dict(os.environ, PYTHONPATH=REPO, CORRECTNESS_CONFIG=json.dumps(cfg),
                   CORRECTNESS_WORKER=name)
        log_path = os.path.join(self.out, 'workers', f'{name}.log')
        start = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        proc = subprocess.Popen(
            [sys.executable, '-m', 'celery', '-A', 't.correctness.app', 'worker', '-n', f'{name}@%h',
             '-c', str(self.args.concurrency), '-Q', QUEUE, '--loglevel=INFO',
             '--without-mingle', '--without-gossip'],
            cwd=REPO, env=env, stdout=open(log_path, 'ab'), stderr=subprocess.STDOUT,
            start_new_session=True)
        self.workers[name] = proc
        self.nemesis('start-worker', worker=name, pid=proc.pid)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with open(log_path, 'rb') as f:
                f.seek(start)
                if b' ready.' in f.read():
                    return proc
            if proc.poll() is not None:
                raise RuntimeError(f'worker {name} exited: see {log_path}')
            time.sleep(0.2)
        raise TimeoutError(f'worker {name} not ready')

    def signal_worker(self, name, sig):
        try:
            os.killpg(self.workers[name].pid, sig)
        except ProcessLookupError:
            pass

    def supervise(self):
        """Restart workers killed by crash hooks (the nemesis restarts its own)."""
        while not self.finished.wait(1):
            with self.wlock:
                for name, proc in list(self.workers.items()):
                    if proc.poll() is not None and name not in self.held:
                        self.start_worker(name)

    def kill_worker(self, name, f='kill-worker'):
        self.nemesis(f, worker=name)  # recorded *before* the kill: it bounds running executions
        self.signal_worker(name, signal.SIGKILL)
        self.workers[name].wait()

    # -- generators ------------------------------------------------------

    def client(self, process, counter):
        from celery import chord
        from kombu.exceptions import EncodeError

        from celery import chain

        from .app import body, execute, flaky, header, scheduled, step
        lo, hi = map(float, self.args.task_duration.split(','))
        eta_lo, eta_hi = map(float, self.args.eta_range.split(','))
        while not self.stop.is_set():
            op = next(counter)
            if op >= self.args.ops:
                return
            self.record(process=process, type='invoke', f='enqueue', value=op)
            try:
                workload = self.args.workload
                if workload == 'single':
                    r = execute.apply_async((op, self.rng.uniform(lo, hi)))
                    tids, extra = [r.id], {}
                elif workload == 'retry':
                    r = flaky.apply_async((op, self.args.retry_fails, self.rng.uniform(lo, hi),
                                           self.args.retry_countdown))
                    tids, extra = [r.id], {}
                elif workload == 'eta':
                    countdown = self.rng.uniform(eta_lo, eta_hi)
                    r = scheduled.apply_async((op, time.time() + countdown), countdown=countdown)
                    tids, extra = [r.id], {'countdown': countdown}
                elif workload == 'chain':
                    c = chain(step.s(None, op, 0, self.rng.uniform(lo, hi)),
                              *[step.s(op, i, self.rng.uniform(lo, hi)) for i in range(1, self.args.chain_length)])
                    r = c.apply_async()
                    step_tids, node = [], r
                    while node is not None:
                        step_tids.insert(0, node.id)
                        node = node.parent
                    tids, extra = step_tids, {'step_tids': step_tids}
                else:
                    c = chord([header.s(op, i, self.rng.uniform(lo, hi)) for i in range(self.args.chord_size)],
                              body.s(op))
                    r = c.apply_async()
                    header_tids = [h.id for h in r.parent.results]
                    tids, extra = header_tids + [r.id], {'header_tids': header_tids, 'body_tid': r.id}
                self.record(process=process, type='ok', f='enqueue', value=op, tid=r.id, extra=extra)
                with self.lock:
                    self.known_tids.extend(tids)
            except EncodeError as exc:
                self.record(process=process, type='fail', f='enqueue', value=op, error=repr(exc))
            except Exception as exc:  # pylint: disable=broad-except
                # Anything else may have reached the broker: indeterminate.
                self.record(process=process, type='info', f='enqueue', value=op, error=repr(exc)[:300])
            time.sleep(self.rng.expovariate(self.args.rate / self.args.clients))

    def read(self, process, tid, final=False):
        from .app import app
        self.record(process=process, type='invoke', f='read', value={'tid': tid, 'final': final})
        try:
            meta = app.backend.get_task_meta(tid, cache=False)  # AsyncResult caches READY states
            result = meta.get('result')
            if not isinstance(result, (int, str, type(None), list)):
                result = repr(result)[:200]
            self.record(process=process, type='ok', f='read',
                        value={'tid': tid, 'state': meta['status'], 'result': result})
        except Exception as exc:  # pylint: disable=broad-except
            self.record(process=process, type='info', f='read', value={'tid': tid}, error=repr(exc)[:300])

    def reader(self):
        while not self.stop.is_set():
            with self.lock:
                tid = self.rng.choice(self.known_tids) if self.known_tids else None
            if tid:
                self.read('r0', tid)
            time.sleep(1 / self.args.read_rate)

    # -- nemesis ---------------------------------------------------------

    def fault(self, kind):
        d = self.args.fault_duration
        name = self.rng.choice(sorted(self.workers))
        if kind == 'kill-worker':
            with self.wlock:
                self.held.add(name)
                self.kill_worker(name)
            time.sleep(d)
            with self.wlock:
                self.held.discard(name)
                if self.workers[name].poll() is not None:
                    self.start_worker(name)
        elif kind == 'pause-worker':
            self.nemesis(kind, worker=name)
            self.signal_worker(name, signal.SIGSTOP)
            time.sleep(d)
            self.signal_worker(name, signal.SIGCONT)
            self.nemesis('resume-worker', worker=name)
        elif kind in ('partition-broker', 'blackhole-broker', 'partition-backend', 'blackhole-backend'):
            mode, target = kind.split('-')
            proxy = self.proxies[name][0 if target == 'broker' else 1]
            self.nemesis(kind, worker=name)
            proxy.set_mode('reset' if mode == 'partition' else 'blackhole')
            time.sleep(d)
            proxy.set_mode('pass')
            self.nemesis('heal-' + kind, worker=name)
        elif kind in ('kill-broker', 'kill-backend', 'pause-broker', 'pause-backend'):
            action, target = kind.split('-')
            service = self.broker if target == 'broker' else self.backend
            self.nemesis(kind, service=target)
            if action == 'kill':
                service.kill()
                time.sleep(d)
                service.restart() if service.kind != 'redis' else service.start()
            else:
                service.pause()
                time.sleep(d)
                service.resume()
            self.nemesis('heal-' + kind, service=target)
        else:
            raise ValueError(kind)

    def nemesis_loop(self, kinds):
        while not self.stop.wait(self.rng.uniform(0.5, 1.5) * self.args.fault_interval):
            self.fault(self.rng.choice(kinds))

    # -- phases ----------------------------------------------------------

    def depth(self):
        try:
            return self.broker.depth(QUEUE)
        except Exception:  # pylint: disable=broad-except
            return None

    def recover(self):
        with self.wlock:
            for name, proc in self.workers.items():
                if proc.poll() is not None:
                    self.start_worker(name)
        # A worker killed while holding kombu's Redis `unacked_mutex` blocks every
        # restore until the lock expires (unacked_mutex_expire, 300s by default).
        default = self.args.visibility_timeout + (360 if self.args.broker == 'redis' else 120)
        timeout = self.args.recovery_timeout or default
        deadline = time.monotonic() + timeout
        rec = self.recorder.client()
        last, quiet_since = None, time.monotonic()
        while time.monotonic() < deadline:
            state = (self.depth(), rec.llen('events'))
            if state != last:
                last, quiet_since = state, time.monotonic()
            elif state[0] == 0 and time.monotonic() - quiet_since >= self.args.settle:
                return True
            time.sleep(0.5)
        return False

    def run(self):
        a = self.args
        print(f'seed={self.seed} out={self.out}', flush=True)
        for s in (self.recorder, self.broker, self.backend):
            s.start()
        self.recorder.client().delete('events')
        b = a.port_base
        for i in range(a.workers):
            name = f'w{i}'
            self.proxies[name] = (Proxy(b + 10 + 2 * i, '127.0.0.1', self.broker.port),
                                  Proxy(b + 11 + 2 * i, '127.0.0.1', self.backend.port))
        os.environ['CORRECTNESS_CONFIG'] = json.dumps(self.config(
            self.service_url(self.broker), self.service_url(self.backend)))
        from .app import app
        app.backend  # noqa: B018 -- create the backend (and SQL tables) before workers start
        for name in self.proxies:
            self.start_worker(name)
        threading.Thread(target=self.supervise, daemon=True).start()
        self.t0 = time.time()
        counter = itertools.count()
        threads = [threading.Thread(target=self.client, args=(f'c{i}', counter)) for i in range(a.clients)]
        threads.append(threading.Thread(target=self.reader, daemon=True))
        kinds = [k for k in a.faults.split(',') if k and k != 'none']
        nemesis = threading.Thread(target=self.nemesis_loop, args=(kinds,)) if kinds else None
        for t in threads + ([nemesis] if nemesis else []):
            t.start()
        for t in threads[:-1]:
            t.join()
        self.stop.set()
        if nemesis:
            nemesis.join()
        for bp, rp in self.proxies.values():
            bp.set_mode('pass'), rp.set_mode('pass')
        self.nemesis('recovery-start')
        settled = self.recover()
        residue = None
        if not settled:
            try:
                residue = self.broker.residue(QUEUE)
            except Exception as exc:  # pylint: disable=broad-except
                residue = {'error': repr(exc)}
        self.nemesis('recovery-end', settled=settled, depth=self.depth(), residue=residue)
        with self.lock:
            tids = list(dict.fromkeys(self.known_tids))
        for tid in tids:
            self.read('f0', tid, final=True)
        self.finished.set()
        with self.wlock:
            for name in self.workers:
                self.kill_worker(name, f='teardown')
        history = self.collect()
        cfg = dict(vars(a), workload=a.workload, result_chord_ordered=not a.chord_unordered,
                   eta_max=float(a.eta_range.split(',')[1]))
        results = checker.check(history, cfg)
        results.update(seed=self.seed, settled=settled, config=vars(a))
        if a.jepsen_jar and a.workload == 'single':
            results['jepsen'] = self.jepsen(a.jepsen_jar)
        with open(os.path.join(self.out, 'results.json'), 'w') as f:
            json.dump(results, f, indent=2, default=str)
        return results

    def jepsen(self, jar):
        """Independent cross-check: jepsen.checker/total-queue on the same history."""
        history = os.path.join(self.out, 'history.jsonl')
        edn = os.path.join(self.out, 'history.edn')
        subprocess.run([sys.executable, os.path.join(HERE, 'jepsen_bridge', 'to_edn.py'), history,
                        '-o', edn, '--drop-failed-enqueues'], check=True)
        proc = subprocess.run(['java', '-jar', jar, edn, 'total-queue', '--format', 'json'],
                              capture_output=True, text=True)
        try:
            return json.loads(proc.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError):
            return {'error': proc.stderr[-2000:], 'exit': proc.returncode}

    def collect(self):
        events = [json.loads(e) for e in self.recorder.client().lrange('events', 0, -1)]
        ops = sorted(self.ops + events, key=lambda o: o['time'])
        with open(os.path.join(self.out, 'history.jsonl'), 'w') as f:
            for i, op in enumerate(ops):
                op['index'], op['time'] = i, op['time'] - self.t0
                if op['f'] == 'execute-eta':  # task argument: absolute wall-clock time
                    op['value']['eta'] -= self.t0
                f.write(json.dumps(op, default=str) + '\n')
        return ops

    def cleanup(self):
        self.finished.set()
        for name in self.workers:
            self.signal_worker(name, signal.SIGKILL)
        for bp, rp in self.proxies.values():
            bp.close(), rp.close()
        for s in (self.broker, self.backend, self.recorder):
            try:
                s.stop()
            except Exception:  # pylint: disable=broad-except
                pass


def summarize(results):
    lines = [f"valid={results['valid']} ops={results['ops']} settled={results['settled']} "
             f"faults={','.join(results['faults'])}"]
    if 'jepsen' in results:
        j = results['jepsen'].get('total-queue', results['jepsen'])
        lines.append(f"  jepsen total-queue: valid={j.get('valid?')} lost={j.get('lost-count')} "
                     f"unexpected={j.get('unexpected-count')} duplicated={j.get('duplicated-count')}")
    for kind, n in results['counts'].items():
        why = results['explained'].get(kind)
        tag = 'VIOLATION' if kind in results['violations'] else f'ok ({why})' if why else 'info'
        lines.append(f'  {kind:28} {n:5}  {tag}  e.g. {results["examples"][kind][:5]}')
    return '\n'.join(lines)


def main(argv=None):
    test = Test(parse_args(argv))
    try:
        results = test.run()
    finally:
        test.cleanup()
    print(summarize(results))
    return 0 if results['valid'] else 1


if __name__ == '__main__':
    sys.exit(main())
