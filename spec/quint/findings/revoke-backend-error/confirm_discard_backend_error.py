"""Confirm revoke.qnt's expire_backenderr counterexamples on the real code.

A worker discards a task that expired (or was revoked): _announce_revoked
stores REVOKED and then acks (request.py:464-475). The model says a
backend error in that write leaves the handler before the ack, and

  * with a Redis result backend on a Redis broker the error is redis'
    ConnectionError, which the consumer takes for a lost broker
    connection: it reconnects and puts back every unacked message of the
    worker, including the messages of runs that go on and of runs that
    finished and were not acked yet; they run again (noRerunAfterSuccess,
    noParallelRuns);
  * with any other backend error the worker logs "Unrecoverable error"
    and stops.

Cases (one prefork worker, acks_late, Redis broker on 6379):

  inject     the REVOKED write raises redis ConnectionError for 3 s from
             the first try (a RedisBackend subclass that fails that write
             only; injected). -c 1: task A
             runs 3 s; B (expires=1.5) and C wait behind it. A's SUCCESS is
             stored, then B is discarded at the pool slot A freed.
  injectdb   the same write raises RuntimeError (any backend error that is
             not one of the broker's, for example a database's).
  natural    a real Redis result backend in its own container
             (quint-backend, port 6391), stopped (docker stop). -c 2: task
             X (6 s) was published first, then B with an expires that has
             already passed; the worker starts during the outage. Nothing
             else is injected. The backend comes back 15 s later.
  naturalretry  natural with result_backend_always_retry.

A supervisor loop restarts the worker when it exits (up to 4 starts).

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_discard_backend_error.py <case> [runs]

Run it from a directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone

import celery
import kombu
import redis

APP = '''
import time
import redis
from celery import Celery
from celery.backends.redis import RedisBackend

CASE = %(case)r
r = redis.Redis(db=15)


class DownAtRevoked(RedisBackend):
    def _store_result(self, task_id, result, state, *args, **kwargs):
        # down for 3 s from the first REVOKED write, then up again
        first = float(r.get('downsince') or 0)
        down = state == 'REVOKED' and (not first or time.time() < first + 3)
        if down and CASE.startswith('inject'):
            r.setnx('downsince', time.time())
            r.incr('revokeerr')
            if CASE == 'injectdb':
                raise RuntimeError('injected: database down')
            raise redis.exceptions.ConnectionError('injected: backend down')
        return super()._store_result(task_id, result, state, *args, **kwargs)


if CASE.startswith('natural'):
    backend = 'redis://localhost:6391/0'
else:
    backend = 'dbeapp:DownAtRevoked+redis://localhost:6379/11'
app = Celery('dbeapp', broker='redis://localhost:6379/10', backend=backend)
app.conf.task_acks_late = True
app.conf.broker_transport_options = {'visibility_timeout': 60}
app.conf.result_backend_always_retry = CASE.endswith('retry')
app.conf.redis_backend_health_check_interval = None


@app.task
def t(tag, secs):
    r.rpush('bodies', tag)
    time.sleep(secs)
    return tag
'''

BACKEND = 'quint-backend'


def docker(*args):
    subprocess.run(['docker', *args], check=True, capture_output=True)


def supervise(cmd, cwd, env, log, starts, stop):
    n = 0
    while n < 4 and not stop.is_set():
        n += 1
        starts.append(time.time())
        p = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=log,
                             stderr=subprocess.STDOUT)
        while p.poll() is None and not stop.is_set():
            time.sleep(0.2)
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(20)
            except subprocess.TimeoutExpired:
                p.kill()
        time.sleep(1)


def one(case: str, d: str, env: dict) -> str:
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    natural = case.startswith('natural')
    if natural:
        docker('start', BACKEND)
        time.sleep(1)
        redis.Redis(port=6391).flushdb()
    sys.path.insert(0, d)
    import dbeapp
    log = open(os.path.join(d, 'worker.log'), 'a')
    conc = '2' if natural else '1'
    cmd = [sys.executable, '-m', 'celery', '-A', 'dbeapp', 'worker',
           '-P', 'prefork', '-c', conc, '-l', 'info', '--without-gossip',
           '--without-mingle', '--without-heartbeat']
    starts: list = []
    stop = threading.Event()
    r = redis.Redis(db=15)
    if natural:
        x = dbeapp.t.apply_async(('X', 6))
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        b = dbeapp.t.apply_async(('B', 0), expires=past)
        docker('stop', '-t', '1', BACKEND)
        sup = threading.Thread(target=supervise,
                               args=(cmd, d, env, log, starts, stop))
        sup.start()
        time.sleep(15)
        docker('start', BACKEND)
        time.sleep(25)
        ids = {'X': x.id, 'B': b.id}
    else:
        sup = threading.Thread(target=supervise,
                               args=(cmd, d, env, log, starts, stop))
        sup.start()
        time.sleep(4)
        a = dbeapp.t.apply_async(('A', 3))
        b = dbeapp.t.apply_async(('B', 0), expires=1.5)
        c = dbeapp.t.apply_async(('C', 0))
        time.sleep(25)
        ids = {'A': a.id, 'B': b.id, 'C': c.id}
    stop.set()
    sup.join()
    log.close()
    bodies = [x.decode() for x in r.lrange('bodies', 0, -1)]
    states = {k: dbeapp.app.backend.get_task_meta(v, cache=False)['status']
              for k, v in ids.items()}
    text = open(os.path.join(d, 'worker.log')).read()
    sys.path.remove(d)
    del sys.modules['dbeapp']
    return (f'bodies {bodies}, states {states}, worker starts {len(starts)}, '
            f'reconnects {text.count("Connection to broker lost")}, '
            f'unrecoverable {text.count("Unrecoverable error")}, '
            f'restart limit {text.count("Frequent restarts detected")}, '
            f'revoke write errors {r.get("revokeerr")}')


def main() -> int:
    case = sys.argv[1] if len(sys.argv) > 1 else 'inject'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('case', case)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    print('redis-py', redis.__version__)
    for i in range(runs):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, 'dbeapp.py'), 'w') as f:
            f.write(APP % {'case': case})
        env = dict(os.environ, PYTHONPATH=d + os.pathsep
                   + os.environ.get('PYTHONPATH', ''))
        print(f'run {i + 1}: {one(case, d, env)} (log {d}/worker.log)',
              flush=True)
    if case.startswith('natural'):
        docker('start', BACKEND)
    return 0


if __name__ == '__main__':
    sys.exit(main())
