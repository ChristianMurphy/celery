"""Confirm redelivery.qnt's redis_dedup_readerr counterexample on the real code.

worker_deduplicate_successful_tasks on, acks_late, a Redis broker (6379,
visibility_timeout 2 s, sweep every second) and a Redis result backend in
its own container (`quint-backend`, 6391). Worker A starts the task (3 s)
and is killed with SIGKILL before the task ends, so nothing is stored. The
message is restored, marked redelivered, and worker B takes it. When B's
deduplication check reads the stored state, the read fails:

  down      the backend container is stopped while B takes the message, and
            started again 8 s later (a backend outage at the wrong moment);
  readonce  the backend is up, and a RedisBackend subclass raises redis'
            ConnectionError on the first read of the task's meta in B
            (_get_task_meta_for, under the backend's retry policy; the only
            change).

The model says the backend's error leaves the check (only
BackendGetMetaError, raised with result_backend_always_retry, is let
through), the tracer reports an internal error, on_error stores FAILURE
(if the backend takes the write) and the message is acked as a failure:
the task never runs again. With `retry` appended to the mode,
result_backend_always_retry is set: the check lets the error pass and the
task runs.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_dedup_read_error.py [down|readonce|downretry|readonceretry] [runs]

Injected: SIGKILL of worker A, and the outage or the failing read. Run it
from a directory outside the checkouts.
"""
import os
import signal
import subprocess
import sys
import tempfile
import time

import celery
import kombu
import redis

BACKEND = 'redis://localhost:6391/0'
NAME = 'quint-backend'

APP = '''
import time
import redis
from celery import Celery
from celery.backends.redis import RedisBackend

MODE = %(mode)r
r = redis.Redis(db=15)


class FailFirstRead(RedisBackend):
    def _get_task_meta_for(self, task_id):
        if MODE.startswith('readonce') and r.set('failed:' + task_id, 1, nx=True):
            raise redis.exceptions.ConnectionError('injected: first read fails')
        return super()._get_task_meta_for(task_id)


app = Celery('ddapp', broker='redis://localhost:6379/3',
             backend='ddapp:FailFirstRead+' + %(backend)r)
app.conf.broker_transport_options = {
    'visibility_timeout': 2, 'unacked_restore_interval': 1,
    'unacked_restore_throttle': 1,
}
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.worker_deduplicate_successful_tasks = True
app.conf.result_backend_always_retry = MODE.endswith('retry')
app.conf.result_backend_max_retries = 3


@app.task
def t():
    r.rpush('runs', '%%.1f' %% time.time())
    time.sleep(3)
    r.rpush('done', '%%.1f' %% time.time())
    return 'ok'
'''


def docker(*args: str) -> None:
    subprocess.run(['docker', *args], capture_output=True)


def worker(d: str, env: dict, log: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'ddapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=open(log, 'w'), stderr=subprocess.STDOUT,
        start_new_session=True)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else 'down'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('mode', mode)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    docker('start', NAME)
    if subprocess.run(['docker', 'inspect', NAME], capture_output=True).returncode:
        docker('run', '-d', '--name', NAME, '-p', '6391:6379',
               'mirror.gcr.io/library/redis:7.4', 'redis-server', '--save', '')
    time.sleep(1)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'ddapp.py'), 'w') as f:
        f.write(APP % {'mode': mode, 'backend': BACKEND})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    sys.path.insert(0, d)
    import ddapp  # noqa: E402
    r = redis.Redis(db=15)
    bad = 0
    for i in range(runs):
        redis.Redis(db=3).flushdb()
        r.flushdb()
        redis.Redis(port=6391).flushdb()
        a = worker(d, env, os.path.join(d, f'a{i}.log'))
        time.sleep(3)
        res = ddapp.t.delay()
        while not r.llen('runs'):
            time.sleep(0.05)
        time.sleep(1)
        os.killpg(a.pid, signal.SIGKILL)       # worker A and its pool
        a.wait()
        if mode.startswith('down'):
            docker('stop', '-t', '0', NAME)
        blog = os.path.join(d, f'b{i}.log')
        b = worker(d, env, blog)
        time.sleep(8)
        if mode.startswith('down'):
            docker('start', NAME)
            time.sleep(2)
        time.sleep(8)
        os.killpg(b.pid, signal.SIGTERM)
        b.wait(30)
        started = r.llen('runs')
        finished = r.llen('done')
        state = ddapp.app.backend.get_task_meta(res.id, cache=False)['status']
        broker = redis.Redis(db=3)
        left = broker.llen('celery') + broker.hlen('unacked')
        text = open(blog).read()
        internal = text.count('internal error') + text.count('Traceback')
        lost = finished == 0 and left == 0
        bad += lost
        print(f'run {i + 1}: started {started}, finished {finished}, state '
              f'{state}, left in the broker {left}, worker B log: '
              f'{internal} internal-error/traceback lines; never finished '
              f'and nothing left to run it: {lost}')
    docker('start', NAME)
    print(f'{bad} of {runs} runs: the task never ran to the end and the broker '
          'holds nothing; worker B logs are in', d)
    return 0


if __name__ == '__main__':
    sys.exit(main())
