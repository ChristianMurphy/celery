"""Confirm redelivery.qnt's redis_late_backenderr counterexamples on the real code.

One acks_late task with task_track_started, a real prefork worker on a
Redis broker (6379) and a Redis result backend whose write of one state
raises redis' ConnectionError (a RedisBackend subclass; the only change),
as a backend that is down at that moment would, and keeps failing for the
on_error write that follows:

  started   the STARTED write before the body (trace.py:553-557)
  success   the SUCCESS write after the body (mark_as_done)

The model says the error is reported as an internal error, on_error tries
to store FAILURE (here it fails too), and the message is acked as a
failure: nothing is stored, nothing is left to run it. With
result_backend_always_retry (mode + "retry", one failure only) the backend
retries and the task ends SUCCESS.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_backend_error_outside_body.py [started|success|startedretry|successretry] [runs]

Run it from a directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import time

import celery
import kombu
import redis

APP = '''
import time
import redis
from celery import Celery
from celery.backends.redis import RedisBackend

MODE = %(mode)r
r = redis.Redis(db=15)
FAILING = 'STARTED' if MODE.startswith('started') else 'SUCCESS'


class DownAtOneWrite(RedisBackend):
    def _store_result(self, task_id, result, state, *args, **kwargs):
        down = r.get('down:' + task_id)
        if state == FAILING and r.set('down:' + task_id, 1, nx=True):
            raise redis.exceptions.ConnectionError('injected: backend down')
        if down and not MODE.endswith('retry'):
            # still down for the FAILURE write of on_error
            raise redis.exceptions.ConnectionError('injected: still down')
        return super()._store_result(task_id, result, state, *args, **kwargs)


app = Celery('beapp', broker='redis://localhost:6379/10',
             backend='beapp:DownAtOneWrite+redis://localhost:6379/11')
app.conf.task_acks_late = True
app.conf.task_track_started = True
app.conf.result_backend_always_retry = MODE.endswith('retry')


@app.task
def t(tag):
    r.rpush('bodies:' + tag, 1)
    return 'ok'
'''


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else 'started'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('mode', mode)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'beapp.py'), 'w') as f:
        f.write(APP % {'mode': mode})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'beapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=open(os.path.join(d, 'w.log'), 'w'),
        stderr=subprocess.STDOUT)
    sys.path.insert(0, d)
    import beapp  # noqa: E402
    time.sleep(4)
    r = redis.Redis(db=15)
    b = redis.Redis(db=10)
    for i in range(runs):
        tag = str(i)
        res = beapp.t.delay(tag)
        time.sleep(4)
        state = redis.Redis(db=11).get(f'celery-task-meta-{res.id}')
        state = state and state.decode().split('"status": "')[1].split('"')[0]
        print(f'run {i + 1}: body ran {r.llen("bodies:" + tag)} times, stored '
              f'{state or "nothing (PENDING)"}, left in the broker '
              f'{b.llen("celery") + b.hlen("unacked")}')
    w.terminate()
    w.wait(30)
    print('worker log', os.path.join(d, 'w.log'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
