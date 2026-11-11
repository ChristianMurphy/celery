"""Confirm retry.qnt's ok_none / fail_none counterexample on the real code.

A task that always calls self.retry(countdown=0, max_retries=1) on a Redis
broker and result backend, two real prefork workers. retry() publishes the
copy (task.py:956-958) before the tracer stores RETRY (handle_retry,
trace.py:191-202). If the copy is delivered to another process, runs, and
stores FAILURE (MaxRetriesExceeded, or the exception passed as exc) before
the first run's RETRY write lands, that RETRY replaces the FAILURE: the
task reads RETRY for ever and get() never returns. The Redis backend only
refuses to replace SUCCESS (base.py:1369-1370).

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_retry_over_failure.py [tasks] [natural|delay]

natural: nothing injected; N tasks at once, two workers -c 4.
delay:   the backend's mark_as_retry sleeps 0.2 s first (a RedisBackend
         subclass; the only change), to show the window deterministically.
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
from celery import Celery
from celery.backends.redis import RedisBackend

MODE = %(mode)r


class SlowRetryWrite(RedisBackend):
    def mark_as_retry(self, *args, **kwargs):
        if MODE == 'delay':
            time.sleep(0.2)
        return super().mark_as_retry(*args, **kwargs)


app = Celery('rfapp', broker='redis://localhost:6379/6',
             backend='rfapp:SlowRetryWrite+redis://localhost:6379/7')
app.conf.task_acks_late = True
app.conf.result_expires = None


@app.task(bind=True)
def flaky(self):
    raise self.retry(countdown=0, max_retries=1, exc=ValueError('still failing'))
'''


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    mode = sys.argv[2] if len(sys.argv) > 2 else 'natural'
    print('mode', mode)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'rfapp.py'), 'w') as f:
        f.write(APP % {'mode': mode})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    for db in (6, 7):
        redis.Redis(db=db).flushdb()
    workers = [subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'rfapp', 'worker', '-P',
         'prefork', '-c', '4', '-n', f'w{i}@%h', '-l', 'warning',
         '--without-gossip', '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for i in range(2)]
    sys.path.insert(0, d)
    import rfapp  # noqa: E402
    time.sleep(4)
    ids = [rfapp.flaky.delay().id for _ in range(n)]
    b = redis.Redis(db=6)
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(2)
        if b.llen('celery') == 0 and b.hlen('unacked') == 0:
            break
    time.sleep(3)
    for w in workers:
        w.terminate()
        w.wait(30)
    states: dict[str, int] = {}
    for i in ids:
        s = rfapp.app.backend.get_task_meta(i, cache=False)['status']
        states[s] = states.get(s, 0) + 1
    print(f'{n} tasks, two workers -c 4, final states: {states}')
    print(f'{states.get("RETRY", 0)} of {n} read RETRY for ever after their '
          'last attempt failed')
    return 0


if __name__ == '__main__':
    sys.exit(main())
