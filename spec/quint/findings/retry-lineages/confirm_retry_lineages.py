"""Confirm retry.qnt's fail_node counterexample on the real code.

An acks_late task that always calls self.retry(countdown=0, max_retries=1),
a Redis broker (visibility_timeout 2 s) and result backend, real prefork
workers. The worker is killed with SIGKILL (worker and pool) after retry()
published the copy and before the original message was acked: here, when
the first RETRY is written (a RedisBackend subclass kills its own worker in
mark_as_retry, once; the only change). The original comes back after the
visibility timeout and retries again, so two lineages run: with
max_retries 1 the body runs 4 times instead of 2.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_retry_lineages.py [runs]

Injected: the kill, at that point. Run it from a directory outside the
checkouts.
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

APP = '''
import os
import signal
import redis
from celery import Celery
from celery.backends.redis import RedisBackend

r = redis.Redis(db=15)


class KillAtFirstRetry(RedisBackend):
    def mark_as_retry(self, task_id, *args, **kwargs):
        if r.set('killed:' + task_id, 1, nx=True):
            os.killpg(os.getpgid(0), signal.SIGKILL)
        return super().mark_as_retry(task_id, *args, **kwargs)


app = Celery('rlapp', broker='redis://localhost:6379/8',
             backend='rlapp:KillAtFirstRetry+redis://localhost:6379/9')
app.conf.broker_transport_options = {
    'visibility_timeout': 2, 'unacked_restore_interval': 1,
    'unacked_restore_throttle': 1,
}
app.conf.task_acks_late = True


@app.task(bind=True)
def flaky(self, tag):
    r.rpush('bodies:' + tag, self.request.retries)
    raise self.retry(countdown=0, max_retries=1, exc=ValueError('still failing'))
'''


def worker(d: str, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'rlapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'warning', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True)


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'rlapp.py'), 'w') as f:
        f.write(APP)
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    sys.path.insert(0, d)
    import rlapp  # noqa: E402
    r = redis.Redis(db=15)
    over = 0
    for i in range(runs):
        for db in (8, 9, 15):
            redis.Redis(db=db).flushdb()
        w = worker(d, env)
        time.sleep(3)
        tag = str(i)
        res = rlapp.flaky.delay(tag)
        time.sleep(3)
        if w.poll() is not None:        # the supervisor
            w = worker(d, env)
        time.sleep(10)
        os.killpg(w.pid, signal.SIGTERM)
        w.wait(30)
        bodies = [int(x) for x in r.lrange('bodies:' + tag, 0, -1)]
        state = res.backend.get_task_meta(res.id, cache=False)['status']
        over += len(bodies) > 2
        print(f'run {i + 1}: max_retries 1; the body ran {len(bodies)} times '
              f'(retries values {bodies}); state {state}')
    print(f'{over} of {runs} runs ran the body more than max_retries + 1 times')
    return 0


if __name__ == '__main__':
    sys.exit(main())
