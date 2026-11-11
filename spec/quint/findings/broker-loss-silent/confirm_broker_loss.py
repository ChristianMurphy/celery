"""Confirm redelivery.qnt's redis_late_loss counterexample on the real code.

A Redis broker without persistence (--save "" --appendonly no, in its own
container `quint-redis-nopersist` on 6390) restarts after apply_async()
returned and before a worker took the task: the write Redis acknowledged
is gone. A real worker then starts. The model says nobody is told: the
task never runs, its state stays PENDING, and nothing logs the loss
(documented in redis.rst:268-273). The result backend is the separate
Redis on 6379, so the state read is not lost with the broker.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_broker_loss.py [runs]

Injected: the restart of the broker (docker restart). Run it from a
directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import time

import celery
import kombu
import redis

NAME = 'quint-redis-nopersist'
APP = '''
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('lossapp', broker='redis://localhost:6390/0',
             backend='redis://localhost:6379/14')
app.conf.task_acks_late = True


@app.task
def t():
    r.incr('ran')
'''


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    if subprocess.run(['docker', 'start', NAME], capture_output=True).returncode:
        subprocess.run(['docker', 'run', '-d', '--name', NAME, '-p', '6390:6379',
                        'mirror.gcr.io/library/redis:7.4', 'redis-server',
                        '--save', '', '--appendonly', 'no'], check=True,
                       capture_output=True)
    time.sleep(1)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'lossapp.py'), 'w') as f:
        f.write(APP)
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    sys.path.insert(0, d)
    import lossapp  # noqa: E402
    silent = 0
    for i in range(runs):
        redis.Redis(db=15).delete('ran')
        res = lossapp.t.delay()
        queued = redis.Redis(port=6390).llen('celery')
        subprocess.run(['docker', 'restart', NAME], check=True,
                       capture_output=True)
        time.sleep(1)
        log = os.path.join(d, f'worker{i}.log')
        w = subprocess.Popen(
            [sys.executable, '-m', 'celery', '-A', 'lossapp', 'worker', '-P',
             'prefork', '-c', '1', '-l', 'info', '--without-gossip',
             '--without-mingle', '--without-heartbeat'],
            cwd=d, env=env, stdout=open(log, 'w'), stderr=subprocess.STDOUT)
        time.sleep(8)
        w.terminate()
        w.wait(30)
        ran = int(redis.Redis(db=15).get('ran') or 0)
        state = res.backend.get_task_meta(res.id, cache=False)['status']
        text = open(log).read()
        warned = sum(text.count(w) for w in ('ERROR', 'CRITICAL', 'lost'))
        silent += ran == 0 and state == 'PENDING' and warned == 0
        print(f'run {i + 1}: queued after publish {queued}; after the broker '
              f'restart the task ran {ran} times, state {state}, errors or '
              f'warnings about it in the worker log {warned}')
    subprocess.run(['docker', 'stop', NAME], capture_output=True)
    print(f'{silent} of {runs} runs lost the task with nothing logged')
    return 0


if __name__ == '__main__':
    sys.exit(main())
