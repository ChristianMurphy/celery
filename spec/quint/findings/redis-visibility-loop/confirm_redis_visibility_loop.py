"""Confirm redelivery.qnt's Redis loop on the real code.

One acks_late task that runs longer than the visibility timeout, a real
prefork worker with one process on a real Redis broker (6379) and result
backend. The model says the task runs again and again when no consumer has
prefetch credit for the restored copy before the first run acks: with a
prefetch count of 1 (worker_prefetch_multiplier 1, the setting
docs/userguide/optimizing.rst recommends with acks_late) and with
worker_disable_prefetch. The first run's ack then removes no record (the
restore already did), the copy is delivered after it, gets a record of its
own, and that record times out in turn. With a prefetch count of 2 the
copy is delivered while the first run goes on, the first run's ack removes
its record (the same delivery tag), and the task runs twice; with
worker_deduplicate_successful_tasks the copy is skipped.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_redis_visibility_loop.py [seconds] [case ...]

Cases: p1 (prefetch multiplier 1), nopf (worker_disable_prefetch with the
default multiplier), p2 (multiplier 2), dedup (p1 with
worker_deduplicate_successful_tasks), short (p1, a task shorter than the
timeout). The visibility timeout is 2 s and the task sleeps 5 s; the
sweep runs every second (unacked_restore_interval 1, throttle 1). Nothing
is injected: the scaled timeout is the only change from the defaults.
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

BROKER = 'redis://localhost:6379/11'
BACKEND = 'redis://localhost:6379/12'
LOG_DB = 13

APP = '''
import time
import redis
from celery import Celery

r = redis.Redis(db=%(log)d)
app = Celery('loopapp', broker=%(broker)r, backend=%(backend)r)
app.conf.broker_transport_options = {
    'visibility_timeout': 2,
    'unacked_restore_interval': 1,
    'unacked_restore_throttle': 1,
}
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = %(mult)d
app.conf.worker_disable_prefetch = %(nopf)r
app.conf.worker_deduplicate_successful_tasks = %(dedup)r


@app.task
def slow(secs):
    r.rpush('runs', '%%.1f' %% time.time())
    time.sleep(secs)
    return 'done'
'''

CASES = {
    # name: (multiplier, disable_prefetch, dedup, task seconds)
    'p1': (1, False, False, 5),
    'nopf': (4, True, False, 5),
    'p2': (2, False, False, 5),
    'dedup': (1, False, True, 5),
    'short': (1, False, False, 1),
}


def run(name: str, secs: float) -> tuple[int, int]:
    mult, nopf, dedup, task_secs = CASES[name]
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'loopapp.py'), 'w') as f:
        f.write(APP % {'broker': BROKER, 'backend': BACKEND, 'log': LOG_DB,
                       'mult': mult, 'nopf': nopf, 'dedup': dedup})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    for db in (11, 12, LOG_DB):
        redis.Redis(db=db).flushdb()
    log = open(os.path.join(d, 'worker.log'), 'w')
    worker = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'loopapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=log, stderr=subprocess.STDOUT)
    sys.path.insert(0, d)
    sys.modules.pop('loopapp', None)
    import loopapp  # noqa: E402
    time.sleep(3)
    t0 = time.time()
    res = loopapp.slow.delay(task_secs)
    try:
        time.sleep(secs)
    finally:
        worker.terminate()
        worker.wait(30)
        sys.path.remove(d)
    runs = [float(x) - t0 for x in redis.Redis(db=LOG_DB).lrange('runs', 0, -1)]
    left = redis.Redis(db=11).hlen('unacked') + redis.Redis(db=11).llen('celery')
    state = res.backend.get_task_meta(res.id, cache=False)['status']
    skipped = open(os.path.join(d, 'worker.log')).read().count(
        'Task already completed successfully')
    print(f'{name}: multiplier {mult}, disable_prefetch {nopf}, dedup {dedup}, '
          f'task {task_secs} s, timeout 2 s, watched {secs:.0f} s: '
          f'{len(runs)} runs (starts at {", ".join(f"{t:.1f}" for t in runs)} s), '
          f'skipped as done {skipped}, state {state}, left in the broker {left}')
    return len(runs), left


def main() -> int:
    secs = float(sys.argv[1]) if len(sys.argv) > 1 else 40
    cases = sys.argv[2:] or list(CASES)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    for name in cases:
        run(name, secs)
    return 0


if __name__ == '__main__':
    sys.exit(main())
