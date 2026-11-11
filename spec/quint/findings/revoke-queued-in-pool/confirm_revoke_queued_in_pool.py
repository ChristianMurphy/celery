"""Confirm revoke.qnt's noRunAfterRevokeKnown counterexample on the real code.

The model: a revoke the worker has applied (the id is in its revoked
set) does not stop a run whose request already passed
execute_using_pool's check (request.py:1003-1006) and waits in the pool
for a free process: nothing checks the revoked set when the pool starts
it (on_accepted, request.py:524-533). With prefork the semaphore keeps
that wait short (worker.py:288-289, components.py:147-152); the threads,
gevent and eventlet pools have no semaphore (base.py:68), so every
prefetched request is handed to the pool at once and waits there.

One worker on a Redis broker and backend (6379), acks_late, task A runs
4 s, task B is published behind it; 1 s later app.control.revoke(B)
(no terminate). Nothing is injected.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_revoke_queued_in_pool.py <pool> [runs]

pool: threads, gevent, eventlet, prefork, solo; with ":expire" B carries
expires=2 (it passes while B waits behind A) and nothing is revoked. Run it from a directory
outside the checkouts.
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

r = redis.Redis(db=15)
app = Celery('rqapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.task_acks_late = True


@app.task
def t(tag, secs):
    r.rpush('bodies', tag)
    time.sleep(secs)
    return tag
'''


def one(pool: str, d: str, env: dict) -> str:
    pool, _, mode = pool.partition(':')
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    sys.path.insert(0, d)
    import rqapp
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'rqapp', 'worker', '-P', pool,
         '-c', '1', '-l', 'info', '--without-gossip', '--without-mingle',
         '--without-heartbeat'], cwd=d, env=env, stdout=log,
        stderr=subprocess.STDOUT)
    time.sleep(5)
    a = rqapp.t.apply_async(('A', 4))
    if mode == 'expire':
        b = rqapp.t.apply_async(('B', 0), expires=2)
        time.sleep(1)
    else:
        b = rqapp.t.apply_async(('B', 0))
        time.sleep(1)
        rqapp.app.control.revoke(b.id)
    time.sleep(8)
    w.terminate()
    w.wait(20)
    log.close()
    r = redis.Redis(db=15)
    bodies = [x.decode() for x in r.lrange('bodies', 0, -1)]
    text = open(os.path.join(d, 'worker.log')).read()
    states = {k: rqapp.app.backend.get_task_meta(v.id, cache=False)['status']
              for k, v in (('A', a), ('B', b))}
    sys.path.remove(d)
    del sys.modules['rqapp']
    return (f'bodies {bodies}, states {states}, '
            f'flagged {"Tasks flagged as revoked" in text}, '
            f'discarded {"Discarding revoked task" in text}')


def main() -> int:
    pool = sys.argv[1] if len(sys.argv) > 1 else 'threads'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('pool', pool)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    for i in range(runs):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, 'rqapp.py'), 'w') as f:
            f.write(APP)
        env = dict(os.environ, PYTHONPATH=d + os.pathsep
                   + os.environ.get('PYTHONPATH', ''))
        print(f'run {i + 1}: {one(pool, d, env)}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
