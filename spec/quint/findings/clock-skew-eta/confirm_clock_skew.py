"""Confirm eta.qnt's counterexamples on the real code.

The client turns countdown and a numeric expires into absolute times
with its own clock (amqp.py:386-400, app.now()); the worker compares
them with its own clock (strategy.py:160-209, request.py:415-421). The
skew is injected: the client's app.now returns the real time plus an
offset, as a client host whose clock is off would. The worker's clock is
the real one.

    cd_wahead   client 10 s behind, countdown=5: when does it start?
    exp_wahead  client 10 s behind, expires=5: REVOKED without a run?
    exp_wbehind client 10 s ahead, expires=2, behind a 5 s task on -c 1:
                does it run after its 2 s?
    cd_same, exp_same: the controls, no offset.

One prefork worker -c 1, Redis broker and backend (6379, dbs 10 and 11;
start times in db 15).

    PYTHONPATH=<celery>:<kombu> python confirm_clock_skew.py <case> [runs]

Run it from a directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import time
from datetime import timedelta

import celery
import kombu
import redis

APP = '''
import time
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('skapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')


@app.task
def t(tag, secs=0):
    r.hset('started', tag, repr(time.time()))
    time.sleep(secs)
    return tag
'''

CASES = {
    'cd_same': (0, dict(countdown=5), False),
    'cd_wahead': (-10, dict(countdown=5), False),
    'exp_same': (0, dict(expires=2), True),
    'exp_wahead': (-10, dict(expires=5), False),
    'exp_wbehind': (10, dict(expires=2), True),
}


def one(case: str, d: str) -> str:
    offset, opts, behind = CASES[case]
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    env = dict(os.environ)
    env['PYTHONPATH'] = d + os.pathsep + env.get('PYTHONPATH', '')
    sys.path.insert(0, d)
    import skapp
    app = skapp.app
    real_now = app.now
    log = open(os.path.join(d, f'worker_{case}.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'skapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d, env=env,
        stdout=log, stderr=subprocess.STDOUT)
    time.sleep(5)
    if behind:
        skapp.t.delay('A', 5)
    app.now = lambda: real_now() + timedelta(seconds=offset)
    t0 = time.time()
    res = skapp.t.apply_async(('B',), **opts)
    app.now = real_now
    time.sleep(12)
    w.terminate()
    w.wait(20)
    log.close()
    r = redis.Redis(db=15)
    started = r.hget('started', 'B')
    after = f'{float(started) - t0:.1f} s' if started else 'never'
    state = app.backend.get_task_meta(res.id, cache=False)['status']
    return (f'{case}: client offset {offset} s, {opts}: B started {after} '
            f'after the call, state {state}')


def main() -> None:
    print('celery', celery.__file__, 'kombu', kombu.__file__)
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    for _ in range(runs):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'skapp.py'), 'w') as f:
                f.write(APP)
            print(one(case, d), flush=True)
            sys.modules.pop('skapp', None)
            sys.path.remove(d)


if __name__ == '__main__':
    main()
