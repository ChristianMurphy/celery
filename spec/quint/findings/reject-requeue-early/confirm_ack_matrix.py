"""Run cells of ackmatrix.qnt on the real code (Redis broker).

Each case is <ack mode>_<ending>[_noackfail]: early or late (acks_late),
an ending (rejectrequeue: Reject(requeue=True) on the first run;
rejectdrop: Reject(requeue=False); ignores: Ignore(); raises), and
noackfail (task_acks_on_failure off). A second run of the same task
returns. The script prints the runs, the stored state and what is left
on the queue. Nothing is injected.

    PYTHONPATH=<celery>:<kombu> python confirm_ack_matrix.py <case> [runs]

One prefork worker -c 1, Redis broker and backend (6379, dbs 10 and 11;
counts in db 15). Run it from a directory outside the checkouts.
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
import os
import redis
from celery import Celery
from celery.exceptions import Ignore, Reject

r = redis.Redis(db=15)
app = Celery('amapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.task_acks_late = os.environ.get('AM_LATE') == '1'
if os.environ.get('AM_NOACKFAIL') == '1':
    app.conf.task_acks_on_failure = False


@app.task
def t(ending):
    n = r.incr('runs')
    if n > 1:
        return 'ok'
    if ending == 'rejectrequeue':
        raise Reject('again', requeue=True)
    if ending == 'rejectdrop':
        raise Reject('drop', requeue=False)
    if ending == 'ignores':
        raise Ignore()
    if ending == 'raises':
        raise ValueError('x')
    return 'ok'
'''


def one(case: str, d: str) -> str:
    mode, ending, *rest = case.split('_')
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    env = dict(os.environ)
    env['PYTHONPATH'] = d + os.pathsep + env.get('PYTHONPATH', '')
    env['AM_LATE'] = '1' if mode == 'late' else '0'
    env['AM_NOACKFAIL'] = '1' if 'noackfail' in rest else '0'
    sys.path.insert(0, d)
    import amapp
    log = open(os.path.join(d, f'worker_{case}.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'amapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d, env=env,
        stdout=log, stderr=subprocess.STDOUT)
    time.sleep(5)
    res = amapp.t.delay(ending)
    time.sleep(6)
    w.terminate()
    w.wait(20)
    log.close()
    r = redis.Redis(db=15)
    b = redis.Redis(db=10)
    state = amapp.app.backend.get_task_meta(res.id, cache=False)['status']
    return (f'{case}: runs {int(r.get("runs") or 0)}, state {state}, '
            f'queued {b.llen("celery")}, unacked {b.hlen("unacked")}')


def main() -> None:
    print('celery', celery.__file__, 'kombu', kombu.__file__)
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    for _ in range(runs):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'amapp.py'), 'w') as f:
                f.write(APP)
            print(one(case, d), flush=True)
            sys.modules.pop('amapp', None)
            sys.path.remove(d)


if __name__ == '__main__':
    main()
