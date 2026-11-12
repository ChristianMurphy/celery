"""Confirm handlers.qnt's hooksOnce, failedIsFinal and failureCallsHandler
counterexamples on the real code.

hard (hard_default.hooksOnce): on a hard time limit billiard calls the
error callback, Request.on_failure with TimeLimitExceeded, and then the
timeout callback, Request.on_timeout (billiard pool.py,
TimeoutHandler.on_hard_timeout: job._set, then job.handle_timeout). Both
call mark_as_failure, which calls the link_error errbacks, and both send
task_failure (request.py:539-614, 745-796).

noackto (hard_noackto.failedIsFinal): with task_acks_on_timeout off the
error callback requeues the message and stores nothing, and the timeout
callback still stores FAILURE, calls the errback, on_failure and
task_failure. The task runs again; here the second run returns.

lost (lost_default.failureCallsHandler): the pool process is lost
(the task SIGKILLs its own process, as an OOM kill would): FAILURE is
stored, the errback runs and task_failure is sent, Task.on_failure and
after_return are not called.

raise: the control, a task that raises.

One prefork worker -c 1, acks_late, Redis broker and backend (6379, dbs
10 and 11; counts in db 15). Nothing is injected in hard, noackto and
raise; in lost the kill is the fault.

    PYTHONPATH=<celery>:<kombu> python confirm_hard_limit.py <case> [runs]

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
import os
import signal
import time
import redis
from celery import Celery, signals

r = redis.Redis(db=15)
app = Celery('hlapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.task_acks_late = True
if os.environ.get('HL_ACKS_ON_TIMEOUT') == '0':
    app.conf.task_acks_on_timeout = False


class Counted(app.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        r.rpush('on_failure', type(exc).__name__)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        r.rpush('after_return', status)


@signals.task_failure.connect
def _failed(sender=None, task_id=None, exception=None, **kw):
    r.rpush('task_failure', type(exception).__name__)


@app.task(base=Counted, time_limit=1)
def hard():
    n = r.incr('runs')
    if n == 1:
        time.sleep(5)
    return 'ok'


@app.task(base=Counted)
def lost():
    r.incr('runs')
    os.kill(os.getpid(), signal.SIGKILL)


@app.task(base=Counted)
def fails():
    r.incr('runs')
    raise ValueError('x')


@app.task
def errback(*args, **kwargs):
    r.incr('errback')
'''


def one(case: str, d: str) -> str:
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    env = dict(os.environ)
    env['PYTHONPATH'] = d + os.pathsep + env.get('PYTHONPATH', '')
    if case == 'noackto':
        env['HL_ACKS_ON_TIMEOUT'] = '0'
    sys.path.insert(0, d)
    import hlapp
    log = open(os.path.join(d, f'worker_{case}.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'hlapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d, env=env,
        stdout=log, stderr=subprocess.STDOUT)
    time.sleep(5)
    task = {'hard': hlapp.hard, 'noackto': hlapp.hard, 'lost': hlapp.lost,
            'raise': hlapp.fails}[case]
    res = task.apply_async(link_error=hlapp.errback.si())
    time.sleep(9)
    w.terminate()
    w.wait(20)
    log.close()
    r = redis.Redis(db=15)
    b = redis.Redis(db=10)
    state = hlapp.app.backend.get_task_meta(res.id, cache=False)['status']
    lst = lambda k: [x.decode() for x in r.lrange(k, 0, -1)]
    return (f'{case}: runs {int(r.get("runs") or 0)}, '
            f'errback {int(r.get("errback") or 0)}, '
            f'task_failure {len(lst("task_failure"))}, '
            f'on_failure {len(lst("on_failure"))}, '
            f'after_return {lst("after_return")}, state {state}, '
            f'queued {b.llen("celery")}, unacked {b.hlen("unacked")}')


def main() -> None:
    print('celery', celery.__file__, 'kombu', kombu.__file__)
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    for _ in range(runs):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'hlapp.py'), 'w') as f:
                f.write(APP)
            print(one(case, d), flush=True)
            sys.modules.pop('hlapp', None)
            sys.path.remove(d)


if __name__ == '__main__':
    main()
