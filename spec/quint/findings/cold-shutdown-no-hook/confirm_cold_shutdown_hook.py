"""Confirm shutdown.qnt's quit.hookOnShutdown on the real code.

A prefork worker -c 1 with acks_late runs a 4 s task; 1 s in, the script
sends the case's signals (1 s apart) and waits for the worker to exit. It
prints the exit code, whether worker_shutting_down and worker_shutdown
were dispatched (the handlers write to Redis), the task's state and what
is left in the queue. Nothing is injected.

    quit      SIGQUIT (a cold shutdown)                 the finding
    term      SIGTERM (a warm shutdown)                 control
    termquit  SIGTERM, then SIGQUIT (cold during warm)  control
    int2      SIGINT twice (warm, then cold)            control

Redis broker and backend on 6379 (dbs 13 and 14; db 15 holds the events).

    PYTHONPATH=<celery>:<kombu> python confirm_cold_shutdown_hook.py <case> [runs]

Run it from a directory outside the checkouts.
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
import os, time
from celery import Celery, signals
import redis
app = Celery('csapp', broker='redis://localhost:6379/13',
             backend='redis://localhost:6379/14')
app.conf.worker_hijack_root_logger = False
app.conf.task_acks_late = True
R = redis.Redis(db=15)

@app.task(name='cs.slow')
def slow(n=4):
    R.rpush('ev', 'task-start')
    time.sleep(n)
    R.rpush('ev', 'task-end')
    return n

@signals.worker_shutting_down.connect
def on_shutting_down(sig=None, how=None, **kw):
    R.rpush('ev', f'worker_shutting_down {sig} {how}')

@signals.worker_shutdown.connect
def on_shutdown(**kw):
    R.rpush('ev', 'worker_shutdown')
'''

SIGS = {'quit': [signal.SIGQUIT], 'term': [signal.SIGTERM],
        'termquit': [signal.SIGTERM, signal.SIGQUIT],
        'int2': [signal.SIGINT, signal.SIGINT]}


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='cs_')
    open(os.path.join(d, 'csapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import csapp
    R = redis.Redis(db=15)
    broker = redis.Redis(db=13)
    for i in range(runs):
        for db in (13, 14, 15):
            redis.Redis(db=db).flushdb()
        log = open(os.path.join(d, f'worker{i}.log'), 'w')
        w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'csapp',
                              'worker', '-c', '1', '-l', 'info',
                              '--without-gossip', '--without-mingle'],
                             cwd=d, stdout=log, stderr=subprocess.STDOUT)
        time.sleep(4)
        r = csapp.slow.delay(4)
        time.sleep(1)
        for s in SIGS[case]:
            w.send_signal(s)
            time.sleep(1)
        try:
            code = w.wait(30)
        except subprocess.TimeoutExpired:
            w.kill()
            code = 'still running after 30 s'
        ev = [x.decode() for x in R.lrange('ev', 0, -1)]
        st = csapp.app.backend.get_task_meta(r.id, cache=False)['status']
        print(f'{case} run {i + 1}: exit code {code}; worker_shutdown '
              f'{"dispatched" if "worker_shutdown" in ev else "NOT dispatched"}; '
              f'task {st}; queued {broker.llen("celery")}; events {ev}')
    sys.stdout.flush()
    os._exit(0)


if __name__ == '__main__':
    main()
