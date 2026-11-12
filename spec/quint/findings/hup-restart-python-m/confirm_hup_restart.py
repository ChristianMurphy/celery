"""Confirm shutdown.qnt's hup_pym.restartResumes on the real code.

A prefork worker -c 1 (no controlling terminal, so the SIGHUP handler is
installed) runs a 4 s task A; 1 s in, the script sends SIGHUP. 8 s later
it sends task B and waits 6 s. It prints whether the worker process is
still alive, the states of A and B, what is left in the queue, and the
worker's log lines about the restart. Nothing is injected.

    pym     the worker started as `python -m celery ...`   the finding
    script  the worker started by the `celery` console script next to
            sys.executable                                 control

Redis broker and backend on 6379 (dbs 13 and 14).

    PYTHONPATH=<celery>:<kombu> python confirm_hup_restart.py <case> [runs]

Run it from a directory outside the checkouts. The control needs a
`celery` console script in the same bin directory as the interpreter.
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
import time
from celery import Celery
app = Celery('hrapp', broker='redis://localhost:6379/13',
             backend='redis://localhost:6379/14')
app.conf.worker_hijack_root_logger = False
app.conf.task_acks_late = True

@app.task(name='hr.slow')
def slow(n):
    time.sleep(n)
    return n
'''


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='hr_')
    open(os.path.join(d, 'hrapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import hrapp
    broker = redis.Redis(db=13)
    cmd = ([sys.executable, '-m', 'celery'] if case == 'pym' else
           [os.path.join(os.path.dirname(sys.executable), 'celery')])
    for i in range(runs):
        for db in (13, 14):
            redis.Redis(db=db).flushdb()
        path = os.path.join(d, f'worker{i}.log')
        log = open(path, 'w')
        w = subprocess.Popen(cmd + ['-A', 'hrapp', 'worker', '-c', '1', '-l',
                                    'info', '--without-gossip',
                                    '--without-mingle'],
                             cwd=d, stdout=log, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True)
        time.sleep(4)
        a = hrapp.slow.delay(4)
        time.sleep(1)
        w.send_signal(signal.SIGHUP)
        time.sleep(8)
        b = hrapp.slow.delay(1)
        time.sleep(6)
        alive = w.poll() is None
        sa = hrapp.app.backend.get_task_meta(a.id, cache=False)['status']
        sb = hrapp.app.backend.get_task_meta(b.id, cache=False)['status']
        lines = [l.strip()[:110] for l in open(path).read().splitlines()
                 if 'Restarting' in l or 'ImportError' in l or ' ready.' in l]
        print(f'{case} run {i + 1}: worker alive {alive}; A {sa}; B {sb}; '
              f'queued {broker.llen("celery")}; log {lines}')
        if alive:
            os.killpg(w.pid, signal.SIGTERM)
            w.wait(30)
    sys.stdout.flush()
    os._exit(0)


if __name__ == '__main__':
    main()
