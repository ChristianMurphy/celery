"""Confirm events.qnt's counterexamples on the real code.

A worker started with -E and a monitor that feeds every event into
app.events.State, as Flower does. Each case runs one task, waits, and
prints the events the monitor received for it, the state events.State
shows, and what the result backend stores. Nothing is injected.

    ignore        the task raises Ignore
    updateignore  update_state(state=SUCCESS, meta=...) then raise Ignore,
                  the manual result of tasks.rst
    returns       the control: the task returns
    hard          time_limit=1 and a 5 s sleep (two task-failed?)

One prefork worker -c 2, Redis broker and backend (6379, dbs 10 and 11).

    PYTHONPATH=<celery>:<kombu> python confirm_ignore_events.py <case> [runs]

Run it from a directory outside the checkouts.
"""
import collections
import os
import subprocess
import sys
import tempfile
import threading
import time

import celery
import kombu
import redis

APP = '''
import time
from celery import Celery, states
from celery.exceptions import Ignore

app = Celery('igapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.worker_hijack_root_logger = False

@app.task(name='ig.ignore')
def ignore():
    raise Ignore()

@app.task(name='ig.updateignore', bind=True)
def updateignore(self):
    self.update_state(state=states.SUCCESS, meta={'by': 'hand'})
    raise Ignore()

@app.task(name='ig.returns')
def returns():
    return 1

@app.task(name='ig.hard', time_limit=1)
def hard():
    time.sleep(5)
'''


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='ig_')
    open(os.path.join(d, 'igapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import igapp
    app = igapp.app
    for db in (10, 11):
        redis.Redis(db=db).flushdb()
    state = app.events.State()
    seen = collections.defaultdict(list)

    def on(ev):
        state.event(ev)
        if 'uuid' in ev:
            seen[ev['uuid']].append(ev['type'])

    def listen():
        with app.connection() as c:
            app.events.Receiver(c, handlers={'*': on}).capture(
                limit=None, timeout=None, wakeup=True)

    threading.Thread(target=listen, daemon=True).start()
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'igapp',
                          'worker', '-c', '2', '-E', '-l', 'info',
                          '--without-gossip', '--without-mingle'], cwd=d,
                         stdout=log, stderr=subprocess.STDOUT)
    try:
        time.sleep(5)
        task = {'ignore': igapp.ignore, 'updateignore': igapp.updateignore,
                'returns': igapp.returns, 'hard': igapp.hard}[case]
        for i in range(runs):
            tid = task.delay().id
            time.sleep(8 if case == 'hard' else 4)
            t = state.tasks.get(tid)
            stored = app.backend.get_task_meta(tid, cache=False)['status']
            print(f'{case} run {i + 1}: events {seen[tid]}; events.State '
                  f'{t.state if t else None}; backend {stored}')
    finally:
        w.terminate()
        w.wait()


if __name__ == '__main__':
    main()
