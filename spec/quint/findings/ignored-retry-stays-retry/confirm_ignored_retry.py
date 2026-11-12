"""Confirm housekeeping.qnt's counterexamples on the real code.

A task with ignore_result=True retries once (self.retry(countdown=0)) and
then returns. Nothing is injected.

    storeerr      store_errors_even_if_ignored=True: what is stored at the
                  end, and does get(timeout=5) return, on the caller's
                  AsyncResult (marked ignored) and on a fresh one?
    noerr         the control: store_errors_even_if_ignored off
    plain         the control: the result is not ignored
    forget        a 2 s task (result kept) forgotten 0.7 s in: what is
                  stored at the end?
    groupforget   a group of two tasks, saved, then GroupResult.forget(),
                  then GroupResult.restore(): ready()?

One prefork worker -c 2, Redis broker and backend (6379, dbs 10 and 11;
run counts in db 15).

    PYTHONPATH=<celery>:<kombu> python confirm_ignored_retry.py <case> [runs]

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
import time
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('irapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.worker_hijack_root_logger = False

def body(self):
    r.rpush('ir:runs:' + self.request.id, self.request.retries)
    if self.request.retries == 0:
        raise self.retry(countdown=0, max_retries=2)
    return 'ok'

storeerr = app.task(name='ir.storeerr', bind=True, ignore_result=True,
                    store_errors_even_if_ignored=True)(body)
noerr = app.task(name='ir.noerr', bind=True, ignore_result=True)(body)
plain = app.task(name='ir.plain', bind=True)(body)

@app.task(name='ir.slow')
def slow():
    time.sleep(2)
    return 'done'

@app.task(name='ir.add')
def add(a, b):
    return a + b
'''


def one(case, irapp):
    b = irapp.app.backend
    if case in ('storeerr', 'noerr', 'plain'):
        res = getattr(irapp, case).delay()
        time.sleep(3)
        runs = [int(x) for x in redis.Redis(db=15).lrange('ir:runs:' + res.id, 0, -1)]
        state = b.get_task_meta(res.id, cache=False)['status']
        try:
            got = res.get(timeout=5)
        except Exception as e:
            got = type(e).__name__
        # another client (a status page, Flower, a fresh AsyncResult)
        try:
            other = irapp.app.AsyncResult(res.id).get(timeout=5)
        except Exception as e:
            other = type(e).__name__
        return (f'runs (retries) {runs}; stored {state}; get(timeout=5): '
                f'{got}; a fresh AsyncResult(id).get(timeout=5): {other}')
    if case == 'forget':
        res = irapp.slow.delay()
        time.sleep(0.7)
        res.forget()
        after = b.get_task_meta(res.id, cache=False)['status']
        time.sleep(2.5)
        end = b.get_task_meta(res.id, cache=False)['status']
        return f'after forget {after}; at the end {end}'
    if case == 'groupforget':
        from celery import group
        from celery.result import GroupResult
        g = group(irapp.add.s(1, 1), irapp.add.s(2, 2)).apply_async()
        g.get(timeout=5)
        g.save()
        g.forget()
        back = GroupResult.restore(g.id, app=irapp.app)
        if back is None:
            return 'restore: None'
        try:
            got = back.get(timeout=3)
        except Exception as e:
            got = type(e).__name__
        return f'restore: a group, ready() {back.ready()}; get(timeout=3): {got}'
    raise SystemExit(f'unknown case {case}')


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='ir_')
    open(os.path.join(d, 'irapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import irapp
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'irapp',
                          'worker', '-c', '2', '-l', 'info',
                          '--without-heartbeat', '--without-gossip',
                          '--without-mingle'], cwd=d, stdout=log,
                         stderr=subprocess.STDOUT)
    try:
        time.sleep(5)
        for i in range(runs):
            print(f'{case} run {i + 1}: {one(case, irapp)}')
    finally:
        w.terminate()
        w.wait()


if __name__ == '__main__':
    main()
