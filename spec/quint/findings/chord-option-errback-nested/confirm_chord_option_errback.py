"""Confirm nested.qnt's nested_fail_option.errbackOnFailure on the real code.

outer = chord([chord([add(1), boom()], tsum), add(10)], tsum). The inner
chord's header fails. Each case gives the outer chord an errback eb and
prints, after 6 s, the outer body's stored state and the calls the tasks
recorded. Nothing is injected.

    option   outer.apply_async(link_error=eb.si())      (the finding)
    onerror  outer.on_error(eb.si()).apply_async()      (control)
    flat     chord([add(1), boom()], tsum).apply_async(link_error=eb.si())
             (control: no nested chord)

One prefork worker -c 2, Redis broker and backend (6379, dbs 13 and 14;
db 15 holds the calls).

    PYTHONPATH=<celery>:<kombu> python confirm_chord_option_errback.py <case> [runs]

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
from celery import Celery
import redis
app = Celery('coeapp', broker='redis://localhost:6379/13',
             backend='redis://localhost:6379/14')
app.conf.worker_hijack_root_logger = False
R = redis.Redis(db=15)

@app.task(name='coe.add')
def add(x, y=0):
    R.rpush('calls', 'add')
    return x + y

@app.task(name='coe.tsum')
def tsum(xs):
    R.rpush('calls', 'tsum')
    return sum(xs)

@app.task(name='coe.boom')
def boom(*a):
    R.rpush('calls', 'boom')
    raise ValueError('boom')

@app.task(name='coe.eb')
def eb(*a, **k):
    R.rpush('calls', 'eb')
'''


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='coe_')
    open(os.path.join(d, 'coeapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import coeapp as m
    from celery import chord
    R = redis.Redis(db=15)
    for db in (13, 14, 15):
        redis.Redis(db=db).flushdb()
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'coeapp',
                          'worker', '-c', '2', '-l', 'info',
                          '--without-gossip', '--without-mingle'], cwd=d,
                         stdout=log, stderr=subprocess.STDOUT)
    try:
        time.sleep(5)
        for i in range(runs):
            R.delete('calls')
            inner = chord([m.add.s(1), m.boom.s()], m.tsum.s())
            try:
                if case == 'option':
                    r = chord([inner, m.add.s(10)], m.tsum.s()).apply_async(
                        link_error=m.eb.si())
                elif case == 'onerror':
                    r = chord([inner, m.add.s(10)], m.tsum.s()).on_error(
                        m.eb.si()).apply_async()
                else:
                    r = chord([m.add.s(1), m.boom.s()], m.tsum.s()).apply_async(
                        link_error=m.eb.si())
            except Exception as e:  # noqa: BLE001
                print(f'{case} run {i + 1}: the call raised '
                      f'{type(e).__name__}: {e}')
                continue
            time.sleep(6)
            st = m.app.backend.get_task_meta(r.id, cache=False)['status']
            calls = [c.decode() for c in R.lrange('calls', 0, -1)]
            print(f'{case} run {i + 1}: outer body {st}; errback calls '
                  f'{calls.count("eb")}; calls {sorted(calls)}')
    finally:
        w.terminate()
        w.wait()
        sys.stdout.flush()
        os._exit(0)   # skip AsyncResult.__del__ at interpreter exit


if __name__ == '__main__':
    main()
