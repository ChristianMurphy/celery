"""retry-over-failure with nothing injected: a task that retries once
(countdown=0) and then returns, one at a time, on one prefork worker.
With -c 2 the copy runs in the second process and can store SUCCESS
between the first run's read of the stored state and its RETRY write
(base.py:1355-1377: the SUCCESS guard reads, then writes), which then
replaces SUCCESS for ever. With -c 1 the copy waits for the first run.

Redis on 6391 (dbs 4 broker, 5 results, 6 runs). 3 s after each call the
stored state is read; every non-SUCCESS id is read again at the end.

    PYTHONPATH=<celery>:<kombu> python confirm_retry_then_success.py [n] [concurrency]

Run it from a directory outside the checkouts.
"""
import collections
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
r = redis.Redis(port=6391, db=6)
app = Celery('rofapp', broker='redis://localhost:6391/4',
             backend='redis://localhost:6391/5')
app.conf.worker_hijack_root_logger = False

@app.task(name='rof.plain', bind=True)
def plain(self):
    r.rpush('runs:' + self.request.id, self.request.retries)
    if self.request.retries == 0:
        raise self.retry(countdown=0, max_retries=2)
    return 'ok'
'''


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    c = sys.argv[2] if len(sys.argv) > 2 else '2'
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='rof_')
    open(os.path.join(d, 'rofapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import rofapp
    for db in (4, 5, 6):
        redis.Redis(port=6391, db=db).flushdb()
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'rofapp',
                          'worker', '-c', c, '-l', 'info',
                          '--without-gossip', '--without-mingle'], cwd=d,
                         stdout=log, stderr=subprocess.STDOUT)
    counts = collections.Counter()
    bad = []
    try:
        time.sleep(5)
        for i in range(n):
            res = rofapp.plain.delay()
            time.sleep(3)
            st = rofapp.app.backend.get_task_meta(res.id, cache=False)['status']
            runs = redis.Redis(port=6391, db=6).lrange('runs:' + res.id, 0, -1)
            counts[st] += 1
            if st != 'SUCCESS':
                bad.append((i, res.id, st, [int(x) for x in runs]))
            if (i + 1) % 50 == 0:
                print(f'after {i + 1}: {dict(counts)}', flush=True)
        # read the odd ones again later: do they stay?
        time.sleep(5)
        for i, tid, st, runs in bad:
            again = rofapp.app.backend.get_task_meta(tid, cache=False)['status']
            print(f'  run {i + 1}: {st} at 3 s, {again} at the end; runs {runs}')
        print(f'natural -c {c}: {counts["RETRY"]} of {n} read RETRY; {dict(counts)}')
    finally:
        w.terminate()
        w.wait()
        sys.stdout.flush()
        os._exit(0)


main()
