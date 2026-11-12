"""Confirm inspect.qnt's bucket_*.inspectSeesAll and bucket_raise.newRateApplies.

One prefork worker -c 1 (prefetch multiplier 4, acks_late) on Redis.
Nothing is injected.

    bucket  four tasks with rate_limit='6/m': the first runs, three wait in
            the worker's token bucket. 3 s later: inspect active, reserved
            and scheduled for the waiting ids, and the number of unacked
            messages.                                         (the finding)
    pool    control: one 6 s task and three quick ones without a rate
            limit; the three wait for the one process. The same reads.
    raise   four tasks at 6/m, then control.rate_limit(..., '600/m') 1 s
            later; three more tasks sent after the change. When did each
            run (seconds after the first send)?               (the finding)

Redis on 6391 (dbs 9 broker, 10 results, 11 runs).

    PYTHONPATH=<celery>:<kombu> python confirm_rate_limit_bucket.py <case> [runs]

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
from celery import Celery
import redis
R = redis.Redis(port=6391, db=11)
app = Celery('rlapp', broker='redis://localhost:6391/9',
             backend='redis://localhost:6391/10')
app.conf.worker_hijack_root_logger = False
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 4

@app.task(name='rl.limited', rate_limit='6/m')
def limited(tag):
    R.rpush('runs', f'{tag} {time.time():.2f}')

@app.task(name='rl.slow')
def slow(n):
    time.sleep(n)
'''


def visible(app, ids):
    i = app.control.inspect(timeout=1.5)
    seen = set()
    for meth in (i.active, i.reserved):
        for lst in (meth() or {}).values():
            seen |= {x['id'] for x in lst}
    for lst in (i.scheduled() or {}).values():
        seen |= {x['request']['id'] for x in lst}
    return len(seen & set(ids))


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='rl_')
    open(os.path.join(d, 'rlapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import rlapp
    app = rlapp.app
    R = redis.Redis(port=6391, db=11)
    B = redis.Redis(port=6391, db=9)
    for k in range(runs):
        for db in (9, 10, 11):
            redis.Redis(port=6391, db=db).flushdb()
        log = open(os.path.join(d, f'worker{k}.log'), 'w')
        w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'rlapp',
                              'worker', '-c', '1', '-l', 'info',
                              '--without-gossip', '--without-mingle'],
                             cwd=d, stdout=log, stderr=subprocess.STDOUT)
        try:
            time.sleep(5)
            if case in ('bucket', 'pool'):
                if case == 'bucket':
                    rs = [rlapp.limited.delay(f'b{i}') for i in range(4)]
                else:
                    rs = [rlapp.slow.delay(6)] + [rlapp.slow.delay(0.1)
                                                  for i in range(3)]
                time.sleep(3)
                waiting = [r.id for r in rs[1:]]
                unacked = B.hlen('unacked')
                n_seen = visible(app, waiting)
                print(f'{case} run {k + 1}: {len(waiting)} tasks waiting in '
                      f'the worker, {unacked} messages unacked; in inspect '
                      f'active/reserved/scheduled {n_seen}')
            else:
                t0 = time.time()
                for i in range(4):
                    rlapp.limited.delay(f'before{i}')
                time.sleep(1)
                rlapp.app.control.rate_limit('rl.limited', '600/m',
                                             reply=True)
                for i in range(3):
                    rlapp.limited.delay(f'after{i}')
                time.sleep(35)
                runs_ = [x.decode().split() for x in R.lrange('runs', 0, -1)]
                at = ', '.join(f'{tag} {float(ts) - t0:.1f}'
                               for tag, ts in runs_)
                print(f'raise run {k + 1}: 6/m raised to 600/m at 1.0 s; '
                      f'ran at: {at}')
        finally:
            w.terminate()
            w.wait()
    sys.stdout.flush()
    os._exit(0)


if __name__ == '__main__':
    main()
