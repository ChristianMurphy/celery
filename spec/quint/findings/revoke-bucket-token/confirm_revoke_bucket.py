"""Confirm inspect.qnt's bucket_revoke.revokeFreesBucket.

One prefork worker -c 1 (prefetch multiplier 4, acks_late) on Redis.
Nothing is injected.

    bucket  four tasks with rate_limit='6/m': b0 runs at once, b1, b2, b3
            wait in the worker's token bucket (due at 10, 20, 30 s). At 2 s:
            inspect query_task b1, then control.revoke(b1, terminate=True,
            reply=True). Read b1's state and the unacked messages at once,
            and when b2 and b3 run. Correct: b2 runs at about 10 s (the
            revoked task needs no token).                      (the finding)
    pool    control: one 6 s task and three quick ones without a rate limit;
            at 2 s query_task and revoke the first quick one. Correct: the
            other two run when the slow one ends (about 6 s).
    eta     control: one task with countdown=10 and three plain ones; at 2 s
            query_task and revoke the eta task. Correct: query_task finds
            it (celery PR 10535) and terminate's reply names it.

Redis on 6391 (dbs 12 broker, 13 results, 14 runs).

    PYTHONPATH=<celery>:<kombu> python -bb confirm_revoke_bucket.py <case> [runs]

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
R = redis.Redis(port=6391, db=14)
app = Celery('rvapp', broker='redis://localhost:6391/12',
             backend='redis://localhost:6391/13')
app.conf.worker_hijack_root_logger = False
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 4

@app.task(name='rv.limited', rate_limit='6/m')
def limited(tag):
    R.rpush('runs', f'{tag} {time.time():.2f}')

@app.task(name='rv.plain')
def plain(tag, sleep=0):
    time.sleep(sleep)
    R.rpush('runs', f'{tag} {time.time():.2f}')
'''


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='rv_')
    open(os.path.join(d, 'rvapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    import rvapp
    app = rvapp.app
    R = redis.Redis(port=6391, db=14)
    B = redis.Redis(port=6391, db=12)
    for k in range(runs):
        for db in (12, 13, 14):
            redis.Redis(port=6391, db=db).flushdb()
        log = open(os.path.join(d, f'worker{k}.log'), 'w')
        w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'rvapp',
                              'worker', '-c', '1', '-l', 'info',
                              '-n', f'rv{os.getpid()}@%h',
                              '--without-gossip', '--without-mingle'],
                             cwd=d, stdout=log, stderr=subprocess.STDOUT)
        try:
            time.sleep(5)
            t0 = time.time()
            if case == 'bucket':
                rs = [rvapp.limited.delay(f'b{i}') for i in range(4)]
                target = rs[1]
                wait = 33
            elif case == 'pool':
                rs = [rvapp.plain.delay('slow', 6)] + [
                    rvapp.plain.delay(f'q{i}') for i in range(1, 4)]
                target = rs[1]
                wait = 10
            else:
                target = rvapp.plain.apply_async(('e',), countdown=10)
                rs = [target] + [rvapp.plain.delay(f'p{i}') for i in range(3)]
                wait = 12
            time.sleep(2)
            q = app.control.inspect(timeout=1.5).query_task(target.id) or {}
            found = any(target.id in v for v in q.values())
            rep = app.control.revoke(target.id, terminate=True, reply=True,
                                     timeout=1.5)
            reply = '; '.join(v.get('ok', str(v)) for x in rep
                              for v in x.values())
            st = target.state
            unacked = B.hlen('unacked')
            time.sleep(wait)
            ran = {tag: float(ts) - t0 for tag, ts in
                   (x.decode().split() for x in R.lrange('runs', 0, -1))}
            at = ', '.join(f'{tag} {v:.1f}' for tag, v in ran.items())
            verdict = ''
            if case == 'bucket':
                b2 = ran.get('b2')
                verdict = ('FINDING: b2 waited for the revoked task\'s token'
                           if b2 is not None and b2 > 15 else
                           'ok: b2 did not wait for the revoked task')
            elif case == 'pool':
                verdict = ('ok: the others ran when the slow one ended'
                           if ran.get('q2', 99) < 8 else 'unexpected')
            else:
                verdict = ('ok: query_task found it and terminate named it'
                           if found and target.id in reply else 'unexpected')
            print(f'{case} run {k + 1}: query_task found={found}; '
                  f'revoke reply="{reply}"; state at once={st}; '
                  f'unacked at once={unacked}; final state={target.state}; '
                  f'unacked at end={B.hlen("unacked")}; ran (s): {at}; '
                  f'{verdict}', flush=True)
        finally:
            w.terminate()
            w.wait()
    sys.stdout.flush()
    os._exit(0)


if __name__ == '__main__':
    main()
