"""Confirm two revoke.qnt counterexamples on the real code.

  kill     statedb_kill.revokeSurvivesRestart: the state db is written at
           exit only (atexit, components.py:216-218), so a worker killed
           with SIGKILL forgets every revoke since it started. A worker
           with --statedb and acks_late (-c 1, Redis broker,
           visibility_timeout 2 s) runs A (5 s) with B reserved behind it;
           app.control.revoke(B) arrives ("Tasks flagged as revoked"); the
           worker is killed with SIGKILL (the fault; injected) and started
           again with the same state db. B comes back after the visibility
           timeout.
  term     the control: SIGTERM (warm shutdown) instead of SIGKILL.
  cold     early_halt.outcomeVisible: an early-ack task (acks_late off)
           cut by a cold shutdown (SIGQUIT 1.5 s into a 6 s run) stores
           RETRY ("cancelled by Celery", request.py:446-457); its message
           was acked at the start, so nothing runs it again. The worker is
           started again and watched for 12 s.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_statedb_kill.py <case> [runs]

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
import time
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('sdapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')
app.conf.task_acks_late = %(late)r
app.conf.broker_transport_options = {'visibility_timeout': 2}


@app.task
def t(tag, secs):
    r.rpush('bodies', tag)
    time.sleep(secs)
    return tag
'''


def start(d, env, log, statedb):
    cmd = [sys.executable, '-m', 'celery', '-A', 'sdapp', 'worker',
           '-P', 'prefork', '-c', '1', '-l', 'info', '--without-gossip',
           '--without-mingle', '--without-heartbeat']
    if statedb:
        cmd.append('--statedb=' + os.path.join(d, 'worker.state'))
    return subprocess.Popen(cmd, cwd=d, env=env, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)


def one(case: str, d: str, env: dict) -> str:
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    sys.path.insert(0, d)
    import sdapp
    log = open(os.path.join(d, 'worker.log'), 'a')
    w = start(d, env, log, case != 'cold')
    time.sleep(5)
    ids = {}
    if case == 'cold':
        a = sdapp.t.apply_async(('A', 6))
        ids['A'] = a.id
        time.sleep(1.5)
        os.kill(w.pid, signal.SIGQUIT)
        w.wait(30)
    else:
        a = sdapp.t.apply_async(('A', 5))
        b = sdapp.t.apply_async(('B', 0))
        ids = {'A': a.id, 'B': b.id}
        time.sleep(1)
        sdapp.app.control.revoke(b.id)
        time.sleep(1)
        if case == 'kill':
            os.killpg(w.pid, signal.SIGKILL)
        else:
            os.kill(w.pid, signal.SIGTERM)
        w.wait(30)
    time.sleep(1)
    w = start(d, env, log, case != 'cold')
    time.sleep(12)
    os.kill(w.pid, signal.SIGTERM)
    w.wait(30)
    log.close()
    r = redis.Redis(db=15)
    bodies = [x.decode() for x in r.lrange('bodies', 0, -1)]
    text = open(os.path.join(d, 'worker.log')).read()
    states = {k: sdapp.app.backend.get_task_meta(v, cache=False)['status']
              for k, v in ids.items()}
    left = redis.Redis(db=10).llen('celery')
    sys.path.remove(d)
    del sys.modules['sdapp']
    return (f'bodies {bodies}, states {states}, queued {left}, '
            f'flagged {text.count("Tasks flagged as revoked")}, '
            f'discarded {text.count("Discarding revoked task")}')


def main() -> int:
    case = sys.argv[1] if len(sys.argv) > 1 else 'kill'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('case', case)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    for i in range(runs):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, 'sdapp.py'), 'w') as f:
            f.write(APP % {'late': case != 'cold'})
        env = dict(os.environ, PYTHONPATH=d + os.pathsep
                   + os.environ.get('PYTHONPATH', ''))
        print(f'run {i + 1}: {one(case, d, env)} (log {d}/worker.log)',
              flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
