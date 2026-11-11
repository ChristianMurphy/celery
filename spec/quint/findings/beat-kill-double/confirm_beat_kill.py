"""Confirm beat.qnt's cron_kill.noDoubleSend counterexample on the real code.

The model: beat reserves an entry (last_run_at = now) and syncs its
schedule file only on the first send after a start, then when sync_every
(3 minutes) has passed (beat.py:416-422, 452-455). A beat killed between
two syncs loses the last_run_at of the sends since the last one; started
again, it reads the older value from the file, and a crontab occurrence it
already sent is due again: it is sent a second time.

A real celery beat (PersistentScheduler, shelve file) with one entry,
crontab(minute='*'), on a Redis broker (db 12), beat_max_loop_interval 1.
The script waits for two sends (two minutes), kills beat 5 s after the
second, starts it again on the same file and watches 20 s. Injected: the
kill (SIGKILL, as an OOM kill or a crash would do), or SIGTERM in the
control case.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_beat_kill.py <kill|term|sync1> [runs]

sync1 is kill with beat_sync_every = 1. Run it from a directory outside
the checkouts. Each run takes about two and a half minutes.
"""
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

import celery
import kombu
import redis

APP = '''
import os
from celery import Celery
from celery.schedules import crontab

app = Celery('beatapp', broker='redis://localhost:6379/12')
app.conf.beat_max_loop_interval = 1
app.conf.beat_sync_every = int(os.environ.get('SYNC_EVERY', '0'))
app.conf.beat_schedule = {
    'every-minute': {'task': 'beatapp.t', 'schedule': crontab(minute='*')},
}


@app.task
def t():
    return 1
'''
SENT = re.compile(r'\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+: INFO/MainProcess\] '
                  r'Scheduler: Sending due task every-minute')


def beat(d: str, env: dict, log: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'beatapp', 'beat', '-l', 'info',
         '-s', os.path.join(d, 'schedule')], cwd=d, env=env,
        stdout=open(os.path.join(d, log), 'w'), stderr=subprocess.STDOUT)


def sends(d: str, log: str) -> list[str]:
    return SENT.findall(open(os.path.join(d, log)).read())


def one(case: str, d: str, env: dict) -> str:
    redis.Redis(db=12).flushdb()
    if case == 'sync1':
        env = dict(env, SYNC_EVERY='1')
    # start a few seconds after a minute boundary
    while not 3 <= time.time() % 60 < 10:
        time.sleep(0.5)
    b = beat(d, env, 'beat1.log')
    end = time.time() + 150
    while len(sends(d, 'beat1.log')) < 2 and time.time() < end:
        time.sleep(0.5)
    time.sleep(5)
    b.send_signal(signal.SIGTERM if case == 'term' else signal.SIGKILL)
    b.wait(30)
    first = sends(d, 'beat1.log')
    b = beat(d, env, 'beat2.log')
    time.sleep(20)
    b.send_signal(signal.SIGTERM)
    b.wait(30)
    second = sends(d, 'beat2.log')
    minutes = [s[:16] for s in first + second]
    dup = len(minutes) - len(set(minutes))
    queued = redis.Redis(db=12).llen('celery')
    return (f'before the {case}: {first}; after the restart: {second}; '
            f'minutes sent twice: {dup}; messages queued: {queued}')


def main() -> int:
    case = sys.argv[1] if len(sys.argv) > 1 else 'kill'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('case', case)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    for i in range(runs):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, 'beatapp.py'), 'w') as f:
            f.write(APP)
        env = dict(os.environ, PYTHONPATH=d + os.pathsep
                   + os.environ.get('PYTHONPATH', ''))
        print(f'run {i + 1}: {one(case, d, env)}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
