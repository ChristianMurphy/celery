"""Confirm redelivery.qnt's classic_early_cut counterexample on the real code.

Early ack (the default), a real prefork worker (-c 1, prefetch multiplier
1, heartbeat 2 s) on RabbitMQ 3.13 through Toxiproxy, a Redis result
backend. Task `blocker` runs 3 s; task `b` is delivered behind it and waits
for the process. While `blocker` runs, the worker's connection is cut (a
`timeout` toxic with timeout 0 in both directions: nothing passes, nothing
closes). When `blocker` ends, `b` starts and the parent sends its early ack
(request.py:524-533) into the cut: the worker gets no error. Heartbeats then
close the connection on both ends; RabbitMQ returns `b`, which it never saw
acked, and the reconnected worker runs it again. The cut is removed 8 s
after it began.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_early_ack_cut.py [runs] [cut|nocut]

Broker amqp://localhost:5682 (direct, for the publisher) and
127.0.0.1:5683 (the proxy, for the worker); Toxiproxy API on 28674.
Injected: the cut (a partition); its timing is chosen so that the early
ack falls inside it. Run it from a directory outside the checkouts.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid

import celery
import kombu
import redis

TOXI = 'http://127.0.0.1:28674/proxies/quint_rabbit/toxics'
LOG_DB = 15

APP = '''
import time
import redis
from celery import Celery

r = redis.Redis(db=%(log)d)
app = Celery('eaapp', broker='amqp://guest:guest@127.0.0.1:5683//',
             backend='redis://localhost:6379/14')
app.conf.task_default_queue = %(queue)r
app.conf.worker_prefetch_multiplier = 1
app.conf.broker_heartbeat = 2


@app.task
def blocker():
    r.rpush('started', 'blocker')
    time.sleep(3)


@app.task
def b(tag):
    r.rpush('runs:' + tag, '%%.1f' %% time.time())
    time.sleep(1)
'''


def toxi(method: str, url: str, body: dict | None = None) -> None:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    urllib.request.urlopen(req).read()


def cut() -> None:
    for stream in ('upstream', 'downstream'):
        toxi('POST', TOXI, {'name': f'cut_{stream}', 'type': 'timeout',
                            'stream': stream, 'attributes': {'timeout': 0}})


def heal() -> None:
    for stream in ('upstream', 'downstream'):
        try:
            toxi('DELETE', f'{TOXI}/cut_{stream}')
        except Exception:
            pass


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    mode = sys.argv[2] if len(sys.argv) > 2 else 'cut'
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    queue = f'ea-{uuid.uuid4().hex[:6]}'
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'eaapp.py'), 'w') as f:
        f.write(APP % {'log': LOG_DB, 'queue': queue})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    r = redis.Redis(db=LOG_DB)
    r.flushdb()
    heal()
    log = open(os.path.join(d, 'worker.log'), 'w')
    worker = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'eaapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=log, stderr=subprocess.STDOUT)
    sys.path.insert(0, d)
    import eaapp  # noqa: E402
    # the publisher goes straight to the broker, not through the proxy
    eaapp.app.conf.broker_url = 'amqp://guest:guest@localhost:5682//'
    time.sleep(5)
    twice = 0
    try:
        for i in range(runs):
            tag = uuid.uuid4().hex
            r.delete('started')
            eaapp.blocker.delay()
            eaapp.b.delay(tag)
            while not r.llen('started'):
                time.sleep(0.05)
            if mode == 'cut':
                time.sleep(0.5)     # b is delivered and waits for the process
                cut()
                time.sleep(8)
                heal()
            time.sleep(12)
            n = r.llen('runs:' + tag)
            twice += n > 1
            print(f'run {i + 1} ({mode}): b ran {n} times')
    finally:
        heal()
        worker.terminate()
        worker.wait(30)
    print(f'{twice} of {runs} runs ran b twice; worker log {log.name}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
