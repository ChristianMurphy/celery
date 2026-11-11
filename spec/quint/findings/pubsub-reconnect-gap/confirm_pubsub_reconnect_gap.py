"""Confirm reader.qnt's pubsub_cut.quietDone counterexample on the real code.

The model: after a lost pub/sub connection the Redis ResultConsumer reads
every subscribed key (MGET, redis.py:251-257) and only then subscribes
again on a new connection (redis.py:260-271). A task that stores its final
state between the two publishes a message that reaches nobody, and the
MGET saw the old state: AsyncResult.get() waits until its timeout (or for
ever) although SUCCESS is stored.

Setup: Redis 7 on 6379 (broker db 10, results db 11), a Toxiproxy proxy
127.0.0.1:6392 -> 6379 (API on 28674) that the client's result backend
uses, a real prefork worker -c 1 that writes results straight to 6379.
The task runs until the script sets a release key, so the script chooses
when it ends. Injected: 1000 ms of latency on the proxy's replies (a slow
link between the client and Redis; it only widens the window), and
CLIENT KILL of the client's pub/sub connection (a lost connection).

Cases:
  gap     the task is released when MONITOR shows the client's MGET of its
          key after the kill: the final write lands between MGET and
          SUBSCRIBE.
  before  the task ends before the kill: the MGET sees SUCCESS.
  after   the task is released when MONITOR shows the new SUBSCRIBE.
  nokill  no kill, latency on: the message arrives.
  natural no latency and no release key: N tasks of 50 ms each, the
          client's pub/sub connection killed at a random time 0 to 80 ms
          after get() subscribed; counts get() calls that time out (3 s)
          with SUCCESS stored. Only the kill is injected.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_pubsub_reconnect_gap.py <case> [runs] [tasks]

Run it from a directory outside the checkouts.
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

import celery
import kombu
import redis

TOXI = 'http://127.0.0.1:28674'
APP = '''
import os
import time
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('gapapp', broker='redis://localhost:6379/10',
             backend=os.environ.get('GAP_BACKEND', 'redis://localhost:6379/11'))
app.conf.worker_enable_remote_control = False


@app.task
def short(secs):
    time.sleep(secs)
    return secs


@app.task
def t(tag):
    r.set('running:' + tag, 1)
    deadline = time.monotonic() + 60
    while not r.exists('release:' + tag) and time.monotonic() < deadline:
        time.sleep(0.01)
    return tag
'''


def toxi(method: str, path: str, body: dict | None = None) -> None:
    req = urllib.request.Request(
        TOXI + path, method=method,
        data=None if body is None else json.dumps(body).encode())
    try:
        urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as exc:
        if method != 'DELETE':
            raise
        exc.read()


def ensure_proxy() -> None:
    try:
        toxi('POST', '/proxies', {'name': 'quint_redis',
                                  'listen': '127.0.0.1:6392',
                                  'upstream': '127.0.0.1:6379',
                                  'enabled': True})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:   # already there
            raise
    toxi('DELETE', '/proxies/quint_redis/toxics/slow')


class Monitor(threading.Thread):
    """Every command Redis runs, from all clients, with a timestamp."""

    def __init__(self):
        super().__init__(daemon=True)
        self.lines: list[tuple[float, str]] = []
        self.r = redis.Redis()
        self.stop = False

    def run(self):
        with self.r.monitor() as m:
            for cmd in m.listen():
                self.lines.append((time.monotonic(), cmd['command']))
                if self.stop:
                    return

    def wait_for(self, words: list[str], since: float, timeout: float) -> float | None:
        end = time.monotonic() + timeout
        while True:
            for ts, line in list(self.lines):
                if ts >= since and line.split()[:1] == words[:1] and \
                        all(w in line for w in words[1:]):
                    return ts
            if time.monotonic() >= end:
                return None
            time.sleep(0.002)


def client_pubsub_ids(r: redis.Redis) -> list[int]:
    """The pub/sub connections on db 11: the client's result consumer."""
    return [c['id'] for c in r.client_list()
            if 'P' in c.get('flags', '') and int(c.get('db', -1)) == 11]


def one(case: str, d: str, env: dict) -> str:
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    ensure_proxy()
    os.environ['GAP_BACKEND'] = 'redis://127.0.0.1:6392/11'
    sys.path.insert(0, d)
    import gapapp
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'gapapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d,
        env=dict(env, GAP_BACKEND='redis://localhost:6379/11'), stdout=log,
        stderr=subprocess.STDOUT)
    r15 = redis.Redis(db=15)
    r = redis.Redis()
    mon = Monitor()
    mon.start()
    out: dict = {}
    try:
        time.sleep(5)
        res = gapapp.t.delay('X')
        key = f'celery-task-meta-{res.id}'
        for _ in range(500):
            if r15.exists('running:X'):
                break
            time.sleep(0.02)

        def waiter():
            t0 = time.monotonic()
            try:
                out['value'] = res.get(timeout=20)
            except Exception as exc:  # noqa: BLE001 - reported
                out['value'] = f'{type(exc).__name__}: {exc}'
            out['secs'] = round(time.monotonic() - t0, 1)

        th = threading.Thread(target=waiter)
        th.start()
        ids = []
        for _ in range(500):
            ids = client_pubsub_ids(r)
            if ids:
                break
            time.sleep(0.01)
        time.sleep(0.3)
        toxi('POST', '/proxies/quint_redis/toxics',
             {'name': 'slow', 'type': 'latency', 'stream': 'downstream',
              'toxicity': 1.0, 'attributes': {'latency': 1000}})
        if case == 'before':
            r15.set('release:X', 1)
            for _ in range(500):
                if b'SUCCESS' in (redis.Redis(db=11).get(key) or b''):
                    break
                time.sleep(0.01)
        killed_at = time.monotonic()
        if case != 'nokill':
            for cid in ids:
                r.client_kill_filter(_id=cid)
        if case == 'gap':
            seen = mon.wait_for(['MGET', key], killed_at, 15)
            out['mget_seen'] = seen is not None
            r15.set('release:X', 1)
        elif case == 'after':
            seen = mon.wait_for(['SUBSCRIBE', key], killed_at, 15)
            out['subscribe_seen'] = seen is not None
            r15.set('release:X', 1)
        elif case == 'nokill':
            r15.set('release:X', 1)
        th.join(40)
        stored = redis.Redis(db=11).get(key)
        out['stored'] = json.loads(stored)['status'] if stored else None
        mget = mon.wait_for(['MGET', key], killed_at, 0)
        sub = mon.wait_for(['SUBSCRIBE', key], killed_at, 0)
        pub = mon.wait_for(['PUBLISH', key], 0, 0)
        out['order'] = ' < '.join(name for _, name in sorted(
            (ts, name) for ts, name in (
                (mget, 'MGET'), (sub, 'SUBSCRIBE'), (pub, 'PUBLISH'))
            if ts is not None))
    finally:
        mon.stop = True
        toxi('DELETE', '/proxies/quint_redis/toxics/slow')
        w.terminate()
        w.wait(20)
        log.close()
        sys.path.remove(d)
        del sys.modules['gapapp']
    return (f"get() -> {out.get('value')!r} after {out.get('secs')} s; "
            f"stored {out.get('stored')}; order after the kill: "
            f"{out.get('order')}")


def natural(n: int, d: str, env: dict) -> str:
    import random
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    ensure_proxy()
    os.environ['GAP_BACKEND'] = 'redis://127.0.0.1:6392/11'
    sys.path.insert(0, d)
    import gapapp
    log = open(os.path.join(d, 'worker.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'gapapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d,
        env=dict(env, GAP_BACKEND='redis://localhost:6379/11'), stdout=log,
        stderr=subprocess.STDOUT)
    r = redis.Redis()
    hung = ok = other = 0
    try:
        time.sleep(5)
        for _ in range(n):
            res = gapapp.short.delay(0.05)
            out = {}

            def waiter():
                try:
                    out['v'] = res.get(timeout=3)
                except Exception as exc:  # noqa: BLE001 - counted
                    out['v'] = exc
            th = threading.Thread(target=waiter)
            th.start()
            ids = []
            for _ in range(500):
                ids = client_pubsub_ids(r)
                if ids:
                    break
                time.sleep(0.001)
            time.sleep(random.uniform(0, 0.08))
            for cid in ids:
                try:
                    r.client_kill_filter(_id=cid)
                except redis.ResponseError:
                    pass
            th.join(10)
            stored = redis.Redis(db=11).get(f'celery-task-meta-{res.id}')
            status = json.loads(stored)['status'] if stored else None
            if out.get('v') == 0.05:
                ok += 1
            elif status == 'SUCCESS' and 'Timeout' in type(out.get('v')).__name__:
                hung += 1
            else:
                other += 1
    finally:
        w.terminate()
        w.wait(20)
        log.close()
        sys.path.remove(d)
        del sys.modules['gapapp']
    return f'{n} tasks: returned {ok}, timed out with SUCCESS stored {hung}, other {other}'


def main() -> int:
    case = sys.argv[1] if len(sys.argv) > 1 else 'gap'
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print('case', case)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    print('redis-py', redis.__version__)
    if case == 'natural':
        for i in range(runs):
            d = tempfile.mkdtemp()
            with open(os.path.join(d, 'gapapp.py'), 'w') as f:
                f.write(APP)
            env = dict(os.environ, PYTHONPATH=d + os.pathsep
                       + os.environ.get('PYTHONPATH', ''))
            n = int(sys.argv[3]) if len(sys.argv) > 3 else 200
            print(f'run {i + 1}: {natural(n, d, env)}', flush=True)
        return 0
    for i in range(runs):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, 'gapapp.py'), 'w') as f:
            f.write(APP)
        env = dict(os.environ, PYTHONPATH=d + os.pathsep
                   + os.environ.get('PYTHONPATH', ''))
        print(f'run {i + 1}: {one(case, d, env)}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
