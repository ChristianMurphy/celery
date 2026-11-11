"""Confirm redelivery.qnt's redis_dedup_republish counterexample on the real code.

worker_deduplicate_successful_tasks on, acks_late, a real prefork worker on
a Redis broker (6379, db 4) and result backend (db 5). The publisher talks
to the broker through Toxiproxy (127.0.0.1:6392, API 28674) with a 1 s
socket timeout. While it publishes, the replies from Redis are dropped (a
`timeout` toxic with timeout 0 on the downstream for 1.5 s): Redis has
pushed the message, the client times out, and the publish is sent again
(redis-py's retry, then kombu's retry policy). The second copy is a new
message, not marked redelivered, so deduplication does not look at it,
and the task runs again after its SUCCESS is stored.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_republish.py [runs] [drop|nodrop]

Injected: the lost replies. Run it from a directory outside the checkouts.
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

TOXI = 'http://127.0.0.1:28674/proxies/quint_redis/toxics'

APP = '''
import time
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('rpapp', broker='redis://localhost:6379/4',
             backend='redis://localhost:6379/5')
app.conf.task_acks_late = True
app.conf.worker_deduplicate_successful_tasks = True


@app.task
def t(tag):
    r.rpush('runs:' + tag, '%.1f' % time.time())
    return 'ok'
'''


def toxi(method: str, url: str, body: dict | None = None) -> None:
    data = json.dumps(body).encode() if body is not None else None
    urllib.request.urlopen(urllib.request.Request(url, data=data,
                                                  method=method)).read()


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    mode = sys.argv[2] if len(sys.argv) > 2 else 'drop'
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'rpapp.py'), 'w') as f:
        f.write(APP)
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    for db in (4, 5, 15):
        redis.Redis(db=db).flushdb()
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'rpapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=open(os.path.join(d, 'w.log'), 'w'),
        stderr=subprocess.STDOUT)
    sys.path.insert(0, d)
    import rpapp  # noqa: E402
    # the publisher goes through the proxy
    rpapp.app.conf.broker_url = 'redis://127.0.0.1:6392/4'
    rpapp.app.conf.broker_transport_options = {'socket_timeout': 1}
    time.sleep(4)
    r = redis.Redis(db=15)
    twice = 0
    try:
        for i in range(runs):
            tag = f'{i}-{time.time()}'
            if mode == 'drop':
                toxi('POST', TOXI, {'name': 'drop', 'type': 'timeout',
                                    'stream': 'downstream',
                                    'attributes': {'timeout': 0}})
                threading.Timer(1.5, lambda: toxi(
                    'DELETE', f'{TOXI}/drop')).start()
            err = ''
            try:
                res = rpapp.t.apply_async((tag,))
            except Exception as exc:  # noqa: BLE001
                err = repr(exc)
                res = None
            time.sleep(6)
            n = r.llen('runs:' + tag)
            state = (res.backend.get_task_meta(res.id, cache=False)['status']
                     if res else '?')
            twice += n > 1
            print(f'run {i + 1} ({mode}): the task ran {n} times, state '
                  f'{state}{", publish raised " + err if err else ""}')
    finally:
        try:
            toxi('DELETE', f'{TOXI}/drop')
        except Exception:  # noqa: BLE001
            pass
        w.terminate()
        w.wait(30)
    log = open(os.path.join(d, 'w.log')).read()
    print(f'{twice} of {runs} runs ran the task twice; skipped as done: '
          f'{log.count("Task already completed successfully")}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
