"""Confirm chain.qnt's late_storeerr counterexamples on the real code.

A chain a.s() | b.si() where a has a link l.si() and an errback e.si(),
run by a real worker (prefork, one process) on a real Redis broker and
result backend (6379). a returns a value the result serializer cannot
encode, so mark_as_done raises EncodeError after the link and the next
step were published (celery/app/trace.py:590-613). The errors go to
on_error: a is stored FAILURE, b is marked FAILURE by _fail_chain, and the
errback runs; b and l were already sent, so they run too, and b's
FAILURE is replaced by SUCCESS.

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_chain_store_error.py [runs] [encode|operational]

`encode` (the default): a returns an object the JSON serializer cannot
encode. `operational`: a returns a plain value, and the result backend (a
RedisBackend subclass, the only change) raises kombu's OperationalError on
a's first SUCCESS write, as a backend that stays unreachable past its retry
policy would; trace.py:610-611 re-raises it once the callbacks were sent.

Run it from a directory outside the checkouts. Each run prints what a
reader of b saw, in order, and how often l, b and e ran.
"""
import os
import subprocess
import sys
import tempfile
import time
import uuid

import celery
import kombu
import redis

BROKER = 'redis://localhost:6379/7'
BACKEND = 'redis://localhost:6379/8'

APP = '''
from celery import Celery
import redis
from celery.backends.redis import RedisBackend
from kombu.exceptions import OperationalError

MODE = %(mode)r


class FlakyOnce(RedisBackend):
    """Raise OperationalError on the first SUCCESS write of each task a."""
    def _store_result(self, task_id, result, state, *args, **kwargs):
        if (MODE == 'operational' and state == 'SUCCESS'
                and result == 'a done' and r.set(task_id + ':failed', 1, nx=True)):
            raise OperationalError('backend unreachable (injected once)')
        return super()._store_result(task_id, result, state, *args, **kwargs)


r = redis.Redis(db=9)
app = Celery('chainapp', broker=%(broker)r,
             backend='chainapp:FlakyOnce+' + %(backend)r if MODE == 'operational' else %(backend)r)
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1

class Unencodable:
    pass

@app.task
def a(tag):
    r.rpush(tag + ':ran', 'a')
    return 'a done' if MODE == 'operational' else Unencodable()

@app.task
def b(tag):
    r.rpush(tag + ':ran', 'b')
    return 'b done'

@app.task
def l(tag):
    r.rpush(tag + ':ran', 'l')

@app.task
def e(tag):
    r.rpush(tag + ':ran', 'e')
'''


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    mode = sys.argv[2] if len(sys.argv) > 2 else 'encode'
    print('mode', mode)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'chainapp.py'), 'w') as f:
        f.write(APP % {'broker': BROKER, 'backend': BACKEND, 'mode': mode})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    r = redis.Redis(db=9)
    for db in (7, 8, 9):
        redis.Redis(db=db).flushdb()
    worker = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'chainapp', 'worker', '-P',
         'prefork', '-c', '1', '-l', 'warning', '--without-gossip',
         '--without-mingle', '--without-heartbeat'],
        cwd=d, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    sys.path.insert(0, d)
    from chainapp import a, b, e, l  # noqa: E402
    time.sleep(3)
    bad = 0
    try:
        for i in range(runs):
            tag = uuid.uuid4().hex
            sig = (a.si(tag).set(link=l.si(tag), link_error=e.si(tag))
                   | b.si(tag))
            res = sig.apply_async()
            seen = []
            deadline = time.time() + 10
            while time.time() < deadline:
                # a fresh read each time: AsyncResult keeps a ready state
                st = res.backend.get_task_meta(res.id, cache=False)['status']
                if not seen or seen[-1] != st:
                    seen.append(st)
                if st == 'SUCCESS' and r.llen(tag + ':ran') >= 4:
                    break
                time.sleep(0.005)
            ran = [x.decode() for x in r.lrange(tag + ':ran', 0, -1)]
            a_state = res.parent.state
            both = 'l' in ran and 'e' in ran
            flip = 'FAILURE' in seen and seen[-1] == 'SUCCESS'
            bad += both or flip
            print(f'run {i + 1}: a {a_state}; b seen {" -> ".join(seen)}; '
                  f'ran {",".join(sorted(ran))}; link and errback both: '
                  f'{both}; b FAILURE then SUCCESS: {flip}')
    finally:
        worker.terminate()
        worker.wait(10)
    print(f'{bad} of {runs} runs ran the link and the errback or changed '
          "b's final state")
    return 0


if __name__ == '__main__':
    sys.exit(main())
