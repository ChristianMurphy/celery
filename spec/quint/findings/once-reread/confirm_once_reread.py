"""celery issue 11036: scenarios.qnt onceReadOnce / onceReadAgain on the
real cache backend and a real memcached (11211).

_complete_chord_once at cd8aa46 read the header results, then the keys of
the missing ones. Header 1 runs twice, so the count reaches the chord size
while header 2 still runs; header 2 stores its result and adds its key
between the two reads. Only the order of the three runs' round trips is
controlled (threads parked between backend calls).

    PYTHONPATH=<celery worktree> python findings/once-reread/confirm_once_reread.py

cd8aa46: both results SUCCESS, body published 0 times, ChordError "Chord
header results missing". 1fa71d4: body published once, no error.
"""
import threading
import uuid

import celery
from celery import Celery
from celery.contrib.testing.mocks import TaskMessage
from celery.worker.request import Request

app = Celery('once', set_as_current=False, broker='memory://')
app.conf.update(result_backend='cache+memcached://127.0.0.1:11211/',
                result_backend_always_retry=False,
                result_chord_join_timeout=2)


@app.task(name='once.header', shared=False)
def header():
    return None


@app.task(name='once.body', shared=False)
def body(results):
    return results


b = app.backend
gid = f'once-{uuid.uuid4().hex}'
tids = [f'{gid}-h{i}' for i in (1, 2)]
body_id = f'{gid}-body'
callback = app.signature('once.body', options={'task_id': body_id})
published, errors = [], []
app.amqp._retry_publish = lambda publish, *a, **kw: published.append(a)
orig_error = b.chord_error_from_stack
b.chord_error_from_stack = lambda cb, exc=None: (errors.append(repr(exc)),
                                                 orig_error(cb, exc))[1]
b.apply_chord((gid, [app.AsyncResult(t) for t in tids]), callback)

role = threading.local()
parked = {n: threading.Event() for n in ('B', 'C')}
go = {n: threading.Event() for n in ('B', 'C')}
orig_mget, orig_incr = b.mget, b.incr


def mget(keys):
    if getattr(role, 'name', None) == 'B':
        role.mgets = getattr(role, 'mgets', 0) + 1
        if role.mgets == 2:          # results were read; keys are next
            parked['B'].set()
            go['B'].wait(10)
    return orig_mget(keys)


def incr(key):
    if getattr(role, 'name', None) == 'C':   # key added, not counted yet
        parked['C'].set()
        go['C'].wait(10)
    return orig_incr(key)


b.mget, b.incr = mget, incr


def request(i):
    msg = TaskMessage('once.header', id=tids[i], group=gid, group_index=i,
                      chord=dict(callback))
    return Request(msg, app=app, task=header)._context


def run(name, i):
    role.name = name
    if not b.thread_safe:
        app._local.backend = b
    b.mark_as_done(tids[i], i, request=request(i))


# A: header 1 returns. B: the broker delivers header 1 again; its count
# reaches the chord size, so it reads the results: header 2 is missing.
run('A', 0)
tb = threading.Thread(target=run, args=('B', 0))
tb.start()
assert parked['B'].wait(10)
# C: header 2 stores its result and adds its key.
tc = threading.Thread(target=run, args=('C', 1))
tc.start()
assert parked['C'].wait(10)
# B reads the key of the "missing" header 2: it is there.
go['B'].set()
tb.join(10)
go['C'].set()
tc.join(10)
print(f'celery {celery.__file__}')
print(f'header results stored: {[b.get_task_meta(t)["status"] for t in tids]}')
print(f'body published {len(published)} time(s); chord_error_from_stack: {errors}; '
      f'body state: {b.get_task_meta(body_id)["status"]}')
