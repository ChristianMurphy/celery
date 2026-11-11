"""celery issue 11039: scenarios.qnt interruptFirstFix on the real Redis
backend and a real Redis (6379).

The body is published and SystemExit (a hard time limit) lands in the
cleanup script call, before it is sent or after it ran.

    PYTHONPATH=<celery worktree> python findings/interrupt-cleanup/confirm_interrupt_cleanup.py [before|after]

5f844ea: body published once and chord_error_from_stack called (body
FAILURE). 768e476, 5446659 and later: published once, no error.

Then the claim expires (the key is deleted here) and a copy of header 1
returns (scenarios.qnt interruptThirdFix / interruptFix). With "before":
768e476 and ee48301 publish the body a second time; ad00b4a does not.
"""
import sys
import uuid

import celery
from celery import Celery, states
from celery.contrib.testing.mocks import TaskMessage
from celery.worker.request import Request

when = sys.argv[1] if len(sys.argv) > 1 else 'before'
app = Celery('tl', set_as_current=False, broker='memory://')
app.conf.update(result_backend='redis://127.0.0.1:6379/11',
                result_backend_always_retry=False)


@app.task(name='tl.header', shared=False)
def header():
    return None


@app.task(name='tl.body', shared=False)
def body(results):
    return results


b = app.backend
gid = f'tl-{uuid.uuid4().hex}'
tids = [f'{gid}-h{i}' for i in (1, 2)]
body_id = f'{gid}-body'
callback = app.signature('tl.body', options={'task_id': body_id})
published = []
app.amqp._retry_publish = lambda publish, *a, **kw: published.append(a)
errors = []
orig_error = b.chord_error_from_stack


def chord_error_from_stack(cb, exc=None):
    errors.append(repr(exc))
    return orig_error(cb, exc)


b.chord_error_from_stack = chord_error_from_stack
b.set_chord_size(gid, 2)
b.apply_chord((gid, [app.AsyncResult(t) for t in tids]), callback)

script = b._chord_cleanup_script


calls = []


def cleanup(*a, **kw):
    # SIGTERM from the hard time limit lands while the worker waits for
    # the cleanup script's reply ('after'), or right before it sends it;
    # only in the first run
    calls.append(1)
    if len(calls) > 1:
        return script(*a, **kw)
    if when == 'after':
        script(*a, **kw)
    raise SystemExit(1)


b.__dict__['_chord_cleanup_script'] = cleanup


def request(i):
    msg = TaskMessage('tl.header', id=tids[i], group=gid, group_index=i,
                      chord=dict(callback))
    return Request(msg, app=app, task=header)._context


b.mark_as_done(tids[0], 1, request=request(0))
try:
    b.mark_as_done(tids[1], 2, request=request(1))
except SystemExit:
    pass
meta = b.get_task_meta(body_id)
print(f'celery {celery.__file__}')
print(f'interrupt {when} the cleanup script ran: body published {len(published)} time(s); '
      f'chord_error_from_stack calls: {errors}; body state stored: {meta["status"]}')
b.client.delete(b.get_key_for_group(gid, '.c'))     # the claim expires
b.mark_as_done(tids[0], 1, request=request(0))      # header 1 again
print(f'after a later copy of header 1: body published {len(published)} '
      f'time(s), chord_error_from_stack calls: {len(errors)}')
