"""celery issue 11039: scenarios.qnt interruptSecondFix / interruptFix on
the real Redis backend and a real Redis (6379).

SystemExit lands before the publish (in the claim-extend call), the claim
expires (the key is deleted here), and a copy of a header task arrives.

    PYTHONPATH=<celery worktree> python findings/interrupt-then-copy/confirm_interrupt_then_copy.py

5446659: the chord is failed, then the copy publishes the body. ee48301
and ad00b4a: failed once, the copy publishes nothing. 768e476: no error, and the copy
publishes the body.
"""
import uuid

import celery
from celery import Celery
from celery.contrib.testing.mocks import TaskMessage
from celery.worker.request import Request

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
published, errors = [], []
app.amqp._retry_publish = lambda publish, *a, **kw: published.append(a)
orig_error = b.chord_error_from_stack
b.chord_error_from_stack = lambda cb, exc=None: (errors.append(repr(exc)),
                                                 orig_error(cb, exc))[1]
b.set_chord_size(gid, 2)
b.apply_chord((gid, [app.AsyncResult(t) for t in tids]), callback)
extend = b._chord_claim_extend_script
calls = []


def interrupted(*a, **kw):
    calls.append(1)
    if len(calls) == 1:
        raise SystemExit(1)      # the hard time limit lands here
    return extend(*a, **kw)


b.__dict__['_chord_claim_extend_script'] = interrupted


def request(i):
    msg = TaskMessage('tl.header', id=tids[i], group=gid, group_index=i,
                      chord=dict(callback))
    return Request(msg, app=app, task=header)._context


b.mark_as_done(tids[0], 1, request=request(0))
try:
    b.mark_as_done(tids[1], 2, request=request(1))
except SystemExit:
    pass
print(f'celery {celery.__file__}')
print(f'after the interrupt: published {len(published)}, errors {errors}, '
      f'body {b.get_task_meta(body_id)["status"]}')
b.client.delete(b.get_key_for_group(gid, '.c'))     # the claim expires
b.mark_as_done(tids[0], 1, request=request(0))      # header 1 again
print(f'after a later copy of header 1: published {len(published)}, '
      f'errors {len(errors)}, body {b.get_task_meta(body_id)["status"]}')
