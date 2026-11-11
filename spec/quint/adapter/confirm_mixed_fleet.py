"""celery issue 11036, a rolling upgrade (instance mixed_dup, quietDone):
header 2 returns twice, once on a worker without the fix and once on one
with it, then header 1 returns on the worker without it. The real cache
backend and a real memcached (11211); each step runs in its own process, so
each can use another checkout.

    gid=$(python -c 'import uuid; print(uuid.uuid4().hex)')
    PYTHONPATH=<main>  python adapter/confirm_mixed_fleet.py $gid setup 2
    PYTHONPATH=<fix>   python adapter/confirm_mixed_fleet.py $gid 2
    PYTHONPATH=<main>  python adapter/confirm_mixed_fleet.py $gid 1
    PYTHONPATH=<fix>   python adapter/confirm_mixed_fleet.py $gid report

1fa71d4 with 768e476: both results SUCCESS, the body never published, no
error: the count went 1, 2, 3; the run with the fix waited for header 1's
key, and the run without it only logs that the counter went past the size.
"""
import sys

import celery
from celery import Celery
from celery.contrib.testing.mocks import TaskMessage
from celery.worker.request import Request

gid, *steps = sys.argv[1:]
app = Celery('mixed', set_as_current=False, broker='memory://')
app.conf.update(result_backend='cache+memcached://127.0.0.1:11211/',
                result_backend_always_retry=False,
                result_chord_join_timeout=2)


@app.task(name='mixed.header', shared=False)
def header():
    return None


@app.task(name='mixed.body', shared=False)
def body(results):
    return results


b = app.backend
tids = {i: f'{gid}-h{i}' for i in (1, 2)}
body_id = f'{gid}-body'
callback = app.signature('mixed.body', options={'task_id': body_id})
published, errors = [], []
app.amqp._retry_publish = lambda publish, *a, **kw: published.append(a)
orig_error = b.chord_error_from_stack
b.chord_error_from_stack = lambda cb, exc=None: (errors.append(repr(exc)),
                                                 orig_error(cb, exc))[1]
fixed = hasattr(b, '_complete_chord_once')
for step in steps:
    if step == 'setup':
        b.apply_chord((gid, [app.AsyncResult(t) for t in tids.values()]),
                      callback)
    elif step == 'report':
        print('header results:',
              [b.get_task_meta(t)['status'] for t in tids.values()],
              '| body:', b.get_task_meta(body_id)['status'],
              '| counter:', b.client.get(b.get_key_for_chord(gid)))
    else:
        i = int(step)
        msg = TaskMessage('mixed.header', id=tids[i], group=gid,
                          group_index=i - 1, chord=dict(callback))
        b.mark_as_done(tids[i], i, request=Request(
            msg, app=app, task=header)._context)
        print(f'header {i} returned on a worker {"with" if fixed else "without"}'
              f' the fix ({celery.__file__.split("/celery/")[0]}): published '
              f'{len(published)}, errors {errors}')
