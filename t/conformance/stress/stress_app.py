"""A plain Celery app for the stress reproductions.

Nothing in Celery is patched, wrapped or delayed. The only evidence comes
from the task bodies themselves, which append one JSON line per event to a
Redis list (``STRESS_EV``) on a Redis server that is neither the broker nor
the result backend of the run, the way a user's task would write to its
own database. Everything else is read from outside after the run.

Configuration comes from environment variables (see ``drive.py``).
"""
from __future__ import annotations

import json
import os
import time

import redis
from celery import Celery

env = os.environ.get

app = Celery('stress', broker=env('STRESS_BROKER'), backend=env('STRESS_BACKEND'))

_transport = {}
if env('STRESS_VT'):
    # Redis broker: scaled-down visibility timeout (default 3600 s).
    _transport.update(
        visibility_timeout=float(env('STRESS_VT')),
        unacked_restore_interval=1,
        unacked_restore_throttle=1,
    )

app.conf.update(
    task_default_queue=env('STRESS_QUEUE', 'stress'),
    task_acks_late=env('STRESS_ACKS_LATE', '0') == '1',
    task_reject_on_worker_lost=env('STRESS_REJECT_LOST', '0') == '1',
    worker_prefetch_multiplier=int(env('STRESS_PREFETCH', '1')),
    broker_transport_options=_transport,
    result_chord_join_timeout=float(env('STRESS_JOIN_TIMEOUT', '3')),
    result_expires=3600,
    task_track_started=env('STRESS_TRACK_STARTED', '0') == '1',
)

_ev = None


def ev(kind, **fields):
    global _ev
    if _ev is None:
        _ev = redis.Redis.from_url(env('STRESS_EV_URL'))
    fields.update(kind=kind, pid=os.getpid(), t=time.time())
    _ev.rpush(env('STRESS_EV'), json.dumps(fields))


@app.task(name='stress.header', bind=True)
def header(self, i, seconds, payload=0):
    """A chord header: sleeps ``seconds`` and returns ``payload`` bytes."""
    rq = self.request
    ev('start', tid=rq.id, gid=rq.group, h=i,
       redelivered=bool((rq.delivery_info or {}).get('redelivered')))
    if seconds:
        time.sleep(seconds)
    ev('end', tid=rq.id, gid=rq.group, h=i)
    return 'x' * payload if payload else i


@app.task(name='stress.body', bind=True)
def body(self, results, tag):
    ev('body', tag=tag, tid=self.request.id, n=len(results))
    return len(results)


@app.task(name='stress.errback')
def errback(request, exc, traceback, tag=None):
    ev('errback', tag=tag, body=request.id, exc=repr(exc)[:300])


@app.task(name='stress.flaky', bind=True, max_retries=1, default_retry_delay=0)
def flaky(self, tag):
    """Fails, retries once at once, then fails for good (es-ready-states)."""
    ev('flaky', tag=tag, tid=self.request.id, retries=self.request.retries)
    if self.request.retries < 1:
        raise self.retry(countdown=0, exc=RuntimeError('first'))
    raise RuntimeError('final')


@app.task(name='stress.flaky_auto', bind=True, autoretry_for=(RuntimeError,),
          retry_backoff=True, max_retries=1)
def flaky_auto(self, tag):
    """The same with autoretry_for and retry_backoff: the first retry's
    countdown is random.randrange(2), so 0 half the time."""
    ev('flaky', tag=tag, tid=self.request.id, retries=self.request.retries)
    raise RuntimeError('fails every time')
