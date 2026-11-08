"""Celery app for the chord harness.

Workers and the driver both import this module. Everything is configured from
``CHORDS_*`` environment variables that ``driver.py`` sets, so a worker started
by hand with the same variables joins a run.

The tasks write to an audit Redis (``CHORDS_AUDIT``) that is neither the broker
nor the result backend, so a body that runs twice is counted even when Celery
itself lost track of it. Signal handlers only append events; they don't change
what Celery does.
"""
from __future__ import annotations

import json
import os
import socket
import time

import redis

from celery import Celery, signals

RUN = os.environ.get('CHORDS_RUN', 'manual')
AUDIT_URL = os.environ.get('CHORDS_AUDIT', 'redis://127.0.0.1:16381/0')
EVENTS = f'chords:{RUN}:events'
NOTIFY = f'chords:{RUN}:notify'
NOTIFY_ON = set(filter(None, os.environ.get('CHORDS_NOTIFY', '').split(',')))
FAST_REDELIVERY = os.environ.get('CHORDS_FAST_REDELIVERY') == '1'
QUEUE = os.environ.get('CHORDS_QUEUE', f'chords-{RUN}')

app = Celery('chords', set_as_current=True)
app.conf.update(
    broker_url=os.environ.get('CHORDS_BROKER', 'redis://127.0.0.1:16379/0'),
    result_backend=os.environ.get('CHORDS_BACKEND', 'redis://127.0.0.1:16380/0'),
    task_default_queue=QUEUE,
    task_acks_late=os.environ.get('CHORDS_ACKS_LATE', '1') == '1',
    task_reject_on_worker_lost=os.environ.get('CHORDS_REJECT_ON_LOST', '1') == '1',
    worker_prefetch_multiplier=int(os.environ.get('CHORDS_PREFETCH', '4')),
    broker_connection_retry_on_startup=True,
    result_extended=True,
    worker_cancel_long_running_tasks_on_connection_loss=False,
)
if os.environ.get('CHORDS_VISIBILITY'):
    vt = int(os.environ['CHORDS_VISIBILITY'])
    # Sweep for expired unacked messages every second, as kombu's docs
    # suggest for a low visibility_timeout (the default sweep is ~100 s).
    app.conf.broker_transport_options = {
        'visibility_timeout': vt,
        'unacked_restore_interval': int(os.environ.get('CHORDS_RESTORE_INTERVAL', '1')),
        'unacked_restore_throttle': 1,
    }
if os.environ.get('CHORDS_QUEUE_TYPE') == 'quorum':
    app.conf.task_default_queue_type = 'quorum'
    app.conf.broker_transport_options = {'confirm_publish': True}
if os.environ.get('CHORDS_BACKEND_CONF'):
    app.conf.update(json.loads(os.environ['CHORDS_BACKEND_CONF']))

_audit = None


def audit():
    global _audit
    if _audit is None or _audit[0] != os.getpid():
        _audit = (os.getpid(), redis.Redis.from_url(AUDIT_URL))
    return _audit[1]


def emit(ev, **fields):
    """Append one event to the run's audit list."""
    fields.update(ev=ev, ts=time.time(), pid=os.getpid(),
                  pgid=os.getpgid(0), host=socket.gethostname())
    data = json.dumps(fields, default=repr)
    client = audit()
    for attempt in range(50):
        try:
            client.rpush(EVENTS, data)
            if ev in NOTIFY_ON:
                client.rpush(NOTIFY, data)
            return
        except redis.RedisError:  # pragma: no cover - audit Redis blip
            time.sleep(0.1)


@app.task(name='chords.header', bind=True)
def header(self, chord_id, idx, sleep=0.0):
    redelivered = bool((self.request.delivery_info or {}).get('redelivered'))
    if redelivered and FAST_REDELIVERY:
        # Only the first delivery is slow, so a task that outlived the
        # visibility timeout once isn't redelivered forever.
        sleep = min(sleep, 0.05)
    if sleep:
        time.sleep(sleep)
    emit('header_return', chord=chord_id, idx=idx, tid=self.request.id)
    return idx


@app.task(name='chords.body', bind=True)
def body(self, results, chord_id=None, header_ids=()):
    states = []
    for tid in header_ids:
        try:
            states.append(app.AsyncResult(tid).state)
        except Exception as exc:  # pragma: no cover - backend down
            states.append(f'error:{exc!r}')
    emit('body_start', chord=chord_id, tid=self.request.id,
         redelivered=bool((self.request.delivery_info or {}).get('redelivered')),
         results=results, header_states=states)
    return len(results)


@app.task(name='chords.on_error')
def on_error(request, exc, traceback, chord_id=None):
    emit('errback', chord=chord_id, tid=getattr(request, 'id', None),
         exc=repr(exc))


def _delivery(task):
    info = task.request.delivery_info or {}
    return bool(info.get('redelivered'))


@signals.task_prerun.connect
def _prerun(sender=None, task_id=None, task=None, args=None, kwargs=None, **_):
    if sender.name == 'chords.header':
        emit('header_start', chord=args[0], idx=args[1], tid=task_id,
             group=task.request.group, redelivered=_delivery(task))
    elif sender.name == 'celery.chord_unlock':
        emit('unlock_start', group=args[0], tid=task_id,
             retries=task.request.retries, redelivered=_delivery(task))


@signals.task_postrun.connect
def _postrun(sender=None, task_id=None, task=None, args=None, state=None, **_):
    # task_postrun fires after the result is stored and, for a header task,
    # after on_chord_part_return() has run.
    if sender.name == 'chords.header':
        emit('header_stored', chord=args[0], idx=args[1], tid=task_id,
             group=task.request.group, state=state)
    elif sender.name == 'celery.chord_unlock':
        emit('unlock_done', group=args[0], tid=task_id, state=state)


@signals.before_task_publish.connect
def _publish(sender=None, headers=None, **_):
    if sender == 'chords.body':
        emit('body_publish', tid=headers.get('id'))
    elif sender == 'celery.chord_unlock':
        emit('unlock_publish', tid=headers.get('id'),
             retries=headers.get('retries'), eta=headers.get('eta'))


@signals.worker_ready.connect
def _ready(sender=None, **_):
    emit('worker_ready', worker=sender.hostname)
