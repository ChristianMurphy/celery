"""Celery app for the natural duplicate-run harness.

Everything is set from ``NATURAL_*`` environment variables, so the driver
(``run.py``) and the workers it starts build the same app. Each run of a task
logs ``start``, ``end`` and ``post`` events to a Redis list, so the driver can
tell how many times a task id ran and when each run stored its result.
"""
from __future__ import annotations

import json
import os
import socket
import time
import uuid

import redis
from celery import Celery, signals

RUN = os.environ.get('NATURAL_RUN', 'manual')
BACKEND = os.environ.get('NATURAL_BACKEND', 'redis')
BROKER = os.environ.get('NATURAL_BROKER', 'redis://127.0.0.1:16479/0')
LOG_URL = os.environ.get('NATURAL_LOG', 'redis://127.0.0.1:16479/1')
LOG_KEY = f'natural:runs:{RUN}'
CLAIM = os.environ.get('NATURAL_CLAIM', '0') == '1'


def _flag(name, default='0'):
    return os.environ.get(name, default) not in ('', '0', 'false', 'False')


# Result backends. Names include the run id where the backend keeps state in a
# database or table it creates, so runs don't see each other's results.
BACKENDS = {
    'redis': ('redis://127.0.0.1:16480/0', {}),
    'memcached': ('cache+memcached://127.0.0.1:11211/', {}),
    'couchdb': (f'couchdb://admin:pw@127.0.0.1:5984/natural_{RUN}', {}),
    'cassandra': ('cassandra://', {
        'cassandra_servers': ['127.0.0.1'],
        'cassandra_keyspace': 'celery',
        'cassandra_table': f'natural_{RUN}',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE',
    }),
    'arangodb': ('arangodb://root:pw@127.0.0.1:8529/_system/natural', {}),
    'mongodb': (f'mongodb://127.0.0.1:27017/natural_{RUN}', {}),
    'postgresql': ('db+postgresql+psycopg2://postgres:pw@127.0.0.1:5432/celery', {}),
}
BACKENDS['redis'] = (os.environ.get('NATURAL_REDIS_BACKEND', BACKENDS['redis'][0]), {})

backend_url, backend_conf = BACKENDS[BACKEND]

app = Celery('natural', broker=BROKER, backend=backend_url)
app.conf.update(
    task_track_started=_flag('NATURAL_TRACK_STARTED'),
    task_acks_late=_flag('NATURAL_ACKS_LATE'),
    task_reject_on_worker_lost=_flag('NATURAL_REJECT_ON_WORKER_LOST'),
    worker_prefetch_multiplier=int(os.environ.get('NATURAL_PREFETCH', '4')),
    broker_connection_retry_on_startup=True,
    **backend_conf,
)
if os.environ.get('NATURAL_TIME_LIMIT'):
    app.conf.task_time_limit = float(os.environ['NATURAL_TIME_LIMIT'])
if os.environ.get('NATURAL_VISIBILITY'):
    # The two restore settings make kombu sweep for expired messages every
    # second, as its docs suggest for a low visibility_timeout.
    app.conf.broker_transport_options = {
        'visibility_timeout': float(os.environ['NATURAL_VISIBILITY']),
        'unacked_restore_interval': 1,
        'unacked_restore_throttle': 1,
    }

_log = None


def log(**event):
    global _log
    if _log is None:
        _log = redis.Redis.from_url(LOG_URL)
    event.update(t=time.time(), pid=os.getpid(), host=socket.gethostname())
    _log.rpush(LOG_KEY, json.dumps(event))


@app.task(name='natural.work', bind=True)
def work(self, duration):
    token = self.request.natural_token = uuid.uuid4().hex
    log(ev='start', id=self.request.id, token=token,
        redelivered=bool((self.request.delivery_info or {}).get('redelivered')))
    time.sleep(duration)
    log(ev='end', id=self.request.id, token=token)
    if CLAIM:
        # A task that does its side effect once: the run that loses the
        # claim fails, as an insert under a unique key would.
        if not _log.set(f'natural:claim:{RUN}:{self.request.id}', token, nx=True, ex=3600):
            raise RuntimeError('already done by another run')
    return {'token': token, 'pid': os.getpid()}


def seen(task_id):
    """Read the stored state the way a client would, with the read's times."""
    t0 = time.time()
    try:
        meta = app.backend._get_task_meta_for(task_id)
    except Exception as exc:
        return {'t0': t0, 't1': time.time(), 'state': f'ERROR {exc!r}', 'token': None}
    result = meta.get('result')
    return {'t0': t0, 't1': time.time(), 'state': meta['status'],
            'token': result.get('token') if isinstance(result, dict) else None}


@signals.task_postrun.connect
def _postrun(task_id=None, task=None, state=None, **kwargs):
    # The run has stored its result; read it back right away, as a client
    # waiting on the result would.
    token = getattr(task.request, 'natural_token', None)
    log(ev='post', id=task_id, token=token, state=state, seen=seen(task_id))


@signals.task_failure.connect
def _failure(task_id=None, exception=None, **kwargs):
    # On a hard time limit this runs in the worker's main process.
    log(ev='failure', id=task_id, token=None, exc=type(exception).__name__)
