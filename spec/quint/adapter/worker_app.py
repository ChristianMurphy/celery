"""A Celery app whose worker logs chord steps as JSON lines (code -> spec).

Run by adapter.record_chords. Each pool child wraps the backend's chord
calls; every logged call runs under an exclusive flock on the log file, so
the order of the lines is the order the calls reached the backend, across
processes. Nothing is delayed or injected: duplicates come from the Redis
broker's visibility timeout redelivering a header task that is still
running (acks_late), which is how they happen in production.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager

from celery import Celery, signals

PROTO = os.environ.get('QUINT_PROTO', 'redis')
LOG = os.environ.get('QUINT_LOG', '/tmp/quint-chords.jsonl')
VT = int(os.environ.get('QUINT_VT', '2'))
BACKENDS = {'redis': 'redis://127.0.0.1:6379/11',
            'kv': 'cache+memcached://127.0.0.1:11211/'}

app = Celery('quintw', broker='redis://127.0.0.1:6379/12',
             backend=BACKENDS[PROTO])
app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_transport_options={'visibility_timeout': VT,
                              'unacked_restore_interval': 1,
                              'unacked_restore_throttle': 1},
    result_chord_join_timeout=3,
    # a queue of its own per recording, on the shared Redis
    task_default_queue=os.environ.get('QUINT_QUEUE', 'quint'),
)


@app.task(name='quint.header')
def header(i, sleep):
    time.sleep(sleep)
    return {'h': i, 'pid': os.getpid()}


@app.task(name='quint.body')
def body(results):
    with _locked() as f:
        _write(f, {'kind': 'body_ran', 'n': len(results)})
    return len(results)


@app.task(name='quint.errback')
def errback(request, exc, traceback):
    # link_error of the body: called by chord_error_from_stack
    with _locked() as f:
        _write(f, {'kind': 'errback', 'body': request.id, 'exc': repr(exc)})


@contextmanager
def _locked():
    with open(LOG, 'a') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield f
        finally:
            f.flush()
            fcntl.flock(f, fcntl.LOCK_UN)


def _write(f, rec):
    rec.setdefault('pid', os.getpid())
    rec.setdefault('t', time.time())
    f.write(json.dumps(rec) + '\n')


def _gid(request):
    return getattr(request, 'group', None)


@signals.task_prerun.connect
def _prerun(task_id=None, task=None, **kw):
    if task.name == 'quint.header':
        with _locked() as f:
            _write(f, {'kind': 'deliver', 'tid': task_id,
                       'gid': task.request.group,
                       'h': task.request.group_index + 1})


@signals.task_postrun.connect
def _postrun(task_id=None, task=None, **kw):
    if task.name == 'quint.header':
        with _locked() as f:
            _write(f, {'kind': 'finish', 'tid': task_id,
                       'gid': task.request.group})


@signals.worker_process_init.connect
def _hook(**kw):
    if os.environ.get('QUINT_NOHOOK'):
        return   # unmodified backend: only the task signals log
    b = app.backend
    orig_store = b.store_result

    def store_result(task_id, result, state, traceback=None, request=None,
                     **kwargs):
        if request is None or not getattr(request, 'chord', None):
            return orig_store(task_id, result, state, traceback, request,
                              **kwargs)
        with _locked() as f:
            r = orig_store(task_id, result, state, traceback, request, **kwargs)
            _write(f, {'kind': 'store', 'tid': task_id, 'state': state,
                       'gid': request.group})
        return r
    b.store_result = store_result

    orig_pub = app.amqp._retry_publish

    def _retry_publish(publish, *a, **kw):
        with _locked() as f:
            try:
                r = orig_pub(publish, *a, **kw)
            except Exception:
                _write(f, {'kind': 'publish', 'outcome': 'refused'})
                raise
            _write(f, {'kind': 'publish', 'outcome': 'ok'})
        return r
    app.amqp._retry_publish = _retry_publish

    orig_err = b.chord_error_from_stack

    def chord_error_from_stack(callback, exc=None):
        with _locked() as f:
            _write(f, {'kind': 'chord_error', 'exc': repr(exc)})
        return orig_err(callback, exc)
    b.chord_error_from_stack = chord_error_from_stack

    if PROTO == 'redis':
        for attr, kind in (('_chord_part_return_script', 'partReturn'),
                           ('_chord_claim_extend_script', 'extend'),
                           ('_chord_cleanup_script', 'cleanup')):
            script = getattr(b, attr)

            def wrapper(*a, _s=script, _k=kind, **kw):
                with _locked() as f:
                    r = _s(*a, **kw)
                    _write(f, {'kind': _k, 'ret': [x.decode() if isinstance(x, bytes) else x
                                                   for x in r] if isinstance(r, list) else r})
                return r
            b.__dict__[attr] = wrapper
    else:
        orig_incr = b.incr

        def incr(key):
            with _locked() as f:
                r = orig_incr(key)
                _write(f, {'kind': 'count', 'ret': r})
            return r
        b.incr = incr
