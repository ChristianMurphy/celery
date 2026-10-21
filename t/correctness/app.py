"""Instrumented Celery app loaded by both the harness client and its workers.

Every task execution is logged to a *recorder* Redis that the nemesis never
touches, as Jepsen-style ``invoke``/``ok`` operations.  An execution that is
killed mid-run simply has no completion.  Configuration comes from the
``CORRECTNESS_CONFIG`` environment variable (JSON) so that each worker can be
pointed at its own fault-injecting proxy.
"""
import json
import os
import random
import signal
import socket
import time

import redis
from celery import Celery, signals

CONFIG = json.loads(os.environ.get('CORRECTNESS_CONFIG', '{}'))
RECORDER = redis.Redis.from_url(CONFIG.get('recorder_url', 'redis://127.0.0.1:6391/0'))
WORKER = os.environ.get('CORRECTNESS_WORKER', 'client')

app = Celery('correctness', broker=CONFIG.get('broker_url'), backend=CONFIG.get('backend_url'))

transport_options = {'visibility_timeout': CONFIG.get('visibility_timeout', 30)}
if CONFIG.get('broker_url', '').startswith('redis'):
    # Sweep for abandoned messages every second, otherwise kombu scans only
    # every unacked_restore_interval * unacked_restore_throttle (100s).
    transport_options.update(unacked_restore_interval=1, unacked_restore_throttle=1)
if CONFIG.get('confirm_publish'):
    transport_options['confirm_publish'] = True
if CONFIG.get('result_chord_ordered') is not None:
    result_transport_options = {'result_chord_ordered': CONFIG['result_chord_ordered']}
else:
    result_transport_options = {}

app.conf.update(
    task_acks_late=CONFIG.get('acks_late', False),
    task_reject_on_worker_lost=CONFIG.get('reject_on_worker_lost', False),
    task_track_started=CONFIG.get('track_started', False),
    worker_prefetch_multiplier=CONFIG.get('prefetch_multiplier', 1),
    worker_deduplicate_successful_tasks=CONFIG.get('deduplicate_successful_tasks', False),
    worker_cancel_long_running_tasks_on_connection_loss=CONFIG.get('cancel_on_connection_loss', False),
    broker_transport_options=transport_options,
    result_backend_transport_options=result_transport_options,
    result_expires=None,
    result_extended=False,
    broker_connection_retry_on_startup=True,
    task_default_queue=CONFIG.get('queue', 'correctness'),
)
if CONFIG.get('result_backend_always_retry'):
    # Only set when asked: leaving it unset keeps each backend's own default.
    app.conf.result_backend_always_retry = True
if CONFIG.get('queue_type'):
    app.conf.task_default_queue_type = CONFIG['queue_type']
    app.conf.worker_detect_quorum_queues = True


def record(op):
    op.setdefault('time', time.time())
    RECORDER.rpush('events', json.dumps(op))


def _execution(f, value, task):
    token = random.getrandbits(62)
    value = dict(value, token=token, attempt=task.request.retries)
    delivery = task.request.delivery_info or {}
    process = f'{WORKER}.{os.getpid()}.{token}'
    meta = {'tid': task.request.id, 'worker': WORKER, 'host': socket.gethostname(),
            'redelivered': delivery.get('redelivered')}
    record(dict(meta, process=process, type='invoke', f=f, value=value))
    return token, process, value, meta


@app.task(bind=True)
def execute(self, op, duration):
    token, process, value, meta = _execution('execute', {'op': op}, self)
    time.sleep(duration)
    record(dict(meta, process=process, type='ok', f='execute', value=value))
    return token


@app.task(bind=True)
def header(self, op, part, duration):
    token, process, value, meta = _execution('execute-header', {'op': op, 'part': part}, self)
    time.sleep(duration)
    record(dict(meta, process=process, type='ok', f='execute-header', value=value))
    return token


@app.task(bind=True)
def body(self, results, op):
    token, process, value, meta = _execution('execute-body', {'op': op}, self)
    record(dict(meta, process=process, type='ok', f='execute-body', value=dict(value, input=results)))
    return token


@app.task(bind=True)
def flaky(self, op, fails, duration, countdown):
    """Fails ``fails`` times through ``self.retry`` before succeeding."""
    token, process, value, meta = _execution('execute-retry', {'op': op}, self)
    time.sleep(duration)
    if self.request.retries < fails:
        record(dict(meta, process=process, type='ok', f='execute-retry', value=dict(value, outcome='retry')))
        raise self.retry(countdown=countdown, max_retries=fails)
    record(dict(meta, process=process, type='ok', f='execute-retry', value=dict(value, outcome='success')))
    return token


@app.task(bind=True)
def scheduled(self, op, eta):
    """Published with a countdown; ``eta`` is the wall-clock time it was due."""
    token, process, value, meta = _execution('execute-eta', {'op': op, 'eta': eta}, self)
    record(dict(meta, process=process, type='ok', f='execute-eta', value=value))
    return token


@app.task(bind=True)
def step(self, previous, op, part, duration):
    """One link of a chain; ``previous`` is the parent's return value."""
    token, process, value, meta = _execution('execute-chain', {'op': op, 'part': part, 'input': previous}, self)
    time.sleep(duration)
    record(dict(meta, process=process, type='ok', f='execute-chain', value=value))
    return token


def _crash(point):
    """Protocol-step fault: SIGKILL this worker's whole process group."""
    record({'process': 'nemesis', 'type': 'info', 'f': 'crash-hook',
            'value': {'point': point, 'worker': WORKER}})
    os.killpg(os.getpgid(os.getppid()), signal.SIGKILL)


def _maybe(point):
    p = CONFIG.get('crash_hooks', {}).get(point, 0)
    return p and random.random() < p


@signals.task_success.connect
def _after_store_before_ack(sender=None, **kwargs):
    # Runs after the result (and chord part) is stored, before the ack.
    if _maybe('after_store_before_ack'):
        _crash('after_store_before_ack')


@signals.task_prerun.connect
def _before_execute(sender=None, **kwargs):
    # Runs after the message was received (and early-acked, if enabled).
    if _maybe('before_execute'):
        _crash('before_execute')


@signals.task_retry.connect
def _after_retry_before_ack(sender=None, **kwargs):
    # Runs after the retry message was published, before the original is acked.
    if _maybe('after_retry_before_ack'):
        _crash('after_retry_before_ack')


@signals.task_postrun.connect
def _child_exit_after_store(sender=None, **kwargs):
    # Pool child dies after the result is stored; the parent survives
    # and sees WorkerLostError (the path from celery/celery#10724).
    if _maybe('child_exit_after_store'):
        record({'process': 'nemesis', 'type': 'info', 'f': 'crash-hook',
                'value': {'point': 'child_exit_after_store', 'worker': WORKER}})
        os._exit(1)
