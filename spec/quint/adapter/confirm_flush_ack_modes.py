"""worker.qnt, instances pr_early_shutdown / pr_late_shutdown and
keepwritten_*: flush() between the write of a job and the read of its
accept, with early and with late ack.

A Request built from a message, the prefork pool and its event loop, no
broker: the job is written to the pool process, then flush() runs as
Consumer.on_close() calls it at a warm shutdown, then the loop turns until
the child is done.

    PYTHONPATH=<celery worktree> python adapter/confirm_flush_ack_modes.py

768e476: with either ack mode the child runs the task, the parent never
gets the result and the message is never acked, so the shutdown restores it
and it runs again. With `elif job._write_to is None` in flush() (the
keep-written variant): the result arrives and the message is acked.
"""
import os
import sys
import tempfile
import time

import celery
from celery import Celery
from celery.concurrency.prefork import TaskPool
from celery.worker.request import Request
from kombu.asynchronous import Hub
from kombu.message import Message
from kombu.serialization import dumps
from kombu.utils.uuid import uuid

app = Celery('flushack', broker='memory://', set_as_current=True)
app.conf.result_backend = None
marker_dir = tempfile.mkdtemp()


@app.task(name='flushack.touch', shared=False)
def touch(path):
    open(path, 'w').close()


class Channel:
    """Where the message's ack goes: only records it."""

    no_ack_consumers = set()

    def __init__(self):
        self.acked = []

    def basic_ack(self, delivery_tag, multiple=False):
        self.acked.append(delivery_tag)


def task_message(channel, path):
    headers, properties, body, _ = app.amqp.as_task_v2(
        uuid(), 'flushack.touch', args=(path,))
    content_type, encoding, data = dumps(body, 'json')
    return Message(body=data, channel=channel, delivery_tag=uuid(),
                   content_type=content_type, content_encoding=encoding,
                   headers=headers, properties=properties,
                   delivery_info={'exchange': '', 'routing_key': 'celery'})


def run(acks_late):
    marker = os.path.join(marker_dir, f'ran-{acks_late}')
    hub = Hub()
    pool = TaskPool(1, app=app, threads=False, forking_enable=True,
                    initargs=(app, 'worker@host'))
    pool.start()
    pool.register_with_event_loop(hub)
    touch.acks_late = acks_late
    channel = Channel()
    message = task_message(channel, marker)
    # as the consumer's strategy builds it (worker/strategy.py)
    request = Request(message, app=app, task=touch, hostname='worker@host',
                      on_ack=message.ack_log_error)
    result = request.execute_using_pool(pool)
    loop = hub.create_loop()
    # Turn the loop until the job is written, not until its accept is read.
    while result._write_to is None or pool._pool._active_writers:
        next(loop)
    written, accepted = result._write_to is not None, result._accepted
    pool.flush()
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline and not (
            os.path.exists(marker) and channel.acked):
        next(loop)
        time.sleep(0.01)
    print(f'{"late" if acks_late else "early"} ack: written {written}, '
          f'accepted before flush {accepted} | child ran the task: '
          f'{os.path.exists(marker)} | message acked: {bool(channel.acked)}')
    pool.terminate()


print(f'celery {os.path.dirname(os.path.dirname(celery.__file__))}')
for mode in (False, True):
    run(mode)
sys.stdout.flush()
os._exit(0)
