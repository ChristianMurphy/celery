"""worker.qnt, instances pr_late_cut / resend_late_cut (property
noAvoidableDup): an ack that fails with its connection, for a task that
has finished.

kombu's Redis transport against a real Redis (6379). A consumer receives
one message; its ack fails with a connection error (injected: the one call
raises); the channel closes; a new connection consumes.

    PYTHONPATH=<kombu worktree> python adapter/confirm_ack_after_loss.py

kombu 445027e and c61b525: the closing channel puts the message back on
the queue, and it is delivered again. With the resend prototype: the ack is
sent again by the next channel, and nothing is delivered again.

The other half of that design, an ack through a channel whose connection
was collected, needs a worker: the runs are in the reply drafted for celery
issue 11054 (4 of 8 workers exit on main, 0 of 8 with the prototype).
"""
import os
import socket
import uuid

import kombu
import redis
from kombu import Connection, Consumer, Producer, Queue

URL = 'redis://127.0.0.1:6379/12'
raw = redis.Redis(port=6379, db=12)


def drain(conn, seconds=0.3):
    try:
        conn.drain_events(timeout=seconds)
    except (socket.timeout, TimeoutError):
        pass


def receive_one():
    queue = Queue(f'ackloss-{uuid.uuid4().hex}')
    with Connection(URL) as p:
        Producer(p).publish({'n': 1}, exchange='', routing_key=queue.name,
                            declare=[queue])
    conn = Connection(URL)
    held = []
    consumer = Consumer(conn, [queue], prefetch_count=1,
                        on_message=held.append)
    consumer.consume()
    while not held:
        drain(conn)
    return queue, conn, held[0]


def redelivered(queue):
    """Deliveries to a new consumer, and what Redis still has."""
    got = []
    with Connection(URL) as conn:
        consumer = Consumer(conn, [queue], prefetch_count=1,
                            on_message=lambda m: (got.append(1), m.ack()))
        consumer.consume()
        for _ in range(8):
            drain(conn, 0.25)
    return (f'delivered again: {len(got)}, left in unacked: '
            f'{raw.hlen("unacked")}')


print(f'kombu {os.path.dirname(os.path.dirname(kombu.__file__))}')
raw.delete('unacked', 'unacked_index')

queue, conn, message = receive_one()
qos = message.channel.qos
remove = qos._remove_from_indices


def fails_once(*args, **kwargs):
    qos._remove_from_indices = remove
    raise redis.exceptions.ConnectionError('injected: connection lost')


qos._remove_from_indices = fails_once
try:
    message.ack()
    outcome = 'ack returned'
except redis.exceptions.ConnectionError:
    outcome = 'ack failed'
conn.release()        # the channel closes and restores what it holds
print(f'failed: {outcome}; {redelivered(queue)}')
raw.delete('unacked', 'unacked_index')
