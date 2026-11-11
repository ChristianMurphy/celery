"""worker.qnt: a channel restores by delivery tag, and a message keeps its
tag when it is delivered again. A consumer that held a message past the
visibility timeout, and then closes, restores the copy another consumer is
running.

kombu's Redis transport against a real Redis (6379), visibility timeout
2 s. A holds one message unacked (prefetched, or an ETA) for 3 s; B then
receives it and keeps it unacked (its task runs); A closes; C consumes.

    PYTHONPATH=<kombu worktree> python findings/restore-by-tag/confirm_restore_by_tag.py

kombu 445027e and c61b525: C receives the message while B still runs it, a
third delivery, and B's copy has no record any more.
"""
import os
import socket
import time
import uuid

import kombu
import redis
from kombu import Connection, Consumer, Producer, Queue

URL = 'redis://127.0.0.1:6379/12'
OPTIONS = {'visibility_timeout': 2, 'unacked_restore_throttle': 1}
raw = redis.Redis(port=6379, db=12)
raw.delete('unacked', 'unacked_index')
queue = Queue(f'bytag-{uuid.uuid4().hex}')


def drain(conn, seconds=0.2):
    try:
        conn.drain_events(timeout=seconds)
    except (socket.timeout, TimeoutError):
        pass


def consume(conn, into):
    consumer = Consumer(conn, [queue], prefetch_count=1,
                        on_message=into.append)
    consumer.consume()
    return consumer


with Connection(URL) as p:
    Producer(p).publish({'n': 1}, exchange='', routing_key=queue.name,
                        declare=[queue])
a, b, c = (Connection(URL, transport_options=OPTIONS) for _ in range(3))
at_a, at_b, at_c = [], [], []
keep = [consume(a, at_a)]
while not at_a:
    drain(a)
time.sleep(3)
keep.append(consume(b, at_b))
deadline = time.monotonic() + 5
while not at_b and time.monotonic() < deadline:
    drain(b)
recorded = raw.hlen('unacked')
a.release()                      # A's channel closes and restores its tag
keep.append(consume(c, at_c))
for _ in range(10):
    drain(c)
print(f'kombu {os.path.dirname(os.path.dirname(kombu.__file__))}')
print(f'deliveries: A {len(at_a)}, B {len(at_b)} (unacked, still running), '
      f'C {len(at_c)} | same delivery tag: '
      f'{len({m.delivery_tag for m in at_a + at_b + at_c}) == 1} | records '
      f'in unacked before A closed: {recorded}')
raw.delete('unacked', 'unacked_index', queue.name)
