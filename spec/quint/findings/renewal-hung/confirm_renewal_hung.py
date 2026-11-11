"""worker.qnt, instances pr_late_hang / renewall_late_hang (property
quietDone): a consumer that is alive and never acks, as a worker whose task
hangs, holds one message it started and one it only prefetched.

kombu's Redis transport against a real Redis (6379), visibility timeout 2 s.
Consumer A takes both messages and acks neither; consumer B waits for 9 s.

    PYTHONPATH=<kombu worktree> python findings/renewal-hung/confirm_renewal_hung.py

kombu 445027e: B gets both messages after the timeout (the recovery of a
hung task, and a duplicate if A's task was only slow). With the renewal
prototype (every unacked tag renewed): B gets neither while A lives.
"""
import os
import socket
import time
import uuid

import kombu
from kombu import Connection, Consumer, Producer, Queue

URL = 'redis://127.0.0.1:6379/12'
OPTIONS = {'visibility_timeout': 2, 'unacked_restore_throttle': 1,
           'unacked_restore_interval': 1}
queue = Queue(f'hung-{uuid.uuid4().hex}', no_declare=False)


def drain(conn, seconds):
    try:
        conn.drain_events(timeout=seconds)
    except (socket.timeout, TimeoutError):
        pass


a, b = (Connection(URL, transport_options=OPTIONS) for _ in range(2))
held, got = [], []
with Connection(URL) as p:
    producer = Producer(p)
    for body in ('started', 'prefetched'):
        producer.publish({'task': body}, exchange='', routing_key=queue.name,
                         declare=[queue])
ca = Consumer(a, [queue], prefetch_count=2,
              on_message=lambda m: held.append(m))
ca.consume()
while len(held) < 2:
    drain(a, 0.2)
cb = Consumer(b, [queue], prefetch_count=2,
              on_message=lambda m: (got.append(m.payload['task']), m.ack()))
cb.consume()
start = time.monotonic()
while time.monotonic() - start < 9:
    drain(a, 0.1)
    drain(b, 0.1)
print(f'kombu {os.path.dirname(os.path.dirname(kombu.__file__))}')
print(f'A holds {len(held)} unacked for 9 s, visibility timeout 2 s; '
      f'B received: {got}')
for conn in (b, a):
    conn.release()
