"""redis_transport.qnt, instance renewblind_held_credit (property
noStranded): kombu PR 2831's put-back next to the renewal prototype for
kombu issue 337.

kombu's Redis transport (the PR with the renewal patch on top) against a
real Redis (6379), visibility timeout 2 s:

  1. A receives m and is blocked for longer than the timeout
  2. B restores m and pops it: the script records it under the same tag
  3. B loses its credit before it reads the reply
  4. A runs again and renews what it holds (`renew`), or not (`control`)
  5. B reads the reply and puts the message back

    PYTHONPATH=<kombu c61b525 + kombu-eo-lease.patch> \
        python adapter/confirm_putback_renewal.py

control: m is back on the queue and its record is gone. renew: A's ZADD XX
moved the score of B's record, PUT_BACK compares the score and does nothing,
and B drops the reply: m is on no queue and stays in unacked, renewed by A,
until A acks its own copy or exits.
"""
import os
import socket
import sys
import time
import uuid

import kombu
import redis
from kombu import Connection, Consumer, Producer, Queue

URL = 'redis://127.0.0.1:6379/12'
OPTIONS = {'visibility_timeout': 2}
raw = redis.Redis(port=6379, db=12)


def drain(conn, seconds=0.3):
    try:
        conn.drain_events(timeout=seconds)
    except (socket.timeout, TimeoutError):
        pass


def run(renew):
    raw.delete('unacked', 'unacked_index')
    q, q0 = (Queue(f'putback-{uuid.uuid4().hex}') for _ in range(2))
    with Connection(URL) as p:
        producer = Producer(p)
        producer.publish({'n': 'm'}, exchange='', routing_key=q.name,
                         declare=[q])
        producer.publish({'n': 'other'}, exchange='', routing_key=q0.name,
                         declare=[q0])
    a, b = (Connection(URL, transport_options=OPTIONS) for _ in range(2))
    held = []
    ca = Consumer(a, [q], prefetch_count=1, on_message=held.append)
    ca.consume()
    while not held:
        drain(a)
    tag = held[0].delivery_tag
    time.sleep(2.5)                          # 1. A is blocked
    got = []
    cb = Consumer(b, [q], prefetch_count=1, on_message=got.append)
    cb.consume()
    chan = cb.channel
    chan.qos.restore_visible(interval=1)     # 2. B restores m ...
    b.transport.cycle._register_BRPOP(chan)  #    ... and its script pops it
    time.sleep(0.2)
    recorded = raw.zscore('unacked_index', tag)
    chan.basic_get(q0.name)                  # 3. B's credit is used up
    if renew:
        ca.channel.qos.renew_unacked()       # 4. A renews its tags
    drain(b, 1)                              # 5. B reads the reply
    print(f'{"renew" if renew else "control"}: B received m: {bool(got)} | '
          f'm on the queue: {raw.llen(q.name)} | record under its tag: '
          f'{bool(raw.hexists("unacked", tag))} | score moved since the '
          f'pop: {raw.zscore("unacked_index", tag) not in (None, recorded)}')
    raw.delete('unacked', 'unacked_index', q.name, q0.name)
    for conn in (a, b):
        try:
            conn.release()
        except Exception:  # pylint: disable=broad-except
            pass
    raw.delete('unacked', 'unacked_index', q.name, q0.name)


print(f'kombu {os.path.dirname(os.path.dirname(kombu.__file__))}')
for mode in (False, True):
    run(mode)
sys.stdout.flush()
