"""Confirm badmsg.qnt's counterexamples on the real code.

unknown (unknown_errback, unknown_chain, unknown_chord .failurePropagates):
a task name the worker does not know. on_unknown_task rejects the
message and stores FAILURE (NotRegistered) with a request of its own,
chord None and errbacks None and no chain (consumer.py:752-783), so the
errback is not called, the next step of a chain stays PENDING and the
chord's body is never run or failed.

raise: the control, a registered task that raises, in the same three
contexts.

undecodable, disallowed (outcomeVisible, documented): a body that is not
JSON, and a pickle message to a worker that accepts JSON only. The
message is acked or rejected, nothing is stored. The script also checks
that the worker stays up (open celery issue 4308): a good task sent
after the bad one runs.

One prefork worker -c 2, Redis broker and backend (6379, dbs 10 and 11;
marks in db 15). Nothing is injected.

    PYTHONPATH=<celery>:<kombu> python confirm_bad_messages.py <case> [runs]

Run it from a directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import time
import uuid

import celery
import kombu
import redis
from celery import chain, chord

APP = '''
import redis
from celery import Celery

r = redis.Redis(db=15)
app = Celery('bmapp', broker='redis://localhost:6379/10',
             backend='redis://localhost:6379/11')


@app.task
def quick(x):
    r.rpush('ran', f'quick{x}')
    return x


@app.task
def body(xs):
    r.rpush('ran', 'body')
    return xs


@app.task
def boom():
    r.rpush('ran', 'boom')
    raise ValueError('boom')


@app.task
def errback(*a, **kw):
    r.rpush('ran', 'errback')
'''


def one(case: str, d: str) -> str:
    for db in (10, 11, 15):
        redis.Redis(db=db).flushdb()
    env = dict(os.environ)
    env['PYTHONPATH'] = d + os.pathsep + env.get('PYTHONPATH', '')
    sys.path.insert(0, d)
    import bmapp
    app = bmapp.app
    log = open(os.path.join(d, f'worker_{case}.log'), 'w')
    w = subprocess.Popen(
        [sys.executable, '-m', 'celery', '-A', 'bmapp', 'worker', '-P',
         'prefork', '-c', '2', '-l', 'info', '--without-gossip',
         '--without-mingle', '--without-heartbeat'], cwd=d, env=env,
        stdout=log, stderr=subprocess.STDOUT)
    time.sleep(5)
    st = lambda i: app.backend.get_task_meta(i, cache=False)['status']
    r = redis.Redis(db=15)
    if case in ('unknown', 'raise'):
        sig = (app.signature('bmapp.not_deployed') if case == 'unknown'
               else bmapp.boom.si())
        a = sig.clone().apply_async(link_error=bmapp.errback.si())
        c = chain(sig.clone(), bmapp.quick.si(1)).apply_async()
        h = chord([sig.clone(), bmapp.quick.si(2)], bmapp.body.s()).apply_async()
        time.sleep(8)
        ran = [x.decode() for x in r.lrange('ran', 0, -1)]
        out = (f'{case}: alone {st(a.id)}, errback ran {ran.count("errback")}; '
               f'chain {st(c.parent.id)} then {st(c.id)}; '
               f'chord header {[st(x.id) for x in h.parent.results]} '
               f'body {st(h.id)}')
    else:
        tid = str(uuid.uuid4())
        if case == 'undecodable':
            with app.producer_or_acquire() as p:
                p.publish(b'{not json', exchange='', routing_key='celery',
                          content_type='application/json',
                          content_encoding='utf-8',
                          headers={'lang': 'py', 'task': 'bmapp.quick',
                                   'id': tid, 'root_id': tid,
                                   'argsrepr': '(9,)', 'kwargsrepr': '{}'})
        else:
            bmapp.quick.apply_async((9,), task_id=tid, serializer='pickle')
        time.sleep(3)
        good = bmapp.quick.delay(7)
        time.sleep(4)
        ran = [x.decode() for x in r.lrange('ran', 0, -1)]
        b = redis.Redis(db=10)
        out = (f'{case}: bad {st(tid)}, ran {ran}, good {st(good.id)}, '
               f'worker alive {w.poll() is None}, queued {b.llen("celery")}, '
               f'unacked {b.hlen("unacked")}')
    w.terminate()
    w.wait(20)
    log.close()
    text = open(os.path.join(d, f'worker_{case}.log')).read()
    for needle in ('Received unregistered task', "Can't decode message body",
                   'Received invalid task message', 'ContentDisallowed'):
        if needle in text:
            out += f'; log: {needle}'
    return out


def main() -> None:
    print('celery', celery.__file__, 'kombu', kombu.__file__)
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    for _ in range(runs):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'bmapp.py'), 'w') as f:
                f.write(APP)
            print(one(case, d), flush=True)
            sys.modules.pop('bmapp', None)
            sys.path.remove(d)


if __name__ == '__main__':
    main()
