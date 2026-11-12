"""Confirm routing.qnt's rabbit_undeclared counterexamples on the real code.

A task is published to an exchange that was never declared
(apply_async(exchange='ghost')), then a second, correct task to the
default queue on the same connection. A worker consumes the default
queue. Nothing is injected: the exchange name stands for a typo or a
route to an exchange nobody declared.

    rabbit        RabbitMQ (5682), confirm_publish off (the default)
    rabbitconfirm RabbitMQ, confirm_publish on (the control)
    redis         Redis broker (6379 db 10)

For each run: what each apply_async did (returned or raised, and the
error), whether each task ran (a mark in Redis db 15 written by the
task), and the depth of the default queue.

    PYTHONPATH=<celery>:<kombu> python confirm_undeclared_exchange.py <case> [runs]

Run it from a directory outside the checkouts.
"""
import os
import subprocess
import sys
import tempfile
import time

import celery
import kombu
import redis

APP = '''
import redis
from celery import Celery
import os
r = redis.Redis(db=15)
app = Celery('uxapp', broker=os.environ['UX_BROKER'],
             backend='redis://localhost:6379/11')
if os.environ.get('UX_CONFIRM'):
    app.conf.broker_transport_options = {'confirm_publish': True}
app.conf.worker_hijack_root_logger = False

@app.task(name='uxapp.mark')
def mark(tag):
    r.rpush('ux:ran', tag)
    return tag
'''

BROKERS = {'rabbit': 'amqp://guest:guest@localhost:5682//',
           'rabbitconfirm': 'amqp://guest:guest@localhost:5682//',
           'redis': 'redis://localhost:6379/10'}


def run(case, d):
    os.environ['UX_BROKER'] = BROKERS[case]
    if case == 'rabbitconfirm':
        os.environ['UX_CONFIRM'] = '1'
    else:
        os.environ.pop('UX_CONFIRM', None)
    r = redis.Redis(db=15)
    r.delete('ux:ran')
    redis.Redis(db=10).flushdb()
    sys.modules.pop('uxapp', None)
    import uxapp
    app = uxapp.app
    with app.connection_for_write() as c:
        try:
            c.default_channel.queue_purge('celery')
        except Exception:
            pass
    log = open(os.path.join(d, f'worker_{case}.log'), 'w')
    w = subprocess.Popen([sys.executable, '-m', 'celery', '-A', 'uxapp',
                          'worker', '-P', 'solo', '-l', 'info',
                          '--without-heartbeat', '--without-gossip',
                          '--without-mingle'], cwd=d, stdout=log,
                         stderr=subprocess.STDOUT)
    time.sleep(4)
    out = []
    for tag, opts in (('ghost', {'exchange': 'ghost', 'routing_key': 'ghost'}),
                      ('victim', {}), ('third', {})):
        try:
            uxapp.mark.apply_async((tag,), **opts)
            out.append(f'{tag}: returned')
        except Exception as e:
            out.append(f'{tag}: raised {type(e).__name__}: {str(e)[:70]}')
        time.sleep(0.3)
    time.sleep(3)
    ran = [x.decode() for x in r.lrange('ux:ran', 0, -1)]
    w.terminate()
    w.wait()
    return out, ran


def main():
    case = sys.argv[1]
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='ux_')
    open(os.path.join(d, 'uxapp.py'), 'w').write(APP)
    sys.path.insert(0, d)
    for i in range(runs):
        out, ran = run(case, d)
        print(f'{case} run {i + 1}: ' + '; '.join(out) + f'; ran: {ran}')


if __name__ == '__main__':
    main()
