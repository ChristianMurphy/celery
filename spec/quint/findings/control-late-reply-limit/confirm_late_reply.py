"""Confirm pidbox.qnt's reused_limit.noLostReply.

A late reply to one broadcast (reply=True) is taken by a later broadcast
from the same process that reuses the reply queue name, and counts toward
that call's limit: the later call returns [] although its worker answered.

One worker; the client is this process. Each run sends N pairs of
broadcasts, 1 s apart, and counts the second calls that do not return the
worker's pong.

    late     a control command that takes 1.5 s (timeout 1, so its reply
             is late), then at once ping(destination=[worker]): limit 1.
             Injected: the slow command.                   (the finding)
    solo     no injection: a solo-pool worker runs a 1.5 s task; ping
             (destination=[worker], timeout 1) while it runs, then at once
             ping again. The first reply is late because the worker is
             busy.                                         (the finding)
    nodest   control: as late, the second ping without a destination (no
             limit): it waits the whole timeout and returns the pong.
    gap      control: as late, with 1.5 s between the calls (the late reply
             arrives while no call waits): the pong is returned.

    PYTHONPATH=<celery>:<kombu> python -bb confirm_late_reply.py <redis|amqp> <case> [runs] [N]

Redis on 6391 db 12 (broker, flushed per run), or RabbitMQ on 5682.
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

BROKERS = {'redis': 'redis://localhost:6391/12',
           'amqp': 'amqp://guest:guest@localhost:5682//'}

APP = '''
import time
from celery import Celery
from celery.worker.control import control_command
app = Celery('cpapp', broker=%r)
app.conf.worker_hijack_root_logger = False

@control_command()
def slow_ping(state, **kw):
    time.sleep(1.5)
    return {'ok': 'slow pong'}

@app.task(name='cp.busy')
def busy():
    time.sleep(1.5)
'''


def main():
    broker, case = sys.argv[1], sys.argv[2]
    runs = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    n = int(sys.argv[4]) if len(sys.argv) > 4 else 10
    print('celery', celery.__file__, celery.__version__)
    print('kombu', kombu.__file__, kombu.__version__)
    d = tempfile.mkdtemp(prefix='cp_')
    open(os.path.join(d, 'cpapp.py'), 'w').write(APP % BROKERS[broker])
    sys.path.insert(0, d)
    import cpapp
    app = cpapp.app
    node = f'cp{os.getpid()}@probe'
    pong = [{node: {'ok': 'pong'}}]
    for k in range(runs):
        if broker == 'redis':
            redis.Redis(port=6391, db=12).flushdb()
        log = open(os.path.join(d, f'worker{k}.log'), 'w')
        cmd = [sys.executable, '-m', 'celery', '-A', 'cpapp', 'worker',
               '-l', 'info', '-n', node, '--without-gossip',
               '--without-mingle', '--without-heartbeat']
        cmd += ['-P', 'solo'] if case == 'solo' else ['-c', '1']
        w = subprocess.Popen(cmd, cwd=d, stdout=log,
                             stderr=subprocess.STDOUT)
        try:
            time.sleep(6)
            assert app.control.ping(destination=[node], timeout=2) == pong
            wrong, firsts, examples = 0, [], []
            for j in range(n):
                if case == 'solo':
                    cpapp.busy.delay()
                    time.sleep(0.2)
                    r1 = app.control.ping(destination=[node], timeout=1)
                else:
                    r1 = app.control.broadcast('slow_ping', reply=True,
                                               timeout=1,
                                               destination=[node])
                firsts.append(len(r1))
                if case == 'gap':
                    time.sleep(1.5)
                t = time.time()
                r2 = app.control.broadcast(
                    'ping', reply=True, timeout=1,
                    destination=None if case == 'nodest' else [node])
                if r2 != pong:
                    wrong += 1
                    examples.append(f'{r2} after {time.time() - t:.1f} s')
                time.sleep(1)
            assert app.control.ping(destination=[node], timeout=2) == pong
            un = sum(len(v) for v in app.control.mailbox.unclaimed.values())
            print(f'{broker} {case} run {k + 1}: {wrong} of {n} second calls '
                  f'returned no pong ({examples[:2]}); first calls with a '
                  f'reply: {sum(firsts)} of {n}; stale replies kept in '
                  f'unclaimed: {un}', flush=True)
        finally:
            w.terminate()
            w.wait()
    os._exit(0)


if __name__ == '__main__':
    main()
