"""Confirm redelivery.qnt's RabbitMQ counterexamples on the real code.

A real prefork worker (-c 1, prefetch multiplier 1, acks_late) on a real
RabbitMQ broker with consumer_timeout scaled to 5 s (checked every second),
a Redis result backend, and a supervisor loop in this script that starts
the worker again whenever it exits. Cases:

  exit    a 9 s task on a classic queue: the model (classic_late_long) says
          the channel closes at the timeout and returns the message, the
          worker stops warm, the run finishes and its ack goes to the
          closed channel, and the restarted worker gets the message again:
          a loop.
  retry   the same with broker_channel_error_retry (classic_retry_long): the
          worker reconnects instead of exiting; still a loop.
  dedup   the same with worker_deduplicate_successful_tasks
          (classic_dedup_long): the loop ends.
  quorum  a quorum queue with x-delivery-limit 2 on RabbitMQ 4
          (quorum_late_long): the loop ends at the limit, with SUCCESS
          stored.
  kill    a quorum queue with x-delivery-limit 1 on RabbitMQ 4, and a task
          that kills its own worker (SIGKILL of the main process and the
          child, as an out-of-memory kill would) (quorum_late_kill): the
          message is dropped after the limit, the state stays PENDING and
          nothing logs the drop (documented, rabbitmq.rst:246-257).

    PYTHONPATH=<celery worktree>:<kombu worktree> python confirm_consumer_timeout_loop.py [seconds] [case ...]

Brokers: amqp://localhost:5682 (3.13) and amqp://localhost:5692 (4.1),
both started with -rabbit consumer_timeout 5000 -rabbit
channel_tick_interval 1000. Nothing else is injected. Run it from a
directory outside the checkouts.
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

BACKEND = 'redis://localhost:6379/14'
LOG_DB = 15

APP = '''
import os
import signal
import time
import redis
from celery import Celery
from kombu import Queue

r = redis.Redis(db=%(log)d)
app = Celery('ctapp', broker=%(broker)r, backend=%(backend)r)
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.broker_channel_error_retry = %(retry)r
app.conf.worker_deduplicate_successful_tasks = %(dedup)r
app.conf.task_default_queue = %(queue)r
app.conf.task_queues = [Queue(%(queue)r, queue_arguments=%(qargs)r)]
app.conf.broker_transport_options = {'confirm_publish': True}


@app.task
def slow(secs, kill):
    r.rpush('runs', '%%.1f' %% time.time())
    if kill:
        os.kill(os.getppid(), signal.SIGKILL)
        os.kill(os.getpid(), signal.SIGKILL)
    time.sleep(secs)
    return 'done'
'''

CASES = {
    # name: (broker, retry, dedup, queue arguments, task seconds, kill)
    'exit': ('amqp://guest:guest@localhost:5682//', False, False, {}, 9, False),
    'retry': ('amqp://guest:guest@localhost:5682//', True, False, {}, 9, False),
    'dedup': ('amqp://guest:guest@localhost:5682//', False, True, {}, 9, False),
    'quorum': ('amqp://guest:guest@localhost:5692//', False, False,
               {'x-queue-type': 'quorum', 'x-delivery-limit': 2}, 9, False),
    'kill': ('amqp://guest:guest@localhost:5692//', False, False,
             {'x-queue-type': 'quorum', 'x-delivery-limit': 1}, 1, True),
}


def run(name: str, secs: float) -> None:
    broker, retry, dedup, qargs, task_secs, kill = CASES[name]
    queue = f'ct-{name}-{uuid.uuid4().hex[:6]}'
    d = tempfile.mkdtemp()
    with open(os.path.join(d, 'ctapp.py'), 'w') as f:
        f.write(APP % {'broker': broker, 'backend': BACKEND, 'log': LOG_DB,
                       'retry': retry, 'dedup': dedup, 'queue': queue,
                       'qargs': qargs})
    env = dict(os.environ, PYTHONPATH=d + os.pathsep
               + os.environ.get('PYTHONPATH', ''))
    for db in (14, LOG_DB):
        redis.Redis(db=db).flushdb()
    logpath = os.path.join(d, 'worker.log')
    log = open(logpath, 'w')

    def start():
        return subprocess.Popen(
            [sys.executable, '-m', 'celery', '-A', 'ctapp', 'worker', '-P',
             'prefork', '-c', '1', '-l', 'info', '--without-gossip',
             '--without-mingle', '--without-heartbeat'],
            cwd=d, env=env, stdout=log, stderr=subprocess.STDOUT)

    sys.path.insert(0, d)
    sys.modules.pop('ctapp', None)
    import ctapp  # noqa: E402
    worker = start()
    time.sleep(4)
    t0 = time.time()
    res = ctapp.slow.delay(task_secs, kill)
    starts = 1
    try:
        while time.time() - t0 < secs:
            if worker.poll() is not None:
                worker = start()     # the supervisor
                starts += 1
            time.sleep(0.2)
    finally:
        worker.terminate()
        try:
            worker.wait(30)
        except subprocess.TimeoutExpired:
            worker.kill()
        sys.path.remove(d)
    runs = [float(x) - t0 for x in redis.Redis(db=LOG_DB).lrange('runs', 0, -1)]
    state = res.backend.get_task_meta(res.id, cache=False)['status']
    text = open(logpath).read()
    with ctapp.app.connection_for_write() as conn:
        left = conn.default_channel.queue_declare(
            queue, passive=True).message_count
    print(f'{name}: {broker.rsplit("@", 1)[1]} {qargs or "classic"}, '
          f'broker_channel_error_retry {retry}, dedup {dedup}, task '
          f'{task_secs} s, consumer_timeout 5 s, watched {secs:.0f} s: '
          f'{len(runs)} runs (starts at {", ".join(f"{t:.1f}" for t in runs)} s), '
          f'worker starts {starts}, PRECONDITION_FAILED logged '
          f'{text.count("PRECONDITION_FAILED")}, "Couldn\'t ack" logged '
          f'{text.count("Couldn" + chr(39) + "t ack")}, skipped as done '
          f'{text.count("Task already completed successfully")}, state {state}, '
          f'left on the queue {left}')


def main() -> int:
    secs = float(sys.argv[1]) if len(sys.argv) > 1 else 60
    cases = sys.argv[2:] or list(CASES)
    print('celery', celery.__version__, celery.__file__)
    print('kombu', kombu.__version__, kombu.__file__)
    for name in cases:
        run(name, secs)
    return 0


if __name__ == '__main__':
    sys.exit(main())
