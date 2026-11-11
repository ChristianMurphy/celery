"""Workers for the natural reproduction of the restore-by-tag duplicate."""
import os

import redis
from celery import Celery

app = Celery('bytagapp', broker='redis://127.0.0.1:6379/14')
app.conf.update(
    broker_transport_options={'visibility_timeout': 6,
                              'unacked_restore_interval': 1,
                              'unacked_restore_throttle': 1},
    task_acks_late=True, worker_prefetch_multiplier=4,
    task_default_queue='bytag', result_backend=None,
)
runs = redis.Redis(port=6379, db=13)


@app.task(name='bytag.work')
def work(n):
    runs.rpush(f'run:{n}', os.environ.get('WORKER', '?'))
