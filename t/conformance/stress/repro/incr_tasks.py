import os
import time

from celery import Celery

app = Celery("incr_tasks",
             broker=os.environ.get("BROKER", "redis://localhost:6379/0"),
             backend=os.environ.get("BACKEND", "cache+memcached://localhost:11211/"))
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
# visibility_timeout is 1 hour by default; 2 seconds stands for it here,
# with tasks of 3 and 12 seconds standing for tasks of 1.5 and 6 hours.
app.conf.broker_transport_options = {
    "visibility_timeout": 2,
    "unacked_restore_interval": 1,
    "unacked_restore_throttle": 1,
}
app.conf.task_default_queue = os.environ.get("QUEUE", "celery")


@app.task
def part(seconds):
    time.sleep(seconds)
    return seconds


@app.task
def total(results):
    return sum(results)
