import os
import time

from celery import Celery

app = Celery("lost_tasks",
             broker=os.environ.get("BROKER", "redis://localhost:6379/0"),
             backend=os.environ.get("BACKEND", "redis://localhost:6379/1"))
app.conf.task_acks_late = True
app.conf.task_reject_on_worker_lost = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_default_queue = os.environ.get("QUEUE", "celery")


@app.task
def part(i):
    time.sleep(0.05)
    return "x" * 20_000


@app.task
def total(results):
    return len(results)
