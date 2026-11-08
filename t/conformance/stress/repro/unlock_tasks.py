import os
import time

from celery import Celery

app = Celery("unlock_tasks",
             broker=os.environ.get("BROKER", "redis://localhost:6379/0"),
             backend=os.environ.get("BACKEND", "db+postgresql://postgres:pw@localhost/celery"))
app.conf.task_default_queue = os.environ.get("QUEUE", "celery")


@app.task
def part(i):
    time.sleep(0.05)
    return i


@app.task
def total(results):
    return len(results)
