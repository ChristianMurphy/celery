import os

from celery import Celery

app = Celery("retry_tasks",
             broker=os.environ.get("BROKER", "redis://localhost:6379/0"),
             backend=os.environ.get("BACKEND", "redis://localhost:6379/1"))
app.conf.task_default_queue = os.environ.get("QUEUE", "celery")


@app.task(autoretry_for=(RuntimeError,), retry_backoff=True, max_retries=1)
def flaky():
    raise RuntimeError("fails every time")
