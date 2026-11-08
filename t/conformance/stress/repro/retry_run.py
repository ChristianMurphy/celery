from collections import Counter

from retry_tasks import app, flaky

ids = []
for _ in range(150):
    batch = [flaky.delay() for _ in range(20)]
    for result in batch:
        try:
            result.get(timeout=10, propagate=False)
        except Exception:  # still not ready after 10 s
            pass
    ids += [result.id for result in batch]
print(Counter(app.AsyncResult(i).state for i in ids))
