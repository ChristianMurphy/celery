import time
from collections import Counter

from celery import chord

from lost_tasks import app, part, total

bodies = []
for i in range(300):
    bodies.append(chord(part.s(j) for j in range(50))(total.s()))
    if i >= 2:
        try:
            bodies[i - 2].get(timeout=30, propagate=False)
        except Exception:  # still pending
            pass
time.sleep(30)
print(Counter(app.AsyncResult(b.id).state for b in bodies))
