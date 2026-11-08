from collections import Counter

from celery import chord

from incr_tasks import part, total

outcomes = Counter()
for _ in range(3):
    result = chord([part.s(3), part.s(12)])(total.s())
    try:
        outcomes[repr(result.get(timeout=120))] += 1
    except Exception as exc:  # the chord's error
        outcomes[repr(exc)[:90]] += 1
print(dict(outcomes))
