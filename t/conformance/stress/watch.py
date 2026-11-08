"""Watch Redis keys from outside: keyspace notifications on our own server.

`CONFIG SET notify-keyspace-events E$` is a server setting (stress-redis is
ours), not a change to Celery. Every SET/SETEX of a key in the watched DB
is timestamped on receipt and handed to a callback.
"""
from __future__ import annotations

import threading
import time

import redis


class KeyWatch(threading.Thread):
    def __init__(self, url: str, db: int, callback=None):
        super().__init__(daemon=True)
        self.r = redis.Redis.from_url(f'{url}/{db}')
        self.r.config_set('notify-keyspace-events', 'E$')
        self.ps = self.r.pubsub()
        self.ps.subscribe(f'__keyevent@{db}__:set')
        self.callback = callback
        self.log: list[tuple[float, str]] = []
        self.stop_flag = False
        self.ready = threading.Event()

    def run(self):
        while not self.stop_flag:
            m = self.ps.get_message(timeout=0.2)
            if m is None:
                continue
            if m['type'] == 'subscribe':
                self.ready.set()
                continue
            t = time.time()
            key = m['data'].decode()
            self.log.append((t, key))
            if self.callback:
                self.callback(key, t)

    def first(self, key: str, after: float = 0) -> float | None:
        for t, k in self.log:
            if k == key and t >= after:
                return t
        return None

    def all(self, key: str) -> list[float]:
        return [t for t, k in self.log if k == key]

    def stop(self):
        self.stop_flag = True
