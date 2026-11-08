"""Shared helpers for the stress drivers: services, worker, evidence, stats."""
from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
TREE = Path(os.environ.get('STRESS_TREE', '/home/user/wt/main'))
PY = os.environ.get('STRESS_PY', '/home/user/.venv314/bin/python')
RESULTS = HERE / 'results'

# Services of our own (containers named stress-*), see README.md.
REDIS = 'redis://127.0.0.1:16579'
MEMCACHED = 'cache+memcached://127.0.0.1:11311/'
RABBIT = 'amqp://guest:guest@127.0.0.1:5682//'
POSTGRES = 'db+postgresql+psycopg://postgres:pw@127.0.0.1:5442/stress'
EV_URL = f'{REDIS}/29'          # evidence: a Redis DB that is neither broker nor backend


def wilson(n: int, m: int, z: float = 1.96) -> tuple[float, float]:
    if m == 0:
        return (0.0, 1.0)
    p = n / m
    d = 1 + z * z / m
    c = (p + z * z / (2 * m)) / d
    h = z * math.sqrt(p * (1 - p) / m + z * z / (4 * m * m)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def fmt_rate(n: int, m: int) -> str:
    lo, hi = wilson(n, m)
    return f'{n} of {m} (95% Wilson {lo:.1%} to {hi:.1%})'


def load() -> str:
    return '%.2f %.2f %.2f' % os.getloadavg()


def base_env(**cfg) -> dict:
    env = dict(os.environ)
    env['PYTHONPATH'] = f'{TREE}:{HERE}'
    env['STRESS_EV_URL'] = EV_URL
    for k, v in cfg.items():
        env[f'STRESS_{k.upper()}'] = str(v)
    return env


def apply_env(env: dict) -> None:
    """Configure this (driver) process the same way as the worker."""
    for k, v in env.items():
        if k.startswith('STRESS_'):
            os.environ[k] = v
    sys.path[:0] = [str(TREE), str(HERE)]


def proof() -> str:
    import celery
    import kombu
    return f'celery {celery.__version__} {celery.__file__}; kombu {kombu.__version__} {kombu.__file__}'


class Worker:
    """A celery worker as a separate process group (so it can be lost whole)."""

    def __init__(self, env: dict, log: Path, pool='prefork', concurrency=2,
                 extra=(), docker: dict | None = None):
        self.name = f'stress-{uuid.uuid4().hex[:6]}'
        args = ['-m', 'celery', '-A', 'stress_app', 'worker', '-P', pool,
                '-c', str(concurrency), '-l', 'info', '--without-gossip',
                '--without-mingle', '--without-heartbeat',
                '-n', f'{self.name}@%h', *extra]
        self.log = log
        self.docker = docker
        if docker:
            # Same interpreter and trees mounted at the same paths; host
            # network; resource limits as a Kubernetes pod would have.
            cmd = ['docker', 'run', '--rm', '--name', self.name,
                   '--network', 'host',
                   '-v', '/home/user:/home/user:ro',
                   '-v', '/root/.local/share/uv:/root/.local/share/uv:ro',
                   '-w', str(HERE)]
            if docker.get('memory'):
                cmd += ['--memory', docker['memory'], '--memory-swap', docker['memory']]
            if docker.get('cpus'):
                cmd += ['--cpus', str(docker['cpus'])]
            for k, v in env.items():
                if k.startswith('STRESS_') or k == 'PYTHONPATH':
                    cmd += ['-e', f'{k}={v}']
            cmd += ['-e', 'PYTHONDONTWRITEBYTECODE=1',
                    'mirror.gcr.io/library/debian:bookworm-slim', PY, *args]
            self.proc = subprocess.Popen(cmd, stdout=open(log, 'w'),
                                         stderr=subprocess.STDOUT)
        else:
            self.proc = subprocess.Popen([PY, *args], cwd=HERE, env=env,
                                         stdout=open(log, 'w'),
                                         stderr=subprocess.STDOUT,
                                         start_new_session=True)

    def wait_ready(self, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            if 'ready.' in self.log.read_text(errors='replace'):
                return
            if self.proc.poll() is not None:
                raise RuntimeError(f'worker exited: {self.log}')
            time.sleep(0.2)
        raise RuntimeError(f'worker not ready: {self.log}')

    def children(self) -> list[int]:
        """Pool children (pids on this host) of the worker's main process."""
        main = self.main_pid()
        if main is None:
            return []
        out = subprocess.run(['pgrep', '-P', str(main)], capture_output=True, text=True)
        return [int(x) for x in out.stdout.split()]

    def main_pid(self):
        if self.docker:
            out = subprocess.run(['docker', 'inspect', '-f', '{{.State.Pid}}', self.name],
                                 capture_output=True, text=True)
            pid = out.stdout.strip()
            return int(pid) if pid and pid != '0' else None
        return self.proc.pid

    def kill_all(self):
        """Node loss: SIGKILL the whole worker (main process and children)."""
        if self.docker:
            subprocess.run(['docker', 'kill', self.name], capture_output=True)
        else:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.proc.wait(30)

    def stop(self):
        if self.proc.poll() is not None:
            return
        if self.docker:
            subprocess.run(['docker', 'kill', '-s', 'TERM', self.name], capture_output=True)
        else:
            os.killpg(self.proc.pid, signal.SIGTERM)
        try:
            self.proc.wait(30)
        except subprocess.TimeoutExpired:
            self.kill_all()


def events(key: str) -> list[dict]:
    import redis
    r = redis.Redis.from_url(EV_URL)
    return [json.loads(x) for x in r.lrange(key, 0, -1)]


def save(name: str, data: dict) -> Path:
    RESULTS.mkdir(exist_ok=True)
    p = RESULTS / f'{name}.json'
    p.write_text(json.dumps(data, indent=1, default=str))
    return p
