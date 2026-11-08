"""A tiny client for Toxiproxy's HTTP API (https://github.com/Shopify/toxiproxy).

The concurrency checks put one writer's connection behind a proxy that delays
the server's replies, the way a slow network or a busy server would. That
widens the gap between a backend reading what is stored and writing over it,
without touching the backend's code.
"""
from __future__ import annotations

import contextlib
import json
import socket
import urllib.request

API = 'http://127.0.0.1:8474'


def _call(method, path, body=None):
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


def available() -> bool:
    try:
        _call('GET', '/version')
        return True
    except OSError:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def delayed(upstream_port: int, latency_ms: int, name: str):
    """Proxy ``upstream_port`` on a new port, delaying replies by ``latency_ms``."""
    port = _free_port()
    with contextlib.suppress(Exception):
        _call('DELETE', f'/proxies/{name}')
    _call('POST', '/proxies', {
        'name': name, 'listen': f'127.0.0.1:{port}',
        'upstream': f'127.0.0.1:{upstream_port}', 'enabled': True})
    try:
        _call('POST', f'/proxies/{name}/toxics', {
            'name': 'slow-replies', 'type': 'latency', 'stream': 'downstream',
            'attributes': {'latency': latency_ms, 'jitter': 0}})
        yield port
    finally:
        with contextlib.suppress(Exception):
            _call('DELETE', f'/proxies/{name}')
