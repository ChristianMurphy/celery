"""Read Quint ITF traces (https://apalache-mc.org/docs/adr/015adr-trace.html)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def decode(value: Any) -> Any:
    """Turn ITF values into plain Python values."""
    if isinstance(value, dict):
        if '#bigint' in value:
            return int(value['#bigint'])
        if '#map' in value:
            return {_hashable(decode(k)): decode(v) for k, v in value['#map']}
        if '#set' in value:
            return frozenset(_hashable(decode(v)) for v in value['#set'])
        if '#tup' in value:
            return tuple(decode(v) for v in value['#tup'])
        if 'tag' in value and 'value' in value and len(value) == 2:
            return (value['tag'], decode(value['value']))
        return {k: decode(v) for k, v in value.items() if k != '#meta'}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def _hashable(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((k, _hashable(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(_hashable(v) for v in value)
    return value


def load(path: str | Path) -> list[dict[str, Any]]:
    """Return the trace's states, with variable names stripped of the
    ``module::instance::`` prefix that ``quint run --main`` adds."""
    data = json.loads(Path(path).read_text())
    states = []
    for raw in data['states']:
        state = {}
        for name, value in raw.items():
            if name == '#meta':
                continue
            state[name.rsplit('::', 1)[-1]] = decode(value)
        states.append(state)
    return states
