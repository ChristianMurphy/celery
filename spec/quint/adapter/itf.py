"""Read Quint ITF traces (https://apalache-mc.org/docs/adr/015adr-trace.html).

The specs use sum types and typed ids, which ITF writes as

    {"tag": "Success", "value": {"#tup": []}}        a variant without fields
    {"tag": "Claimed", "value": {"tok": ...}}        a variant with a value
    {"w": {"#bigint": "1"}}                          an id (ExecId, W(1))

``decode`` is the one place that knows this. It returns a variant without
fields as its tag (``'Success'``), a variant with a value as
``Variant(tag, value)``, and an id as its int. ``split`` gives ``(tag,
value)`` for either kind of variant.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NamedTuple

#: Field names of the specs' id types (one-field records, see chordtypes).
ID_FIELDS = frozenset({'h', 'w', 'tok', 'rn', 'c', 'm'})


class Variant(NamedTuple):
    tag: str
    value: Any


def split(value: Any) -> tuple[str, Any]:
    """``(tag, value)`` of a decoded variant; value is None without fields."""
    if isinstance(value, Variant):
        return value.tag, value.value
    return value, None


def tag(value: Any) -> str:
    return split(value)[0]


def _is_id(value: dict) -> bool:
    if len(value) != 1:
        return False
    (name, inner), = value.items()
    return (name in ID_FIELDS
            and (isinstance(inner, int) and not isinstance(inner, bool)
                 or isinstance(inner, dict) and '#bigint' in inner))


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
            inner = decode(value['value'])
            return value['tag'] if inner == () else Variant(value['tag'], inner)
        if _is_id(value):
            return decode(next(iter(value.values())))
        return {k: decode(v) for k, v in value.items() if k != '#meta'}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def _hashable(value: Any) -> Any:
    if isinstance(value, Variant):
        return Variant(value.tag, _hashable(value.value))
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
