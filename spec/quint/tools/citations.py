#!/usr/bin/env python3
"""Check the code citations of the specs.

A spec cites the code it models in two ways. The prose says where, for a
reader (``request.py:525-533``). A structured citation says it so that a
script can check it, on a comment line of its own:

    // @cite celery celery/worker/request.py:525-533 "def on_accepted"
    // @cite kombu@c61b525 kombu/transport/redis.py:223-305 "POP_UNACKED"

``celery`` or ``kombu`` names the repository; ``@sha`` overrides the SHA the
spec is pinned to in properties.json (``pins``). The quoted anchor must
occur within the cited lines at that SHA, and somewhere in the same file on
current main, so that a citation that moved is noticed and one whose code
is gone fails.

Every action of a spec whose checklist is enforced needs a structured
citation in the comment block above it (or in its body). The prose
citations of the other specs are checked for a file and a range that exist
at the pinned SHA, where the file name is unambiguous.

    python3 tools/citations.py             # every spec
    python3 tools/citations.py worker.qnt  # one

Repositories: ``CELERY_REPO`` and ``KOMBU_REPO`` (any checkout or clone with
the pinned commits); current main is ``CELERY_MAIN`` and ``KOMBU_MAIN``
(a ref in that repository, default ``HEAD``). The celery repository
defaults to the one this spec tree is in. A repository that is not
configured is reported and its citations skipped.
"""
from __future__ import annotations

import functools
import os
import re
import subprocess
import sys
from pathlib import Path

import catalog

ROOT = catalog.ROOT
CITE = re.compile(r'^\s*//\s*@cite\s+(\w+)(?:@([0-9a-f]{7,40}))?\s+'
                  r'(\S+?):(\d+)(?:-(\d+))?\s+"([^"]+)"\s*$')
PROSE = re.compile(r'\b([\w/]+\.py):(\d+)(?:-(\d+))?')
ACTION = re.compile(r'^\s*action\s+(\w+)')
#: Actions that only keep or set variables, or wrap others; no code of
#: their own to cite.
HELPERS = {'init', 'step', 'stutter', 'stepOrQuiet', 'did', 'keepBroker',
           'keepStarts', 'keepRuns', 'setJob', 'setConn', 'restoreAll',
           'keepObs', 'setRun', 'dinit', 'dkeep', 'inner'}


def repo(name: str) -> Path | None:
    env = os.environ.get(f'{name.upper()}_REPO')
    if env:
        return Path(env)
    if name == 'celery':
        top = subprocess.run(['git', 'rev-parse', '--show-toplevel'],
                             cwd=ROOT, capture_output=True, text=True)
        return Path(top.stdout.strip()) if top.returncode == 0 else None
    return None


@functools.cache
def show(name: str, rev: str, path: str) -> list[str] | None:
    r = repo(name)
    if r is None:
        return None
    out = subprocess.run(['git', '-C', str(r), 'show', f'{rev}:{path}'],
                         capture_output=True, text=True)
    return out.stdout.splitlines() if out.returncode == 0 else []


@functools.cache
def tree(name: str, rev: str) -> list[str]:
    r = repo(name)
    out = subprocess.run(['git', '-C', str(r), 'ls-tree', '-r', '--name-only',
                          rev], capture_output=True, text=True)
    return out.stdout.splitlines()


def main_ref(name: str) -> str:
    return os.environ.get(f'{name.upper()}_MAIN', 'HEAD')


def structured(spec: str, text: str, pins: dict) -> tuple[list[str],
                                                          list[str], int]:
    errors, skipped, n = [], [], 0
    for i, line in enumerate(text.splitlines(), 1):
        if '@cite' not in line:
            continue
        m = CITE.match(line)
        if not m:
            errors.append(f'{spec}:{i}: malformed @cite')
            continue
        name, sha, path, a, b, anchor = m.groups()
        n += 1
        rev = sha or pins.get(name)
        if rev is None:
            errors.append(f'{spec}:{i}: {name} is not pinned')
            continue
        lines = show(name, rev, path)
        if lines is None:
            skipped.append(name)
            continue
        a, b = int(a), int(b or a)
        if not lines:
            errors.append(f'{spec}:{i}: {path} does not exist at {rev}')
        elif b > len(lines) or a > b:
            errors.append(f'{spec}:{i}: {path}:{a}-{b} is past the end '
                          f'({len(lines)} lines) at {rev}')
        elif not any(anchor in l for l in lines[a - 1:b]):
            errors.append(f'{spec}:{i}: "{anchor}" is not in {path}:{a}-{b} '
                          f'at {rev}')
        cur = show(name, main_ref(name), path)
        if cur is not None and not any(anchor in l for l in cur):
            errors.append(f'{spec}:{i}: "{anchor}" is gone from {path} on '
                          f'{name} {main_ref(name)}')
    return errors, skipped, n


def actions_without_cite(text: str) -> list[str]:
    """Actions with no @cite in the comment block above them or in their
    body (up to the next blank line that is followed by a non-comment)."""
    lines = text.splitlines()
    missing = []
    for i, line in enumerate(lines):
        m = ACTION.match(line)
        if not m or m[1] in HELPERS:
            continue
        j, found = i - 1, False
        while j >= 0 and lines[j].strip().startswith('//'):
            found |= '@cite' in lines[j] or '@env' in lines[j]
            j -= 1
        k = i + 1
        while k < len(lines) and not ACTION.match(lines[k]) \
                and lines[k].rstrip() != '  }':
            found |= '@cite' in lines[k] or '@env' in lines[k]
            k += 1
        if not found:
            missing.append(m[1])
    return missing


def prose(spec: str, text: str, pins: dict) -> list[str]:
    """A prose citation whose file name is unambiguous in the pinned trees
    must exist with its range at the pinned SHA."""
    errors = []
    names = [n for n in pins if n in ('celery', 'kombu')]
    if any(repo(n) is None for n in names):
        return []      # a file name is unambiguous only across every tree
    for i, line in enumerate(text.splitlines(), 1):
        if '@cite' in line or not line.strip().startswith('//'):
            continue
        if re.search(r'\bat [0-9a-f]{7}\b|\bcommit\b', line):
            continue      # names another commit; checked by hand
        for f, a, b in PROSE.findall(line):
            hits = []
            for name, rev in pins.items():
                if name not in ('celery', 'kombu') or repo(name) is None:
                    continue
                hits += [(name, rev, p) for p in tree(name, rev)
                         if p == f or p.endswith('/' + f)]
            if len(hits) != 1:
                continue
            name, rev, path = hits[0]
            n = len(show(name, rev, path) or [])
            if int(b or a) > n:
                errors.append(f'{spec}:{i}: {f}:{a}-{b or a} is past the end '
                              f'of {path} ({n} lines) at {name} {rev}')
    return errors


def check(only: str | None = None) -> tuple[list[str], list[str]]:
    cat = catalog.load()
    errors, notes = [], []
    for spec, s in cat['specs'].items():
        if only not in (None, spec):
            continue
        text = (ROOT / s['family'] / spec).read_text()
        pins = {k: v for k, v in s['pins'].items() if ' ' not in k}
        e, skipped, n = structured(spec, text, pins)
        errors += e
        errors += prose(spec, text, pins)
        for name in sorted(set(skipped)):
            notes.append(f'{spec}: {name.upper()}_REPO is not set; its '
                         'citations were not checked')
        missing = actions_without_cite(text)
        if missing:
            line = f'{spec}: actions without @cite: {", ".join(missing)}'
            (errors if s['checklist'] == 'enforced' and s.get('cite')
             == 'enforced' else notes).append(line)
        notes.append(f'{spec}: {n} structured citations')
    return errors, notes


def main() -> int:
    errors, notes = check(sys.argv[1] if len(sys.argv) > 1 else None)
    for line in notes:
        print(line)
    for line in errors:
        print('CITE', line)
    print('citations:', 'ok' if not errors else f'{len(errors)} problems')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
