"""Exhaustive checking with TLC (explicit-state breadth-first search).

``quint compile --target=tlaplus`` turns one instance into TLA+; TLC (shipped
inside the Apalache jar that ``quint verify`` downloads) then visits every
reachable state. One compile per instance keeps every property's operator,
so each property is checked by its own TLC run on the same file:

* a property that holds was checked on every reachable state (no bound);
* a violated one comes with a shortest counterexample (one worker, BFS);
* with a step counter in the state, the search depth is the longest run;
* with the specs' ``stepOrQuiet`` as the step and deadlock checking on, a
  state without a successor is a run that stopped without being quiet.

    python3 tlc.py chord.qnt redis_dup atMostOnce quietDone
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CACHE = HERE / '.cache'
JAR = Path.home() / '.quint/apalache-dist-0.62.4/apalache/lib/apalache.jar'
# TLC keeps its queue and fingerprints in the metadir; keep that off the disk
SCRATCH = '/dev/shm' if os.path.isdir('/dev/shm') else None


def _spec_hash(spec: Path) -> str:
    h = hashlib.sha1()
    for f in sorted(spec.parent.glob('*.qnt')):
        h.update(f.name.encode())
        h.update(f.read_bytes())
    return h.hexdigest()[:16]


#: In a file of instances, the line after which the instance modules follow.
INSTANCES = '// ---- instances'


def instances_file(spec: Path) -> Path:
    """Where the instances of ``spec`` are: ``<spec>_instances.qnt`` next to
    it, or the spec file itself."""
    other = spec.with_name(f'{spec.stem}_instances.qnt')
    return other if other.exists() else spec


def single_instance(spec_text: str, instances_text: str, main: str) -> str:
    """The text of a spec followed by its one instance ``main``."""
    base = spec_text.partition(INSTANCES)[0]
    rest = instances_text.partition(INSTANCES)[2]
    m = re.search(rf'^module {main} \{{\n.*?^\}}\n', rest, re.M | re.S)
    if not m:
        raise KeyError(f'no instance {main} after "{INSTANCES}"')
    # the instance sits in the spec's own file here
    return base + re.sub(r' from "\./\w+"', '', m[0])


def instance_file(spec: str | Path, main: str) -> Path:
    """A file that holds ``spec`` and only its instance ``main``.

    quint flattens every module of every file it loads, which takes about a
    second per instance module, whichever one is the main module. With one
    instance in the file, a compile or a simulator start takes a fifth of
    the time."""
    spec = (HERE / spec).resolve()
    key = hashlib.sha1(f'{_spec_hash(spec)}:{spec.name}:{main}'.encode()
                       ).hexdigest()[:16]
    out = CACHE / key / spec.name
    if not out.exists():
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(single_instance(
            spec.read_text(), instances_file(spec).read_text(), main))
    return out


def compile_tla(spec: str | Path, main: str, props: list[str],
                step: str = 'step') -> Path:
    """Compile instance ``main`` of ``spec`` to TLA+, keeping ``props``.

    Not safe to call in parallel: quint talks to one Apalache server."""
    single = instance_file(spec, main)
    key = hashlib.sha1(f'{step}:{",".join(props)}'.encode()).hexdigest()[:8]
    out = single.parent / key / f'{main}.tla'
    if out.exists():
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    # stdout to a file, not a pipe: quint can exit before a pipe took all
    # of its output, which cuts the module off at 64 KiB
    with open(out, 'w') as f:
        p = subprocess.run(
            ['quint', 'compile', '--target=tlaplus', f'--main={main}',
             str(single), f'--step={step}', f'--invariant={",".join(props)}'],
            cwd=HERE, stdout=f, stderr=subprocess.PIPE, text=True)
    text = out.read_text()
    if p.returncode or 'q_step ==' not in text:
        out.unlink()
        raise RuntimeError(f'quint compile {single.name} {main}: '
                           + (p.stderr or text)[-2000:])
    out.write_text(text[text.index('----'):])
    return out


def operator(tla: Path, name: str) -> str:
    """The TLA+ operator that a Quint definition was compiled to."""
    m = re.search(rf'^(\w*_)?{re.escape(name)} ==', tla.read_text(), re.M)
    if not m:
        raise KeyError(f'{name} not in {tla}')
    return (m.group(1) or '') + name


def run_tlc(tla: Path, invariants: list[str] | str | None, workers: int = 1,
            heap: str = '-Xmx6G', timeout: float | None = None,
            deadlock: bool = False) -> dict:
    """Run TLC on a compiled instance. Returns
    {result: 'holds'|'violated'|'deadlock'|'error', states, distinct, depth,
    trace, secs, violated}. ``trace`` is the number of states in the
    counterexample, ``violated`` the Quint name of the invariant. With
    ``deadlock`` a state without a successor is reported."""
    if isinstance(invariants, str):
        invariants = [invariants]
    names = {operator(tla, i): i for i in invariants or []}
    tmp = Path(tempfile.mkdtemp(prefix='tlc-', dir=SCRATCH))
    try:
        shutil.copy(tla, tmp / tla.name)
        cfg = 'INIT q_init\nNEXT q_step\n'
        for name in names:
            cfg += f'INVARIANT {name}\n'
        (tmp / f'{tla.stem}.cfg').write_text(cfg)
        t0 = time.time()
        try:
            p = subprocess.run(
                ['java', heap, '-Xss515m', f'-Djava.io.tmpdir={tmp}', '-cp',
                 str(JAR), 'tlc2.TLC', *([] if deadlock else ['-deadlock']),
                 '-workers', str(workers),
                 '-metadir', str(tmp / 'meta'), str(tmp / tla.name)],
                capture_output=True, text=True, timeout=timeout,
                env={k: v for k, v in os.environ.items()
                     if k != 'JAVA_TOOL_OPTIONS'})
        except subprocess.TimeoutExpired:
            return {'result': 'timeout', 'secs': round(time.time() - t0, 1)}
        secs = round(time.time() - t0, 1)
        text = p.stdout
        res: dict = {'secs': secs}
        m = re.search(r'(\d+) states generated, (\d+) distinct states found, '
                      r'(\d+) states left', text)
        if m:
            res['states'], res['distinct'] = int(m[1]), int(m[2])
        m = re.search(r'depth of the complete state graph search is (\d+)', text)
        if m:
            res['depth'] = int(m[1])
        if p.returncode == 0:
            res['result'] = 'holds'
        elif 'is violated' in text or 'Deadlock reached' in text:
            m = re.search(r'Error: Invariant (\w+) is violated', text)
            res['result'] = 'violated' if m else 'deadlock'
            if m:
                res['violated'] = names.get(m[1], m[1])
            res['trace'] = max(int(n) for n in
                               re.findall(r'^State (\d+):', text, re.M))
            res['text'] = text[text.index('Error: '):]
        else:
            res['result'] = 'error'
            res['text'] = (text + p.stderr)[-3000:]
        return res
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == '__main__':
    spec_, main_, *props_ = sys.argv[1:]
    tla_ = compile_tla(spec_, main_, props_)
    for prop in props_ or [None]:
        r = run_tlc(tla_, prop)
        text_ = r.pop('text', '')
        print(prop, r)
        if os.environ.get('TLC_VERBOSE'):
            print(text_)
