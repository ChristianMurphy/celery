#!/usr/bin/env python3
"""The catalog of specs and properties (properties.json) and what is made
from it: the table check.py runs, the README tables, and the checklist a
model has to meet.

    python3 tools/catalog.py            # check the catalog and the checklist
    python3 tools/catalog.py --write    # regenerate the tables in the READMEs
    python3 tools/catalog.py --reasons  # every expected violation, with why

properties.json has two parts.

``specs``: one entry per spec file: its family directory, the module with
its ``Action`` type, the SHAs its citations are pinned to, its properties
and witnesses in table order, the instances of the fast tier, one instance
that passes every property (so that a model that goes vacuous shows up),
how lemma 1 of the liveness argument is established, and whether the
checklist is enforced for it or only reported (``grandfathered``).

``properties``: one entry per property: id, the spec and the definition in
it, a plain statement, the kind (safety, liveness, witness), the pattern of
Dwyer, Avrunin and Corbett it is an instance of (with its scope), the
assumptions, the source (a sentence of the documentation or code at a
file and line), the instances it is checked on, the evidence on the code
side, a witness that shows it is not vacuous, a spec mutant that makes it
fail, and for every expected violation a reason:

    documented   Celery or kombu documents the behavior (``where``: the file
                 and lines)
    open         reported or drafted (``where``: the tracker id and the
                 thread in words)
    fixed        fixed in a named commit or PR (``where``)
    by design    a stated trade-off, or the point of a design variant

A violation rule names instances with a shell pattern; the first rule that
matches an instance gives its reason.
"""
from __future__ import annotations

import fnmatch
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / 'properties.json'
EXPECTED = ROOT / 'expected.json'
REASONS = ('documented', 'open', 'fixed', 'by design')
KINDS = ('safety', 'liveness', 'witness')
#: The fast tier's budget: instances over all specs.
FAST_BUDGET = 24
#: The liveness lemmas, under the same names in every spec.
LEMMAS = ('progress', 'stepOrQuiet', 'quietDone')
NO_MEASURE = 'finite by exhaustive search with the step counter in the state'


def load() -> dict:
    return json.loads(CATALOG.read_text())


def expected() -> dict:
    return json.loads(EXPECTED.read_text()) if EXPECTED.exists() else {}


def entries(cat: dict, spec: str) -> dict[str, dict]:
    return {p['definition']: p for p in cat['properties'] if p['spec'] == spec}


def reason(entry: dict, instance: str) -> dict | None:
    """The first violation rule of ``entry`` that matches ``instance``."""
    for rule in entry.get('violations', []):
        if fnmatch.fnmatchcase(instance, rule['instances']):
            return rule
    return None


def mutant_names() -> set[str]:
    text = (ROOT / 'tools' / 'mutate_spec.py').read_text()
    return set(re.findall(r"^    \('((?:[^'\\]|\\.)*)',", text, re.M)) | {
        m.replace("\\'", "'")
        for m in re.findall(r"^    \('((?:[^'\\]|\\.)*)',", text, re.M)}


def check(cat: dict | None = None) -> list[str]:
    """Every problem with the catalog and the checklist, as text."""
    cat = cat or load()
    exp = expected()
    errors: list[str] = []
    notes: list[str] = []
    mutants = mutant_names()
    fast = 0
    for spec, s in cat['specs'].items():
        strict = s['checklist'] == 'enforced'
        report = errors if strict else notes
        ents = entries(cat, spec)
        fast += len(s['fast'])
        text = (ROOT / s['family'] / spec).read_text()
        # an entry per property and witness, and per lemma
        for name in s['properties'] + s['witnesses'] + ['stepOrQuiet',
                                                         'progress']:
            if name not in ents:
                errors.append(f'{spec}: {name} has no catalog entry')
        for name, e in ents.items():
            if name not in s['properties'] + s['witnesses'] + list(LEMMAS):
                errors.append(f'{spec}: catalog entry {name} is not checked')
            if e['kind'] not in KINDS:
                errors.append(f'{e["id"]}: kind {e["kind"]}')
            for key in ('statement', 'pattern', 'source'):
                if not e.get(key):
                    errors.append(f'{e["id"]}: no {key}')
            if not re.search(rf'\b(val|def|action|temporal) {name}\b', text) \
                    and not (name == 'progress'
                             and s['progress'] == NO_MEASURE):
                errors.append(f'{e["id"]}: no definition {name} in {spec}')
            if e['kind'] == 'witness' or name == 'stepOrQuiet':
                continue
            if e.get('witness') not in s['witnesses'] \
                    and name in s['properties']:
                report.append(f'{e["id"]}: no witness')
            if not e.get('mutant') and name in s['properties']:
                report.append(f'{e["id"]}: no spec mutant')
            elif e.get('mutant') and e['mutant'] not in mutants:
                errors.append(f'{e["id"]}: mutant "{e["mutant"]}" is not in '
                              'mutate_spec.py')
        # lemma 1
        if s['progress'] != NO_MEASURE and 'progress' not in s['properties']:
            errors.append(f'{spec}: a closed-form measure needs `progress` '
                          'among its properties')
        # every expected violation has a reason, every rule is used
        used: set[tuple[str, str]] = set()
        for inst, cell in exp.get(spec, {}).items():
            for name in cell['violated']:
                if name in s['witnesses']:
                    continue
                rule = reason(ents.get(name, {}), inst)
                if rule is None:
                    errors.append(f'{spec}: {inst}.{name} is expected to be '
                                  'violated and has no reason')
                else:
                    used.add((name, rule['instances']))
                    if rule['reason'] not in REASONS or not rule.get('where'):
                        errors.append(f'{spec}: {name} {rule["instances"]}: '
                                      'reason or where missing')
        if spec in exp:
            for name, e in ents.items():
                for rule in e.get('violations', []):
                    if (name, rule['instances']) not in used:
                        errors.append(f'{e["id"]}: the rule for '
                                      f'{rule["instances"]} matches no '
                                      'expected violation')
            # the fast tier keeps one instance that passes every property
            keep = s['passes_all']
            if keep not in s['fast']:
                errors.append(f'{spec}: {keep} is not in the fast tier')
            broken = [p for p in exp[spec].get(keep, {'violated': {'?': 0}})
                      ['violated'] if p not in s['witnesses']]
            if broken:
                errors.append(f'{spec}: {keep} should pass every property, '
                              f'and violates {", ".join(broken)}')
        elif strict:
            errors.append(f'{spec}: no results in expected.json')
    if fast > FAST_BUDGET:
        errors.append(f'the fast tier has {fast} instances; the budget is '
                      f'{FAST_BUDGET}')
    for p in cat['properties']:
        if p['spec'] not in cat['specs']:
            errors.append(f'{p["id"]}: unknown spec {p["spec"]}')
    check.notes = notes  # type: ignore[attr-defined]
    return errors + check_findings()


# ---- findings ---------------------------------------------------------------
def findings() -> list[dict]:
    """findings/<id>/finding.json: a counterexample that the real code
    reproduced, with its trace, its confirm script, its status and the
    upstream thread in words."""
    return [json.loads(p.read_text())
            for p in sorted((ROOT / 'findings').glob('*/finding.json'))]


def finding_of(cell: str) -> Path | None:
    """The folder of the finding that claims ``instance.property``."""
    for f in findings():
        if cell in f['cells']:
            return ROOT / 'findings' / f['id']
    return None


def check_findings() -> list[str]:
    errors = []
    cells = {f'{inst}.{prop}' for spec in expected().values()
             for inst, cell in spec.items() for prop in cell['violated']}
    for f in findings():
        d = ROOT / 'findings' / f['id']
        for script in f['confirm']:
            if not (d / script).exists():
                errors.append(f'findings/{f["id"]}: no {script}')
        if not f['confirm']:
            errors.append(f'findings/{f["id"]}: no confirm script')
        for cell in f['cells']:
            if cell not in cells:
                errors.append(f'findings/{f["id"]}: {cell} is not an '
                              'expected violation')
        if not any((d / f'{cell}.itf.json').exists() for cell in f['cells']):
            errors.append(f'findings/{f["id"]}: no trace')
        if f['status'] not in REASONS:
            errors.append(f'findings/{f["id"]}: status {f["status"]}')
    return errors


def findings_table(cat: dict, _: str | None = None) -> str:
    rows = ['| Finding | Instance and property | Outcome | Status | Thread | '
            'On the real code |', '|---|---|---|---|---|---|']
    for f in findings():
        rows.append(
            f'| [{f["id"]}]({f["id"]}/): {f["title"]} | '
            f'{", ".join(f"`{c}`" for c in f["cells"])} | {f["outcome"]} | '
            f'{f["status"]}: {f["where"]} | {f["thread"]} | '
            f'{", ".join(f"`{c}`" for c in f["confirm"])}. {f["observed"]} |')
    return '\n'.join(rows)


# ---- tables -----------------------------------------------------------------
def property_table(cat: dict, spec: str) -> str:
    s = cat['specs'][spec]
    ents = entries(cat, spec)
    rows = ['| Property | Kind | Statement | Pattern | Source |',
            '|---|---|---|---|---|']
    order = s['properties'] + [n for n in LEMMAS if n in ents
                               and n not in s['properties']] + s['witnesses']
    for name in order:
        e = ents[name]
        text = e['statement']
        if e.get('assumptions'):
            text += f' Assumes: {e["assumptions"]}.'
        rows.append(f'| `{name}` | {e["kind"]} | {text} | {e["pattern"]} | '
                    f'{e["source"]} |')
    return '\n'.join(rows)


def results_table(cat: dict, spec: str) -> str:
    s = cat['specs'][spec]
    exp = expected().get(spec, {})
    rows = ['| instance | D | violated | reachable (witnesses) |',
            '|---|---|---|---|']
    for inst, cell in exp.items():
        v = cell['violated']
        props = ', '.join(f'{p} ({v[p]})' for p in s['properties'] if p in v)
        wit = ', '.join(f'{p} ({v[p]})' for p in s['witnesses'] if p in v)
        rows.append(f'| {inst} | {cell["D"]} | {props or "none"} | '
                    f'{wit or "none"} |')
    return '\n'.join(rows)


def reasons_table(cat: dict, spec: str) -> str:
    """Every expected violation of ``spec``, grouped by property and rule."""
    s = cat['specs'][spec]
    ents = entries(cat, spec)
    exp = expected().get(spec, {})
    rows = ['| Property | Instances (states in a shortest counterexample) | '
            'Why | Where |', '|---|---|---|---|']
    for name in s['properties']:
        groups: dict[str, list[str]] = {}
        for inst, cell in exp.items():
            if name in cell['violated']:
                rule = reason(ents[name], inst)
                key = rule['instances'] if rule else '?'
                groups.setdefault(key, []).append(
                    f'{inst} ({cell["violated"][name]})')
        for rule in ents[name].get('violations', []):
            if rule['instances'] in groups:
                where = rule['where'] + (f'. {rule["note"]}'
                                         if rule.get('note') else '')
                rows.append(f'| `{name}` | '
                            f'{", ".join(groups[rule["instances"]])} | '
                            f'{rule["reason"]} | {where} |')
    return '\n'.join(rows)


def spec_index(cat: dict) -> str:
    exp = expected()
    rows = ['| Spec | Family | Pinned to | Properties | Instances | '
            'Fast tier | Lemma 1 (every run is finite) |',
            '|---|---|---|---|---|---|---|']
    for spec, s in cat['specs'].items():
        pins = ', '.join(f'{k} {v}' for k, v in s['pins'].items())
        rows.append(
            f'| [`{spec}`]({s["family"]}/{spec}) | '
            f'[{s["family"]}/]({s["family"]}/README.md) | {pins} | '
            f'{len(s["properties"])} and {len(s["witnesses"])} witnesses | '
            f'{len(exp.get(spec, {}))} | {", ".join(s["fast"])} | '
            f'{"`progress`, a closed-form measure" if s["progress"] != NO_MEASURE else s["progress"]} |')
    return '\n'.join(rows)


def designs_table(cat: dict, spec: str) -> str:
    """One table per design, for specs whose instances are named
    ``<design>_<ack mode>_<class>``: a row per ack mode and class, a column
    per property, ``ok`` or the states in a shortest counterexample."""
    s = cat['specs'][spec]
    exp = expected().get(spec, {})
    designs: dict[str, list[str]] = {}
    for inst in exp:
        designs.setdefault(inst.split('_')[0], []).append(inst)
    props = s['properties']
    out = []
    for design, insts in designs.items():
        out += [f'`{design}`', '',
                '| ack, class | D | ' + ' | '.join(props) + ' |',
                '|---' * (len(props) + 2) + '|']
        for inst in insts:
            cell = exp[inst]
            _, ack, cls = inst.split('_', 2)
            vals = [str(cell['violated'].get(p, 'ok')) for p in props]
            out.append(f'| {ack}, {cls} | {cell["D"]} | '
                       + ' | '.join(vals) + ' |')
        out.append('')
    return '\n'.join(out).rstrip('\n')


GENERATORS = {'properties': property_table, 'results': results_table,
              'reasons': reasons_table, 'findings': findings_table,
              'designs': designs_table}
MARK = re.compile(r'(<!-- generated: (\w+)(?: (\S+))? -->\n)(.*?)'
                  r'(<!-- end generated -->)', re.S)


def render(text: str, cat: dict) -> str:
    """Fill every ``<!-- generated: kind spec -->`` block of a README."""
    def fill(m: re.Match) -> str:
        kind, spec = m[2], m[3]
        body = (spec_index(cat) if kind == 'index'
                else GENERATORS[kind](cat, spec))
        return f'{m[1]}{body}\n{m[5]}'
    return MARK.sub(fill, text)


def readmes() -> list[Path]:
    return sorted(p for p in [ROOT / 'README.md', *ROOT.glob('*/README.md')]
                  if '<!-- generated:' in p.read_text())


def stale(cat: dict | None = None) -> list[str]:
    cat = cat or load()
    return [str(p.relative_to(ROOT)) for p in readmes()
            if render(p.read_text(), cat) != p.read_text()]


def main() -> int:
    cat = load()
    if '--write' in sys.argv:
        for p in readmes():
            p.write_text(render(p.read_text(), cat))
            print('wrote', p.relative_to(ROOT))
    if '--reasons' in sys.argv:
        for spec in cat['specs']:
            print(f'\n{spec}\n\n{reasons_table(cat, spec)}')
    errors = check(cat)
    errors += [f'{p}: generated tables are stale; run tools/catalog.py '
               '--write' for p in stale(cat)]
    for line in errors:
        print('CATALOG', line)
    notes = check.notes  # type: ignore[attr-defined]
    if notes:
        print(f'{len(notes)} checklist gaps in grandfathered specs '
              '(--gaps lists them)')
        if '--gaps' in sys.argv:
            print('\n'.join(notes))
    n = len(cat['properties'])
    print(f'catalog: {n} entries, {len(cat["specs"])} specs, '
          f'{"ok" if not errors else f"{len(errors)} problems"}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
