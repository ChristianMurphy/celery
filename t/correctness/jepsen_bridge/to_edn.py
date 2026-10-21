#!/usr/bin/env python3
"""Convert a canonical Celery-harness JSONL history to a Jepsen EDN history.

The output is the *queue view* of the history, for jepsen.checker/total-queue
(and friends): an EDN vector of op maps with dense :index 0, 1, 2, ...

Mapping (see README.md for the reasoning and caveats):

  enqueue invoke/ok/fail/info  -> :f :enqueue, same :type, :value op-id
  execute invoke               -> :f :dequeue :type :invoke :value nil
  execute ok                   -> :f :dequeue :type :ok     :value op-id
  execute fail/info (if any)   -> :f :dequeue, same :type,  :value nil
  execute with no completion   -> synthesized :f :dequeue :type :info
                                  :value nil, appended at the end
  nemesis info                 -> :process :nemesis :type :info :f :nemesis
  anything else (read, ...)    -> dropped

Processes are renumbered to integers 0, 1, 2, ... in order of first
appearance (Jepsen treats only integer processes as client processes);
"nemesis" becomes :nemesis. Each op keeps its original process as
:src-process and its original index as :src-index. Times are converted from
float seconds to integer nanoseconds.

Usage: to_edn.py HISTORY.jsonl [-o OUT.edn] [--process-map MAP.json]
                               [--no-nemesis] [--drop-failed-enqueues]

Stdlib only.
"""

import argparse
import json
import math
import re
import sys

CLIENT_TYPES = ("invoke", "ok", "fail", "info")
_KEYWORD_RE = re.compile(r"^[A-Za-z*!_?<>=+\-][A-Za-z0-9*!_?<>=+\-.]*$")


class HistoryError(Exception):
    pass


# ---------------------------------------------------------------------------
# EDN emission


def kw(name):
    return Keyword(name)


class Keyword(str):
    """A string that is emitted as an EDN keyword."""


def edn(x):
    """Render a JSON-ish Python value as EDN."""
    if x is None:
        return "nil"
    if x is True:
        return "true"
    if x is False:
        return "false"
    if isinstance(x, Keyword):
        return ":" + x
    if isinstance(x, int):
        return str(x)
    if isinstance(x, float):
        if math.isnan(x):
            return "##NaN"
        if math.isinf(x):
            return "##Inf" if x > 0 else "##-Inf"
        return repr(x)
    if isinstance(x, str):
        # JSON string escapes (\" \\ \n \t \r \b \f \uXXXX) are all valid in
        # Clojure's EDN reader.
        return json.dumps(x)
    if isinstance(x, (list, tuple)):
        return "[" + " ".join(edn(v) for v in x) + "]"
    if isinstance(x, dict):
        parts = []
        for k, v in x.items():
            if isinstance(k, str) and not isinstance(k, Keyword) and _KEYWORD_RE.match(k):
                k = Keyword(k)
            parts.append(edn(k) + " " + edn(v))
        return "{" + ", ".join(parts) + "}"
    raise HistoryError(f"can't render {type(x).__name__} as EDN: {x!r}")


# ---------------------------------------------------------------------------
# Conversion


def _proc_key(p):
    # Keep 0 and "0" distinct.
    return (type(p).__name__, p)


def _op_id(value, what, src):
    if isinstance(value, dict):
        if "op" not in value:
            raise HistoryError(f"{what} value has no 'op' key at index {src}: {value!r}")
        return value["op"]
    return value


def convert(records, include_nemesis=True, drop_failed_enqueues=False):
    """Convert canonical records (dicts, real-time order) to a list of Jepsen op
    dicts (Python values; keyword-valued fields are Keyword instances).

    With drop_failed_enqueues, an enqueue whose completion is "fail" is left
    out entirely (invoke and completion), so total-queue reports any execution
    of it as :unexpected instead of quietly counting it as :recovered.

    Returns (ops, process_map) where process_map is a list of
    {"process": original, "jepsen": int} entries."""
    procs = {}  # _proc_key(original) -> int
    proc_list = []
    outstanding = {}  # _proc_key -> (invoke record, jepsen op)
    ops = []
    last_time = None
    max_ns = 0

    def jproc(p):
        k = _proc_key(p)
        if k not in procs:
            procs[k] = len(proc_list)
            proc_list.append({"process": p, "jepsen": procs[k]})
        return procs[k]

    for n, r in enumerate(records):
        f = r.get("f")
        if f not in ("enqueue", "execute", "nemesis"):
            continue
        src = r.get("index", n)
        t = r.get("time")
        if not isinstance(t, (int, float)):
            raise HistoryError(f"op at index {src} has no numeric time: {r!r}")
        if last_time is not None and t < last_time:
            print(f"warning: time goes backwards at index {src} "
                  f"({t} < {last_time})", file=sys.stderr)
        last_time = t
        ns = int(round(t * 1e9))
        max_ns = max(max_ns, ns)
        typ = r.get("type")
        p = r.get("process")

        if f == "nemesis":
            if include_nemesis:
                ops.append({"time": ns, "type": kw("info"), "process": kw("nemesis"),
                            "f": kw("nemesis"), "value": r.get("value"),
                            "src-index": src})
            continue

        if typ not in CLIENT_TYPES:
            raise HistoryError(f"bad :type {typ!r} at index {src}")
        if p is None or p == "nemesis":
            raise HistoryError(f"{f} op at index {src} has process {p!r}")
        pk = _proc_key(p)

        if typ == "invoke":
            if pk in outstanding:
                prev = outstanding[pk][0]
                raise HistoryError(
                    f"process {p!r} invokes at index {src} while its op at index "
                    f"{prev.get('index')} is still outstanding")
            if f == "enqueue":
                value = r.get("value")
                op = {"time": ns, "type": kw("invoke"), "process": jproc(p),
                      "f": kw("enqueue"), "value": value, "src-index": src}
            else:
                v = r.get("value") or {}
                op = {"time": ns, "type": kw("invoke"), "process": jproc(p),
                      "f": kw("dequeue"), "value": None, "src-index": src,
                      "src-process": str(p), "op": _op_id(v, "execute", src),
                      "token": v.get("token") if isinstance(v, dict) else None,
                      "attempt": v.get("attempt") if isinstance(v, dict) else None}
            outstanding[pk] = (r, op)
            ops.append(op)
            continue

        # Completion
        if pk not in outstanding:
            raise HistoryError(f"{typ} at index {src} for process {p!r} with no "
                               f"outstanding invocation")
        inv_rec, inv_op = outstanding.pop(pk)
        if inv_rec.get("f") != f:
            raise HistoryError(f"completion f={f!r} at index {src} doesn't match "
                               f"invocation f={inv_rec.get('f')!r} at index "
                               f"{inv_rec.get('index')}")
        if f == "enqueue":
            if typ == "fail" and drop_failed_enqueues:
                inv_op["dropped"] = True
                continue
            value = r.get("value")
            if value is None:
                value = inv_op["value"]
            elif value != inv_op["value"]:
                raise HistoryError(f"enqueue completion at index {src} has value "
                                   f"{value!r}, invocation had {inv_op['value']!r}")
            ops.append({"time": ns, "type": kw(typ), "process": inv_op["process"],
                        "f": kw("enqueue"), "value": value, "src-index": src})
        else:
            v = r.get("value")
            op_id = inv_op["op"] if v is None else _op_id(v, "execute", src)
            if op_id != inv_op["op"]:
                raise HistoryError(f"execute completion at index {src} is for op "
                                   f"{op_id!r}, invocation was for {inv_op['op']!r}")
            ops.append({"time": ns, "type": kw(typ), "process": inv_op["process"],
                        "f": kw("dequeue"),
                        # Only a completed execution dequeues a known element.
                        "value": op_id if typ == "ok" else None,
                        "src-index": src, "src-process": inv_op["src-process"],
                        "op": op_id, "token": inv_op["token"],
                        "attempt": inv_op["attempt"]})

    # Executions (or client ops) that never completed: in Jepsen a crashed
    # operation is completed with :info, and its process is never reused.
    for _pk, (inv_rec, inv_op) in outstanding.items():
        crashed = {"time": max_ns, "type": kw("info"), "process": inv_op["process"],
                   "f": inv_op["f"], "value": inv_op["value"] if inv_op["f"] == "enqueue" else None,
                   "src-index": None, "synthetic": True}
        for k in ("src-process", "op", "token", "attempt"):
            if k in inv_op:
                crashed[k] = inv_op[k]
        ops.append(crashed)

    ops = [op for op in ops if not op.get("dropped")]
    for i, op in enumerate(ops):
        op["index"] = i
    return ops, proc_list


def read_jsonl(path):
    records = []
    with (sys.stdin if path == "-" else open(path)) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise HistoryError(f"{path}:{lineno}: bad JSON: {e}") from e
    return records


FIELD_ORDER = ("index", "time", "type", "process", "f", "value")


def write_edn(ops, fh):
    fh.write("[")
    for i, op in enumerate(ops):
        keys = [k for k in FIELD_ORDER if k in op] + \
               [k for k in op if k not in FIELD_ORDER and op[k] is not None]
        body = ", ".join(":" + k + " " + edn(op[k]) for k in keys)
        fh.write(("" if i == 0 else "\n ") + "{" + body + "}")
    fh.write("]\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("history", help="canonical JSONL history ('-' for stdin)")
    ap.add_argument("-o", "--output", default="-", help="EDN output path (default stdout)")
    ap.add_argument("--process-map", help="write the process renumbering here as JSON")
    ap.add_argument("--no-nemesis", action="store_true", help="drop nemesis ops")
    ap.add_argument("--drop-failed-enqueues", action="store_true",
                    help="omit enqueues that completed with fail, so executing one "
                         "shows up as :unexpected (total-queue otherwise counts it "
                         "as :recovered)")
    args = ap.parse_args(argv)
    try:
        ops, pmap = convert(read_jsonl(args.history), include_nemesis=not args.no_nemesis,
                            drop_failed_enqueues=args.drop_failed_enqueues)
    except HistoryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 3
    if args.output == "-":
        write_edn(ops, sys.stdout)
    else:
        with open(args.output, "w") as fh:
            write_edn(ops, fh)
    if args.process_map:
        with open(args.process_map, "w") as fh:
            json.dump(pmap, fh, indent=1)
    synth = sum(1 for o in ops if o.get("synthetic"))
    print(f"wrote {len(ops)} ops ({synth} synthesized :info completions, "
          f"{len(pmap)} processes)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
