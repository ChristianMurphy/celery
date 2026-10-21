#!/usr/bin/env python3
"""Convert an early-spike history file to the canonical JSONL history format.

Spike format: {"history": [client ops], "events": [worker events]}
  client ops: {"t", "f": "enqueue", "type": "invoke"|"ok"|"info"|"fail", "op", "tid"?, "err"?}
              {"t", "f": "nemesis", "value"}
              {"t", "f": "read", "op", "tid", "state", "value"|"err"}   (single record)
  events:     {"kind": "start"|"end", "op", "t", "pid", "token", "tid", "host", "redelivered"?}

Canonical format: one JSON object per line, real-time order,
  {"index", "time" (s since test start), "process", "type", "f", "value", ...}

  - enqueue/read client ops use process 0 (the spike had one client thread);
  - each spike read becomes an invoke {"op"} + ok {"op", "state", "result"}
    (info if the read raised);
  - each execution is process f"{pid}.{token}"; start -> execute invoke,
    end -> execute ok, value {"op", "token", "attempt"} where attempt counts
    starts of that op in time order (1-based);
  - nemesis ops are process "nemesis", type "info".

Usage: spike_to_canonical.py SPIKE.json [-o OUT.jsonl]
"""

import argparse
import collections
import json
import sys


def convert(spike):
    rows = []  # (t, source order, [records without index/time])
    seq = 0

    def add(t, *recs):
        nonlocal seq
        rows.append((t, seq, recs))
        seq += 1

    for x in spike["history"]:
        f, t = x.get("f"), x["t"]
        if f == "enqueue":
            extra = {k: x[k] for k in ("tid", "err") if k in x}
            add(t, dict(process=0, type=x["type"], f="enqueue", value=x["op"], **extra))
        elif f == "nemesis":
            add(t, dict(process="nemesis", type="info", f="nemesis", value=x.get("value")))
        elif f == "read":
            inv = dict(process=0, type="invoke", f="read", value={"op": x["op"]})
            if "err" in x:
                comp = dict(process=0, type="info", f="read", value={"op": x["op"]}, err=x["err"])
            else:
                comp = dict(process=0, type="ok", f="read",
                            value={"op": x["op"], "state": x.get("state"),
                                   "result": x.get("value")})
            add(t, inv, comp)

    attempts = collections.Counter()
    started = {}
    for e in sorted(spike["events"], key=lambda e: e["t"]):
        proc = f"{e['pid']}.{e['token']}"
        if e["kind"] == "start":
            attempts[e["op"]] += 1
            value = {"op": e["op"], "token": e["token"], "attempt": attempts[e["op"]]}
            started[proc] = value
            add(e["t"], dict(process=proc, type="invoke", f="execute", value=value,
                             tid=e.get("tid"), host=e.get("host"),
                             redelivered=e.get("redelivered")))
        elif e["kind"] == "end":
            value = started.get(proc) or {"op": e["op"], "token": e["token"], "attempt": None}
            add(e["t"], dict(process=proc, type="ok", f="execute", value=value,
                             tid=e.get("tid"), host=e.get("host")))

    rows.sort(key=lambda r: (r[0], r[1]))
    t0 = rows[0][0] if rows else 0.0
    out = []
    for t, _, recs in rows:
        for r in recs:
            out.append(dict(index=len(out), time=round(t - t0, 9), **r))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("spike")
    ap.add_argument("-o", "--output", default="-")
    args = ap.parse_args(argv)
    with open(args.spike) as fh:
        recs = convert(json.load(fh))
    fh = sys.stdout if args.output == "-" else open(args.output, "w")
    with fh:
        for r in recs:
            fh.write(json.dumps(r) + "\n")
    print(f"wrote {len(recs)} canonical ops", file=sys.stderr)


if __name__ == "__main__":
    main()
