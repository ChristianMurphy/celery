#!/bin/sh
# Run the driver over a list of configurations, one per line on stdin:
#   <broker> <backend> <pool> <fault> [extra driver args...]
# Blank lines and lines starting with # are skipped. Results go to $OUT
# (default t/conformance/chords/results), one summary line per run in
# summary.jsonl.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
PY=${PY:-python}
OUT=${OUT:-$HERE/results}
cd "$ROOT" || exit 1
while read -r broker backend pool fault rest; do
    case "$broker" in ''|'#'*) continue ;; esac
    # shellcheck disable=SC2086
    timeout 1200 "$PY" -m t.conformance.chords.driver --broker "$broker" \
        --backend "$backend" --pool "$pool" --fault "$fault" --out "$OUT" \
        $rest </dev/null 2>"$OUT/last-stderr.log" | grep '^{'
done
