#!/bin/sh
# SIGKILL a random pool child of the worker about every second, as an
# OOM killer or a crashing extension would.
main=$1
while kill -0 "$main" 2>/dev/null; do
    sleep "$(awk 'BEGIN { srand(); print -1 * log(1 - rand()) }')"
    pid=$(pgrep -P "$main" | shuf -n 1)
    [ -n "$pid" ] && kill -9 "$pid"
done
