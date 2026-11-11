#!/usr/bin/env bash
# worker.qnt, instances pr_late_etashutdown / ownrecord_late_etashutdown
# (property restoresOwnOnly), with real workers and no injected delay.
#
# Worker a alone receives 12 tasks with a 10 s countdown; workers b and c
# start; the visibility timeout (6 s here, 1 hour by default) passes, so
# every message is delivered again; a gets the signal before the ETA.
#
#   PY=python PYTHONPATH=<kombu>:<celery> findings/restore-by-tag/natural_restore_by_tag.sh TERM
#   PY=python PYTHONPATH=<kombu>:<celery> findings/restore-by-tag/natural_restore_by_tag.sh KILL
#
# Needs Redis on 6379 (dbs 13 and 14 are flushed). kombu 445027e with
# celery 768e476: after SIGKILL every task runs once; after SIGTERM 11 of
# 12 run twice, because a's closing channel restores, by delivery tag, the
# copies b and c hold.
set -u
PY=${PY:-python3}
cd "$(dirname "$0")"
export PYTHONPATH=${PYTHONPATH:-}:$PWD
$PY -c "
import redis
for db in (13, 14): redis.Redis(port=6379, db=db).flushdb()"
start() { WORKER=$1 $PY -m celery -A bytagapp worker -n "$1@h" -c 2 -l warning \
    --without-gossip --without-mingle --without-heartbeat > "/tmp/bytag-$1.log" 2>&1 & }
start a; pa=$!
sleep 3
$PY -c "
from bytagapp import work
for n in range(12): work.apply_async((n,), countdown=10)"
sleep 1
start b; pb=$!; start c; pc=$!
sleep 7                       # past the visibility timeout, before the ETA
kill "-$1" $pa
sleep 22
kill -TERM $pb $pc 2>/dev/null; wait 2>/dev/null
$PY - <<'PYEOF'
import redis
from collections import Counter
r = redis.Redis(port=6379, db=13, decode_responses=True)
runs = {k: r.lrange(k, 0, -1) for k in r.keys('run:*')}
print('runs per task:', dict(sorted(Counter(len(v) for v in runs.values()).items())),
      '| tasks:', len(runs), '| left in the queue:',
      redis.Redis(port=6379, db=14).llen('bytag'))
PYEOF
