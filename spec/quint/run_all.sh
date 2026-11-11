#!/usr/bin/env bash
# Check the specs, in tiers. Each tier includes the ones before it.
#
#   ./run_all.sh fast   typecheck, type errors, simulator, scenarios
#                       (about a minute; needs only quint)
#   ./run_all.sh full   exhaustive search with TLC and Apalache at the proven
#                       depth, spec mutants, replay of generated traces on
#                       the real backends, recorded histories against the
#                       specs, code mutants (tens of minutes)
#   ./run_all.sh long   Apalache on the large instances, a larger simulator
#                       sample, and new histories recorded from a real worker
#
# Needs: quint 0.33.1 (npm i -g @informalsystems/quint), Java 17+ from the
# full tier on, and for the replays Redis 6379, MongoDB 27017, Postgres 5432
# (postgres/pw, db celery), Elasticsearch 9200, memcached 11211.
set -euo pipefail
cd "$(dirname "$0")"
TIER=${1:-fast}
ROOT=$(git rev-parse --show-toplevel)
PY=${PY:-python3}
export PYTHONPATH="$ROOT:$PWD"
START=$SECONDS
tier_done() { echo "== $1 tier done in $((SECONDS - START)) s"; }

echo '== fast: typecheck, type errors, simulator, scenarios'
python3 tools/check.py --tier fast
for m in redisLost kvDup unlockChild onceReadOnce interruptFix; do
  quint test --main=$m canvas/scenarios.qnt
done
# worker.qnt projects onto the delivery environment of lib/delivery.qnt
for m in proj_main_late_partition proj_pr_late_cut proj_pr_late_shutdown proj_pr_early_crash proj_pr_late_long; do
  quint run --main=$m worker/worker_projection.qnt --init=pinit --step=pstep \
    --invariant=projectionOk --max-steps=40 --max-samples=2000 --seed=5 | tail -1
done
tier_done fast
[ "$TIER" = fast ] && exit 0

echo '== the other scenarios'
for m in onceReadAgain interruptMain interruptFirstFix interruptSecondFix interruptThirdFix; do
  quint test --main=$m canvas/scenarios.qnt
done

echo "== $TIER: exhaustive search (TLC), Apalache at the proven depth"
python3 tools/check.py --tier "$TIER"
echo '== spec mutants'
python3 tools/mutate_spec.py

$PY -c 'import celery; print("celery from", celery.__file__)'
echo '== spec -> code: generate traces and replay them'
python3 tools/check.py --cex
for m in rtw mongo sql es; do
  mkdir -p traces/$m
  quint run --backend=typescript --main=$m result/resultstate.qnt --max-steps=14 \
    --n-traces=40 --max-samples=40 --seed=7 --out-itf="traces/$m/random_{seq}.itf.json" >/dev/null
done
$PY -m adapter.replay_results --backend redis traces/rtw/*.json
$PY -m adapter.replay_results --backend mongo traces/mongo/*.json
$PY -m adapter.replay_results --backend sql traces/sql/*.json
$PY -m adapter.replay_results --backend es traces/es/*.json
for p in redis kv; do
  for f in none dup refuse wlost_late wlost_early child_late child_early child_reject lease; do
    m=${p}_$f; [ "$m" = kv_lease ] && continue
    mkdir -p traces/chord/$m
    quint run --main=$m canvas/chord_instances.qnt --max-steps=40 --n-traces=20 --max-samples=20 \
      --seed=11 --out-itf="traces/chord/$m/random_{seq}.itf.json" >/dev/null
    $PY -m adapter.replay_chord --instance $m traces/chord/$m/*.json $(ls traces/cex/$m.*.json 2>/dev/null)
  done
done
mkdir -p traces/scenarios
for m in redisLost kvDup unlockChild; do
  quint test --backend=typescript --main=$m canvas/scenarios.qnt --out-itf="traces/scenarios/${m}_{test}.itf.json" >/dev/null
done
$PY -m adapter.replay_chord --instance redis_wlost_late traces/scenarios/redisLost_lost*.json
$PY -m adapter.replay_chord --instance redis_child_late traces/scenarios/redisLost_child*.json
$PY -m adapter.replay_chord --instance kv_dup traces/scenarios/kvDup_*.json

echo '== code -> spec: recorded histories against the specs'
if [ "$TIER" = long ]; then
  for b in redis mongo es; do
    $PY -m adapter.record_results --backend $b --n 200 --seed 1 > histories/$b.jsonl
  done
fi
$PY -m adapter.check_histories histories/redis.jsonl histories/mongo.jsonl histories/es.jsonl
if [ "$TIER" = long ]; then
  $PY -m adapter.record_chords --proto redis --chords 12
  $PY -m adapter.record_chords --proto kv --chords 12
else
  $PY -m adapter.record_chords --proto redis --no-record
  $PY -m adapter.record_chords --proto kv --no-record
fi

echo '== code mutants'
m() { $PY -m adapter.$2 --mutate "$1" "${@:3}" | tail -1; }
m kv_no_success_check replay_results --backend redis traces/rtw/*.json
m mongo_no_filter replay_results --backend mongo traces/mongo/*.json
m sql_no_success_check replay_results --backend sql traces/sql/*.json
m sql_no_row_lock replay_results --backend sql traces/sql/*.json
m redis_lua_no_dedupe replay_chord --instance redis_dup traces/chord/redis_dup/*.json
m redis_lua_claim_no_nx replay_chord --instance redis_dup traces/chord/redis_dup/*.json
m redis_lua_no_done_marker replay_chord --instance redis_dup traces/chord/redis_dup/*.json
m redis_cleanup_any_owner replay_chord --instance redis_lease traces/chord/redis_lease/*.json
m redis_extend_any_owner replay_chord --instance redis_lease traces/chord/redis_lease/*.json
m kv_count_early replay_chord --instance kv_dup traces/chord/kv_dup/*.json
m kv_keep_counter replay_chord --instance kv_dup traces/chord/kv_dup/*.json
m worker_lost_ignores_success replay_chord --instance redis_child_late traces/chord/redis_child_late/*.json
tier_done "$TIER"
