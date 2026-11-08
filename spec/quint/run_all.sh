#!/usr/bin/env bash
# Regenerate traces, check every property, replay against the real backends,
# record histories and check them, and run the mutation checks.
# Needs: quint (npm i -g @informalsystems/quint), Java 17+ for --verify,
# Redis 6379, MongoDB 27017, Postgres 5432 (postgres/pw, db celery),
# Elasticsearch 9200, memcached 11211.
set -euo pipefail
cd "$(dirname "$0")"
ROOT=$(git rev-parse --show-toplevel)
PY=${PY:-/home/user/.venv314/bin/python}
export PYTHONPATH="$ROOT:$PWD"
$PY -c 'import celery; print("celery from", celery.__file__)'

echo '== 1. properties (simulator), scenarios'
python3 check.py --samples 2000 --steps 40 --jobs 4
for m in redisLost kvDup unlockChild; do quint test --main=$m scenarios.qnt; done

echo '== 2a. spec -> code: generate traces and replay them'
for m in rtw mongo sql es; do
  mkdir -p traces/$m
  quint run --backend=typescript --main=$m resultstate.qnt --max-steps=14 \
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
    quint run --main=$m chord.qnt --max-steps=40 --n-traces=20 --max-samples=20 \
      --seed=11 --out-itf="traces/chord/$m/random_{seq}.itf.json" >/dev/null
    $PY -m adapter.replay_chord --instance $m traces/chord/$m/*.json $(ls traces/cex/$m.*.json 2>/dev/null)
  done
done
mkdir -p traces/scenarios
for m in redisLost kvDup unlockChild; do
  quint test --backend=typescript --main=$m scenarios.qnt --out-itf="traces/scenarios/${m}_{test}.itf.json" >/dev/null
done
$PY -m adapter.replay_chord --instance redis_wlost_late traces/scenarios/redisLost_lost*.json
$PY -m adapter.replay_chord --instance redis_child_late traces/scenarios/redisLost_child*.json
$PY -m adapter.replay_chord --instance kv_dup traces/scenarios/kvDup_*.json

echo '== 2b. code -> spec: record histories and check them'
for b in redis mongo es; do
  $PY -m adapter.record_results --backend $b --n 200 --seed 1 > histories/$b.jsonl
done
$PY -m adapter.check_histories histories/redis.jsonl histories/mongo.jsonl histories/es.jsonl
$PY -m adapter.record_chords --proto redis --chords 12
$PY -m adapter.record_chords --proto kv --chords 12

echo '== 2c. mutations'
python3 mutate_spec.py
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
