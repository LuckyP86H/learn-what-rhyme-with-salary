#!/usr/bin/env bash
# Trigger one call and watch it until it reaches a terminal state.
#   scripts/run_test_pipeline.sh [num_chunks] [faults_json]
#   scripts/run_test_pipeline.sh 8 '{"2":"transient"}'
# Exit code: 0 = COMPLETED, 1 = FAILED, 2 = timed out.
set -euo pipefail

API="${API:-http://localhost:${API_PORT:-8100}}"
CHUNKS="${1:-5}"
FAULTS="${2:-}"
[[ -n "$FAULTS" ]] || FAULTS='{}'
TIMEOUT="${TIMEOUT:-180}"

json() { python3 -c "import sys,json; d=json.load(sys.stdin); $1"; }

body=$(python3 -c "import json,sys; print(json.dumps({'title':'demo call','num_chunks':int(sys.argv[1]),'faults':json.loads(sys.argv[2])}))" "$CHUNKS" "$FAULTS")
call_id=$(curl -fsS -X POST "$API/calls" -H 'content-type: application/json' -d "$body" | json "print(d['call_id'])")
echo "call $call_id  ($CHUNKS chunks, faults=$FAULTS)"

start=$(date +%s)
last=""
while :; do
  state=$(curl -fsS "$API/calls/$call_id")
  line=$(echo "$state" | json "
c=d.get('chunks',[]); n=lambda s: sum(1 for x in c if x['status']==s)
print(f\"{d['status']:<10} chunks: {n('COMPLETED')} done, {n('PROCESSING')} running, {n('PENDING')} pending, {n('FAILED')} failed\")")
  elapsed=$(( $(date +%s) - start ))
  if [[ "$line" != "$last" ]]; then printf '[%3ss] %s\n' "$elapsed" "$line"; last="$line"; fi
  status=$(echo "$state" | json "print(d['status'])")
  if [[ "$status" == "COMPLETED" || "$status" == "FAILED" ]]; then break; fi
  if (( elapsed > TIMEOUT )); then echo "timed out after ${TIMEOUT}s (status $status)"; exit 2; fi
  sleep 1
done

echo "$state" | json "
print('summary:', d['summary']['text'] if d.get('summary') else None)
print('error:  ', d['error'])
for ch in d['chunks']:
    print(f\"  chunk {ch['index']}: {ch['status']:<10} attempts={ch['attempts']} worker={ch['worker']} fault={ch['fault']}\")"
echo "events: make events ID=$call_id"
[[ "$status" == "COMPLETED" ]]
