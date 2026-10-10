#!/usr/bin/env bash
# Run the chaos experiments from the README against a chosen Celery configuration.
#
#   scripts/test_spot_failure.sh hardened   # acks_late + reject_on_worker_lost + prefetch 1   (expect: survives)
#   scripts/test_spot_failure.sh default    # Celery's stock settings                          (expect: work lost)
#   scripts/test_spot_failure.sh prefetch   # prefetch 4 vs 1, no failures involved
#
# Env: MODES="sigkill child"  which eviction styles to run for hardened/default.
# The stack is switched into the requested mode first and put back to hardened afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."

SCENARIO="${1:-hardened}"
MODES="${MODES:-sigkill child}"
API_PORT="${API_PORT:-8100}"
API="${API:-http://localhost:$API_PORT}"
RMQ="${RMQ:-http://localhost:15672}"

# Demo-friendly Task Police timing so the "default" experiment finishes in about a minute.
# (The shipped defaults are 120s threshold / 60s sweep; see config.py.)
export STALL_THRESHOLD_SECONDS="${STALL_THRESHOLD_SECONDS:-30}"
export POLICE_INTERVAL_SECONDS="${POLICE_INTERVAL_SECONDS:-10}"
export API_PORT

rmq_consumers() { # queue name -> consumer count
  curl -fs -u pipeline:pipeline "$RMQ/api/queues/%2F/$1" 2>/dev/null |
    python3 -c "import sys,json; print(json.load(sys.stdin).get('consumers',0))" 2>/dev/null || echo 0
}

wait_ready() { # block until the API answers and the workers are consuming
  for _ in $(seq 1 60); do
    if curl -fs "$API/health" >/dev/null && [[ "$(rmq_consumers heavy_processing)" -ge 2 && "$(rmq_consumers default)" -ge 1 ]]; then
      sleep 2; return 0
    fi
    sleep 2
  done
  echo "workers did not come up; try: docker compose logs worker-spot" >&2; exit 1
}

apply_mode() { # reliability_mode [prefetch_multiplier]
  echo ">>> switching the stack to RELIABILITY_MODE=$1 ${2:+CELERY_PREFETCH_MULTIPLIER=$2}"
  RELIABILITY_MODE="$1" CELERY_PREFETCH_MULTIPLIER="${2:-}" \
    docker compose up -d --force-recreate --no-deps api worker-spot worker-core beat >/dev/null 2>&1
  wait_ready
}

restore() {
  echo ">>> restoring hardened mode"
  RELIABILITY_MODE=hardened CELERY_PREFETCH_MULTIPLIER="" STALL_THRESHOLD_SECONDS="" POLICE_INTERVAL_SECONDS="" \
    docker compose up -d --force-recreate --no-deps api worker-spot worker-core beat >/dev/null 2>&1 || true
  # Wait too, so whatever runs next (another scenario, or you) does not hit a half-started stack.
  wait_ready || true
}
trap restore EXIT

case "$SCENARIO" in
  hardened)
    apply_mode hardened
    rc=0
    for mode in $MODES; do
      echo; echo "================ hardened / $mode ================"
      python3 app/chaos/spot_killer.py --api "$API" --mode "$mode" || rc=$?
      [[ $rc -eq 0 ]] || { echo "FAIL: expected the call to complete without police help (exit $rc)"; exit "$rc"; }
    done
    echo; echo "PASS: hardened Celery survived: $MODES"
    ;;
  default)
    apply_mode default
    for mode in $MODES; do
      echo; echo "================ default / $mode ================"
      # Non-zero here is the EXPECTED lesson, not a script failure.
      python3 app/chaos/spot_killer.py --api "$API" --mode "$mode" --wait-for-police 120 || echo "(exit $? : the call did not complete on its own, as predicted for stock defaults)"
    done
    ;;
  prefetch)
    for prefetch in 4 1; do
      apply_mode hardened "$prefetch"
      echo; echo "================ worker_prefetch_multiplier=$prefetch ================"
      python3 app/chaos/prefetch_demo.py --api "$API" --rabbitmq "$RMQ"
    done
    ;;
  *)
    echo "usage: $0 hardened|default|prefetch" >&2; exit 64 ;;
esac
