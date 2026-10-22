#!/bin/sh
# Container entrypoint for ROLE=worker: maps SIGTERM (docker stop / compose down / orchestrator eviction)
# to a graceful drain. The worker stops accepting streams, finishes its live calls, waits for its event
# spools to be acknowledged, then exits by itself (see engine/worker/lifecycle.py). Keep the container's
# stop grace period above DRAIN_TIMEOUT_SECONDS (compose: stop_grace_period: 16m).
set -u
PORT="${WORKER_HTTP_PORT:-8091}"
TOKEN="${INTERNAL_TOKEN:-dev-internal-token}"

gvox-worker start &
CHILD=$!

drain() {
  echo "worker-entrypoint: SIGTERM received, asking the worker to drain"
  if python - "$PORT" "$TOKEN" <<'EOF'
import sys, urllib.request
port, token = sys.argv[1], sys.argv[2]
req = urllib.request.Request(f"http://127.0.0.1:{port}/internal/drain", method="POST",
                             headers={"X-Internal-Token": token})
urllib.request.urlopen(req, timeout=5).read()
EOF
  then
    echo "worker-entrypoint: drain accepted; waiting for the worker to finish its calls"
  else
    echo "worker-entrypoint: drain request failed; forwarding SIGTERM"
    kill -TERM "$CHILD" 2>/dev/null || true
  fi
  wait "$CHILD"
  exit $?
}
trap drain TERM INT

wait "$CHILD"
