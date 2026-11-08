#!/usr/bin/env bash
# Runs the bundled synthetic generator against a live local Kafka broker
# (Homebrew's kafka, KRaft mode, default port 9092) while the monitor
# watches, then prints the alerts it produced. Starts the `kafka` brew
# service if it isn't already listening on 9092, and leaves it running
# (it's a normal background service, not a per-demo resource) unless this
# script was the one that started it.
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC="dqstream-demo-$$"
STARTED_KAFKA=0

if ! nc -z localhost 9092 2>/dev/null; then
  echo "--- starting local kafka (brew services) ---"
  brew services start kafka >/dev/null
  STARTED_KAFKA=1
  for i in $(seq 1 30); do
    nc -z localhost 9092 2>/dev/null && break
    sleep 1
  done
fi

cleanup() {
  if [[ "$STARTED_KAFKA" == "1" ]]; then
    echo "--- stopping kafka (this script started it) ---"
    brew services stop kafka >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "--- generating synthetic events on topic $TOPIC (anomalies injected after 10s) ---"
./bin/dqstream generate -topic "$TOPIC" -rate 30 -duration 35s -inject-at 10s &
GEN_PID=$!

sleep 2
echo "--- monitoring (40s) ---"
./bin/dqstream monitor -topic "$TOPIC" -window 3s -freshness-threshold 5s -alert-cooldown 5s &
MON_PID=$!

sleep 40
kill "$MON_PID" 2>/dev/null || true
wait "$GEN_PID" 2>/dev/null || true
wait "$MON_PID" 2>/dev/null || true

echo "--- demo complete ---"
