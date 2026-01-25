#!/usr/bin/env bash
set -euo pipefail

BROKERS="${BROKERS:-redpanda:9092}"

echo "[topic-init] Waiting for Redpanda at ${BROKERS}..."
for i in {1..60}; do
  if rpk cluster info --brokers "${BROKERS}" > /dev/null 2>&1; then
    echo "[topic-init] Redpanda is ready."
    break
  fi
  sleep 1
done

echo "[topic-init] Creating topic 'transactions' (idempotent)..."
rpk topic create transactions \
  --brokers "${BROKERS}" \
  --partitions 3 \
  --replicas 1 \
  2>/dev/null || true

echo "[topic-init] Done."
