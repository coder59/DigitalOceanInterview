#!/usr/bin/env bash
# Smoke-test the compose stack after: docker compose up --build -d
set -euo pipefail

BASE="${BASE_URL:-http://localhost}"

echo "== health =="
curl -sS "$BASE/health"
echo

echo "== ingest =="
RESP=$(curl -sS -X POST "$BASE/api/v1/ingest" \
  -H 'Content-Type: application/json' \
  -d '[{"prompt":"hello from compose"},{"prompt":"second prompt"}]')
echo "$RESP"
BATCH_ID=$(echo "$RESP" | sed -n 's/.*"batch_id":"\([^"]*\)".*/\1/p')
echo "batch_id=$BATCH_ID"

echo "== poll / wait for results =="
for i in $(seq 1 60); do
  STATUS=$(curl -sS "$BASE/api/v1/ingest/batches/$BATCH_ID")
  echo "$STATUS"
  RESULTS=$(curl -sS -o /tmp/results.json -w '%{http_code}' "$BASE/api/v1/ingest/batches/$BATCH_ID/results" || true)
  if [ "$RESULTS" = "200" ]; then
    COUNT=$(sed -n 's/.*"count":\([0-9]*\).*/\1/p' /tmp/results.json)
    if [ "${COUNT:-0}" -ge 2 ]; then
      echo "== compiled results =="
      cat /tmp/results.json
      echo
      echo "== pool =="
      curl -sS "$BASE/api/v1/pool"
      echo
      echo "OK"
      exit 0
    fi
  fi
  sleep 1
done

echo "TIMEOUT waiting for results"
exit 1
