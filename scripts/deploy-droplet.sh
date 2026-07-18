#!/usr/bin/env bash
# Runs on the DigitalOcean droplet after code is synced by GitHub Actions.
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/go-ingestion-api}"
cd "$APP_DIR"

echo "==> Deploying go-ingestion-api in $APP_DIR"

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
  else
    echo "docker compose not found" >&2
    exit 1
  fi
else
  echo "docker not found" >&2
  exit 1
fi

"${COMPOSE[@]}" pull || true
"${COMPOSE[@]}" up --build -d --remove-orphans

echo "==> Waiting for health..."
for i in $(seq 1 60); do
  if curl -fsS http://127.0.0.1/health >/dev/null 2>&1; then
    curl -fsS http://127.0.0.1/health
    echo
    echo "==> Deploy OK"
    "${COMPOSE[@]}" ps
    exit 0
  fi
  sleep 2
done

echo "Health check failed" >&2
"${COMPOSE[@]}" logs --tail=100
exit 1
