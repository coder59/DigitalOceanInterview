#!/usr/bin/env bash
# Runs on the DigitalOcean droplet after code is synced by GitHub Actions.
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/go-ingestion-api}"
cd "$APP_DIR"

echo "==> Deploying go-ingestion-api in $APP_DIR"

# Small droplets (512MB) OOM during `go build` without swap.
if ! swapon --show | grep -q .; then
  echo "==> Enabling 2G swap for Docker builds"
  if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile 2>/dev/null || dd if=/dev/zero of=/swapfile bs=1M count=2048
    chmod 600 /swapfile
    mkswap /swapfile
    grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
  fi
  swapon /swapfile || true
fi

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
