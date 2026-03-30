#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${RIMROCK_COMPOSE_FILE:-docker-compose.jetson.yml}"

cd "$PROJECT_DIR"

echo "=== docker compose ps ==="
docker compose -f "$COMPOSE_FILE" ps

echo
echo "=== api status probe ==="
if curl -fsS "http://127.0.0.1:${API_PORT:-8420}/api/status" >/dev/null 2>&1; then
  echo "API: UP"
else
  echo "API: DOWN"
fi

