#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${RIMROCK_COMPOSE_FILE:-docker-compose.jetson.yml}"

cd "$PROJECT_DIR"
if [[ "${RIMROCK_BUILD:-0}" == "1" ]]; then
  echo "[$(date)] Starting rimrock service with image build..."
  docker compose -f "$COMPOSE_FILE" up -d --build
else
  echo "[$(date)] Starting rimrock service without rebuild (set RIMROCK_BUILD=1 to rebuild)..."
  docker compose -f "$COMPOSE_FILE" up -d --no-build
fi
