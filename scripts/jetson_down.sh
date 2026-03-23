#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${RIMROCK_COMPOSE_FILE:-docker-compose.jetson.yml}"

cd "$PROJECT_DIR"
docker compose -f "$COMPOSE_FILE" down
