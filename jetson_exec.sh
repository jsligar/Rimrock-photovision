#!/bin/bash
# Execute commands inside the Jetson runtime container.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="${RIMROCK_COMPOSE_FILE:-docker-compose.jetson.yml}"
SERVICE="${RIMROCK_SERVICE:-rimrock}"

cd "$PROJECT_DIR"

ARCH="$(uname -m)"
if [[ "$ARCH" != "aarch64" && "${RIMROCK_ALLOW_NON_JETSON:-0}" != "1" ]]; then
  echo "[$(date)] This command is intended to run on Jetson (aarch64)."
  echo "[$(date)] Current architecture: $ARCH"
  echo "[$(date)] If you intentionally want to try non-Jetson emulation, set RIMROCK_ALLOW_NON_JETSON=1."
  exit 1
fi

if ! docker compose -f "$COMPOSE_FILE" ps --status running --services | grep -qx "$SERVICE"; then
  echo "[$(date)] Starting Jetson container service '$SERVICE'..."
  if ! docker compose -f "$COMPOSE_FILE" up -d --no-build "$SERVICE"; then
    echo "[$(date)] Container image is missing. Build first with:"
    echo "[$(date)]   DOCKER_BUILDKIT=1 docker build --network=host -f Dockerfile.jetson -t rimrock-photovision-rimrock:latest ."
    exit 1
  fi
fi

if [ "$#" -eq 0 ]; then
  docker compose -f "$COMPOSE_FILE" exec "$SERVICE" bash
else
  cmd="$*"
  docker compose -f "$COMPOSE_FILE" exec "$SERVICE" bash -lc "$cmd"
fi
