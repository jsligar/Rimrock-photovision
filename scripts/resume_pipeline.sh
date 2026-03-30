#!/bin/bash
# Resume the pipeline from the last incomplete phase.
set -e

if [[ -z "${RIMROCK_IN_CONTAINER:-}" && "${RIMROCK_SKIP_CONTAINER:-0}" != "1" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  exec bash "$SCRIPT_DIR/jetson_exec.sh" "bash scripts/resume_pipeline.sh"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "[$(date)] Checking pipeline state..."
python scripts/resume_helper.py
