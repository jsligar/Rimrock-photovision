#!/bin/bash
# Resume the pipeline from the last incomplete phase.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "[$(date)] Checking pipeline state..."
python scripts/resume_helper.py
