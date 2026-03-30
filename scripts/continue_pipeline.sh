#!/bin/bash
# Run Organize and Tag phases. Run after cluster review is complete.
set -e

if [[ -z "${RIMROCK_IN_CONTAINER:-}" && "${RIMROCK_SKIP_CONTAINER:-0}" != "1" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  exec bash "$SCRIPT_DIR/jetson_exec.sh" "bash scripts/continue_pipeline.sh"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG=/local/rimrock/photos/rimrock_photos.log

cd "$PROJECT_DIR"

echo "[$(date)] ================================================================" | tee -a "$LOG"
echo "[$(date)] Continuing pipeline: Organize → Tag" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"

echo "[$(date)] Phase 4: Organize..." | tee -a "$LOG"
python pipeline/phase4_organize.py || { echo "[$(date)] ORGANIZE FAILED." | tee -a "$LOG"; exit 1; }

echo "[$(date)] Phase 5: Tag..." | tee -a "$LOG"
python pipeline/phase5_tag.py || { echo "[$(date)] TAG FAILED." | tee -a "$LOG"; exit 1; }

echo "" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
echo "[$(date)] Organize and Tag complete." | tee -a "$LOG"
echo "[$(date)] Review organized output in the web UI." | tee -a "$LOG"
echo "[$(date)] When ready to push to NAS: run scripts/push_to_nas.sh" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
