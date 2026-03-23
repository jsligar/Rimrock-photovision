#!/bin/bash
# Run all pipeline phases through Cluster. Stop on any error.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG=/local/rimrock/photos/rimrock_photos.log

cd "$PROJECT_DIR"

echo "[$(date)] ================================================================" | tee -a "$LOG"
echo "[$(date)] Starting full pipeline run (Preflight → Cluster)" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"

echo "[$(date)] Phase 0: Preflight..." | tee -a "$LOG"
python pipeline/phase0_preflight.py || { echo "[$(date)] PREFLIGHT FAILED — aborting." | tee -a "$LOG"; exit 1; }

echo "[$(date)] Phase 1: Pull..." | tee -a "$LOG"
python pipeline/phase1_pull.py || { echo "[$(date)] PULL FAILED." | tee -a "$LOG"; exit 1; }

echo "[$(date)] Phase 2: Process (face + semantic)..." | tee -a "$LOG"
python pipeline/phase2_process.py || { echo "[$(date)] PROCESS FAILED." | tee -a "$LOG"; exit 1; }

echo "[$(date)] Phase 3: Cluster..." | tee -a "$LOG"
python pipeline/phase3_cluster.py || { echo "[$(date)] CLUSTER FAILED." | tee -a "$LOG"; exit 1; }

echo "" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
echo "[$(date)] Cluster phase complete." | tee -a "$LOG"
echo "[$(date)] Review clusters in the web UI before continuing." | tee -a "$LOG"
echo "[$(date)] Web UI: http://rimrock:8420" | tee -a "$LOG"
echo "[$(date)] Run scripts/continue_pipeline.sh when ready." | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
