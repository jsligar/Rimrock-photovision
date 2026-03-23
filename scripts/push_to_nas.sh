#!/bin/bash
# Final NAS push — explicit separate step requiring confirmation.
set -e

if [[ -z "${RIMROCK_IN_CONTAINER:-}" && "${RIMROCK_SKIP_CONTAINER:-0}" != "1" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  exec bash "$SCRIPT_DIR/jetson_exec.sh" "bash scripts/push_to_nas.sh"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG=/local/rimrock/photos/rimrock_photos.log

cd "$PROJECT_DIR"

echo ""
echo "================================================================"
echo "  RIMROCK PHOTO TAGGER — NAS PUSH"
echo "================================================================"
echo ""
echo "  This will push organized photos to the NAS."
echo "  Destination: NAS/organized/ (new folder, originals NOT touched)"
echo ""
echo "  Preflight:"
echo "    Source:  /local/rimrock/photos/organized/"
echo "    Dest:    \$(NAS_SOURCE_DIR)/../organized/"
echo ""

read -p "  Type CONFIRM to proceed: " confirm
if [ "$confirm" != "CONFIRM" ]; then
  echo "  Aborted."
  exit 1
fi

echo ""
echo "[$(date)] Phase 6: Push..." | tee -a "$LOG"
python pipeline/phase6_push.py || { echo "[$(date)] PUSH FAILED." | tee -a "$LOG"; exit 1; }

echo "[$(date)] Phase 7: Verify..." | tee -a "$LOG"
python pipeline/phase7_verify.py || { echo "[$(date)] VERIFY FAILED." | tee -a "$LOG"; exit 1; }

echo "" | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
echo "[$(date)] Push and verify complete. Pipeline finished." | tee -a "$LOG"
echo "[$(date)] ================================================================" | tee -a "$LOG"
