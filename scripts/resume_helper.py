"""
resume_helper.py — determine the last incomplete phase and resume from there.
Used by resume_pipeline.sh.
"""

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db

PHASE_ORDER = ['preflight', 'pull', 'process', 'cluster', 'organize', 'tag', 'push', 'verify']

PHASE_SCRIPTS = {
    'preflight': 'pipeline/phase0_preflight.py',
    'pull':      'pipeline/phase1_pull.py',
    'process':   'pipeline/phase2_process.py',
    'cluster':   'pipeline/phase3_cluster.py',
    'organize':  'pipeline/phase4_organize.py',
    'tag':       'pipeline/phase5_tag.py',
    'push':      'pipeline/phase6_push.py',
    'verify':    'pipeline/phase7_verify.py',
}

# Phases that require explicit user confirmation before resuming
MANUAL_PAUSE_AFTER = {'cluster'}


def main():
    try:
        db.init_db()
    except Exception as e:
        print(f"Cannot open database: {e}")
        sys.exit(1)

    conn = db.get_db()
    rows = conn.execute("SELECT phase, status FROM pipeline_state").fetchall()
    conn.close()

    state = {r['phase']: r['status'] for r in rows}

    # Find the first phase that is not 'complete'
    resume_from = None
    for phase in PHASE_ORDER:
        status = state.get(phase, 'pending')
        if status != 'complete':
            resume_from = phase
            break

    if resume_from is None:
        print("All phases already complete. Nothing to resume.")
        sys.exit(0)

    print(f"Resuming from phase: {resume_from}")

    for phase in PHASE_ORDER[PHASE_ORDER.index(resume_from):]:
        status = state.get(phase, 'pending')

        if status == 'complete':
            print(f"  [{phase}] already complete — skipping")
            continue

        if phase in MANUAL_PAUSE_AFTER:
            print(f"\n  [{phase}] Requires manual cluster review before continuing.")
            print("  Open the web UI, review clusters, then run scripts/continue_pipeline.sh")
            sys.exit(0)

        # Stop at push phase — always require explicit confirmation
        if phase in ('push', 'verify'):
            print(f"\n  [{phase}] Requires explicit confirmation.")
            print("  Run scripts/push_to_nas.sh when ready.")
            sys.exit(0)

        script = PHASE_SCRIPTS[phase]
        print(f"\n  [{phase}] Running {script}...")
        result = subprocess.run([sys.executable, script])
        if result.returncode != 0:
            print(f"\n  [{phase}] FAILED (exit {result.returncode}). Fix the error and re-run resume.")
            sys.exit(result.returncode)

        # Re-check state after cluster — pause for review
        if phase == 'cluster':
            print(f"\n  [cluster] complete. Review clusters in the web UI.")
            print("  Run scripts/continue_pipeline.sh when ready.")
            sys.exit(0)

    print("\nAll phases up to push complete.")
    print("Run scripts/push_to_nas.sh to push to NAS.")


if __name__ == '__main__':
    main()
