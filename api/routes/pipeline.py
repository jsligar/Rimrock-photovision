"""Pipeline control routes — trigger and stop phases."""

import threading
from fastapi import APIRouter, HTTPException

import db

router = APIRouter()

# Module-level state for the running pipeline thread
_running_thread: threading.Thread | None = None
_running_phase: str | None = None
_lock = threading.Lock()


def _phase_runner(phase: str) -> None:
    global _running_phase
    try:
        if phase == "preflight":
            from pipeline.phase0_preflight import run_preflight
            run_preflight()
        elif phase == "pull":
            from pipeline.phase1_pull import run_pull
            run_pull()
        elif phase == "process":
            from pipeline.phase2_process import run_process
            run_process()
        elif phase == "cluster":
            from pipeline.phase3_cluster import run_cluster
            run_cluster()
        elif phase == "organize":
            from pipeline.phase4_organize import run_organize
            run_organize()
        elif phase == "tag":
            from pipeline.phase5_tag import run_tag
            run_tag()
        elif phase == "push":
            from pipeline.phase6_push import run_push
            run_push()
        elif phase == "verify":
            from pipeline.phase7_verify import run_verify
            run_verify()
        else:
            db.mark_phase_error(phase, f"Unknown phase: {phase}")
    except Exception as e:
        db.mark_phase_error(phase, str(e))
    finally:
        with _lock:
            _running_phase = None


@router.post("/pipeline/run/{phase}")
def run_phase(phase: str):
    global _running_thread, _running_phase

    valid_phases = ['preflight', 'pull', 'process', 'cluster', 'organize', 'tag', 'push', 'verify']
    if phase not in valid_phases:
        raise HTTPException(status_code=400, detail=f"Unknown phase: {phase}")

    with _lock:
        if _running_thread and _running_thread.is_alive():
            raise HTTPException(
                status_code=409,
                detail=f"Phase '{_running_phase}' is already running"
            )
        _running_phase = phase
        _running_thread = threading.Thread(
            target=_phase_runner,
            args=(phase,),
            daemon=True,
            name=f"pipeline-{phase}",
        )
        _running_thread.start()

    return {"started": phase}


@router.post("/pipeline/stop")
def stop_pipeline():
    """Request graceful shutdown of the running phase."""
    import signal
    import os

    # Set shutdown flag in process module if running
    try:
        from pipeline import phase2_process
        phase2_process.shutdown_requested = True
    except Exception:
        pass

    return {"requested": "stop"}
