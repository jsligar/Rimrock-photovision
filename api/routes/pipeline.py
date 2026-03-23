"""Pipeline control routes — trigger and stop phases."""

import threading
from fastapi import APIRouter, HTTPException

import db
from pipeline import shutdown

router = APIRouter()

# Module-level state for the running pipeline thread
_running_thread: threading.Thread | None = None
_running_phase: str | None = None
_lock = threading.Lock()


def _phase_runner(phase: str) -> None:
    global _running_phase
    shutdown.clear()
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
    shutdown.request()
    return {"requested": "stop"}


@router.get("/pipeline/log-tail")
def log_tail(lines: int = 50):
    """Return the last N lines of the pipeline log file."""
    import config as _config
    log_path = _config.LOG_PATH
    if not log_path.exists():
        return {"lines": []}
    try:
        with open(log_path, "r", errors="replace") as f:
            all_lines = f.readlines()
        tail = [l.rstrip("\n") for l in all_lines[-lines:]]
        return {"lines": tail}
    except Exception as e:
        return {"lines": [], "error": str(e)}
