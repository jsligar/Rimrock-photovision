"""Pipeline control routes — trigger and stop phases."""

import json
import threading
from fastapi import APIRouter, HTTPException

import db
from pipeline import shutdown

router = APIRouter()

# Module-level state for the running pipeline thread
_running_thread: threading.Thread | None = None
_running_phase: str | None = None
_running_workflow: str | None = None
_lock = threading.Lock()
_WORKFLOWS = {
    "intake": ["preflight", "pull", "process", "cluster"],
    "delivery": ["organize", "tag", "push", "verify"],
    "documents": ["ocr"],
}


def _set_active_workflow(name: str | None, phases: list[str] | None = None) -> None:
    conn = db.get_db()
    values = [
        ("active_workflow_name", name or ""),
        ("active_workflow_steps", json.dumps(phases or [])),
        ("active_workflow_started_at", db._now() if name else ""),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES (?, ?)",
        values,
    )
    conn.commit()
    conn.close()


def _execute_phase(phase: str) -> bool:
    if phase == "preflight":
        from pipeline.phase0_preflight import run_preflight

        result = run_preflight()
    elif phase == "pull":
        from pipeline.phase1_pull import run_pull

        result = run_pull()
    elif phase == "process":
        from pipeline.phase2_process import run_process

        result = run_process()
    elif phase == "cluster":
        from pipeline.phase3_cluster import run_cluster

        result = run_cluster()
    elif phase == "organize":
        from pipeline.phase4_organize import run_organize

        result = run_organize()
    elif phase == "tag":
        from pipeline.phase5_tag import run_tag

        result = run_tag()
    elif phase == "push":
        from pipeline.phase6_push import run_push

        result = run_push()
    elif phase == "verify":
        from pipeline.phase7_verify import run_verify

        result = run_verify()
    elif phase == "ocr":
        from pipeline.phase_ocr_documents import run_ocr_documents

        result = run_ocr_documents()
    else:
        db.mark_phase_error(phase, f"Unknown phase: {phase}")
        return False

    return True if result is None else bool(result)


def _phase_runner(phase: str) -> None:
    global _running_phase, _running_workflow
    shutdown.clear()
    try:
        _execute_phase(phase)
    except Exception as e:
        db.mark_phase_error(phase, str(e))
    finally:
        with _lock:
            _running_phase = None
            _running_workflow = None
        _set_active_workflow(None)


def _workflow_runner(workflow_name: str, phases: list[str]) -> None:
    global _running_phase, _running_workflow

    shutdown.clear()
    _set_active_workflow(workflow_name, phases)

    try:
        for phase in phases:
            if shutdown.is_requested():
                break

            with _lock:
                _running_phase = phase

            ok = _execute_phase(phase)
            if not ok or shutdown.is_requested():
                break
    except Exception as exc:
        if _running_phase:
            db.mark_phase_error(_running_phase, str(exc))
    finally:
        with _lock:
            _running_phase = None
            _running_workflow = None
        _set_active_workflow(None)


@router.post("/pipeline/run/{phase}")
def run_phase(phase: str):
    global _running_thread, _running_phase, _running_workflow

    valid_phases = ['preflight', 'pull', 'process', 'cluster', 'organize', 'tag', 'push', 'verify', 'ocr']
    if phase not in valid_phases:
        raise HTTPException(status_code=400, detail=f"Unknown phase: {phase}")

    with _lock:
        if _running_thread and _running_thread.is_alive():
            raise HTTPException(
                status_code=409,
                detail=f"Phase '{_running_phase}' is already running"
            )
        _running_phase = phase
        _running_workflow = None
        _running_thread = threading.Thread(
            target=_phase_runner,
            args=(phase,),
            daemon=True,
            name=f"pipeline-{phase}",
        )
        _running_thread.start()

    return {"started": phase}


@router.get("/pipeline/workflows")
def list_workflows():
    return {
        "workflows": [
            {"name": name, "phases": phases}
            for name, phases in _WORKFLOWS.items()
        ]
    }


@router.post("/pipeline/workflows/{workflow_name}")
def run_workflow(workflow_name: str):
    global _running_thread, _running_phase, _running_workflow

    phases = _WORKFLOWS.get(workflow_name)
    if not phases:
        raise HTTPException(status_code=400, detail=f"Unknown workflow: {workflow_name}")

    with _lock:
        if _running_thread and _running_thread.is_alive():
            running_label = _running_workflow or _running_phase or "pipeline"
            raise HTTPException(
                status_code=409,
                detail=f"Workflow or phase '{running_label}' is already running",
            )
        _running_phase = phases[0]
        _running_workflow = workflow_name
        _running_thread = threading.Thread(
            target=_workflow_runner,
            args=(workflow_name, phases),
            daemon=True,
            name=f"workflow-{workflow_name}",
        )
        _running_thread.start()

    return {"started": workflow_name, "phases": phases}


@router.post("/pipeline/stop")
def stop_pipeline():
    """Request graceful shutdown of the running phase."""
    shutdown.request()
    return {"requested": "stop"}


_PHASE_ORDER = [
    "preflight", "pull", "process", "cluster", "organize", "tag", "push", "verify"
]
_AUX_PHASES = {"ocr"}
_RESETTABLE_PHASES = set(_PHASE_ORDER) | _AUX_PHASES


def _downstream_phases(phase: str) -> list[str]:
    """Return all phases that come after `phase` in pipeline order."""
    try:
        idx = _PHASE_ORDER.index(phase)
    except ValueError:
        return []
    return _PHASE_ORDER[idx + 1:]


@router.post("/pipeline/reset/{phase}")
def reset_phase(phase: str, cascade: bool = True):
    """
    Reset a phase to 'pending'. By default also resets all downstream phases.

    Pass ?cascade=false to reset only the named phase.
    Refuses if the target phase or any cascade target is currently running —
    stop it first with POST /api/pipeline/stop.
    """
    if phase not in _RESETTABLE_PHASES:
        raise HTTPException(status_code=400, detail=f"Unknown phase: {phase}")

    phases_to_reset = [phase] + (_downstream_phases(phase) if cascade else [])

    with _lock:
        if _running_thread and _running_thread.is_alive() and _running_phase in phases_to_reset:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot reset: phase '{_running_phase}' is currently running. "
                    "Stop it first with POST /api/pipeline/stop."
                ),
            )

    db.reset_phase_state(phases_to_reset)
    return {"reset": phases_to_reset, "cascade": cascade}


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
