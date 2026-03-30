"""Settings routes — read and write .env configuration."""

import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException

from batch_scope import resolve_optional_path
import config
import db
import nvidia_burst
from api.models import SettingsUpdate

router = APIRouter()

ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


def _iter_env_lines(text: str):
    """Yield normalized KEY=VALUE lines, including repair of literal '\\n' corruption."""
    for raw_line in text.replace("\r\n", "\n").splitlines():
        for part in raw_line.split("\\n"):
            line = part.strip()
            if line:
                yield line


def _read_env() -> dict:
    env = {}
    if ENV_PATH.exists():
        text = ENV_PATH.read_text(encoding="utf-8")
        for line in _iter_env_lines(text):
            if not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env


def _write_env(data: dict) -> None:
    lines = []
    for k, v in data.items():
        clean = str(v).replace("\r", " ").replace("\n", " ").strip()
        lines.append(f"{k}={clean}\n")
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


@router.get("/settings")
def get_settings():
    disk = shutil.disk_usage(str(config.LOCAL_BASE.parent))
    conn = db.get_db()
    db_size = 0
    try:
        db_size = config.DB_PATH.stat().st_size if config.DB_PATH.exists() else 0
    except Exception:
        pass
    total_photos = conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
    total_faces = conn.execute("SELECT COUNT(*) FROM faces").fetchone()[0]
    conn.close()

    return {
        "nas_source_dir": str(config.NAS_SOURCE_DIR),
        "local_base": str(config.LOCAL_BASE),
        "test_year_scope": config.TEST_YEAR_SCOPE,
        "batch_manifest_path": str(config.BATCH_MANIFEST_PATH) if config.BATCH_MANIFEST_PATH else None,
        "api_port": config.API_PORT,
        "yolo_conf_threshold": config.YOLO_CONF_THRESHOLD,
        "clip_tag_threshold": config.CLIP_TAG_THRESHOLD,
        "max_inference_dim": config.MAX_INFERENCE_DIM,
        "det_thresh": config.DET_THRESH,
        "umap_n_neighbors": config.UMAP_N_NEIGHBORS,
        "hdbscan_min_cluster_size": config.HDBSCAN_MIN_CLUSTER_SIZE,
        "hdbscan_min_samples": config.HDBSCAN_MIN_SAMPLES,
        # Read-only stats
        "nvme_free_gb": round(disk.free / 1024**3, 1),
        "nvme_total_gb": round(disk.total / 1024**3, 1),
        "db_size_mb": round(db_size / 1024**2, 2),
        "total_photos": total_photos,
        "total_faces": total_faces,
        "search_layer_enabled": config.ENABLE_SEARCH_LAYER,
        "burst": nvidia_burst.get_usage_summary(),
    }


@router.post("/settings")
def update_settings(body: SettingsUpdate):
    env = _read_env()

    if body.nas_source_dir is not None:
        env["NAS_SOURCE_DIR"] = body.nas_source_dir
    if body.local_base is not None:
        env["LOCAL_BASE"] = body.local_base
    if body.batch_manifest_path is not None:
        env["BATCH_MANIFEST_PATH"] = body.batch_manifest_path.strip()
    if body.yolo_conf_threshold is not None:
        if not (0.0 < body.yolo_conf_threshold < 1.0):
            raise HTTPException(status_code=400, detail="yolo_conf_threshold must be between 0 and 1")
        env["YOLO_CONF_THRESHOLD"] = str(body.yolo_conf_threshold)
    if body.clip_tag_threshold is not None:
        if not (0.0 < body.clip_tag_threshold < 1.0):
            raise HTTPException(status_code=400, detail="clip_tag_threshold must be between 0 and 1")
        env["CLIP_TAG_THRESHOLD"] = str(body.clip_tag_threshold)
    if body.max_inference_dim is not None:
        if body.max_inference_dim < 320 or body.max_inference_dim > 4096:
            raise HTTPException(status_code=400, detail="max_inference_dim must be between 320 and 4096")
        env["MAX_INFERENCE_DIM"] = str(body.max_inference_dim)
    if body.det_thresh is not None:
        if not (0.05 <= body.det_thresh <= 0.95):
            raise HTTPException(status_code=400, detail="det_thresh must be between 0.05 and 0.95")
        env["DET_THRESH"] = str(body.det_thresh)
    if body.umap_n_neighbors is not None:
        if not (2 <= body.umap_n_neighbors <= 200):
            raise HTTPException(status_code=400, detail="umap_n_neighbors must be between 2 and 200")
        env["UMAP_N_NEIGHBORS"] = str(body.umap_n_neighbors)
    if body.hdbscan_min_cluster_size is not None:
        if not (2 <= body.hdbscan_min_cluster_size <= 50):
            raise HTTPException(status_code=400, detail="hdbscan_min_cluster_size must be between 2 and 50")
        env["HDBSCAN_MIN_CLUSTER_SIZE"] = str(body.hdbscan_min_cluster_size)
    if body.hdbscan_min_samples is not None:
        if not (1 <= body.hdbscan_min_samples <= 30):
            raise HTTPException(status_code=400, detail="hdbscan_min_samples must be between 1 and 30")
        env["HDBSCAN_MIN_SAMPLES"] = str(body.hdbscan_min_samples)

    _write_env(env)

    # Apply changes to the live config module so they take effect immediately
    # without an API restart.  Only update tunable numeric settings.
    _apply_live = {
        "YOLO_CONF_THRESHOLD": ("yolo_conf_threshold", float),
        "CLIP_TAG_THRESHOLD": ("clip_tag_threshold", float),
        "MAX_INFERENCE_DIM": ("max_inference_dim", int),
        "DET_THRESH": ("det_thresh", float),
        "UMAP_N_NEIGHBORS": ("umap_n_neighbors", int),
        "HDBSCAN_MIN_CLUSTER_SIZE": ("hdbscan_min_cluster_size", int),
        "HDBSCAN_MIN_SAMPLES": ("hdbscan_min_samples", int),
    }
    for env_key, (attr, cast) in _apply_live.items():
        if env_key in env:
            setattr(config, attr.upper(), cast(env[env_key]))

    if "BATCH_MANIFEST_PATH" in env:
        config.BATCH_MANIFEST_PATH = resolve_optional_path(
            env["BATCH_MANIFEST_PATH"],
            base_dir=Path(env.get("LOCAL_BASE", str(config.LOCAL_BASE))),
        )

    return {"ok": True, "note": "Settings saved and applied."}


@router.post("/settings/clear-db")
def clear_db():
    """Destructively reset the SQLite database and reinitialize schema."""
    conn = db.get_db()
    running = conn.execute(
        "SELECT phase FROM pipeline_state WHERE status='running' LIMIT 1"
    ).fetchone()
    running_job = conn.execute(
        "SELECT job_name FROM background_jobs WHERE status='running' LIMIT 1"
    ).fetchone()
    conn.close()

    if running:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot clear DB while phase '{running['phase']}' is running",
        )
    if running_job:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot clear DB while background job '{running_job['job_name']}' is running",
        )

    try:
        if config.DB_PATH.exists():
            config.DB_PATH.unlink()
        db.init_db()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear database: {e}")

    return {
        "ok": True,
        "note": "Database cleared and reinitialized. Person memory and batch manifest settings were preserved.",
    }
