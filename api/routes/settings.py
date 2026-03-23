"""Settings routes — read and write .env configuration."""

import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException

import config
import db
from api.models import SettingsUpdate

router = APIRouter()

ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


def _read_env() -> dict:
    env = {}
    if ENV_PATH.exists():
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()
    return env


def _write_env(data: dict) -> None:
    lines = []
    for k, v in data.items():
        lines.append(f"{k}={v}\n")
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


@router.get("/settings")
def get_settings():
    env = _read_env()

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
        "nas_source_dir": env.get("NAS_SOURCE_DIR", str(config.NAS_SOURCE_DIR)),
        "local_base": env.get("LOCAL_BASE", str(config.LOCAL_BASE)),
        "api_port": int(env.get("API_PORT", config.API_PORT)),
        "yolo_conf_threshold": config.YOLO_CONF_THRESHOLD,
        "clip_tag_threshold": config.CLIP_TAG_THRESHOLD,
        "max_inference_dim": config.MAX_INFERENCE_DIM,
        # Read-only stats
        "nvme_free_gb": round(disk.free / 1024**3, 1),
        "nvme_total_gb": round(disk.total / 1024**3, 1),
        "db_size_mb": round(db_size / 1024**2, 2),
        "total_photos": total_photos,
        "total_faces": total_faces,
    }


@router.post("/settings")
def update_settings(body: SettingsUpdate):
    env = _read_env()

    if body.nas_source_dir is not None:
        env["NAS_SOURCE_DIR"] = body.nas_source_dir
    if body.local_base is not None:
        env["LOCAL_BASE"] = body.local_base
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

    _write_env(env)
    return {"ok": True, "note": "Restart the API server for changes to take effect."}
