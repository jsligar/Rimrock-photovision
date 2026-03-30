"""FastAPI application for Rimrock Photo Tagger."""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

import config
import db
import person_memory
from api.routes import clusters, objects, photos, pipeline, settings as settings_router, status


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    config.CROPS_DIR.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config.ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    conn = db.get_db()
    try:
        person_memory.sync_person_memory_from_db(
            conn,
            min_approved_faces=config.CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES,
            min_clean_face_score=config.CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE,
            preserve_existing_on_empty=True,
        )
    finally:
        conn.close()
    yield
    db.close_thread_db()


app = FastAPI(
    title="Rimrock Photo Tagger",
    description="Local photo pipeline: face detection, semantic tagging, NAS sync.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(status.router, prefix="/api", tags=["status"])
app.include_router(pipeline.router, prefix="/api", tags=["pipeline"])
app.include_router(clusters.router, prefix="/api", tags=["clusters"])
if getattr(config, "ENABLE_SEARCH_LAYER", False):
    from api.routes import search

    app.include_router(search.router, prefix="/api", tags=["search"])
if getattr(config, "NVIDIA_BURST_ENABLED", False):
    from api.routes import burst

    app.include_router(burst.router, prefix="/api", tags=["burst"])
app.include_router(photos.router, prefix="/api", tags=["photos"])
app.include_router(objects.router, prefix="/api", tags=["objects"])
app.include_router(settings_router.router, prefix="/api", tags=["settings"])

_web_dir = Path(__file__).resolve().parent.parent / "web"
_STATIC_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, max-age=0",
    "Pragma": "no-cache",
}


def _resolve_asset_path(base_dir: Path, file_path: str, marker: str | None = None) -> Path | None:
    decoded = unquote(file_path).replace("\\", "/").strip()
    if not decoded:
        return None

    normalized = "/" + decoded.lstrip("/")
    if marker and marker in normalized:
        normalized = normalized.split(marker, 1)[1]
    normalized = normalized.lstrip("/")
    parts = [part for part in normalized.split("/") if part]
    if not parts or any(part in {".", ".."} for part in parts):
        return None

    base_resolved = base_dir.resolve(strict=False)
    candidate = base_resolved.joinpath(*parts).resolve(strict=False)
    try:
        candidate.relative_to(base_resolved)
    except ValueError:
        return None

    if candidate.exists() and candidate.is_file():
        return candidate
    return None


@app.get("/crops/{file_path:path}")
def serve_crop(file_path: str):
    full_path = _resolve_asset_path(config.CROPS_DIR, file_path, marker="/crops/")
    if full_path is None:
        raise HTTPException(status_code=404, detail="Crop not found")
    return FileResponse(str(full_path))


@app.get("/organized/{file_path:path}")
def serve_organized(file_path: str):
    full_path = _resolve_asset_path(config.OUTPUT_DIR, file_path)
    if full_path is None:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(full_path))


@app.get("/originals/{file_path:path}")
def serve_original(file_path: str):
    full_path = _resolve_asset_path(config.ORIGINALS_DIR, file_path)
    if full_path is None:
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(full_path))


@app.get("/")
def serve_index():
    return FileResponse(str(_web_dir / "index.html"), headers=_STATIC_NO_CACHE_HEADERS)


@app.get("/style.css")
def serve_css():
    return FileResponse(
        str(_web_dir / "style.css"),
        media_type="text/css",
        headers=_STATIC_NO_CACHE_HEADERS,
    )


@app.get("/app.js")
def serve_js():
    return FileResponse(
        str(_web_dir / "app.js"),
        media_type="application/javascript",
        headers=_STATIC_NO_CACHE_HEADERS,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT,
        reload=False,
    )
