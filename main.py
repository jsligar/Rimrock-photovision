"""FastAPI application — Rimrock Photo Tagger API."""

import sys
from pathlib import Path
from urllib.parse import unquote

from dotenv import load_dotenv

# Load .env before importing config so env vars take effect
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

import config
import db

from api.routes import status, pipeline, clusters, photos, objects, settings as settings_router

app = FastAPI(
    title="Rimrock Photo Tagger",
    description="Local photo pipeline: face detection, semantic tagging, NAS sync.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routes
app.include_router(status.router, prefix="/api", tags=["status"])
app.include_router(pipeline.router, prefix="/api", tags=["pipeline"])
app.include_router(clusters.router, prefix="/api", tags=["clusters"])
app.include_router(photos.router, prefix="/api", tags=["photos"])
app.include_router(objects.router, prefix="/api", tags=["objects"])
app.include_router(settings_router.router, prefix="/api", tags=["settings"])

if config.ENABLE_SEARCH_LAYER:
    from api.routes import search
    app.include_router(search.router, prefix="/api", tags=["search"])

# Static file serving for crops and organized photos
# Only mount if directories exist
@app.on_event("startup")
def startup():
    db.init_db()
    config.CROPS_DIR.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config.ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)


@app.get("/crops/{file_path:path}")
def serve_crop(file_path: str):
    from fastapi import HTTPException

    decoded = unquote(file_path).replace("\\", "/").lstrip("/")
    candidates: list[Path] = []

    raw_path = Path(file_path)
    if raw_path.is_absolute():
        candidates.append(raw_path)

    # Preferred current shape: /crops/<face_file>
    candidates.append(config.CROPS_DIR / decoded)
    # Backward-compatible legacy shape: /crops/crops/<face_file>
    candidates.append(config.LOCAL_BASE / decoded)
    if decoded.startswith("crops/"):
        candidates.append(config.CROPS_DIR / decoded[len("crops/"):])

    seen = set()
    for path in candidates:
        norm = str(path)
        if norm in seen:
            continue
        seen.add(norm)
        if path.exists() and path.is_file():
            return FileResponse(str(path))

    raise HTTPException(status_code=404, detail="Crop not found")


@app.get("/organized/{file_path:path}")
def serve_organized(file_path: str):
    full_path = config.OUTPUT_DIR / file_path
    if not full_path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(full_path))


@app.get("/originals/{file_path:path}")
def serve_original(file_path: str):
    full_path = config.ORIGINALS_DIR / file_path
    if not full_path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(full_path))


# Serve web UI
_web_dir = Path(__file__).resolve().parent.parent / "web"

@app.get("/")
def serve_index():
    return FileResponse(str(_web_dir / "index.html"))

@app.get("/style.css")
def serve_css():
    return FileResponse(str(_web_dir / "style.css"), media_type="text/css")

@app.get("/app.js")
def serve_js():
    return FileResponse(str(_web_dir / "app.js"), media_type="application/javascript")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=False,
        workers=1,
    )
