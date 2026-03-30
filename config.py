import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

# Load .env for all entrypoints (API + direct phase scripts).
# Already-exported env vars keep precedence over file values.
ENV_FILE = Path(
    os.getenv("RIMROCK_ENV_FILE", str(Path(__file__).resolve().parent / ".env"))
)
load_dotenv(ENV_FILE, override=False)


def _resolve_optional_path(raw_value: str | None, *, base_dir: Path | None = None) -> Path | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


def _default_local_base() -> str:
    if os.name == "nt":
        return r"C:\local\rimrock\photos"
    return "/local/rimrock/photos"


def _default_trt_engine_cache() -> str:
    if os.name == "nt":
        return r"C:\local\rimrock\trt_cache"
    return "/local/rimrock/trt_cache"


def _default_exiftool_bin() -> str:
    resolved = shutil.which("exiftool")
    if resolved:
        return resolved
    if os.name == "nt":
        candidate = Path.home() / "AppData" / "Local" / "Programs" / "ExifTool" / "ExifTool.exe"
        if candidate.exists():
            return str(candidate)
    return "exiftool"

# ── Storage ────────────────────────────────────────────────────────────────────
NAS_SOURCE_DIR  = Path(os.getenv("NAS_SOURCE_DIR",  "/mnt/mycloud/photos"))
LOCAL_BASE      = Path(os.getenv("LOCAL_BASE", _default_local_base()))
ORIGINALS_DIR   = LOCAL_BASE / "originals"
OUTPUT_DIR      = LOCAL_BASE / "organized"
DOCUMENTS_DIR   = LOCAL_BASE / "documents"
CROPS_DIR       = LOCAL_BASE / "crops"
DB_PATH         = LOCAL_BASE / "rimrock_photos.db"
PERSON_MEMORY_PATH = LOCAL_BASE / "person_memory.json"
LOG_PATH        = LOCAL_BASE / "rimrock_photos.log"
RSYNC_PULL_LOG  = LOCAL_BASE / "rsync_pull.log"
RSYNC_PUSH_LOG  = LOCAL_BASE / "rsync_push.log"
# Phase 1 prefilter output: files whose extension and binary signature disagree.
# Phase 2 will skip any relative paths listed here.
PREFILTER_REJECTS_PATH = LOCAL_BASE / "prefilter_rejects.tsv"

# Optional test scope to limit pull/process volume by year (e.g. "2025").
# Empty by default (full dataset).
TEST_YEAR_SCOPE = os.getenv("TEST_YEAR_SCOPE", "").strip() or None

# Optional exact-file scope. When set, only manifest-listed files under
# ORIGINALS_DIR are eligible for process/organize/tag/push/verify.
BATCH_MANIFEST_PATH = _resolve_optional_path(
    os.getenv("BATCH_MANIFEST_PATH", ""),
    base_dir=LOCAL_BASE,
)

# Minimum free NVMe space before pull is allowed (bytes)
MIN_FREE_BYTES  = 50 * 1024**3   # 50 GB safety margin

# ── InsightFace ────────────────────────────────────────────────────────────────
INSIGHTFACE_MODEL = "buffalo_l"
DET_SIZE          = (640, 640)
DET_THRESH        = float(os.getenv("DET_THRESH", 0.4))
CTX_ID            = 0

# ── UMAP ──────────────────────────────────────────────────────────────────────
UMAP_N_NEIGHBORS   = int(os.getenv("UMAP_N_NEIGHBORS", 30))
UMAP_MIN_DIST      = 0.0
UMAP_N_COMPONENTS  = 64
UMAP_METRIC        = "cosine"

# ── HDBSCAN ───────────────────────────────────────────────────────────────────
HDBSCAN_MIN_CLUSTER_SIZE = int(os.getenv("HDBSCAN_MIN_CLUSTER_SIZE", 3))
HDBSCAN_MIN_SAMPLES      = int(os.getenv("HDBSCAN_MIN_SAMPLES", 1))
HDBSCAN_METRIC           = "euclidean"

# ── Incremental clustering ────────────────────────────────────────────────────
# When True and approved+labeled clusters exist, freeze them and only cluster
# new/unknown faces. Set CLUSTER_INCREMENTAL_MODE=false to force a full re-cluster.
CLUSTER_INCREMENTAL_MODE             = os.getenv("CLUSTER_INCREMENTAL_MODE", "true").lower() == "true"
CLUSTER_INCREMENTAL_ASSIGN_THRESHOLD = float(os.getenv("CLUSTER_INCREMENTAL_ASSIGN_THRESHOLD", "0.65"))

# ── Ground Truth ──────────────────────────────────────────────────────────────
GT_MIN_FACES       = 3
GT_MIN_CONFIDENCE  = 0.75

# ── Cluster Review Prototypes ─────────────────────────────────────────────────
# A person label becomes "usable" for review suggestions only after enough
# clean approved faces support its prototype embedding.
CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES = int(
    os.getenv("CLUSTER_REVIEW_USABLE_MIN_APPROVED_FACES", 5)
)
CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE = float(
    os.getenv("CLUSTER_REVIEW_MIN_CLEAN_FACE_SCORE", 0.75)
)
PERSON_PROTOTYPE_MATCH_THRESHOLD = float(
    os.getenv("PERSON_PROTOTYPE_MATCH_THRESHOLD", 0.65)
)

# ── YOLOv8 ────────────────────────────────────────────────────────────────────
# YOLOv8s chosen over YOLOv8m to preserve ~500MB RAM headroom on 8GB unified memory
# Set YOLO_MODEL=yolov8s.engine in docker-compose after TRT export.
YOLO_MODEL          = os.getenv("YOLO_MODEL", "yolov8s.pt")
YOLO_CONF_THRESHOLD = float(os.getenv("YOLO_CONF_THRESHOLD", 0.45))
YOLO_IOU_THRESHOLD  = 0.5

# ── TensorRT ────────────────────────────────────────────────────────────────
TRT_ENGINE_CACHE    = Path(os.getenv("TRT_ENGINE_CACHE", _default_trt_engine_cache()))
EXIFTOOL_BIN        = os.getenv("EXIFTOOL_BIN", _default_exiftool_bin())

# ── CLIP ──────────────────────────────────────────────────────────────────────
CLIP_MODEL          = "ViT-B/32"
CLIP_DEVICE         = "cuda"
CLIP_TAG_THRESHOLD  = float(os.getenv("CLIP_TAG_THRESHOLD", 0.26))

# ── Image Processing ──────────────────────────────────────────────────────────
# Resize on longest edge before inference — reduces buffer memory for large photos
MAX_INFERENCE_DIM   = int(os.getenv("MAX_INFERENCE_DIM", 1920))
IMAGE_EXTENSIONS    = {".jpg", ".jpeg", ".png", ".heic", ".tiff", ".tif", ".webp"}
RAW_EXTENSIONS      = {".raw", ".cr2", ".cr3", ".nef", ".arw", ".dng"}
# Exclude screenshot-like files from process phase discovery.
# Override with env var SCREENSHOT_EXCLUDE_PATTERNS as comma-separated tokens.
_SCREENSHOT_DEFAULTS = ["screenshot", "screen shot", "screenrecording", "screen recording"]
SCREENSHOT_EXCLUDE_PATTERNS = [
    token.strip().lower()
    for token in os.getenv("SCREENSHOT_EXCLUDE_PATTERNS", ",".join(_SCREENSHOT_DEFAULTS)).split(",")
    if token.strip()
]

# ── Semantic Tag Vocabulary ────────────────────────────────────────────────────
SEMANTIC_TAG_GROUPS = {
    # First-run core vocabulary: smaller tag set for faster CLIP inference.
    "vehicles": {
        "pickup truck": ["a pickup truck", "a truck in a driveway"],
        "SUV":          ["an SUV", "a sport utility vehicle"],
        "car":          ["a passenger car", "a sedan"],
    },
    "animals": {
        "dog":    ["a dog", "a pet dog", "a dog playing outside"],
        "cat":    ["a cat", "a pet cat", "a cat indoors"],
        "horse":  ["a horse", "a horse in a field"],
        "cattle": ["cattle", "cows in a field", "livestock"],
        "deer":   ["a deer", "whitetail deer"],
        "bird":   ["a bird", "birds in the yard"],
    },
    "scenes": {
        "outdoors": ["outdoor scenery", "nature landscape"],
        "birthday": ["a birthday party", "birthday cake"],
        "holiday":  ["holiday decorations", "Christmas tree"],
        "military": ["military uniform", "Army uniform", "military ceremony"],
    },
}

YOLO_DIRECT_TAGS = {
    "car": "car",
    "truck": "pickup truck",
    "dog": "dog",
    "cat": "cat",
    "horse": "horse",
    "cow": "cattle",
    "bird": "bird",
    "sheep": "cattle",
}

# ── API ────────────────────────────────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", 8420))
UNDATED_DIR = "undated"

# ── Search Layer (gated) ─────────────────────────────────────────────────────
ENABLE_SEARCH_LAYER   = os.getenv("ENABLE_SEARCH_LAYER", "").lower() in ("1", "true", "yes")
SEARCH_TOP_K          = int(os.getenv("SEARCH_TOP_K", 50))
SEARCH_CLIP_WEIGHT    = float(os.getenv("SEARCH_CLIP_WEIGHT", 0.6))
SEARCH_TEXT_WEIGHT    = float(os.getenv("SEARCH_TEXT_WEIGHT", 0.25))
SEARCH_TAG_WEIGHT     = float(os.getenv("SEARCH_TAG_WEIGHT", 0.15))
SEARCH_KEYWORD_WEIGHT = float(os.getenv("SEARCH_KEYWORD_WEIGHT", 0.35))
SEARCH_KEEP_CLIP_WARM = os.getenv("SEARCH_KEEP_CLIP_WARM", "true").lower() in ("1", "true", "yes")
# ── Document Detection ────────────────────────────────────────────────────────
# CLIP-based document scoring: computed for free after image embedding.
# Photos scoring above threshold are flagged is_document=1 and routed to
# DOCUMENTS_DIR for separate OCR processing.
DOCUMENT_DETECTION_ENABLED = os.getenv("DOCUMENT_DETECTION_ENABLED", "true").lower() in ("1", "true", "yes")
DOCUMENT_CLIP_THRESHOLD    = float(os.getenv("DOCUMENT_CLIP_THRESHOLD", "0.22"))
DOCUMENT_CLIP_PROMPTS = [
    "a document with printed text",
    "a receipt or invoice",
    "a page of written text",
    "a letter or form",
    "a whiteboard with handwriting",
    "a sign with text",
    "a page from a book",
    "a screenshot of text",
]

SEARCH_OCR_ENABLED    = os.getenv("SEARCH_OCR_ENABLED", "true").lower() in ("1", "true", "yes")
SEARCH_OCR_LANG       = os.getenv("SEARCH_OCR_LANG", "eng")
SEARCH_OCR_PSM        = int(os.getenv("SEARCH_OCR_PSM", 6))
SEARCH_OCR_TIMEOUT_SEC = int(os.getenv("SEARCH_OCR_TIMEOUT_SEC", 20))
SEARCH_OCR_MAX_DIM    = int(os.getenv("SEARCH_OCR_MAX_DIM", 1600))
SEARCH_OCR_MAX_CHARS  = int(os.getenv("SEARCH_OCR_MAX_CHARS", 2000))
SEARCH_OCR_MIN_CHARS  = int(os.getenv("SEARCH_OCR_MIN_CHARS", 3))

# ── NVIDIA Burst Intelligence (Phase 4, gated — default OFF) ────────────────
NVIDIA_BURST_ENABLED       = os.getenv("NVIDIA_BURST_ENABLED", "").lower() in ("1", "true", "yes")
NVIDIA_API_KEY             = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_API_BASE_URL        = os.getenv("NVIDIA_API_BASE_URL", "https://integrate.api.nvidia.com/v1")
# Models — OpenAI-compatible NIM endpoints
NVIDIA_LLM_MODEL           = os.getenv("NVIDIA_LLM_MODEL", "meta/llama-3.1-70b-instruct")
NVIDIA_VISION_MODEL        = os.getenv("NVIDIA_VISION_MODEL", "microsoft/phi-3.5-vision-instruct")
NVIDIA_RERANK_MODEL        = os.getenv("NVIDIA_RERANK_MODEL", "nvidia/nv-rerankqa-mistral-4b-v3")
# Budget controls
NVIDIA_BURST_DAILY_REQUEST_CAP = int(os.getenv("NVIDIA_BURST_DAILY_REQUEST_CAP", 100))
NVIDIA_BURST_DAILY_TOKEN_CAP   = int(os.getenv("NVIDIA_BURST_DAILY_TOKEN_CAP", 50000))
# Privacy — when enabled, strip EXIF and resize images before sending
NVIDIA_BURST_PRIVACY_MODE  = os.getenv("NVIDIA_BURST_PRIVACY_MODE", "true").lower() in ("1", "true", "yes")
# Cache — content-hash dedup to avoid repeat API calls
NVIDIA_BURST_CACHE_ENABLED = os.getenv("NVIDIA_BURST_CACHE_ENABLED", "true").lower() in ("1", "true", "yes")
NVIDIA_BURST_CACHE_TTL_HOURS = int(os.getenv("NVIDIA_BURST_CACHE_TTL_HOURS", 168))  # 7 days
# Timeouts
NVIDIA_BURST_TIMEOUT_SEC   = int(os.getenv("NVIDIA_BURST_TIMEOUT_SEC", 30))
# Search integration — only active when burst + search layer both enabled
NVIDIA_BURST_QUERY_REWRITE = os.getenv("NVIDIA_BURST_QUERY_REWRITE", "true").lower() in ("1", "true", "yes")
NVIDIA_BURST_RERANK        = os.getenv("NVIDIA_BURST_RERANK", "").lower() in ("1", "true", "yes")
NVIDIA_BURST_RERANK_TOP_N  = int(os.getenv("NVIDIA_BURST_RERANK_TOP_N", 20))
