import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env for all entrypoints (API + direct phase scripts).
# Already-exported env vars keep precedence over file values.
ENV_FILE = Path(
    os.getenv("RIMROCK_ENV_FILE", str(Path(__file__).resolve().parent / ".env"))
)
load_dotenv(ENV_FILE, override=False)

# ── Storage ────────────────────────────────────────────────────────────────────
NAS_SOURCE_DIR  = Path(os.getenv("NAS_SOURCE_DIR",  "/mnt/mycloud/photos"))
LOCAL_BASE      = Path(os.getenv("LOCAL_BASE",      "/local/rimrock/photos"))
ORIGINALS_DIR   = LOCAL_BASE / "originals"
OUTPUT_DIR      = LOCAL_BASE / "organized"
CROPS_DIR       = LOCAL_BASE / "crops"
DB_PATH         = LOCAL_BASE / "rimrock_photos.db"
LOG_PATH        = LOCAL_BASE / "rimrock_photos.log"
RSYNC_PULL_LOG  = LOCAL_BASE / "rsync_pull.log"
RSYNC_PUSH_LOG  = LOCAL_BASE / "rsync_push.log"
# Phase 1 prefilter output: files whose extension and binary signature disagree.
# Phase 2 will skip any relative paths listed here.
PREFILTER_REJECTS_PATH = LOCAL_BASE / "prefilter_rejects.tsv"

# Optional test scope to limit pull/process volume by year (e.g. "2025").
# Empty by default (full dataset).
TEST_YEAR_SCOPE = os.getenv("TEST_YEAR_SCOPE", "").strip() or None

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

# ── Ground Truth ──────────────────────────────────────────────────────────────
GT_MIN_FACES       = 3
GT_MIN_CONFIDENCE  = 0.75

# ── YOLOv8 ────────────────────────────────────────────────────────────────────
# YOLOv8s chosen over YOLOv8m to preserve ~500MB RAM headroom on 8GB unified memory
# Set YOLO_MODEL=yolov8s.engine in docker-compose after TRT export.
YOLO_MODEL          = os.getenv("YOLO_MODEL", "yolov8s.pt")
YOLO_CONF_THRESHOLD = float(os.getenv("YOLO_CONF_THRESHOLD", 0.45))
YOLO_IOU_THRESHOLD  = 0.5

# ── TensorRT ────────────────────────────────────────────────────────────────
TRT_ENGINE_CACHE    = Path(os.getenv("TRT_ENGINE_CACHE", "/local/rimrock/trt_cache"))

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
API_HOST = "0.0.0.0"
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
SEARCH_OCR_ENABLED    = os.getenv("SEARCH_OCR_ENABLED", "true").lower() in ("1", "true", "yes")
SEARCH_OCR_LANG       = os.getenv("SEARCH_OCR_LANG", "eng")
SEARCH_OCR_PSM        = int(os.getenv("SEARCH_OCR_PSM", 6))
SEARCH_OCR_TIMEOUT_SEC = int(os.getenv("SEARCH_OCR_TIMEOUT_SEC", 20))
SEARCH_OCR_MAX_DIM    = int(os.getenv("SEARCH_OCR_MAX_DIM", 1600))
SEARCH_OCR_MAX_CHARS  = int(os.getenv("SEARCH_OCR_MAX_CHARS", 2000))
SEARCH_OCR_MIN_CHARS  = int(os.getenv("SEARCH_OCR_MIN_CHARS", 3))
