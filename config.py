import os
from pathlib import Path

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

# Minimum free NVMe space before pull is allowed (bytes)
MIN_FREE_BYTES  = 50 * 1024**3   # 50 GB safety margin

# ── InsightFace ────────────────────────────────────────────────────────────────
INSIGHTFACE_MODEL = "buffalo_l"
DET_SIZE          = (640, 640)
DET_THRESH        = 0.4
CTX_ID            = 0

# ── UMAP ──────────────────────────────────────────────────────────────────────
UMAP_N_NEIGHBORS   = 30
UMAP_MIN_DIST      = 0.0
UMAP_N_COMPONENTS  = 64
UMAP_METRIC        = "cosine"

# ── HDBSCAN ───────────────────────────────────────────────────────────────────
HDBSCAN_MIN_CLUSTER_SIZE = 3
HDBSCAN_MIN_SAMPLES      = 1
HDBSCAN_METRIC           = "euclidean"

# ── Ground Truth ──────────────────────────────────────────────────────────────
GT_MIN_FACES       = 3
GT_MIN_CONFIDENCE  = 0.75

# ── YOLOv8 ────────────────────────────────────────────────────────────────────
# YOLOv8s chosen over YOLOv8m to preserve ~500MB RAM headroom on 8GB unified memory
YOLO_MODEL          = "yolov8s.pt"
YOLO_CONF_THRESHOLD = 0.45
YOLO_IOU_THRESHOLD  = 0.5

# ── CLIP ──────────────────────────────────────────────────────────────────────
CLIP_MODEL          = "ViT-B/32"
CLIP_DEVICE         = "cuda"
CLIP_TAG_THRESHOLD  = 0.26

# ── Image Processing ──────────────────────────────────────────────────────────
# Resize on longest edge before inference — reduces buffer memory for large photos
MAX_INFERENCE_DIM   = 1920
IMAGE_EXTENSIONS    = {".jpg", ".jpeg", ".png", ".heic", ".tiff", ".tif", ".webp"}
RAW_EXTENSIONS      = {".raw", ".cr2", ".cr3", ".nef", ".arw", ".dng"}

# ── Semantic Tag Vocabulary ────────────────────────────────────────────────────
SEMANTIC_TAG_GROUPS = {
    "vehicles": {
        "pickup truck":     ["a pickup truck", "a truck in a driveway"],
        "classic truck":    ["a vintage pickup truck", "a classic 1970s truck", "an old farm truck"],
        "SUV":              ["an SUV", "a sport utility vehicle"],
        "military vehicle": ["a military vehicle", "an Army truck", "a HMMWV"],
        "tractor":          ["a farm tractor", "agricultural equipment"],
        "ATV / UTV":        ["an ATV", "a side-by-side UTV", "a four-wheeler"],
        "motorcycle":       ["a motorcycle", "a dirt bike"],
        "car":              ["a passenger car", "a sedan"],
    },
    "animals": {
        "dog":    ["a dog", "a pet dog", "a dog playing outside"],
        "cat":    ["a cat", "a pet cat", "a cat indoors"],
        "puppy":  ["a puppy", "a young dog"],
        "kitten": ["a kitten", "a baby cat"],
        "horse":  ["a horse", "a horse in a field"],
        "cattle": ["cattle", "cows in a field", "livestock"],
        "deer":   ["a deer", "whitetail deer"],
        "bird":   ["a bird", "birds in the yard"],
    },
    "scenes": {
        "outdoors":    ["outdoor scenery", "nature landscape"],
        "birthday":    ["a birthday party", "birthday cake"],
        "holiday":     ["holiday decorations", "Christmas tree"],
        "camping":     ["camping", "a campfire", "tent camping outdoors"],
        "sports":      ["sports activity", "playing sports"],
        "Boy Scouts":  ["Boy Scouts activity", "scouting outdoors"],
        "graduation":  ["graduation ceremony", "graduation gown"],
        "military":    ["military uniform", "Army uniform", "military ceremony"],
    },
}

YOLO_DIRECT_TAGS = {
    "car": "car", "truck": "pickup truck", "bus": "bus",
    "motorcycle": "motorcycle", "bicycle": "bicycle",
    "dog": "dog", "cat": "cat", "horse": "horse",
    "cow": "cattle", "bird": "bird", "sheep": "livestock",
}

# ── API ────────────────────────────────────────────────────────────────────────
API_HOST = "0.0.0.0"
API_PORT = int(os.getenv("API_PORT", 8420))
UNDATED_DIR = "undated"
