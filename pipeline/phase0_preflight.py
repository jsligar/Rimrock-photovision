"""Phase 0 — Preflight: verify all prerequisites before starting the pipeline."""

import shutil
import subprocess
import sys
import importlib
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
import db


REQUIRED_PACKAGES = [
    "insightface",
    "ultralytics",
    "clip",
    "umap",
    "hdbscan",
    "cv2",
    "PIL",
]


def _gb(n: int) -> float:
    return n / 1024**3


def check_nvme_space() -> tuple[bool, str]:
    usage = shutil.disk_usage(str(config.LOCAL_BASE.parent))
    free = usage.free
    if free < config.MIN_FREE_BYTES:
        return False, (
            f"Insufficient NVMe space. Need 50GB free, have {_gb(free):.1f}GB"
        )
    return True, f"NVMe free: {_gb(free):.1f} GB"


def check_ollama_not_running() -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["pgrep", "-x", "ollama"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return False, (
                "Ollama is running and will consume ~2GB RAM. "
                "Stop it first: sudo systemctl stop ollama"
            )
    except FileNotFoundError:
        pass  # pgrep not available — skip check
    return True, "Ollama running: NO"


def check_nas_mount() -> tuple[bool, str]:
    if not config.NAS_SOURCE_DIR.exists():
        return False, f"NAS not mounted at {config.NAS_SOURCE_DIR}. Mount it before running preflight."
    try:
        list(config.NAS_SOURCE_DIR.iterdir())
    except PermissionError:
        return False, f"NAS at {config.NAS_SOURCE_DIR} is not readable."
    return True, f"NAS reachable: YES ({config.NAS_SOURCE_DIR})"


def check_directories() -> tuple[bool, str]:
    errors = []
    for d in [config.ORIGINALS_DIR, config.OUTPUT_DIR, config.CROPS_DIR]:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            errors.append(str(e))
    if errors:
        return False, "Directory creation failed: " + "; ".join(errors)
    return True, "All directories created/verified"


def check_db() -> tuple[bool, str]:
    try:
        config.LOCAL_BASE.mkdir(parents=True, exist_ok=True)
        db.init_db()
    except Exception as e:
        return False, f"DB initialization failed: {e}"
    return True, "DB initialized: YES"


def check_exiftool() -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["exiftool", "-ver"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            return False, "exiftool check failed"
        return True, f"exiftool version: {result.stdout.strip()}"
    except FileNotFoundError:
        return False, "exiftool is not installed. Install it: sudo apt install libimage-exiftool-perl"


def check_insightface_model() -> tuple[bool, str]:
    # Check common cache locations
    home = Path.home()
    cache_paths = [
        home / ".insightface" / "models" / config.INSIGHTFACE_MODEL,
        home / ".cache" / "insightface" / "models" / config.INSIGHTFACE_MODEL,
        Path("/root/.insightface/models") / config.INSIGHTFACE_MODEL,
    ]
    for p in cache_paths:
        if p.exists():
            return True, f"Models cached: YES ({p})"
    return True, "Models cached: NO (will download on first run)"


def check_python_packages() -> tuple[bool, str]:
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            importlib.import_module(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        return False, f"Missing Python packages: {', '.join(missing)}"
    return True, "All Python packages importable"


def run_preflight() -> bool:
    print("=" * 60)
    print("RIMROCK PHOTO TAGGER — PREFLIGHT CHECK")
    print("=" * 60)

    db.mark_phase_running("preflight")

    checks = [
        ("NVMe Space",       check_nvme_space),
        ("Ollama Not Running", check_ollama_not_running),
        ("NAS Mount",        check_nas_mount),
        ("Directories",      check_directories),
        ("Database",         check_db),
        ("exiftool",         check_exiftool),
        ("InsightFace Model", check_insightface_model),
        ("Python Packages",  check_python_packages),
    ]

    all_passed = True
    results = {}

    for name, check_fn in checks:
        passed, message = check_fn()
        results[name] = (passed, message)
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}: {message}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        # Pull NVMe free space for summary
        usage = shutil.disk_usage(str(config.LOCAL_BASE.parent))
        free_gb = _gb(usage.free)
        nas_ok = results["NAS Mount"][0]
        ollama_ok = results["Ollama Not Running"][0]
        model_msg = results["InsightFace Model"][1]
        models_cached = "YES" if "YES" in model_msg else "NO (will download on first run)"

        print("PREFLIGHT PASSED")
        print(f"  NVMe free:       {free_gb:.1f} GB")
        print(f"  NAS reachable:   {'YES' if nas_ok else 'NO'}")
        print(f"  Ollama running:  {'NO' if ollama_ok else 'YES'}")
        print(f"  Models cached:   {models_cached}")
        print(f"  DB initialized:  YES")
        db.mark_phase_complete("preflight")
    else:
        print("PREFLIGHT FAILED — resolve the above errors before continuing.")
        db.mark_phase_error("preflight", "One or more preflight checks failed")

    return all_passed


if __name__ == "__main__":
    success = run_preflight()
    sys.exit(0 if success else 1)
