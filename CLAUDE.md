# CLAUDE.md — Rimrock Photo Tagger

## Project Overview

Rimrock Photo Tagger is a local-first photo pipeline running on an NVIDIA Jetson Orin Nano Super (8GB). It pulls photos from a WD MyCloud NAS, runs face detection (InsightFace), object/scene tagging (YOLOv8s + CLIP), identity clustering (UMAP + HDBSCAN), organizes by date, writes XMP tags, and pushes back to NAS.

## Architecture

- **Backend:** FastAPI (Python 3.10), SQLite (WAL mode), single-threaded + background phase worker
- **Frontend:** Vanilla ES6 JS + CSS, 5 tabs (Dashboard, Clusters, Objects, Photos, Settings)
- **Pipeline:** 8 sequential phases (preflight → pull → process → cluster → organize → tag → push → verify)
- **Target:** Jetson Orin Nano Super, JetPack 6.2, 8GB unified RAM

## Development Environment

- **Dev machine:** Windows 11, VS Code
- **Rimrock (Jetson):** `ssh jsligar@172.16.0.156` — Ubuntu 22.04 aarch64
- **NAS:** WD MyCloud at 172.16.0.107, SMB share `JDSLIGAR`
- **Web UI:** http://172.16.0.156:8420

## Key Constraints

- **Memory budget:** ~3.6GB for models, ~2.9GB headroom for photo buffers. Never add models that push past 6GB total.
- **numpy <2:** Jetson torch and ORT wheels compiled against numpy 1.x. Never allow numpy >=2.
- **No PyPI torch:** Use Jetson-specific wheels only. torchvision is built from source. Don't `pip install torch torchvision`.
- **jetson-containers:** Use dusty-nv jetson-containers for GPU-accelerated packages, not manual wheel hunting.
- **NAS is read-only source:** Pipeline never modifies files in originals/ or on NAS source.

## Custom Commands

- `/rimrock-status` — Pipeline status, GPU, memory, disk health
- `/rimrock-logs` — Tail pipeline logs, filter by phase
- `/rimrock-ssh` — Quick SSH command or health check
- `/deploy` — Sync project to Rimrock, optionally restart API
- `/nas-mount` — Mount/unmount/check NAS on Rimrock

## Custom Agents

- **pipeline-monitor** — Autonomous pipeline watcher: progress, throughput, ETA, error detection, stall detection
- **jetson-ops** — Jetson system operations: power modes, clocks, thermal, Docker, packages
- **db-inspector** — Query the SQLite database: photo counts, cluster stats, tag distribution, pipeline state

## Directory Layout on Rimrock

```
/local/rimrock/photos/
├── originals/           ← Read-only (pulled from NAS)
├── organized/           ← YYYY/YYYY-MM/ output
├── crops/               ← Face & detection crops
├── rimrock_photos.db    ← SQLite database
└── rimrock_photos.log   ← Pipeline log
```

## Running Tests

```bash
pytest tests/                # All tests
pytest tests/test_db.py      # Database schema tests
pytest tests/ -k cluster     # Filter by name
```

## Common Operations

```bash
# Check pipeline status via API
ssh jsligar@172.16.0.156 "curl -s http://localhost:8420/api/status | python3 -m json.tool"

# Start a pipeline phase from CLI
ssh jsligar@172.16.0.156 "curl -X POST http://localhost:8420/api/pipeline/run/phase2"

# Stop pipeline gracefully
ssh jsligar@172.16.0.156 "curl -X POST http://localhost:8420/api/pipeline/stop"

# Set MAXN power mode for inference
ssh jsligar@172.16.0.156 "sudo nvpmodel -m 2 && sudo jetson_clocks"
```

## Code Style

- Python: standard library conventions, no type stubs required
- JS: vanilla ES6, no frameworks, no build step
- CSS: vanilla, CSS custom properties for theming
- Keep changes minimal — don't refactor code you're not working on
- Don't add docstrings or comments unless the logic is non-obvious
