# Rimrock Photo Tagger

**Target:** NVIDIA Jetson Orin Nano Super ("Rimrock"), JetPack 6 / Ubuntu 22.04
**Storage:** Samsung 990 Evo Plus 1TB NVMe
**NAS:** Western Digital My Cloud 4TB (SMB, local gigabit switch)

A fully local photo pipeline: face detection, semantic tagging, date-based organization, and NAS sync — no cloud, no subscription, no internet required.

---

## Pipeline Overview

```
[0] Preflight  → verify storage, models, dependencies
[1] Pull       → rsync NAS → NVMe (originals/)
[2] Process    → InsightFace faces + YOLOv8s + CLIP tags (single pass per image)
[3] Cluster    → UMAP + HDBSCAN identity clustering
      ↑ USER REVIEWS CLUSTERS IN WEB UI
[4] Organize   → copy to organized/YYYY/YYYY-MM/
[5] Tag        → write XMP:PersonInImage + XMP:Subject via exiftool
[6] Push       → rsync organized/ → NAS/organized/ (explicit confirmation required)
[7] Verify     → SHA-256 spot-check, undated report
```

---

## Install

### 1. PyTorch (Jetson — do NOT use pip torch)

```bash
# JetPack 6 wheel from NVIDIA:
pip install --no-cache \
  https://developer.download.nvidia.com/compute/redist/jp/v60/pytorch/torch-2.3.0+nv24.5-cp310-cp310-linux_aarch64.whl
```

### 2. Python dependencies

```bash
cd rimrock_photo_tagger
pip install -r requirements.txt
```

> **Note:** Install torch first, then ultralytics and openai-clip, so they pick up the Jetson wheel.

### 3. System packages

```bash
sudo apt install rsync libimage-exiftool-perl
```

### 4. Copy and edit config

```bash
cp .env.example .env
# Edit NAS_SOURCE_DIR and LOCAL_BASE as needed
```

---

## Running

### Option A — Shell scripts (recommended for first run)

```bash
# Full pipeline through cluster review
./scripts/run_pipeline.sh

# After reviewing clusters in the web UI:
./scripts/continue_pipeline.sh

# After reviewing organized output:
./scripts/push_to_nas.sh
```

### Option B — Web UI control

```bash
python -m api.main
# Open: http://rimrock:8420
```

All phases can be triggered from the Dashboard tab. Push requires a checkbox confirmation.

### Option C — Resume after interruption

```bash
./scripts/resume_pipeline.sh
```

Reads `pipeline_state` table and resumes from the first incomplete phase.

---

## Memory Budget (8GB unified RAM)

| Component         | RAM     |
|-------------------|---------|
| OS + system       | ~1.5 GB |
| InsightFace buffalo_l | ~600 MB |
| CLIP ViT-B/32     | ~600 MB |
| YOLOv8s           | ~400 MB |
| PyTorch CUDA rt   | ~500 MB |
| **Total**         | **~3.6 GB** |
| **Headroom**      | **~2.9 GB** |

**Important:** Stop Ollama before running the pipeline.
Ollama with Qwen2.5-3B@Q4_K_M consumes ~2GB and will reduce headroom to ~900MB,
risking OOM on large photo buffers.

```bash
sudo systemctl stop ollama
```

Preflight checks for this automatically.

---

## Web UI

Five tabs:

| Tab | Purpose |
|-----|---------|
| **Dashboard** | Phase status cards, progress bars, run buttons, log tail |
| **Cluster Review** | Face crop grid, label/approve/noise/merge clusters |
| **Objects & Pets** | Tag browser grouped by category, detection management, vocabulary editor |
| **Photo Browser** | Filtered photo grid with face + tag overlays |
| **Settings** | Path and threshold configuration, system stats |

---

## Storage Layout

```
/local/rimrock/photos/
├── originals/        ← pulled from NAS, read-only during pipeline
├── organized/        ← YYYY/YYYY-MM/ output
├── crops/            ← 112×112 face crops + object crops
├── rimrock_photos.db ← SQLite WAL-mode database
├── rimrock_photos.log
├── rsync_pull.log
└── rsync_push.log
```

NAS push writes to `NAS/organized/` — a new sibling folder alongside the originals. **NAS originals are never modified.**

---

## Project — Fort Leonard Wood
