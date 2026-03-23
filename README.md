# Rimrock Photo Tagger

Target: NVIDIA Jetson Orin Nano Super (JetPack 6 / Ubuntu 22.04)

A local-first photo pipeline for face detection, semantic tagging, date-based organization, and NAS sync.

## Pipeline Overview

1. Preflight: verify storage, mount points, dependencies
2. Pull: sync NAS source photos to local NVMe originals/
3. Process: InsightFace + YOLOv8s + CLIP on each image
4. Cluster: UMAP + HDBSCAN identity grouping
5. Organize: copy to organized/YYYY/YYYY-MM/
6. Tag: write XMP face and subject metadata
7. Push: sync organized output back to NAS/organized/
8. Verify: checksums and undated report

## Container-First Setup (Jetson)

This repo is now configured to run through Jetson containers by default.

1. Create/edit `.env`:

```bash
cp .env.example .env
```

Required defaults in `.env`:

```env
NAS_SOURCE_DIR=/mnt/mycloud/photos
LOCAL_BASE=/local/rimrock/photos
API_PORT=8420
```

2. Make sure host paths are available on Jetson:

```bash
sudo mkdir -p /local/rimrock/photos
sudo mkdir -p /mnt/mycloud/photos
```

3. Build and start container service:

```bash
docker compose -f docker-compose.jetson.yml up -d --build
```

`Dockerfile.jetson` uses NVIDIA's Jetson PyTorch `-igpu` container line and installs `requirements.jetson.txt` on top.
`l4t-pytorch` is not updated for JetPack 6+.

The service mounts:
- Repo workspace: `/workspace/rimrock-photovision`
- Local data: `/local/rimrock/photos`
- NAS mount (read-only): `/mnt/mycloud/photos`

Shortcut helpers:

```bash
bash scripts/jetson_up.sh
bash scripts/jetson_down.sh
```

## Running The Pipeline

These scripts now auto-exec inside the Jetson container:

```bash
./scripts/run_pipeline.sh
./scripts/continue_pipeline.sh
./scripts/resume_pipeline.sh
./scripts/push_to_nas.sh
```

You can also open an interactive shell in the runtime container:

```bash
bash scripts/jetson_exec.sh
```

Run a one-off command in container:

```bash
bash scripts/jetson_exec.sh "python -m api.main"
```

## Web UI

Start API in container:

```bash
bash scripts/jetson_exec.sh "python -m api.main"
```

Open `http://rimrock:8420` (or Jetson IP and configured port).

## Native Escape Hatch (No Container)

If needed for debugging, force scripts to run directly on host:

```bash
RIMROCK_SKIP_CONTAINER=1 ./scripts/run_pipeline.sh
```

## Notes

- `scripts/push_to_nas.sh` still requires an explicit `CONFIRM` prompt.
- NAS originals are never modified; push writes to a sibling `organized/` directory.
- Stop memory-heavy background workloads (like Ollama) before full processing runs.
