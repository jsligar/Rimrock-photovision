"""Ensure config loads .env values for non-API entrypoints."""

import json
import os
import subprocess
import sys
from pathlib import Path


def test_config_loads_env_file_for_direct_phase_runs(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "UMAP_N_NEIGHBORS=77",
                "HDBSCAN_MIN_CLUSTER_SIZE=9",
                "HDBSCAN_MIN_SAMPLES=5",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["RIMROCK_ENV_FILE"] = str(env_file)
    env.pop("UMAP_N_NEIGHBORS", None)
    env.pop("HDBSCAN_MIN_CLUSTER_SIZE", None)
    env.pop("HDBSCAN_MIN_SAMPLES", None)

    repo_root = Path(__file__).resolve().parents[1]
    probe = (
        "import json, config; "
        "print(json.dumps({"
        "'umap': config.UMAP_N_NEIGHBORS, "
        "'min_cluster': config.HDBSCAN_MIN_CLUSTER_SIZE, "
        "'min_samples': config.HDBSCAN_MIN_SAMPLES"
        "}))"
    )
    out = subprocess.check_output(
        [sys.executable, "-c", probe],
        cwd=str(repo_root),
        env=env,
        text=True,
    )
    data = json.loads(out.strip())

    assert data["umap"] == 77
    assert data["min_cluster"] == 9
    assert data["min_samples"] == 5
