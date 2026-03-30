"""Shared phase post-mortem logging helpers."""

from __future__ import annotations

import time
from typing import Any


def emit_phase_postmortem(
    logger,
    phase: str,
    started_at: float,
    success: bool,
    metrics: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    """Log a consistent summary block at the end of each pipeline phase."""
    elapsed_s = max(0.0, time.time() - started_at)
    status = "SUCCESS" if success else "FAILED"

    logger.info("-" * 60)
    logger.info("POST-MORTEM [%s]: %s", phase.upper(), status)
    logger.info("  Elapsed: %.1f seconds", elapsed_s)

    if metrics:
        for key in sorted(metrics):
            logger.info("  %s: %s", key, metrics[key])

    if error:
        logger.info("  Error: %s", error)

    logger.info("-" * 60)
