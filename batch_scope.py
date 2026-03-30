from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Iterable, Sequence, TypeVar

import config

T = TypeVar("T")


class BatchScopeError(RuntimeError):
    """Raised when batch-manifest scope configuration is invalid."""


@dataclass(frozen=True)
class BatchScope:
    manifest_path: Path
    relative_paths: tuple[str, ...]
    relative_path_set: frozenset[str]

    @property
    def count(self) -> int:
        return len(self.relative_paths)

    def contains(self, relative_path: str | Path) -> bool:
        return normalize_relative_path(relative_path) in self.relative_path_set


@dataclass(frozen=True)
class ManifestMediaSelection:
    image_paths: tuple[Path, ...]
    raw_paths: tuple[Path, ...]
    missing_relative_paths: tuple[str, ...]
    unsupported_relative_paths: tuple[str, ...]


def resolve_optional_path(raw_value: str | None, *, base_dir: Path | None = None) -> Path | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


def normalize_relative_path(relative_path: str | Path) -> str:
    text = str(relative_path).strip().replace("\\", "/")
    if not text:
        raise BatchScopeError("Batch scope path entries cannot be blank.")

    if _looks_absolute(text):
        resolved = Path(text).expanduser().resolve(strict=False)
        base = config.ORIGINALS_DIR.resolve(strict=False)
        try:
            rel = resolved.relative_to(base)
        except ValueError as exc:
            raise BatchScopeError(
                f"Batch scope path '{text}' is outside originals dir {config.ORIGINALS_DIR}"
            ) from exc
        parts = rel.parts
    else:
        # Resolve symlinks for relative paths so that a symlinked originals/
        # layout produces the same normalized key as a direct path.
        candidate = (config.ORIGINALS_DIR / text).resolve(strict=False)
        base = config.ORIGINALS_DIR.resolve(strict=False)
        try:
            resolved_rel = candidate.relative_to(base)
            parts = resolved_rel.parts
        except ValueError:
            # resolve() crossed outside originals — fall back to pure parse
            rel = PurePosixPath(text)
            parts = tuple(part for part in rel.parts if part not in ("", "."))
        if any(part == ".." for part in parts):
            raise BatchScopeError(f"Batch scope path '{text}' cannot traverse outside originals.")

    normalized = "/".join(parts)
    if not normalized:
        raise BatchScopeError("Batch scope path entries cannot point at the originals root.")
    return normalized


def load_batch_scope(manifest_path: Path | None = None) -> BatchScope | None:
    manifest = manifest_path or config.BATCH_MANIFEST_PATH
    if manifest is None:
        return None

    if not manifest.exists():
        raise BatchScopeError(f"BATCH_MANIFEST_PATH does not exist: {manifest}")
    if not manifest.is_file():
        raise BatchScopeError(f"BATCH_MANIFEST_PATH is not a file: {manifest}")

    relative_paths: list[str] = []
    seen: set[str] = set()
    with open(manifest, "r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            normalized = normalize_relative_path(line)
            if normalized in seen:
                continue
            seen.add(normalized)
            relative_paths.append(normalized)

    if not relative_paths:
        raise BatchScopeError(f"BATCH_MANIFEST_PATH is empty: {manifest}")

    return BatchScope(
        manifest_path=manifest,
        relative_paths=tuple(relative_paths),
        relative_path_set=frozenset(relative_paths),
    )


def resolve_manifest_media_selection(
    batch_scope: BatchScope,
    *,
    base_dir: Path | None = None,
    image_extensions: set[str] | None = None,
    raw_extensions: set[str] | None = None,
) -> ManifestMediaSelection:
    root = base_dir or config.ORIGINALS_DIR
    image_exts = {ext.lower() for ext in (image_extensions or config.IMAGE_EXTENSIONS)}
    raw_exts = {ext.lower() for ext in (raw_extensions or config.RAW_EXTENSIONS)}

    image_paths: list[Path] = []
    raw_paths: list[Path] = []
    missing_relative_paths: list[str] = []
    unsupported_relative_paths: list[str] = []

    for rel_path in batch_scope.relative_paths:
        abs_path = root / Path(rel_path)
        if not abs_path.exists() or not abs_path.is_file():
            missing_relative_paths.append(rel_path)
            continue

        suffix = abs_path.suffix.lower()
        if suffix in image_exts:
            image_paths.append(abs_path)
        elif suffix in raw_exts:
            raw_paths.append(abs_path)
        else:
            unsupported_relative_paths.append(rel_path)

    return ManifestMediaSelection(
        image_paths=tuple(image_paths),
        raw_paths=tuple(raw_paths),
        missing_relative_paths=tuple(missing_relative_paths),
        unsupported_relative_paths=tuple(unsupported_relative_paths),
    )


def filter_by_batch_scope(
    items: Iterable[T],
    *,
    batch_scope: BatchScope | None,
    path_getter: Callable[[T], str | Path],
) -> tuple[list[T], int]:
    if batch_scope is None:
        materialized = list(items)
        return materialized, 0

    kept: list[T] = []
    skipped = 0
    for item in items:
        if batch_scope.contains(path_getter(item)):
            kept.append(item)
        else:
            skipped += 1
    return kept, skipped


def filter_relative_paths(
    relative_paths: Sequence[str | Path],
    *,
    year_scope: str | None = None,
) -> tuple[list[str], int]:
    if not year_scope:
        normalized = [normalize_relative_path(path) for path in relative_paths]
        return normalized, 0

    token = str(year_scope).strip()
    kept: list[str] = []
    skipped = 0
    for path in relative_paths:
        normalized = normalize_relative_path(path)
        padded = f"/{normalized}/"
        if f"/{token}/" in padded:
            kept.append(normalized)
        else:
            skipped += 1
    return kept, skipped


def _looks_absolute(text: str) -> bool:
    if text.startswith("/"):
        return True
    if len(text) >= 3 and text[1] == ":" and text[2] in ("/", "\\"):
        return True
    return False
