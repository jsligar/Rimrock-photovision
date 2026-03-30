"""Create a deterministic manifest of local originals for a scoped batch run."""

from __future__ import annotations

import argparse
from pathlib import Path

import config


def _resolve_within_root(raw_path: str, *, root: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    root_resolved = root.resolve(strict=False)
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise SystemExit(f"Path is outside source root {root}: {raw_path}") from exc
    return resolved


def _iter_images(folder: Path, *, root: Path) -> list[str]:
    found: list[str] = []
    image_exts = {ext.lower() for ext in config.IMAGE_EXTENSIONS}
    for path in folder.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in image_exts:
            continue
        found.append(path.relative_to(root).as_posix())
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a batch manifest from local originals.")
    parser.add_argument(
        "--output",
        required=True,
        help="Manifest file to write. Relative paths are resolved from LOCAL_BASE.",
    )
    parser.add_argument(
        "--include-dir",
        action="append",
        default=[],
        help="Folder under originals to include. Repeat for multiple folders. Defaults to the whole originals tree.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of files to write after sorting.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Optional starting offset after sorting, useful for the next chunk.",
    )
    parser.add_argument(
        "--source-root",
        default=str(config.ORIGINALS_DIR),
        help="Root originals directory to scan. Defaults to config.ORIGINALS_DIR.",
    )
    args = parser.parse_args()

    source_root = Path(args.source_root).expanduser().resolve(strict=False)
    if not source_root.exists():
        raise SystemExit(f"Source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise SystemExit(f"Source root is not a directory: {source_root}")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be >= 1")
    if args.offset < 0:
        raise SystemExit("--offset must be >= 0")

    if args.include_dir:
        scan_roots = [_resolve_within_root(raw, root=source_root) for raw in args.include_dir]
    else:
        scan_roots = [source_root]

    rel_paths: set[str] = set()
    for folder in scan_roots:
        if not folder.exists():
            raise SystemExit(f"Include dir does not exist: {folder}")
        if not folder.is_dir():
            raise SystemExit(f"Include dir is not a directory: {folder}")
        rel_paths.update(_iter_images(folder, root=source_root))

    ordered = sorted(rel_paths)
    if args.offset:
        ordered = ordered[args.offset :]
    if args.limit is not None:
        ordered = ordered[: args.limit]

    output_path = Path(args.output).expanduser()
    if not output_path.is_absolute():
        output_path = config.LOCAL_BASE / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as handle:
        for rel_path in ordered:
            handle.write(f"{rel_path}\n")

    print(f"Source root:      {source_root}")
    print(f"Scan roots:       {len(scan_roots)}")
    print(f"Images selected:  {len(ordered)}")
    print(f"Manifest written: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
