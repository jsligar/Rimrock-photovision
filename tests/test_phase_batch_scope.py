from pathlib import Path

import config


def test_tag_uses_manifest_scoped_tracked_files(tmp_path, tmp_db, monkeypatch):
    import db
    from pipeline import phase5_tag

    output_dir = config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "2024").mkdir(parents=True, exist_ok=True)
    (output_dir / "2024" / "keep.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 64)
    (output_dir / "2024" / "drop.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 64)

    manifest = tmp_path / "batch.txt"
    manifest.write_text("By-Year/2024/keep.jpg\n", encoding="utf-8")
    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)

    conn = db.get_db()
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, dest_path, copy_verified, checksum)
        VALUES ('By-Year/2024/keep.jpg', 'keep.jpg', '2024/keep.jpg', 1, 'abc')
        """
    )
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, dest_path, copy_verified, checksum)
        VALUES ('By-Year/2024/drop.jpg', 'drop.jpg', '2024/drop.jpg', 1, 'def')
        """
    )
    keep_photo_id = conn.execute(
        "SELECT photo_id FROM photos WHERE source_path='By-Year/2024/keep.jpg'"
    ).fetchone()[0]
    drop_photo_id = conn.execute(
        "SELECT photo_id FROM photos WHERE source_path='By-Year/2024/drop.jpg'"
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO detections (photo_id, model, tag, confidence, approved, created_at) VALUES (?, 'yolo', 'dog', 0.9, 1, '2026-03-26T00:00:00+00:00')",
        (keep_photo_id,),
    )
    conn.execute(
        "INSERT INTO detections (photo_id, model, tag, confidence, approved, created_at) VALUES (?, 'yolo', 'cat', 0.9, 1, '2026-03-26T00:00:00+00:00')",
        (drop_photo_id,),
    )
    conn.commit()
    conn.close()

    calls: list[list[str]] = []

    class FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return FakeResult()

    monkeypatch.setattr(phase5_tag.subprocess, "run", fake_run)

    assert phase5_tag.run_tag() is True
    assert len(calls) == 1
    assert str(output_dir / "2024" / "keep.jpg") in calls[0]
    assert str(output_dir / "2024" / "drop.jpg") not in calls[0]


def test_push_uses_manifest_scoped_tracked_files(tmp_path, tmp_db, monkeypatch):
    import db
    from pipeline import phase6_push

    output_dir = config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "2025").mkdir(parents=True, exist_ok=True)
    (output_dir / "2025" / "keep.jpg").write_bytes(b"keep")
    (output_dir / "2025" / "drop.jpg").write_bytes(b"drop")

    nas_source = tmp_path / "nas" / "photos"
    nas_dest = tmp_path / "nas" / "organized"
    nas_source.mkdir(parents=True)
    nas_dest.mkdir(parents=True)

    manifest = tmp_path / "batch.txt"
    manifest.write_text("By-Year/2025/keep.jpg\n", encoding="utf-8")

    monkeypatch.setattr(config, "BATCH_MANIFEST_PATH", manifest)
    monkeypatch.setattr(config, "NAS_SOURCE_DIR", nas_source)
    monkeypatch.setattr(config, "RSYNC_PUSH_LOG", tmp_path / "rsync_push.log")
    monkeypatch.setattr(phase6_push, "NAS_DEST_DIR", nas_dest)

    conn = db.get_db()
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, dest_path, copy_verified, checksum)
        VALUES ('By-Year/2025/keep.jpg', 'keep.jpg', '2025/keep.jpg', 1, 'abc')
        """
    )
    conn.execute(
        """
        INSERT INTO photos (source_path, filename, dest_path, copy_verified, checksum)
        VALUES ('By-Year/2025/drop.jpg', 'drop.jpg', '2025/drop.jpg', 1, 'def')
        """
    )
    conn.commit()
    conn.close()

    captured: dict[str, object] = {}

    class FakeProc:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            files_from_arg = next(part for part in cmd if part.startswith("--files-from="))
            files_from_path = files_from_arg.split("=", 1)[1]
            captured["files"] = Path(files_from_path).read_text(encoding="utf-8").splitlines()
            self.stdout = iter(["2025/keep.jpg"])
            self.returncode = 0

        def wait(self):
            return None

    monkeypatch.setattr(phase6_push.subprocess, "Popen", FakeProc)

    assert phase6_push.run_push() is True
    assert captured["files"] == ["2025/keep.jpg"]
