import importlib


def test_check_python_packages_uses_prepare_import(monkeypatch):
    from pipeline import phase0_preflight

    calls: list[str] = []

    def prepare():
        calls.append("prepare")

    def fake_import(name: str):
        calls.append(name)
        return object()

    monkeypatch.setattr(
        phase0_preflight,
        "REQUIRED_PACKAGES",
        [("clip", prepare)],
    )
    monkeypatch.setattr(importlib, "import_module", fake_import)

    passed, message = phase0_preflight.check_python_packages()

    assert passed is True
    assert message == "All Python packages importable"
    assert calls == ["prepare", "clip"]


def test_check_python_packages_reports_missing_package(monkeypatch):
    from pipeline import phase0_preflight

    def fake_import(name: str):
        raise ImportError(name)

    monkeypatch.setattr(
        phase0_preflight,
        "REQUIRED_PACKAGES",
        [("clip", lambda: None)],
    )
    monkeypatch.setattr(importlib, "import_module", fake_import)

    passed, message = phase0_preflight.check_python_packages()

    assert passed is False
    assert "clip" in message
