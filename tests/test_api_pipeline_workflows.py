from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_db):
    from api.main import app

    with TestClient(app) as c:
        yield c


def test_list_workflows_exposes_intake_delivery_and_documents(client):
    resp = client.get("/api/pipeline/workflows")
    assert resp.status_code == 200
    workflows = {item["name"]: item["phases"] for item in resp.json()["workflows"]}

    assert workflows["intake"] == ["preflight", "pull", "process", "cluster"]
    assert workflows["delivery"] == ["organize", "tag", "push", "verify"]
    assert workflows["documents"] == ["ocr"]


def test_run_workflow_executes_phases_in_order_and_surfaces_active_status(client, monkeypatch):
    import api.routes.pipeline as pipeline_routes

    seen: list[str] = []

    def fake_execute(phase: str) -> bool:
        seen.append(phase)
        time.sleep(0.03)
        return True

    monkeypatch.setattr(pipeline_routes, "_execute_phase", fake_execute)

    resp = client.post("/api/pipeline/workflows/intake")
    assert resp.status_code == 200

    workflow_active = False
    for _ in range(20):
        status = client.get("/api/status").json()["workflow"]
        if status["active"]:
            workflow_active = True
            assert status["name"] == "intake"
            assert status["steps"] == ["preflight", "pull", "process", "cluster"]
            break
        time.sleep(0.01)

    assert workflow_active is True

    for _ in range(40):
        if seen == ["preflight", "pull", "process", "cluster"]:
            break
        time.sleep(0.01)

    assert seen == ["preflight", "pull", "process", "cluster"]
    final_status = None
    for _ in range(40):
        final_status = client.get("/api/status").json()["workflow"]
        if final_status["active"] is False:
            break
        time.sleep(0.01)

    assert final_status is not None
    assert final_status["active"] is False
    assert final_status["name"] is None
