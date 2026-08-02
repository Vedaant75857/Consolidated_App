"""Focused contract checks for the one-process backend composition host."""

from __future__ import annotations

import importlib
import io
import sys


def test_unified_host_keeps_module_routes_namespaced(monkeypatch, tmp_path):
    # Keep this host import from touching a developer's persistent session data.
    for module in ("MODULE1_SESSION_DB_DIR", "MODULE2_SESSION_DB_DIR", "MODULE3_SESSION_DB_DIR"):
        monkeypatch.setenv(module, str(tmp_path / module.lower()))

    sys.modules.pop("backend.unified_app", None)
    host = importlib.import_module("backend.unified_app").app
    client = host.test_client()

    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"

    for module in ("module1", "module2", "module3"):
        assert client.get(f"/api/{module}/health").status_code == 200

    # A colliding legacy route must never leak onto a shared bare /api surface.
    assert client.get("/api/upload").status_code == 404

    module2 = host.config["UNIFIED_MODULE_APPS"]["module2"]
    analyzer_target = module2.view_functions["transfer_to_analyzer"].__globals__["_analyzer_be"]
    with module2.app_context():
        module2.config["UNIFIED_BACKEND_URL"] = "http://unified.test:8000"
        assert analyzer_target() == "http://unified.test:8000/api/module3"

    upload = client.post(
        "/api/module3/upload",
        data={"file": (io.BytesIO(b"Supplier,Spend\nAcme,10\n"), "source.csv")},
        content_type="multipart/form-data",
    )
    assert upload.status_code == 200
    session_id = upload.get_json()["sessionId"]
    refresh = client.post("/api/module3/preview/refresh-inventory", json={"sessionId": session_id})
    assert refresh.status_code == 200
    assert any(item["table_key"] == "source.csv::" for item in refresh.get_json()["fileInventory"])


def test_unified_host_serves_built_suite_and_spa_fallback(monkeypatch, tmp_path):
    """A built suite is same-origin, while missing API paths stay 404s."""

    for module in ("MODULE1_SESSION_DB_DIR", "MODULE2_SESSION_DB_DIR", "MODULE3_SESSION_DB_DIR"):
        monkeypatch.setenv(module, str(tmp_path / module.lower()))

    dist = tmp_path / "suite-dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!doctype html><div id='root'>suite</div>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("console.log('suite')", encoding="utf-8")
    monkeypatch.setenv("UNIFIED_FRONTEND_DIST", str(dist))

    sys.modules.pop("backend.unified_app", None)
    host = importlib.import_module("backend.unified_app").app
    client = host.test_client()

    assert client.get("/").status_code == 200
    assert client.get("/assets/app.js").get_data(as_text=True) == "console.log('suite')"
    assert client.get("/stitcher/").get_data(as_text=True).endswith("suite</div>")
    assert client.get("/normalizer/workspace").status_code == 200
    assert client.get("/assets/missing.js").status_code == 404
    assert client.get("/api/does-not-exist").status_code == 404
