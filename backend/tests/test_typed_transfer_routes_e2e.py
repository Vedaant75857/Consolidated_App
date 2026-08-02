from __future__ import annotations

import datetime as dt
import importlib
import io
import sys
import time
import uuid
from decimal import Decimal

import duckdb
import pytest

from backend.ingestion.models import OrderedColumnSchema, TableArtifact, ValueType
from backend.ingestion.transfer import TransferManifest
from backend.ingestion.typed_artifact import export_duckdb_parquet, serialize_transport


class _HTTPResponse:
    """Small requests-compatible wrapper around a Flask test response."""

    def __init__(self, response):
        self.status_code = response.status_code
        self.text = response.get_data(as_text=True)
        self.headers = response.headers
        self._json = response.get_json(silent=True)

    def json(self):
        return self._json


def _row_values(row):
    values = getattr(row, "values", None)
    return tuple(values()) if callable(values) else tuple(row)


@pytest.fixture()
def unified_host(monkeypatch, tmp_path):
    for module in ("MODULE1_SESSION_DB_DIR", "MODULE2_SESSION_DB_DIR", "MODULE3_SESSION_DB_DIR"):
        monkeypatch.setenv(module, str(tmp_path / module.lower()))
    sys.modules.pop("backend.unified_app", None)
    host = importlib.import_module("backend.unified_app").app
    host.config.update(TESTING=True)
    for module_app in host.config["UNIFIED_MODULE_APPS"].values():
        module_app.config.update(TESTING=True, UNIFIED_BACKEND_URL="http://unified.test")
    return host, host.test_client()


def _multipart_post(client, endpoint: str, files: dict) -> _HTTPResponse:
    data = {}
    file_item = files["file"]
    filename, stream, mime = file_item
    data["file"] = (io.BytesIO(stream.read()), filename, mime)
    if "manifest" in files:
        manifest_name, manifest_stream, manifest_mime = files["manifest"]
        data["manifest"] = (io.BytesIO(manifest_stream.read()), manifest_name, manifest_mime)
    return _HTTPResponse(client.post(endpoint, data=data, content_type="multipart/form-data"))


def _seed_module1_merge(host) -> str:
    app = host.config["UNIFIED_MODULE_APPS"]["module1"]
    transfer = next(fn for name, fn in app.view_functions.items() if name.endswith("transfer_to_analyzer"))
    globals_ = transfer.__globals__
    session_id = f"typed-m1-{uuid.uuid4().hex}"
    with globals_["get_session_lock"](session_id):
        conn = globals_["get_session_db"](session_id)
        conn.execute(
            '''CREATE TABLE "final_merged" (
                "Invoice No." VARCHAR,
                "Amount" DECIMAL(10, 2),
                "Paid" BOOLEAN,
                "Invoice Date" DATE
            )'''
        )
        conn.execute(
            '''INSERT INTO "final_merged" VALUES
                ('INV-1', 42.50, TRUE, DATE '2024-01-02'),
                ('INV-2', NULL, FALSE, NULL)'''
        )
        conn.commit()
    app.config["UNIFIED_BACKEND_URL"] = "http://unified.test"
    return session_id


def _seed_module2_active(host) -> str:
    app = host.config["UNIFIED_MODULE_APPS"]["module2"]
    transfer = app.view_functions["transfer_to_analyzer"]
    globals_ = transfer.__globals__
    session_id = f"typed-m2-{uuid.uuid4().hex}"
    with globals_["get_session_lock"](session_id):
        conn = globals_["get_session_db"](session_id)
        conn.execute(
            '''CREATE TABLE "active" (
                supplier VARCHAR,
                amount DECIMAL(10, 2),
                approved BOOLEAN,
                paid_on DATE
            )'''
        )
        conn.execute(
            '''INSERT INTO "active" VALUES
                ('Acme', 12.30, TRUE, DATE '2024-02-03'),
                ('Beta', NULL, FALSE, NULL)'''
        )
        globals_["set_meta"](conn, "filename", "normalized.csv")
        conn.commit()
    app.config["UNIFIED_BACKEND_URL"] = "http://unified.test"
    return session_id


def _destination_m2_state(host, client, session_id: str):
    response = client.get("/api/module2/current-inventory", query_string={"sessionId": session_id})
    assert response.status_code == 200
    body = response.get_json()
    assert body["inventory"] and body["inventory"][0]["rows"] == 2
    app = host.config["UNIFIED_MODULE_APPS"]["module2"]
    globals_ = app.view_functions["import_from_stitcher"].__globals__
    with globals_["get_session_lock"](session_id):
        conn = globals_["get_session_db"](session_id)
        key = body["inventory"][0]["table_key"]
        table = globals_["lookup_sql_name"](conn, key)
        schema = conn.execute(f'DESCRIBE "{table}"').fetchall()
        rows = [_row_values(row) for row in conn.execute(f'SELECT * FROM "{table}" ORDER BY 1').fetchall()]
    return body, schema, rows


def _destination_m3_state(host, client, session_id: str):
    response = client.get(f"/api/module3/session/{session_id}/state")
    assert response.status_code == 200
    body = response.get_json()
    assert body["fileInventory"] and body["fileInventory"][0]["rows"] == 2
    app = host.config["UNIFIED_MODULE_APPS"]["module3"]
    import_route = next(fn for name, fn in app.view_functions.items() if name.endswith("import_from_module"))
    globals_ = import_route.__globals__
    with globals_["get_session_lock"](session_id):
        conn = globals_["get_session_db"](session_id)
        key = body["fileInventory"][0]["table_key"]
        registry = conn.execute("SELECT data_table FROM _table_registry WHERE table_key = ?", [key]).fetchone()
        assert registry
        table = registry[0]
        schema = conn.execute(f'DESCRIBE "{table}"').fetchall()
        rows = [_row_values(row) for row in conn.execute(f'SELECT * EXCLUDE RECORD_ID FROM "{table}" ORDER BY 1').fetchall()]
        metadata = conn.execute("SELECT column_key FROM _column_metadata WHERE table_key = ? ORDER BY column_order", [key]).fetchall()
    return body, schema, rows, metadata


def test_module1_to_module2_typed_route_preserves_schema_nulls_and_restore(unified_host, monkeypatch):
    host, client = unified_host
    source_session = _seed_module1_merge(host)
    source = host.config["UNIFIED_MODULE_APPS"]["module1"]
    calls = []

    def fake_post(url, *, files, timeout):
        calls.append(url)
        assert url.endswith("/api/module2/import-from-stitcher")
        return _multipart_post(client, "/api/module2/import-from-stitcher", files)

    transfer = next(fn for name, fn in source.view_functions.items() if name.endswith("transfer_to_normalizer"))
    monkeypatch.setattr(transfer.__globals__["_requests"], "post", fake_post)
    response = client.post("/api/module1/merge/transfer-to-normalizer", json={"sessionId": source_session})
    assert response.status_code == 200
    body = response.get_json()
    assert body["transport"] == "typed_artifact"
    assert calls

    _, schema, rows = _destination_m2_state(host, client, body["normalizerSessionId"])
    assert [row[1] for row in schema] == ["VARCHAR", "DECIMAL(10,2)", "BOOLEAN", "DATE"]
    assert rows == [("INV-1", 42.50, True, dt.date(2024, 1, 2)), ("INV-2", None, False, None)]


def test_module1_to_module3_typed_route_preserves_col_metadata_and_rejects_tamper(unified_host, monkeypatch):
    host, client = unified_host
    source_session = _seed_module1_merge(host)
    source = host.config["UNIFIED_MODULE_APPS"]["module1"]

    def fake_post(url, *, files, timeout):
        assert url.endswith("/api/module3/import")
        return _multipart_post(client, "/api/module3/import", files)

    transfer = next(fn for name, fn in source.view_functions.items() if name.endswith("transfer_to_analyzer"))
    monkeypatch.setattr(transfer.__globals__["_requests"], "post", fake_post)
    response = client.post("/api/module1/merge/transfer-to-analyzer", json={"sessionId": source_session})
    assert response.status_code == 200
    body = response.get_json()
    assert body["transport"] == "typed_artifact"
    state, schema, rows, metadata = _destination_m3_state(host, client, body["analyzerSessionId"])
    assert state["fileInventory"][0]["cols"] == 4
    assert [row[0] for row in metadata if row[0] != "RECORD_ID"] == ["COL_0", "COL_1", "COL_2", "COL_3"]
    assert rows == [("INV-1", 42.50, True, dt.date(2024, 1, 2)), ("INV-2", None, False, None)]
    assert any("DECIMAL" in str(row[1]) for row in schema)

    # The destination verifies bytes before creating a session or touching a DB.
    destination = host.config["UNIFIED_MODULE_APPS"]["module3"]
    import_route = next(fn for name, fn in destination.view_functions.items() if name.endswith("import_from_module"))
    destination_globals = import_route.__globals__
    monkeypatch.setitem(destination_globals, "get_session_db", lambda *_: (_ for _ in ()).throw(AssertionError("mutation before verification")))
    bad = client.post(
        "/api/module3/import",
        data={
            "file": (io.BytesIO(b"PAR1tamperedPAR1"), "bad.parquet", "application/vnd.apache.parquet"),
            "manifest": (io.BytesIO(b"{}"), "manifest.json", "application/json"),
        },
        content_type="multipart/form-data",
    )
    assert bad.status_code == 400


def test_module2_to_module3_typed_route_and_explicit_csv_fallback(unified_host, monkeypatch):
    host, client = unified_host
    source_session = _seed_module2_active(host)
    source = host.config["UNIFIED_MODULE_APPS"]["module2"]
    calls = []

    def fake_post(url, *, files, timeout):
        calls.append(files["file"][2])
        assert url.endswith("/api/module3/import")
        return _multipart_post(client, "/api/module3/import", files)

    transfer = next(fn for name, fn in source.view_functions.items() if name.endswith("transfer_to_analyzer"))
    monkeypatch.setattr(transfer.__globals__["_requests"], "post", fake_post)
    response = client.post("/api/module2/transfer-to-analyzer", json={"sessionId": source_session})
    assert response.status_code == 200
    body = response.get_json()
    assert body["transport"] == "typed"
    assert calls == ["application/vnd.apache.parquet"]
    _, _, rows, metadata = _destination_m3_state(host, client, body["analyzerSessionId"])
    assert rows == [("Acme", Decimal("12.30"), True, dt.date(2024, 2, 3)), ("Beta", None, False, None)]
    assert [row[0] for row in metadata if row[0] != "RECORD_ID"] == ["COL_0", "COL_1", "COL_2", "COL_3"]

    # A destination capability refusal is the only path that may fall back to CSV.
    source_session = _seed_module2_active(host)
    calls.clear()

    class Unsupported:
        status_code = 415
        text = "typed unsupported"
        headers = {"content-type": "application/json"}

        @staticmethod
        def json():
            return {"code": "typed_unsupported"}

    class Accepted:
        status_code = 200
        text = ""
        headers = {"content-type": "application/json"}

        @staticmethod
        def json():
            return {"sessionId": "csv-fallback-session"}

    def fallback_post(url, *, files, timeout):
        calls.append(files["file"][2])
        return Unsupported() if len(calls) == 1 else Accepted()

    monkeypatch.setattr(transfer.__globals__["_requests"], "post", fallback_post)
    fallback = client.post("/api/module2/transfer-to-analyzer", json={"sessionId": source_session})
    assert fallback.status_code == 200
    assert fallback.get_json()["transport"] == "csv"
    assert fallback.get_json()["warnings"][0]["code"] == "LOSSY_CSV_FALLBACK"
    assert calls == ["application/vnd.apache.parquet", "text/csv"]


def test_module2_typed_import_rejects_tamper_before_session_mutation(unified_host, monkeypatch):
    host, client = unified_host
    destination = host.config["UNIFIED_MODULE_APPS"]["module2"]
    import_route = next(fn for name, fn in destination.view_functions.items() if name.endswith("import_from_stitcher"))
    globals_ = import_route.__globals__
    monkeypatch.setitem(globals_, "get_session_db", lambda *_: (_ for _ in ()).throw(AssertionError("mutation before verification")))
    response = client.post(
        "/api/module2/import-from-stitcher",
        data={
            "file": (io.BytesIO(b"PAR1tamperedPAR1"), "bad.parquet", "application/vnd.apache.parquet"),
            "manifest": (io.BytesIO(b"{}"), "manifest.json", "application/json"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert response.get_json()["code"] == "typed_manifest_invalid"


def test_typed_transfer_medium_size_smoke_records_bounded_runtime():
    rows = 15_000
    connection = duckdb.connect(":memory:")
    try:
        connection.execute("CREATE TABLE source (id INTEGER, amount DECIMAL(12,2), active BOOLEAN, note VARCHAR)")
        connection.execute(
            "INSERT INTO source SELECT i, i / 100.0, i % 2 = 0, CASE WHEN i % 10 = 0 THEN NULL ELSE 'row-' || i::VARCHAR END FROM range(?) t(i)",
            [rows],
        )
        columns = (
            OrderedColumnSchema("id", "id", 0, ValueType.INTEGER, False, physical_name="id"),
            OrderedColumnSchema("amount", "amount", 1, ValueType.DECIMAL, True, physical_name="amount"),
            OrderedColumnSchema("active", "active", 2, ValueType.BOOLEAN, True, physical_name="active"),
            OrderedColumnSchema("note", "note", 3, ValueType.TEXT, True, physical_name="note"),
        )
        manifest = TransferManifest.from_tables(
            "medium-smoke", "module1", "module2",
            [TableArtifact("source", "source", columns, rows)],
        )
        started = time.perf_counter()
        bound, payload = export_duckdb_parquet(connection, "source", manifest)
        imported = __import__("backend.ingestion.typed_artifact", fromlist=["import_duckdb_parquet"]).import_duckdb_parquet(
            connection, payload, bound, "destination"
        )
        elapsed = time.perf_counter() - started
        assert imported.row_count == rows
        assert connection.execute("SELECT COUNT(*) FROM destination").fetchone()[0] == rows
        assert elapsed < 30, f"medium typed transfer smoke exceeded 30s: {elapsed:.2f}s for {rows} rows"
    finally:
        connection.close()
