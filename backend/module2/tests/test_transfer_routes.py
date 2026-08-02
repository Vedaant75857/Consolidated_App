from __future__ import annotations

import io
import sys
import uuid
from dataclasses import replace

import pytest


@pytest.fixture()
def module2_app(monkeypatch):
    root = "backend/module2"
    sys.path.insert(0, root)
    try:
        import app as module
        yield module
    finally:
        sys.path.remove(root)


def test_csv_import_is_explicitly_lossy_and_populates_compatibility_tables(module2_app):
    client = module2_app.app.test_client()
    response = client.post(
        "/api/import-from-stitcher",
        data={"file": (io.BytesIO(b"name,amount\nAcme,12\n"), "input.csv")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    body = response.get_json()
    assert body["transport"] == "csv"
    assert body["warnings"][0]["code"] == "LOSSY_CSV_FALLBACK"

    session_id = body["sessionId"]
    with module2_app.get_session_lock(session_id):
        conn = module2_app.get_session_db(session_id)
        key = body["inventory"][0]["table_key"]
        data_sql = module2_app.lookup_sql_name(conn, key)
        assert data_sql and module2_app.table_exists(conn, data_sql)


def test_typed_payload_is_rejected_before_import_without_manifest(module2_app):
    client = module2_app.app.test_client()
    response = client.post(
        "/api/import-from-stitcher",
        data={"file": (io.BytesIO(b"not-parquet"), "input.parquet")},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert response.get_json()["code"] == "typed_manifest_required"


def test_typed_duckdb_round_trip_import_and_transfer(module2_app):
    session_id = f"transfer_{uuid.uuid4().hex}"
    with module2_app.get_session_lock(session_id):
        conn = module2_app.get_session_db(session_id)
        conn.execute('CREATE TABLE "active" ("a" INTEGER)')
        conn.execute('INSERT INTO "active" VALUES (1)')
        conn.commit()

        manifest, payload = module2_app._export_typed_transfer(
            conn, filename="roundtrip.parquet", session_id=session_id,
        )
        from backend.ingestion.typed_artifact import serialize_transport
        manifest = replace(manifest, destination_module="module2", artifact_checksum=None,
                           source_module="module1").bind_payload(
            payload, payload_format="parquet",
        )
        manifest = serialize_transport(manifest)

    client = module2_app.app.test_client()
    response = client.post(
        "/api/import-from-stitcher",
        data={
            "file": (io.BytesIO(payload), "roundtrip.parquet"),
            "manifest": (io.BytesIO(manifest), "roundtrip.manifest.json"),
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200, response.get_json()
    body = response.get_json()
    assert body["transport"] == "typed"
    destination = body["sessionId"]
    with module2_app.get_session_lock(destination):
        imported_conn = module2_app.get_session_db(destination)
        key = body["inventory"][0]["table_key"]
        data_sql = module2_app.lookup_sql_name(imported_conn, key)
        assert data_sql and module2_app.table_exists(imported_conn, data_sql)
        assert module2_app.table_exists(imported_conn, "active")
        assert imported_conn.execute('SELECT COUNT(*) FROM "active"').fetchone()[0] == 1
        assert imported_conn.execute('SELECT "a" FROM "active"').fetchone()[0] == "1"
        assert "INTEGER" in str(imported_conn.execute('PRAGMA table_info("' + data_sql + '")').fetchone()[2]).upper()

    retry = client.post(
        "/api/import-from-stitcher",
        data={
            "file": (io.BytesIO(payload), "roundtrip.parquet"),
            "manifest": (io.BytesIO(manifest), "roundtrip.manifest.json"),
        },
        content_type="multipart/form-data",
    )
    assert retry.status_code == 200
    assert retry.get_json()["sessionId"] == destination
    assert retry.get_json()["idempotent"] is True

    # Same artifact ID with different payload bytes is a collision, never an
    # overwrite of the previously published destination session.
    with module2_app.get_session_lock(session_id):
        source_conn = module2_app.get_session_db(session_id)
        source_conn.execute('UPDATE "active" SET "a" = 2')
        source_conn.commit()
        changed_manifest, changed_payload = module2_app._export_typed_transfer(
            source_conn, filename="roundtrip.parquet", session_id=session_id,
        )
    changed_manifest = replace(changed_manifest, destination_module="module2", artifact_checksum=None,
                               source_module="module1").bind_payload(changed_payload, payload_format="parquet")
    from backend.ingestion.typed_artifact import serialize_transport
    collision = client.post(
        "/api/import-from-stitcher",
        data={
            "file": (io.BytesIO(changed_payload), "roundtrip.parquet"),
            "manifest": (io.BytesIO(serialize_transport(changed_manifest)), "roundtrip.manifest.json"),
        },
        content_type="multipart/form-data",
    )
    assert collision.status_code == 409
    assert collision.get_json()["code"] == "typed_artifact_collision"


def test_transfer_prefers_typed_payload(module2_app, monkeypatch):
    session_id = f"transfer_{uuid.uuid4().hex}"
    with module2_app.get_session_lock(session_id):
        conn = module2_app.get_session_db(session_id)
        module2_app.df_to_sqlite(conn, "active", __import__("pandas").DataFrame({"a": [1]}))

    class Response:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = ""

        @staticmethod
        def json():
            return {"sessionId": "destination-session"}

    def post(url, *, files, timeout):
        assert files["file"][0].endswith(".parquet")
        assert files["manifest"][0].endswith(".manifest.json")
        return Response()

    monkeypatch.setattr(module2_app._requests, "post", post)
    client = module2_app.app.test_client()
    response = client.post("/api/transfer-to-analyzer", json={"sessionId": session_id})
    assert response.status_code == 200
    assert response.get_json() == {
        "ok": True, "analyzerSessionId": "destination-session", "transport": "typed",
    }
