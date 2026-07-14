import os
import shutil
import sys
import tempfile

from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes.preview_routes import preview_bp
from shared import db


def _make_app():
    app = Flask(__name__)
    app.register_blueprint(preview_bp, url_prefix="/api")
    return app


def test_preview_contract_shape_and_no_apply_route():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-playground-contract-")
    db.SESSIONS_DIR = tmpdir
    session_id = "playground-contract-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            conn.execute(
                "CREATE TABLE _table_registry "
                "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
            )
            conn.execute(
                "INSERT INTO _table_registry VALUES (?, ?, ?)",
                ("sample.csv::", "data__sample", "raw__sample"),
            )
            conn.execute('CREATE TABLE "data__sample" ("RECORD_ID" VARCHAR, "A" VARCHAR)')
            conn.execute('CREATE TABLE "raw__sample" ("RAW_0" VARCHAR)')
            conn.execute('INSERT INTO "raw__sample" VALUES (?)', ("A",))
            conn.execute('INSERT INTO "data__sample" VALUES (?, ?)', ("1", "x"))
            conn.commit()

        app = _make_app()
        rules = {rule.rule for rule in app.url_map.iter_rules()}
        assert "/api/preview/apply" not in rules

        with app.test_client() as client:
            response = client.post(
                "/api/preview/state",
                json={"sessionId": session_id, "tableKey": "sample.csv::"},
            )

        assert response.status_code == 200
        body = response.get_json()
        assert set(
            [
                "tableKey",
                "columns",
                "rows",
                "totalRows",
                "offset",
                "limit",
                "undoDepth",
                "redoDepth",
                "dirty",
                "tableVersion",
                "sourceTableKey",
                "playgroundTableName",
            ]
        ).issubset(body.keys())
        assert "dataType" in body["columns"][0]
        assert "values" in body["rows"][0]
        assert "COL_0" in body["rows"][0]["values"]
        assert "columnTypes" not in body
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_preview_operation_rejects_display_or_physical_column_refs():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-playground-contract-")
    db.SESSIONS_DIR = tmpdir
    session_id = "playground-contract-reject-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            conn.execute(
                "CREATE TABLE _table_registry "
                "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
            )
            conn.execute(
                "INSERT INTO _table_registry VALUES (?, ?, ?)",
                ("sample.csv::", "data__sample", "raw__sample"),
            )
            conn.execute('CREATE TABLE "data__sample" ("RECORD_ID" VARCHAR, "A" VARCHAR)')
            conn.execute('CREATE TABLE "raw__sample" ("RAW_0" VARCHAR)')
            conn.execute('INSERT INTO "raw__sample" VALUES (?)', ("A",))
            conn.execute('INSERT INTO "data__sample" VALUES (?, ?)', ("1", "x"))
            conn.commit()

        with _make_app().test_client() as client:
            response = client.post(
                "/api/preview/operation",
                json={
                    "sessionId": session_id,
                    "tableKey": "sample.csv::",
                    "op": "cell_edit",
                    "params": {"rowId": "1", "column": "A", "value": "y"},
                },
            )

        assert response.status_code == 400
        body = response.get_json()
        assert body["code"] == "VALIDATION_ERROR"
        assert "column" in body["details"]["fields"]
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)

