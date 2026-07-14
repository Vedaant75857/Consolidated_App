import os
import shutil
import sys
import tempfile

from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes import upload_routes
from shared import db
from services.upload.file_loader import (
    ensure_column_metadata_for_table,
    load_single_file,
    set_header_row_for_table,
)


def test_upload_persists_stable_column_metadata_with_original_display_names():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "metadata-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"Supplier Name,Supplier Name,,Amount\nAcme,Duplicate,,10\n",
            )

            assert warnings == []
            assert table_keys == ["sample.csv::"]
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0", "COL_1", "COL_2", "COL_3"]
        assert [col["displayName"] for col in visible] == [
            "Supplier Name",
            "Supplier Name",
            "",
            "Amount",
        ]
        assert [col["physicalName"] for col in visible] == [
            "SUPPLIER NAME",
            "SUPPLIER NAME_1",
            "UNNAMED",
            "AMOUNT",
        ]
        assert [col["dataType"] for col in visible] == ["TEXT", "TEXT", "TEXT", "TEXT"]
        assert any(col["key"] == "RECORD_ID" and col["hidden"] for col in metadata)
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_header_row_change_refreshes_column_metadata_display_names():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "header-metadata-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"skip,skip\nVendor,Invoice Amount\nAcme,10\n",
            )
            assert warnings == []

            set_header_row_for_table(
                conn,
                table_keys[0],
                1,
                custom_names={1: "Final Amount"},
            )
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0", "COL_1"]
        assert [col["displayName"] for col in visible] == ["Vendor", "Final Amount"]
        assert [col["physicalName"] for col in visible] == ["VENDOR", "FINAL AMOUNT"]
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_uploaded_record_id_header_is_reserved_for_hidden_row_identity():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "reserved-record-id-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"RECORD_ID,Foo\nexternal-1,bar\n",
            )

            assert warnings == []
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0"]
        assert [col["displayName"] for col in visible] == ["Foo"]
        assert [col["physicalName"] for col in visible] == ["FOO"]
        assert len([col for col in metadata if col["key"] == "RECORD_ID" and col["hidden"]]) == 1
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_legacy_metadata_backfill_filters_record_id_from_raw_display_headers():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "legacy-record-id-backfill-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"RECORD_ID,Foo\nexternal-1,bar\n",
            )
            assert warnings == []

            conn.execute("DELETE FROM _column_metadata WHERE table_key = ?", (table_keys[0],))
            conn.commit()
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0"]
        assert [col["displayName"] for col in visible] == ["Foo"]
        assert [col["physicalName"] for col in visible] == ["FOO"]
        assert len([col for col in metadata if col["key"] == "RECORD_ID" and col["hidden"]]) == 1
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_legacy_metadata_backfill_preserves_aligned_raw_display_headers():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "legacy-aligned-backfill-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"Supplier Name,Paid Amount\nAcme,20\n",
            )
            assert warnings == []

            conn.execute("DELETE FROM _column_metadata WHERE table_key = ?", (table_keys[0],))
            conn.commit()
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0", "COL_1"]
        assert [col["displayName"] for col in visible] == ["Supplier Name", "Paid Amount"]
        assert [col["physicalName"] for col in visible] == ["SUPPLIER NAME", "PAID AMOUNT"]
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_legacy_metadata_backfill_uses_current_columns_when_raw_header_is_stale():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-metadata-")
    db.SESSIONS_DIR = tmpdir
    session_id = "legacy-stale-raw-backfill-session"
    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"Old A,Old B\nNew A,New B\nvalue-a,value-b\n",
            )
            assert warnings == []
            set_header_row_for_table(conn, table_keys[0], 1)

            conn.execute("DELETE FROM _column_metadata WHERE table_key = ?", (table_keys[0],))
            conn.commit()
            metadata = ensure_column_metadata_for_table(conn, table_keys[0])

        visible = [col for col in metadata if not col["hidden"]]
        assert [col["key"] for col in visible] == ["COL_0", "COL_1"]
        assert [col["displayName"] for col in visible] == ["NEW A", "NEW B"]
        assert [col["physicalName"] for col in visible] == ["NEW A", "NEW B"]
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def test_set_header_row_route_uses_session_lock_for_db_work(monkeypatch):
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-header-route-")
    db.SESSIONS_DIR = tmpdir
    session_id = "header-route-lock-session"

    class GuardLock:
        def __init__(self):
            self.entered = False

        def __enter__(self):
            self.entered = True
            return self

        def __exit__(self, exc_type, exc, tb):
            self.entered = False
            return False

    guard = GuardLock()
    session_exists_lock_states = []
    original_route_session_exists = upload_routes.session_exists
    original_route_get_session_db = upload_routes.get_session_db

    def guarded_session_exists(sid):
        session_exists_lock_states.append(guard.entered)
        return original_route_session_exists(sid)

    def guarded_get_session_db(sid):
        assert guard.entered
        return original_route_get_session_db(sid)

    try:
        with db.get_session_lock(session_id):
            conn = db.get_session_db(session_id)
            table_keys, warnings = load_single_file(
                conn,
                "sample.csv",
                b"skip,skip\nVendor,Amount\nAcme,10\n",
            )
            assert warnings == []

        monkeypatch.setattr(upload_routes, "get_session_lock", lambda sid: guard)
        monkeypatch.setattr(upload_routes, "session_exists", guarded_session_exists)
        monkeypatch.setattr(upload_routes, "get_session_db", guarded_get_session_db)

        app = Flask(__name__)
        app.register_blueprint(upload_routes.upload_bp, url_prefix="/api")
        with app.test_client() as client:
            response = client.post("/api/set-header-row", json={
                "sessionId": session_id,
                "tableKey": table_keys[0],
                "headerRowIndex": 1,
            })

        assert response.status_code == 200
        assert session_exists_lock_states == [False, True]
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)
