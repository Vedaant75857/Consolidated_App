import os
import shutil
import sys
import tempfile

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from shared import db
from services.playground import service as playground_service
from services.playground.service import PreviewValidationError
from services.upload.file_loader import ensure_column_metadata_for_table


@pytest.fixture()
def preview_session():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-preview-service-")
    db.SESSIONS_DIR = tmpdir
    session_id = "preview-service-session"
    table_key = "sample.csv::"

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        conn.execute(
            "CREATE TABLE _table_registry "
            "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
        )
        conn.execute(
            "INSERT INTO _table_registry VALUES (?, ?, ?)",
            (table_key, "data__sample", "raw__sample"),
        )
        conn.execute(
            'CREATE TABLE "raw__sample" '
            '("RAW_0" VARCHAR, "RAW_1" VARCHAR, "RAW_2" VARCHAR, "RAW_3" VARCHAR)'
        )
        conn.execute(
            'INSERT INTO "raw__sample" VALUES (?, ?, ?, ?)',
            ("Supplier Name", "Paid Amount", "Invoice Date", "Free form Notes"),
        )
        conn.executemany(
            'INSERT INTO "raw__sample" VALUES (?, ?, ?, ?)',
            [
                ("raw-only", "999", "2099-01-01", "raw should not be previewed"),
                ("another raw", "1000", "2099-01-02", "raw should not be previewed"),
            ],
        )
        conn.execute(
            'CREATE TABLE "data__sample" '
            '("RECORD_ID" VARCHAR, "SUPPLIER NAME" VARCHAR, "PAID AMOUNT" VARCHAR, '
            '"INVOICE DATE" VARCHAR, "FREE FORM NOTES" VARCHAR)'
        )
        conn.executemany(
            'INSERT INTO "data__sample" VALUES (?, ?, ?, ?, ?)',
            [
                ("1", "Bravo", "10", "2024-02-01", "ordinary row"),
                ("2", "Acme", "2", "2023-01-15", "small table needle"),
                ("3", "Acme Supplies", "100", "not-a-date", "large table needle"),
                ("4", "Gamma", "20", "2022-12-31", "ordinary row"),
            ],
        )
        ensure_column_metadata_for_table(conn, table_key)
        conn.execute(
            "UPDATE _column_metadata SET data_type = ? WHERE table_key = ? AND column_key = ?",
            ("DOUBLE", table_key, "COL_1"),
        )
        conn.execute(
            "UPDATE _column_metadata SET data_type = ? WHERE table_key = ? AND column_key = ?",
            ("DATE", table_key, "COL_2"),
        )
        conn.commit()

    try:
        yield session_id, table_key
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def _conn(session_id):
    return db.get_session_db(session_id)


def test_paginated_preview_state_reads_data_table_and_hides_record_id(preview_session):
    session_id, table_key = preview_session

    with db.get_session_lock(session_id):
        state = playground_service.get_preview_state(_conn(session_id), table_key, offset=1, limit=2)

    assert state["totalRows"] == 4
    assert [row["__row_id"] for row in state["rows"]] == ["2", "3"]
    assert [row["values"]["COL_0"] for row in state["rows"]] == ["Acme", "Acme Supplies"]
    assert "raw-only" not in [row["values"]["COL_0"] for row in state["rows"]]
    assert [col["key"] for col in state["columns"]] == ["COL_0", "COL_1", "COL_2", "COL_3"]
    assert all(col["key"] != "RECORD_ID" for col in state["columns"])
    assert all("RECORD_ID" not in row["values"] for row in state["rows"])


def test_preview_state_preserves_original_header_display_text(preview_session):
    session_id, table_key = preview_session

    with db.get_session_lock(session_id):
        state = playground_service.get_preview_state(_conn(session_id), table_key)

    assert [col["displayName"] for col in state["columns"]] == [
        "Supplier Name",
        "Paid Amount",
        "Invoice Date",
        "Free form Notes",
    ]
    assert [col["dataType"] for col in state["columns"]] == ["TEXT", "DOUBLE", "DATE", "TEXT"]


def test_preview_state_search_filter_and_sort_use_stable_column_keys(preview_session):
    session_id, table_key = preview_session

    with db.get_session_lock(session_id):
        state = playground_service.get_preview_state(
            _conn(session_id),
            table_key,
            search="acme",
            filters=[{"columnKey": "COL_0", "op": "contains", "value": "acme"}],
            sort=[{"columnKey": "COL_1", "dir": "asc"}],
        )

    assert state["totalRows"] == 2
    assert [row["values"]["COL_1"] for row in state["rows"]] == ["2", "100"]

    with db.get_session_lock(session_id):
        with pytest.raises(PreviewValidationError):
            playground_service.get_preview_state(
                _conn(session_id),
                table_key,
                sort=[{"columnKey": "PAID AMOUNT", "dir": "asc"}],
            )


def test_preview_state_numeric_and_date_aware_sort(preview_session):
    session_id, table_key = preview_session

    with db.get_session_lock(session_id):
        numeric = playground_service.get_preview_state(
            _conn(session_id),
            table_key,
            sort=[{"columnKey": "COL_1", "dir": "asc"}],
        )
        dated = playground_service.get_preview_state(
            _conn(session_id),
            table_key,
            sort=[{"columnKey": "COL_2", "dir": "asc"}],
        )

    assert [row["values"]["COL_1"] for row in numeric["rows"]] == ["2", "10", "20", "100"]
    assert [row["__row_id"] for row in dated["rows"]] == ["4", "2", "1", "3"]


def test_large_table_search_requires_minimum_length(preview_session, monkeypatch):
    session_id, table_key = preview_session
    monkeypatch.setattr(playground_service, "LARGE_TABLE_SEARCH_ROW_THRESHOLD", 1)
    monkeypatch.setattr(playground_service, "LARGE_TABLE_MIN_SEARCH_LENGTH", 3)

    with db.get_session_lock(session_id):
        with pytest.raises(PreviewValidationError) as exc:
            playground_service.get_preview_state(_conn(session_id), table_key, search="ac")

    assert exc.value.details["field"] == "search"
    assert exc.value.details["minLength"] == 3


def test_large_table_search_caps_searchable_columns_but_small_tables_do_not(preview_session, monkeypatch):
    session_id, table_key = preview_session

    monkeypatch.setattr(playground_service, "LARGE_TABLE_SEARCH_ROW_THRESHOLD", 999)
    monkeypatch.setattr(playground_service, "LARGE_TABLE_SEARCHABLE_COLUMNS", 2)
    with db.get_session_lock(session_id):
        small_state = playground_service.get_preview_state(_conn(session_id), table_key, search="small table needle")

    monkeypatch.setattr(playground_service, "LARGE_TABLE_SEARCH_ROW_THRESHOLD", 1)
    monkeypatch.setattr(playground_service, "LARGE_TABLE_SEARCHABLE_COLUMNS", 2)
    with db.get_session_lock(session_id):
        large_state = playground_service.get_preview_state(_conn(session_id), table_key, search="large table needle")

    assert [row["__row_id"] for row in small_state["rows"]] == ["2"]
    assert large_state["totalRows"] == 0
