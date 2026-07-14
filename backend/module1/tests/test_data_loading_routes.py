"""End-to-end route tests for bulk delete, raw preview, and header-row editing."""

from __future__ import annotations

import importlib
import sys
import uuid
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def app_client():
    """Spin up the Flask app once per module (sessions dir set in conftest)."""
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    flask_app = app_module.app
    flask_app.config.update(TESTING=True)
    return flask_app.test_client(), app_module


@pytest.fixture
def fresh_session(app_client):
    """Return a (client, session_id) pair backed by an empty DuckDB session."""
    from shared.db import get_session_db

    client, _ = app_client
    session_id = uuid.uuid4().hex
    get_session_db(session_id)
    return client, session_id


@pytest.fixture
def session_with_tables(fresh_session):
    """Seed three small uploaded tables into a fresh session."""
    from shared.db import (
        get_session_db,
        quote_id,
        register_table,
        safe_table_name,
    )

    client, session_id = fresh_session
    conn = get_session_db(session_id)

    table_keys = ["alpha.csv::", "beta.csv::", "gamma.csv::"]
    for key in table_keys:
        raw_name = safe_table_name("raw", key)
        tbl_name = safe_table_name("tbl", key)
        rq = quote_id(raw_name)
        tq = quote_id(tbl_name)
        conn.execute(
            f"CREATE TABLE {rq} ({quote_id('RAW_1')} VARCHAR, {quote_id('RAW_2')} VARCHAR)"
        )
        conn.execute(
            f"INSERT INTO {rq} VALUES ('header_a', 'header_b'), ('1', '2'), ('3', '4')"
        )
        conn.execute(
            f"CREATE TABLE {tq} ("
            f"{quote_id('FILE_NAME')} VARCHAR, {quote_id('RECORD_ID')} VARCHAR, "
            f"{quote_id('HEADER_A')} VARCHAR, {quote_id('HEADER_B')} VARCHAR)"
        )
        conn.execute(
            f"INSERT INTO {tq} VALUES "
            f"('{key}', '1', '1', '2'), ('{key}', '2', '3', '4')"
        )
        register_table(conn, key, tbl_name)
    conn.commit()
    return client, session_id, table_keys


def _registry_keys(session_id: str) -> list[str]:
    from shared.db import get_session_db

    conn = get_session_db(session_id)
    rows = conn.execute("SELECT table_key FROM table_registry ORDER BY table_key").fetchall()
    return [r["table_key"] for r in rows]


def test_bulk_delete_removes_selected_tables(session_with_tables):
    client, session_id, keys = session_with_tables
    to_delete = keys[:2]

    resp = client.post(
        "/api/delete-tables",
        json={"sessionId": session_id, "tableKeys": to_delete},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    payload = resp.get_json()
    assert payload["deletedCount"] == 2
    inv_keys = sorted(row["table_key"] for row in payload["inventory"])
    assert inv_keys == [keys[2]]
    assert _registry_keys(session_id) == [keys[2]]


def test_bulk_delete_strict_404_on_missing_key(session_with_tables):
    client, session_id, keys = session_with_tables
    before = _registry_keys(session_id)

    resp = client.post(
        "/api/delete-tables",
        json={"sessionId": session_id, "tableKeys": [keys[0], "does-not-exist.csv::"]},
    )
    assert resp.status_code == 404
    payload = resp.get_json()
    assert "does-not-exist.csv::" in payload["missing"]
    assert _registry_keys(session_id) == before, "DB must be unchanged when validation fails"


def test_bulk_delete_rejects_empty_keys(fresh_session):
    client, session_id = fresh_session
    resp = client.post(
        "/api/delete-tables",
        json={"sessionId": session_id, "tableKeys": []},
    )
    assert resp.status_code == 400


def test_bulk_delete_rejects_non_string_keys(fresh_session):
    client, session_id = fresh_session
    resp = client.post(
        "/api/delete-tables",
        json={"sessionId": session_id, "tableKeys": [123, None]},
    )
    assert resp.status_code == 400


def test_get_raw_preview_serialises_decimal_and_date(fresh_session):
    from shared.db import get_session_db, register_table, quote_id, safe_table_name

    client, session_id = fresh_session
    conn = get_session_db(session_id)

    key = "decimals.csv::"
    raw_name = safe_table_name("raw", key)
    tbl_name = safe_table_name("tbl", key)
    rq = quote_id(raw_name)
    tq = quote_id(tbl_name)
    conn.execute(
        f"CREATE TABLE {rq} ("
        f"{quote_id('RAW_1')} DECIMAL(10,2), {quote_id('RAW_2')} DATE)"
    )
    conn.execute(
        f"INSERT INTO {rq} VALUES (123.45, DATE '2025-01-15'), (NULL, DATE '2025-02-01')"
    )
    conn.execute(f"CREATE TABLE {tq} ({quote_id('FILE_NAME')} VARCHAR, {quote_id('RECORD_ID')} VARCHAR)")
    register_table(conn, key, tbl_name)
    conn.commit()

    resp = client.post(
        "/api/get-raw-preview",
        json={"sessionId": session_id, "tableKey": key},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    rows = resp.get_json()["rawPreview"]
    assert rows[0][0] in ("123.45", 123.45)
    assert str(rows[0][1]).startswith("2025-01-15")
    assert rows[1][0] is None


def test_get_raw_preview_404_for_unknown_table(fresh_session):
    client, session_id = fresh_session
    resp = client.post(
        "/api/get-raw-preview",
        json={"sessionId": session_id, "tableKey": "missing.csv::"},
    )
    assert resp.status_code == 404


def test_set_header_row_rebuilds_table_and_returns_single_preview(fresh_session):
    from shared.db import get_session_db, register_table, quote_id, safe_table_name

    client, session_id = fresh_session
    conn = get_session_db(session_id)

    key = "with_header.csv::"
    raw_name = safe_table_name("raw", key)
    tbl_name = safe_table_name("tbl", key)
    rq = quote_id(raw_name)
    tq = quote_id(tbl_name)
    conn.execute(
        f"CREATE TABLE {rq} ("
        f"{quote_id('RAW_1')} VARCHAR, {quote_id('RAW_2')} VARCHAR, {quote_id('RAW_3')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {rq} VALUES "
        f"('note row', NULL, NULL), "
        f"('Item', 'Qty', 'Price'), "
        f"('Widget', '10', '2.50'), "
        f"('Gizmo', '5', '1.99')"
    )
    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('FILE_NAME')} VARCHAR, {quote_id('RECORD_ID')} VARCHAR, "
        f"{quote_id('ORIGINAL')} VARCHAR)"
    )
    register_table(conn, key, tbl_name)
    conn.commit()

    resp = client.post(
        "/api/set-header-row",
        json={"sessionId": session_id, "tableKey": key, "headerRowIndex": 1},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert "preview" in body
    cols = body["preview"]["columns"]
    assert "FILE_NAME" in cols and "RECORD_ID" in cols
    assert "ITEM" in cols and "QTY" in cols and "PRICE" in cols
    assert len(body["preview"]["rows"]) == 2
    assert [r["ITEM"] for r in body["preview"]["rows"]] == ["WIDGET", "GIZMO"]
    assert "NOTE ROW" not in {str(v) for r in body["preview"]["rows"] for v in r.values()}

    inv_row = next(r for r in body["inventory"] if r["table_key"] == key)
    assert inv_row["rows"] == 2


def test_set_header_row_survives_cursor_column_skew(fresh_session, monkeypatch):
    from shared.db import get_session_db, register_table, quote_id, safe_table_name
    from shared.db import duckdb_compat

    original = duckdb_compat.DuckCursorWrapper._ensure_columns

    def _skewed_columns(self):
        cols = original(self)
        if not cols or cols[-1] == "__SKEW_EXTRA__":
            return cols
        return cols + ("__SKEW_EXTRA__",)

    monkeypatch.setattr(duckdb_compat.DuckCursorWrapper, "_ensure_columns", _skewed_columns)

    client, session_id = fresh_session
    conn = get_session_db(session_id)

    key = "skewed.csv::"
    raw_name = safe_table_name("raw", key)
    tbl_name = safe_table_name("tbl", key)
    rq = quote_id(raw_name)
    tq = quote_id(tbl_name)
    conn.execute(
        f"CREATE TABLE {rq} ("
        f"{quote_id('RAW_1')} VARCHAR, {quote_id('RAW_2')} VARCHAR, {quote_id('RAW_3')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {rq} VALUES "
        f"('note row', NULL, NULL), "
        f"('Item', 'Qty', 'Price'), "
        f"('Widget', '10', '2.50')"
    )
    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('FILE_NAME')} VARCHAR, {quote_id('RECORD_ID')} VARCHAR, "
        f"{quote_id('ORIGINAL')} VARCHAR)"
    )
    register_table(conn, key, tbl_name)
    conn.commit()

    resp = client.post(
        "/api/set-header-row",
        json={"sessionId": session_id, "tableKey": key, "headerRowIndex": 1},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert len(body["preview"]["rows"]) == 1
    assert body["preview"]["rows"][0]["ITEM"] == "WIDGET"


def test_set_header_row_shape_mismatch_returns_400(fresh_session, monkeypatch):
    from shared.db import get_session_db, register_table, quote_id, safe_table_name
    from data_loading import file_loader

    client, session_id = fresh_session
    conn = get_session_db(session_id)

    key = "badshape.csv::"
    raw_name = safe_table_name("raw", key)
    tbl_name = safe_table_name("tbl", key)
    rq = quote_id(raw_name)
    tq = quote_id(tbl_name)
    conn.execute(
        f"CREATE TABLE {rq} ({quote_id('RAW_1')} VARCHAR, {quote_id('RAW_2')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {rq} VALUES ('Item', 'Qty'), ('Widget', '10')"
    )
    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('FILE_NAME')} VARCHAR, {quote_id('RECORD_ID')} VARCHAR, "
        f"{quote_id('ORIGINAL')} VARCHAR)"
    )
    register_table(conn, key, tbl_name)
    conn.commit()

    def _bad_read_cols(_conn, _table_name):
        return ["RAW_1", "RAW_2", "RAW_3"]

    monkeypatch.setattr(file_loader, "read_table_columns", _bad_read_cols)

    resp = client.post(
        "/api/set-header-row",
        json={"sessionId": session_id, "tableKey": key, "headerRowIndex": 0},
    )
    assert resp.status_code == 400
    assert "shape mismatch" in resp.get_json()["error"].lower()


def test_app_atexit_hook_is_main_only():
    """``atexit.register(_on_exit)`` must live inside ``if __name__ == "__main__":``
    so importing ``app.py`` from tests does not wipe active session DBs on exit.
    """
    app_path = Path(__file__).resolve().parent.parent / "app.py"
    source = app_path.read_text(encoding="utf-8").splitlines()

    main_idx = next(
        (i for i, line in enumerate(source) if line.strip().startswith('if __name__ == "__main__"')),
        None,
    )
    register_idx = next(
        (i for i, line in enumerate(source) if "atexit.register(_on_exit)" in line),
        None,
    )

    assert main_idx is not None, "Missing 'if __name__ == \"__main__\":' guard in app.py"
    assert register_idx is not None, "Missing atexit.register(_on_exit) call in app.py"
    assert register_idx > main_idx, (
        "atexit.register(_on_exit) must appear after the __main__ guard to "
        "avoid running session cleanup on plain imports."
    )
