"""Smoke and HTTP tests for preview API routes."""

from __future__ import annotations

import importlib
import sys
import uuid

import pytest

from shared.db import get_session_db, quote_id, register_table, safe_table_name


@pytest.fixture(scope="module")
def app_client():
    """Spin up the Flask app once per module."""
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    flask_app = app_module.app
    flask_app.config.update(TESTING=True)
    return flask_app.test_client()


@pytest.fixture
def seeded_preview_session(app_client):
    """Create a session with one registered table for preview routes."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "sample.csv::Sheet1"
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)
    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('FILE_NAME')} VARCHAR, "
        f"{quote_id('AMOUNT')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {tq} VALUES ('file_a.xls', '100'), ('file_b.xls', '200')"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return app_client, session_id, table_key


def test_preview_routes_are_registered(app_client):
    """Preview blueprint must load so /api/preview/* endpoints are available."""
    rules = {str(rule) for rule in app_client.application.url_map.iter_rules()}
    expected = {
        "/api/preview/state",
        "/api/preview/column-values",
        "/api/preview/operation",
        "/api/preview/undo",
        "/api/preview/redo",
        "/api/preview/apply",
        "/api/preview/refresh-inventory",
    }
    missing = expected - rules
    assert not missing, f"Missing preview routes: {sorted(missing)}"


def test_preview_state_happy_path(seeded_preview_session):
    """POST /api/preview/state returns paginated rows for a registered table."""
    client, session_id, table_key = seeded_preview_session
    resp = client.post(
        "/api/preview/state",
        json={"sessionId": session_id, "tableKey": table_key, "offset": 0, "limit": 50},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["totalRows"] == 2
    assert len(data["rows"]) == 2
    assert "AMOUNT" in data["columns"]
    assert "__row_id" in data["rows"][0]


def test_preview_column_values_happy_path(seeded_preview_session):
    """POST /api/preview/column-values returns distinct values for a column."""
    client, session_id, table_key = seeded_preview_session
    resp = client.post(
        "/api/preview/column-values",
        json={"sessionId": session_id, "tableKey": table_key, "column": "FILE_NAME"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert set(data["values"]) == {"file_a.xls", "file_b.xls"}
    assert data["totalDistinct"] == 2
    assert data["hasBlanks"] is False


def test_preview_operation_view_is_optional_and_scopes_mutation_response(
    seeded_preview_session,
):
    """New callers can request their current page while legacy callers keep defaults."""
    client, session_id, table_key = seeded_preview_session

    scoped = client.post(
        "/api/preview/operation",
        json={
            "sessionId": session_id,
            "tableKey": table_key,
            "op": "cell_edit",
            "params": {"rowId": 1, "column": "AMOUNT", "value": "150"},
            "view": {"offset": 1, "limit": 1},
        },
    )
    assert scoped.status_code == 200
    scoped_data = scoped.get_json()
    assert scoped_data["ok"] is True
    assert scoped_data["offset"] == 1
    assert scoped_data["limit"] == 1
    assert len(scoped_data["rows"]) == 1
    assert scoped_data["rows"][0]["AMOUNT"] == "200"

    legacy = client.post(
        "/api/preview/operation",
        json={
            "sessionId": session_id,
            "tableKey": table_key,
            "op": "cell_edit",
            "params": {"rowId": 2, "column": "AMOUNT", "value": "250"},
        },
    )
    assert legacy.status_code == 200
    legacy_data = legacy.get_json()
    assert legacy_data["ok"] is True
    assert legacy_data["offset"] == 0
    assert len(legacy_data["rows"]) == 2
    assert [row["AMOUNT"] for row in legacy_data["rows"]] == ["150", "250"]


def test_preview_state_missing_table_returns_400(seeded_preview_session):
    """Unknown table_key must return 400 validation error, not 500."""
    client, session_id, _ = seeded_preview_session
    resp = client.post(
        "/api/preview/state",
        json={"sessionId": session_id, "tableKey": "missing::Sheet"},
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
    assert "not found" in data["error"].lower()


def test_preview_state_invalid_session_returns_400(app_client):
    """Invalid sessionId format must return 400."""
    resp = app_client.post(
        "/api/preview/state",
        json={"sessionId": "bad/session!", "tableKey": "x::y"},
    )
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_preview_state_sanitizes_internal_errors(app_client, monkeypatch):
    """Unexpected preview failures must not expose database or SQL details."""
    from routes import preview_routes

    monkeypatch.setattr(
        preview_routes.preview_ops,
        "get_preview_data",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("secret SELECT")),
    )
    response = app_client.post(
        "/api/preview/state",
        json={"sessionId": uuid.uuid4().hex, "tableKey": "x::y"},
    )
    assert response.status_code == 500
    assert response.get_json() == {
        "error": "Preview operation failed.",
        "code": "PREVIEW_INTERNAL_ERROR",
    }


def test_get_preview_happy_path(seeded_preview_session):
    """GET /api/get-preview returns first rows for a registered table."""
    client, session_id, table_key = seeded_preview_session
    resp = client.get(
        f"/api/get-preview?sessionId={session_id}&tableKey={table_key}"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "preview" in data
    assert "AMOUNT" in data["preview"]["columns"]
    assert len(data["preview"]["rows"]) == 2


def test_get_preview_missing_table_returns_404(seeded_preview_session):
    """Unknown table_key on get-preview must return 404."""
    client, session_id, _ = seeded_preview_session
    resp = client.get(
        f"/api/get-preview?sessionId={session_id}&tableKey=missing::Sheet"
    )
    assert resp.status_code == 404
    assert "error" in resp.get_json()
