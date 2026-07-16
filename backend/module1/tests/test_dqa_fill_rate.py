"""Focused regressions for preview identity and typed DQA fill-rate analysis."""

from __future__ import annotations

import importlib
import json
import sys
import uuid

import pytest

from data_quality_assessment.fill_rate_analysis import run_fill_rate_analysis
from data_quality_assessment.currency_analysis import run_currency_analysis_sql
from data_quality_assessment.country_region_analysis import run_country_region_analysis_sql
from data_quality_assessment.date_analysis import run_date_analysis_sql
from data_quality_assessment.metrics import compute_fill_rate_summary
from data_quality_assessment.payment_terms_analysis import run_payment_terms_analysis_sql
from data_quality_assessment.service import collect_column_samples, run_dqa_all_sql
from data_quality_assessment.supplier_analysis import run_supplier_analysis_sql
from preview_operations import service as preview_ops
from shared.db import (
    get_meta,
    get_session_db,
    quote_id,
    register_table,
    safe_table_name,
)


@pytest.fixture
def typed_session():
    session_id = uuid.uuid4().hex
    table_key = "typed.csv::Sheet1"
    sql_name = safe_table_name("tbl", table_key)
    conn = get_session_db(session_id)
    conn.execute(
        f"CREATE TABLE {quote_id(sql_name)} ("
        '"FILE_NAME" VARCHAR, "RECORD_ID" VARCHAR, '
        '"Count" BIGINT, "Amount" DOUBLE, "Occurred" DATE, '
        '"Approved" BOOLEAN, "Label" VARCHAR, "__source_table" VARCHAR)'
    )
    conn.execute(
        f"INSERT INTO {quote_id(sql_name)} VALUES "
        "('a.csv', '1', 1, 10.5, DATE '2026-01-01', TRUE, 'ok', 'legacy'), "
        "('a.csv', '2', NULL, NULL, NULL, FALSE, 'N/A', 'legacy')"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return conn, session_id, table_key, sql_name


def test_preview_then_fill_rate_hides_identity_and_handles_typed_columns(typed_session):
    conn, _, table_key, sql_name = typed_session

    preview = preview_ops.get_preview_data(conn, table_key)
    result = run_fill_rate_analysis(conn, sql_name)

    assert "__row_id" in preview["rows"][0]
    assert "__row_id" not in preview["columns"]
    by_name = {item["columnName"]: item for item in result["columns"]}
    assert set(by_name) == {"Count", "Amount", "Occurred", "Approved", "Label"}
    assert by_name["Count"]["pctRowsCovered"] == 50.0
    assert by_name["Approved"]["pctRowsCovered"] == 100.0
    assert by_name["Label"]["pctRowsCovered"] == 50.0


def test_public_boundary_applies_to_samples_and_legacy_summary(typed_session):
    conn, _, table_key, sql_name = typed_session
    preview_ops.get_preview_data(conn, table_key)

    samples = collect_column_samples(conn, sql_name)
    summary = compute_fill_rate_summary(conn, sql_name)

    assert "__row_id" not in samples
    assert "__source_table" not in samples
    assert {row["columnName"] for row in summary} == {
        "Count", "Amount", "Occurred", "Approved", "Label",
    }


def test_all_dqa_panels_ignore_internal_explicit_overrides(typed_session):
    conn, _, _, sql_name = typed_session
    internal = ["__row_id", "__source_table"]

    outputs = [
        run_currency_analysis_sql(conn, sql_name, internal),
        run_payment_terms_analysis_sql(conn, sql_name, internal),
        run_country_region_analysis_sql(conn, sql_name, internal, internal),
        run_date_analysis_sql(conn, sql_name, "__row_id"),
        run_supplier_analysis_sql(conn, sql_name, "__row_id"),
        run_dqa_all_sql(
            conn,
            sql_name,
            date_column="__row_id",
            country_column="__row_id",
            currency_column="__source_table",
            payment_terms_column="__row_id",
            vendor_column="__source_table",
        ),
    ]

    for output in outputs:
        serialized = json.dumps(output)
        assert "__row_id" not in serialized
        assert "__source_table" not in serialized


def test_preview_then_dqa_routes_keep_identity_out_of_fill_rate(
    monkeypatch, typed_session,
):
    """Both public DQA entry points keep preview identity out of their DTOs."""
    conn, session_id, table_key, _ = typed_session
    preview_ops.get_preview_data(conn, table_key)
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    routes = importlib.import_module("routes.data_quality_routes")
    monkeypatch.setattr(
        routes,
        "run_dqa_financial_ai",
        lambda *args, **kwargs: {
            "dateInsight": None,
            "currencyInsight": None,
            "paymentTermsInsight": None,
        },
    )
    monkeypatch.setattr(
        routes,
        "run_dqa_entity_ai",
        lambda *args, **kwargs: {
            "countryInsight": None,
            "regionInsight": None,
            "supplierInsight": None,
        },
    )
    client = app_module.app.test_client()

    fill_response = client.post(
        "/api/dqa/fill-rate",
        json={"sessionId": session_id, "tableKey": table_key},
    )
    assert fill_response.status_code == 200
    fill_names = {item["columnName"] for item in fill_response.get_json()["columns"]}
    assert fill_names == {"Count", "Amount", "Occurred", "Approved", "Label"}

    all_response = client.post(
        "/api/dqa/all",
        json={"sessionId": session_id, "tableKey": table_key, "apiKey": "test"},
    )
    assert all_response.status_code == 200
    all_fill_names = {
        item["columnName"] for item in all_response.get_json()["fillRate"]["columns"]
    }
    assert all_fill_names == fill_names


def test_empty_typed_table_returns_stable_empty_percentages():
    session_id = uuid.uuid4().hex
    table_key = "empty.csv::Sheet1"
    sql_name = safe_table_name("tbl", table_key)
    conn = get_session_db(session_id)
    conn.execute(
        f"CREATE TABLE {quote_id(sql_name)} ("
        '"FILE_NAME" VARCHAR, "RECORD_ID" VARCHAR, "Amount" DOUBLE, "When" DATE)'
    )
    register_table(conn, table_key, sql_name)
    conn.commit()

    preview_ops.get_preview_data(conn, table_key)
    result = run_fill_rate_analysis(conn, sql_name)

    assert result["columns"] == [
        {"columnName": "Amount", "pctRowsCovered": 0.0, "spendCoverage": None},
        {"columnName": "When", "pctRowsCovered": 0.0, "spendCoverage": None},
    ]
    assert result["spendColumn"] == "Amount"


def test_preview_validation_sort_and_revision_cache(typed_session):
    conn, _, table_key, _ = typed_session

    first = preview_ops.get_preview_data(
        conn, table_key, sort=[{"column": "Approved", "dir": "asc"}],
    )
    assert [row["__row_id"] for row in first["rows"]] == [2, 1]

    state = get_meta(conn, f"preview_ops_{table_key}")
    assert state["preview_cache"]["unfiltered_count"] == 2
    initial_revision = state["revision"]

    edited = preview_ops.run_operation(
        conn,
        table_key,
        "cell_edit",
        {"rowId": 1, "column": "Label", "value": "changed"},
        {"offset": 1, "limit": 1},
    )
    assert edited["offset"] == 1
    assert edited["limit"] == 1
    assert len(edited["rows"]) == 1
    state = get_meta(conn, f"preview_ops_{table_key}")
    assert state["revision"] == initial_revision + 1
    assert state["preview_cache"]["unfiltered_count"] == 2

    preview_ops.rows_delete(conn, table_key, [2])
    state = get_meta(conn, f"preview_ops_{table_key}")
    assert state["preview_cache"]["unfiltered_count"] == 1


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"offset": -1}, "offset"),
        ({"limit": 0}, "limit"),
        ({"filters": [{"column": "Label", "op": "bogus", "value": "x"}]}, "operator"),
        ({"filters": [{"column": "Label", "op": "in", "values": list(range(501))}]}, "500"),
        ({"sort": [{"column": "Label", "dir": "sideways"}]}, "direction"),
    ],
)
def test_preview_rejects_unbounded_or_invalid_queries(typed_session, kwargs, message):
    conn, _, table_key, _ = typed_session
    with pytest.raises(ValueError, match=message):
        preview_ops.get_preview_data(conn, table_key, **kwargs)


@pytest.mark.parametrize(
    "op, params, expected_column, expected_total",
    [
        (
            "column_rename",
            {"oldName": "Label", "newName": "Description"},
            "Description",
            1,
        ),
        ("column_delete", {"column": "Label"}, None, 2),
    ],
)
def test_shape_change_reconciles_stale_filter_and_sort_view(
    typed_session, op, params, expected_column, expected_total,
):
    conn, _, table_key, _ = typed_session
    view = {
        "limit": 10,
        "filters": [{"column": "Label", "op": "eq", "value": "ok"}],
        "sort": [{"column": "Label", "dir": "asc"}],
    }

    result = preview_ops.run_operation(conn, table_key, op, params, view)

    assert result["totalRows"] == expected_total
    assert "Label" not in result["columns"]
    if expected_column:
        assert expected_column in result["columns"]
        assert result["rows"][0][expected_column] == "ok"


def test_fill_rate_route_sanitizes_binder_errors(monkeypatch, typed_session):
    _, session_id, table_key, _ = typed_session
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    routes = importlib.import_module("routes.data_quality_routes")

    class BinderException(Exception):
        pass

    monkeypatch.setattr(
        routes,
        "run_dqa_fill_rate",
        lambda conn, table_name: (_ for _ in ()).throw(BinderException("SELECT secret SQL")),
    )
    client = app_module.app.test_client()
    response = client.post(
        "/api/dqa/fill-rate",
        json={"sessionId": session_id, "tableKey": table_key},
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "The selected table schema is not valid for this analysis.",
        "code": "DQA_QUERY_VALIDATION_ERROR",
    }


def test_dqa_all_route_uses_the_same_stable_binder_error(monkeypatch, typed_session):
    _, session_id, table_key, _ = typed_session
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    routes = importlib.import_module("routes.data_quality_routes")

    class BinderException(Exception):
        pass

    monkeypatch.setattr(
        routes,
        "run_dqa_all_sql",
        lambda *args, **kwargs: (_ for _ in ()).throw(BinderException("SELECT secret SQL")),
    )
    response = app_module.app.test_client().post(
        "/api/dqa/all",
        json={"sessionId": session_id, "tableKey": table_key, "apiKey": "test"},
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "error": "The selected table schema is not valid for this analysis.",
        "code": "DQA_QUERY_VALIDATION_ERROR",
    }


def test_preview_routes_return_stable_validation_envelopes(typed_session):
    _, session_id, table_key, _ = typed_session
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    client = app_module.app.test_client()

    responses = [
        client.post("/api/preview/state", json={"sessionId": session_id}),
        client.post(
            "/api/preview/state",
            json={"sessionId": session_id, "tableKey": table_key, "limit": 0},
        ),
        client.post(
            "/api/preview/operation",
            json={
                "sessionId": session_id,
                "tableKey": table_key,
                "op": "cell_edit",
                "params": [],
            },
        ),
    ]

    for response in responses:
        assert response.status_code == 400
        assert response.get_json()["code"] == "VALIDATION_ERROR"


def test_operation_undo_and_redo_routes_honor_optional_view(typed_session):
    _, session_id, table_key, _ = typed_session
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    client = app_module.app.test_client()
    view = {"offset": 1, "limit": 1}

    operation = client.post(
        "/api/preview/operation",
        json={
            "sessionId": session_id,
            "tableKey": table_key,
            "op": "cell_edit",
            "params": {"rowId": 1, "column": "Label", "value": "changed"},
            "view": view,
        },
    )
    undo = client.post(
        "/api/preview/undo",
        json={"sessionId": session_id, "tableKey": table_key, "view": view},
    )
    redo = client.post(
        "/api/preview/redo",
        json={"sessionId": session_id, "tableKey": table_key, "view": view},
    )

    for response in (operation, undo, redo):
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["ok"] is True
        assert payload["offset"] == 1
        assert payload["limit"] == 1
        assert len(payload["rows"]) == 1


def test_shape_change_routes_return_safe_view_after_commit(typed_session):
    _, session_id, table_key, _ = typed_session
    sys.modules.pop("app", None)
    app_module = importlib.import_module("app")
    client = app_module.app.test_client()

    rename = client.post(
        "/api/preview/operation",
        json={
            "sessionId": session_id,
            "tableKey": table_key,
            "op": "column_rename",
            "params": {"oldName": "Label", "newName": "Description"},
            "view": {
                "filters": [{"column": "Label", "op": "eq", "value": "ok"}],
                "sort": [{"column": "Label", "dir": "asc"}],
            },
        },
    )
    delete = client.post(
        "/api/preview/operation",
        json={
            "sessionId": session_id,
            "tableKey": table_key,
            "op": "column_delete",
            "params": {"column": "Amount"},
            "view": {
                "filters": [{"column": "Amount", "op": "gt", "value": "0"}],
                "sort": [{"column": "Amount", "dir": "desc"}],
            },
        },
    )

    assert rename.status_code == 200
    assert rename.get_json()["totalRows"] == 1
    assert "Description" in rename.get_json()["columns"]
    assert delete.status_code == 200
    assert delete.get_json()["totalRows"] == 2
    assert "Amount" not in delete.get_json()["columns"]
