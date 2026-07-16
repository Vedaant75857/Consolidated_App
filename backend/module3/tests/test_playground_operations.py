import os
import shutil
import sys
import tempfile

import pytest
from flask import Flask

BACKEND_DIR = os.path.dirname(os.path.dirname(__file__))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from routes.preview_routes import preview_bp
from shared import db
from services.playground import service as playground_service


class CountingConnection:
    def __init__(self, conn):
        self.conn = conn
        self.statement_counts = {}

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self.statement_counts[normalized] = self.statement_counts.get(normalized, 0) + 1
        if params is None:
            return self.conn.execute(query)
        return self.conn.execute(query, params)

    def __getattr__(self, name):
        return getattr(self.conn, name)


@pytest.fixture()
def playground_session():
    original_sessions_dir = db.SESSIONS_DIR
    tmpdir = tempfile.mkdtemp(prefix="summarizer-playground-ops-")
    db.SESSIONS_DIR = tmpdir
    session_id = "playground-ops-session"
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
        conn.execute(
            'CREATE TABLE "data__sample" '
            '("RECORD_ID" VARCHAR, "SUPPLIER" VARCHAR, "AMOUNT" VARCHAR, "REGION" VARCHAR)'
        )
        conn.execute('CREATE TABLE "raw__sample" ("RAW_0" VARCHAR, "RAW_1" VARCHAR, "RAW_2" VARCHAR)')
        conn.execute(
            'INSERT INTO "raw__sample" VALUES (?, ?, ?)',
            ("Supplier", "Amount", "Region"),
        )
        conn.executemany(
            'INSERT INTO "data__sample" VALUES (?, ?, ?, ?)',
            [
                ("1", "Acme", "20", "North"),
                ("2", "Bravo", "10", "South"),
                ("3", "Acme", "30", ""),
                ("4", "Zero Co", "0", None),
            ],
        )
        conn.commit()

    app = Flask(__name__)
    app.register_blueprint(preview_bp, url_prefix="/api")
    try:
        yield session_id, app
    finally:
        try:
            db.delete_session(session_id)
        finally:
            db.SESSIONS_DIR = original_sessions_dir
            shutil.rmtree(tmpdir, ignore_errors=True)


def _post(app, path, session_id, **payload):
    with app.test_client() as client:
        return client.post(path, json={"sessionId": session_id, "tableKey": "sample.csv::", **payload})


def test_column_values_excludes_own_filter_and_reports_blanks(playground_session):
    session_id, app = playground_session
    response = _post(
        app,
        "/api/preview/column-values",
        session_id,
        columnKey="COL_2",
        filters=[
            {"columnKey": "COL_2", "op": "in", "values": ["North"]},
            {"columnKey": "COL_0", "op": "contains", "value": "acme"},
        ],
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body == {
        "columnKey": "COL_2",
        "values": ["North"],
        "hasBlanks": True,
        "totalDistinct": 1,
    }


def test_preview_state_uses_versioned_cache_for_identical_reads(playground_session):
    session_id, _app = playground_session
    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        counted = CountingConnection(conn)
        first = playground_service.get_preview_state(
            counted,
            "sample.csv::",
            offset=0,
            limit=2,
            search="acme",
            filters=[{"columnKey": "COL_0", "op": "contains", "value": "acme"}],
            sort=[{"columnKey": "COL_1", "dir": "desc"}],
        )
        second = playground_service.get_preview_state(
            counted,
            "sample.csv::",
            offset=0,
            limit=2,
            search="acme",
            filters=[{"columnKey": "COL_0", "op": "contains", "value": "acme"}],
            sort=[{"columnKey": "COL_1", "dir": "desc"}],
        )

    data_reads = sum(
        count
        for statement, count in counted.statement_counts.items()
        if 'FROM "data__sample" WHERE' in statement
        and (statement.startswith("SELECT COUNT(*)") or statement.startswith('SELECT "RECORD_ID"'))
    )
    assert first == second
    assert data_reads == 2


def test_column_values_cache_avoids_repeated_distinct_sql(playground_session):
    session_id, _app = playground_session
    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        counted = CountingConnection(conn)
        first = playground_service.get_column_values(
            counted,
            "sample.csv::",
            "COL_2",
            search="o",
            filters=[
                {"columnKey": "COL_2", "op": "in", "values": ["North"]},
                {"columnKey": "COL_0", "op": "contains", "value": "acme"},
            ],
        )
        second = playground_service.get_column_values(
            counted,
            "sample.csv::",
            "COL_2",
            search="o",
            filters=[
                {"columnKey": "COL_2", "op": "in", "values": ["North"]},
                {"columnKey": "COL_0", "op": "contains", "value": "acme"},
            ],
        )

    distinct_reads = sum(
        count
        for statement, count in counted.statement_counts.items()
        if statement.startswith("SELECT DISTINCT CAST")
    )
    assert first == second
    assert distinct_reads == 1


def test_column_values_cache_is_versioned_after_mutation(playground_session):
    session_id, app = playground_session
    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        before = playground_service.get_column_values(conn, "sample.csv::", "COL_1")

    edit = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="cell_edit",
        params={"rowId": "1", "columnKey": "COL_1", "value": "99"},
    )
    assert edit.status_code == 200

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        after = playground_service.get_column_values(conn, "sample.csv::", "COL_1")

    assert "20" in before["values"]
    assert "99" in after["values"]


def test_cell_edit_copy_on_write_and_undo_redo_do_not_mutate_source(playground_session):
    session_id, app = playground_session
    response = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="cell_edit",
        params={"rowId": "1", "columnKey": "COL_1", "value": "25"},
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    assert body["dirty"] is True
    assert body["undoDepth"] == 1
    assert body["redoDepth"] == 0
    assert body["tableVersion"] == 1
    assert body["playgroundTableName"].startswith("pg__")
    assert body["rows"][0]["values"]["COL_1"] == "25"

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        source_value = conn.execute(
            'SELECT "AMOUNT" FROM "data__sample" WHERE "RECORD_ID" = ?',
            ("1",),
        ).fetchone()[0]
    assert source_value == "20"

    undo = _post(app, "/api/preview/undo", session_id)
    assert undo.status_code == 200
    undo_body = undo.get_json()
    assert undo_body["ok"] is True
    assert undo_body["rows"][0]["values"]["COL_1"] == "20"
    assert undo_body["redoDepth"] == 1

    redo = _post(app, "/api/preview/redo", session_id)
    assert redo.status_code == 200
    redo_body = redo.get_json()
    assert redo_body["ok"] is True
    assert redo_body["rows"][0]["values"]["COL_1"] == "25"


def test_metadata_operations_use_stable_keys(playground_session):
    session_id, app = playground_session
    rename = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="column_rename",
        params={"columnKey": "COL_0", "displayName": "Vendor"},
    )
    assert rename.status_code == 200
    assert rename.get_json()["columns"][0]["displayName"] == "Vendor"

    type_change = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="column_change_type",
        params={"columnKey": "COL_1", "newType": "DOUBLE"},
    )
    assert type_change.status_code == 200
    columns = {col["key"]: col for col in type_change.get_json()["columns"]}
    assert columns["COL_1"]["dataType"] == "DOUBLE"

    reorder = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="column_reorder",
        params={"columnKeys": ["COL_2", "COL_1", "COL_0"]},
    )
    assert reorder.status_code == 200
    assert [col["key"] for col in reorder.get_json()["columns"]] == ["COL_2", "COL_1", "COL_0"]

    delete = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="column_delete",
        params={"columnKey": "COL_2"},
    )
    assert delete.status_code == 200
    assert [col["key"] for col in delete.get_json()["columns"]] == ["COL_1", "COL_0"]


def test_calculated_column_requires_metadata_column_keys(playground_session):
    session_id, app = playground_session
    rejected = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="calculated_column",
        params={"name": "Bad", "expression": "[AMOUNT] + 1", "dataType": "DOUBLE"},
    )
    assert rejected.status_code == 400

    accepted = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="calculated_column",
        params={"name": "Adjusted", "expression": "[COL_1] + 5", "dataType": "DOUBLE"},
    )
    assert accepted.status_code == 200
    body = accepted.get_json()
    assert body["columns"][-1]["displayName"] == "Adjusted"
    assert body["rows"][0]["values"][body["columns"][-1]["key"]] == 25.0


def test_invalid_calculated_column_does_not_create_playground_state(playground_session):
    session_id, app = playground_session
    rejected = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="calculated_column",
        params={"name": "Bad", "expression": '"AMOUNT" + 1', "dataType": "DOUBLE"},
    )
    assert rejected.status_code == 400

    state = _post(app, "/api/preview/state", session_id)
    assert state.status_code == 200
    body = state.get_json()
    assert body["dirty"] is False
    assert body["undoDepth"] == 0
    assert body["redoDepth"] == 0
    assert body["playgroundTableName"] is None


def test_failed_calculated_column_rolls_back_working_state(playground_session):
    session_id, app = playground_session
    rejected = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="calculated_column",
        params={
            "name": "Bad Later Rows",
            "expression": "CASE WHEN [COL_1] = 20 THEN 1 ELSE CAST(CONCAT('bad', [COL_1]) AS INTEGER) END",
            "dataType": "INTEGER",
        },
    )
    assert rejected.status_code == 400
    assert rejected.get_json()["code"] == "VALIDATION_ERROR"

    state = _post(app, "/api/preview/state", session_id)
    assert state.status_code == 200
    body = state.get_json()
    assert body["dirty"] is False
    assert body["undoDepth"] == 0
    assert body["redoDepth"] == 0
    assert body["playgroundTableName"] is None
    assert [col["displayName"] for col in body["columns"]] == ["Supplier", "Amount", "Region"]

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        leaked = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "AND (table_name LIKE 'pg__snap__%' OR table_name LIKE 'pg__sample_csv__%')"
        ).fetchall()

    assert leaked == []


def test_missing_row_mutations_do_not_dirty_playground_state(playground_session):
    session_id, app = playground_session
    edit = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="cell_edit",
        params={"rowId": "999", "columnKey": "COL_1", "value": "25"},
    )
    assert edit.status_code == 400

    delete = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="rows_delete",
        params={"rowIds": ["999"]},
    )
    assert delete.status_code == 400

    state = _post(app, "/api/preview/state", session_id)
    assert state.status_code == 200
    body = state.get_json()
    assert body["dirty"] is False
    assert body["undoDepth"] == 0
    assert body["playgroundTableName"] is None


def test_rows_delete_cap_is_enforced(playground_session):
    session_id, app = playground_session
    response = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="rows_delete",
        params={"rowIds": [str(i) for i in range(5001)]},
    )
    assert response.status_code == 400
    assert response.get_json()["code"] == "VALIDATION_ERROR"


def test_pivot_is_playground_only_and_refresh_inventory_reports_it(playground_session):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_0"],
            "columnFields": ["COL_2"],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )
    assert pivot.status_code == 200
    body = pivot.get_json()
    new_table_key = body["newTableKey"]
    assert new_table_key.startswith("playground::pivot::")
    assert body["undoDepth"] == 1

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        registry_row = conn.execute(
            "SELECT COUNT(*) FROM _table_registry WHERE table_key = ?",
            (new_table_key,),
        ).fetchone()[0]
    assert registry_row == 0

    with app.test_client() as client:
        refresh = client.post("/api/preview/refresh-inventory", json={"sessionId": session_id})
    assert refresh.status_code == 200
    refresh_body = refresh.get_json()
    assert refresh_body["ok"] is True
    assert any(item["table_key"] == new_table_key and item["playgroundOnly"] for item in refresh_body["fileInventory"])


def test_pivot_undo_drops_created_table_and_redo_restores_inventory(playground_session):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_2"],
            "columnFields": [],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )
    assert pivot.status_code == 200
    pivot_body = pivot.get_json()
    pivot_key = pivot_body["newTableKey"]
    pivot_table = pivot_body["playgroundTableName"]

    with app.test_client() as client:
        undo = client.post("/api/preview/undo", json={"sessionId": session_id, "tableKey": pivot_key})
    assert undo.status_code == 200
    assert undo.get_json()["dropped"] is True

    with app.test_client() as client:
        refresh = client.post("/api/preview/refresh-inventory", json={"sessionId": session_id})
    assert refresh.status_code == 200
    keys_after_undo = {item["table_key"] for item in refresh.get_json()["fileInventory"]}
    assert pivot_key not in keys_after_undo

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        assert not playground_service._table_exists(conn, pivot_table)

    with app.test_client() as client:
        redo = client.post("/api/preview/redo", json={"sessionId": session_id, "tableKey": pivot_key})
    assert redo.status_code == 200
    assert redo.get_json()["ok"] is True

    with app.test_client() as client:
        refresh = client.post("/api/preview/refresh-inventory", json={"sessionId": session_id})
    assert refresh.status_code == 200
    keys_after_redo = {item["table_key"] for item in refresh.get_json()["fileInventory"]}
    assert pivot_key in keys_after_redo

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        assert playground_service._table_exists(conn, pivot_table)


def test_pivot_rejects_noncanonical_value_field_alias(playground_session):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_0"],
            "columnFields": ["COL_2"],
            "valueFields": [{"field": "COL_1", "aggregation": "sum"}],
        },
    )

    assert pivot.status_code == 400
    assert pivot.get_json()["code"] == "VALIDATION_ERROR"


@pytest.mark.parametrize(
    ("row_fields", "value_fields", "expected_field"),
    [
        (["COL_0"] * 6, [{"columnKey": "COL_1", "aggregation": "sum"}], "rowFields"),
        ([], [{"columnKey": "COL_1", "aggregation": "count"}] * 6, "valueFields"),
    ],
)
def test_pivot_rejects_row_and_value_fields_above_five(
    playground_session,
    row_fields,
    value_fields,
    expected_field,
):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": row_fields,
            "columnFields": [],
            "valueFields": value_fields,
        },
    )

    assert pivot.status_code == 400
    body = pivot.get_json()
    assert body["code"] == "VALIDATION_ERROR"
    assert body["details"]["field"] == expected_field


def test_pivot_rejects_unsupported_aggregation(playground_session):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": [],
            "columnFields": [],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "median"}],
        },
    )

    assert pivot.status_code == 400
    assert pivot.get_json()["code"] == "VALIDATION_ERROR"


def test_pivot_allows_aggregate_only_but_requires_rows_for_column_fields(playground_session):
    session_id, app = playground_session
    aggregate_only = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": [],
            "columnFields": [],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )
    with_columns = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": [],
            "columnFields": ["COL_2"],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )

    assert aggregate_only.status_code == 200
    assert with_columns.status_code == 400
    assert with_columns.get_json()["code"] == "VALIDATION_ERROR"


def test_pivot_supports_multiple_value_fields(playground_session):
    session_id, app = playground_session
    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_0"],
            "columnFields": ["COL_2"],
            "valueFields": [
                {"columnKey": "COL_1", "aggregation": "sum"},
                {"columnKey": "COL_1", "aggregation": "count"},
            ],
        },
    )

    assert pivot.status_code == 200
    body = pivot.get_json()
    assert body["newTableKey"].startswith("playground::pivot::")
    assert len(body["columns"]) >= 3


def test_pivot_rejects_high_cardinality_row_fields_before_materializing(playground_session, monkeypatch):
    session_id, app = playground_session
    monkeypatch.setattr(playground_service, "MAX_PIVOT_OUTPUT_ROWS", 2)

    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_0"],
            "columnFields": [],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )

    assert pivot.status_code == 400
    body = pivot.get_json()
    assert body["code"] == "VALIDATION_ERROR"
    assert body["details"]["maxRows"] == 2

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        row = conn.execute("SELECT COUNT(*) FROM _playground_registry").fetchone()
        pivot_tables = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name LIKE 'pg__pivot__%'"
        ).fetchone()

    assert int(row[0] or 0) == 0
    assert int(pivot_tables[0] or 0) == 0


def test_pivot_failure_after_materialization_cleans_partial_artifacts(playground_session, monkeypatch):
    session_id, app = playground_session
    original_replace = playground_service._replace_playground_metadata

    def fail_for_pivot(conn, table_key, metadata):
        if str(table_key).startswith("playground::pivot::"):
            raise RuntimeError("metadata write failed")
        return original_replace(conn, table_key, metadata)

    monkeypatch.setattr(playground_service, "_replace_playground_metadata", fail_for_pivot)

    pivot = _post(
        app,
        "/api/preview/operation",
        session_id,
        op="pivot",
        params={
            "rowFields": ["COL_2"],
            "columnFields": [],
            "valueFields": [{"columnKey": "COL_1", "aggregation": "sum"}],
        },
    )

    assert pivot.status_code == 500

    with db.get_session_lock(session_id):
        conn = db.get_session_db(session_id)
        registry_rows = conn.execute("SELECT COUNT(*) FROM _playground_registry").fetchone()
        column_rows = conn.execute("SELECT COUNT(*) FROM _playground_columns").fetchone()
        leaked_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "AND (table_name LIKE 'pg__pivot__%' OR table_name LIKE 'pg__pivot_tmp__%')"
        ).fetchall()

    assert int(registry_rows[0] or 0) == 0
    assert int(column_rows[0] or 0) == 0
    assert leaked_tables == []
