"""Focused regression coverage for the reserved append-provenance boundary."""

from __future__ import annotations

import uuid

import pytest

import appending.service as append_service
from appending.service import run_append_execute, run_append_mapping
from merging.guided_merge_service import skip_merge
from routes.header_normalisation_routes import _hn_service
from shared.db import (
    filter_data_columns,
    get_meta,
    get_session_db,
    is_reserved_provenance_column,
    read_physical_table_columns,
    read_table,
    read_table_columns,
    register_table,
    sanitize_reserved_provenance_columns,
    set_meta,
    store_table,
)


def _conn():
    return get_session_db(uuid.uuid4().hex)


@pytest.mark.parametrize(
    "name",
    ["_source_table", "__SOURCE-TABLE", "Source Table", "source.table"],
)
def test_exact_normalized_reserved_aliases(name):
    assert is_reserved_provenance_column(name)


@pytest.mark.parametrize(
    "name",
    ["Data Source System", "source_system", "data_source", "source_table_name"],
)
def test_legitimate_source_fields_are_retained(name):
    assert not is_reserved_provenance_column(name)


def test_store_and_public_reads_exclude_reserved_columns():
    conn = _conn()
    store_table(
        conn,
        "tbl__upload",
        [{"Name": "A", "_source_table": "file-a", "Data Source System": "SAP"}],
    )

    assert read_physical_table_columns(conn, "tbl__upload") == ["Name", "Data Source System"]
    assert read_table_columns(conn, "tbl__upload") == ["Name", "Data Source System"]
    assert read_table(conn, "tbl__upload") == [{"Name": "A", "Data Source System": "SAP"}]


def test_transactional_legacy_cleanup_rewrites_and_safely_drops_only_reserved():
    conn = _conn()
    conn.execute('CREATE TABLE "tbl__legacy" ("Name" VARCHAR, "__source-table" VARCHAR, "__row_id" BIGINT)')
    conn.execute('INSERT INTO "tbl__legacy" VALUES (\'A\', \'file-a\', 7)')
    conn.execute('CREATE TABLE "hn__only_reserved" ("source table" VARCHAR)')
    conn.execute('INSERT INTO "hn__only_reserved" VALUES (\'file-a\')')
    register_table(conn, "legacy", "tbl__legacy")
    register_table(conn, "only", "hn__only_reserved")
    set_meta(conn, "groupSchemaTableRows", [
        {"group_id": "legacy", "columns": ["Name", "_source_table"],
         "cols": 2, "columns_preview": "Name, _source_table"},
        {"group_id": "only", "columns": ["source table"],
         "cols": 1, "columns_preview": "source table"},
    ])
    set_meta(conn, "appendReport", [{
        "group_id": "only", "provenance": {"kind": "append_contribution"}
    }])
    set_meta(conn, "previews", {
        "legacy": {"columns": ["Name", "source table"], "rows": [{"Name": "A", "source_table": "file-a"}]},
        "only": {"columns": ["source_table"], "rows": [{"source_table": "file-a"}]},
    })
    set_meta(conn, "inv", [
        {"table_key": "legacy", "rows": 999, "cols": 99},
        {"table_key": "only", "rows": 1, "cols": 1},
    ])
    set_meta(conn, "filesPayload", [
        {"table_key": "legacy", "n_cols": 99, "columns": ["Name", "_source_table"]},
        {"table_key": "only", "n_cols": 1, "columns": ["source table"]},
    ])

    result = sanitize_reserved_provenance_columns(conn)
    again = sanitize_reserved_provenance_columns(conn)

    assert result["removedColumns"] == 2
    assert read_physical_table_columns(conn, "tbl__legacy") == ["Name", "__row_id"]
    assert read_table(conn, "tbl__legacy") == [{"Name": "A", "__row_id": 7}]
    assert read_physical_table_columns(conn, "hn__only_reserved") == []
    assert get_meta(conn, "groupSchemaTableRows") == [{
        "group_id": "legacy", "columns": ["Name"], "cols": 1, "columns_preview": "Name"
    }]
    assert get_meta(conn, "appendReport") == [{
        "group_id": "only", "provenance": {"kind": "append_contribution"}
    }]
    assert get_meta(conn, "previews") == {"legacy": {"columns": ["Name"], "rows": [{"Name": "A"}]}}
    assert get_meta(conn, "inv") == [{"table_key": "legacy", "rows": 1, "cols": 1}]
    assert get_meta(conn, "filesPayload") == [{
        "table_key": "legacy", "n_cols": 1, "columns": ["Name"]
    }]
    assert again["removedColumns"] == 0


def test_grouped_append_has_public_shape_and_retains_audit_provenance():
    conn = _conn()
    store_table(conn, "tbl__a", [{"Name": "A", "Data Source System": "SAP"}])
    store_table(conn, "tbl__b", [{"Name": "B", "Data Source System": "Oracle"}])
    register_table(conn, "a", "tbl__a")
    register_table(conn, "b", "tbl__b")
    set_meta(conn, "appendGroups", [{"group_id": "g1", "group_name": "Group"}])

    result = run_append_execute(
        conn,
        [{
            "group_id": "g1",
            "canonical_schema": ["Name", "Data Source System", "_source_table"],
            "per_table": [
                {"table_key": "a", "column_mapping": {"Name": "Name", "Data Source System": "Data Source System"}},
                {"table_key": "b", "column_mapping": {"Name": "Name", "Data Source System": "Data Source System"}},
            ],
        }],
        [],
    )

    assert read_physical_table_columns(conn, "appended__g1") == [
        "Name", "Data Source System", "__row_id",
    ]
    assert [
        row["__row_id"] for row in read_table(conn, "appended__g1")
    ] == [1, 2]
    assert result["groupSchema"][0]["cols"] == 2
    report = result["appendReport"][0]
    assert report["total_cols"] == 2
    assert report["provenance"]["tables"] == {"a": 1, "b": 1}


def test_standalone_append_projects_public_columns_and_retains_audit_provenance():
    conn = _conn()
    conn.execute(
        'CREATE TABLE "tbl__standalone" '
        '("Name" VARCHAR, "source table" VARCHAR, "source_system" VARCHAR)'
    )
    conn.execute("INSERT INTO \"tbl__standalone\" VALUES ('A', 'file-a', 'SAP')")
    register_table(conn, "standalone", "tbl__standalone")

    result = run_append_execute(conn, [], ["standalone"])

    assert read_physical_table_columns(conn, "appended__standalone") == [
        "Name", "source_system", "__row_id",
    ]
    assert read_table(conn, "appended__standalone") == [
        {"Name": "A", "source_system": "SAP", "__row_id": 1},
    ]
    report = result["appendReport"][0]
    assert report["is_standalone"] is True
    assert report["total_cols"] == 2
    assert report["provenance"]["tables"] == {"standalone": 1}


def test_single_table_append_mapping_excludes_row_id():
    """Single-table identity maps must omit internal __row_id."""
    conn = _conn()
    store_table(
        conn,
        "tbl__smc",
        [{"Name": "A", "Amount": "10", "__row_id": 1}],
    )
    register_table(conn, "SMC Data 2025.xlsx::Base", "tbl__smc")

    mapped = run_append_mapping(
        conn,
        [{"group_id": "g1", "tables": ["SMC Data 2025.xlsx::Base"]}],
        api_key=None,
    )
    group = mapped["appendGroupMappings"][0]
    assert group["canonical_schema"] == ["Name", "Amount"]
    assert group["per_table"][0]["column_mapping"] == {"Name": "Name", "Amount": "Amount"}
    assert "__row_id" not in group["canonical_schema"]
    assert "__row_id" not in group["per_table"][0]["column_mapping"]


def test_append_execute_ignores_stale_row_id_mapping_target():
    """Stale mappings that still target __row_id must not fail execute."""
    conn = _conn()
    store_table(
        conn,
        "tbl__smc",
        [{"Name": "A", "Amount": "10", "__row_id": 99}],
    )
    register_table(conn, "SMC Data 2025.xlsx::Base", "tbl__smc")
    set_meta(conn, "appendGroups", [{"group_id": "g1", "group_name": "SMC"}])

    result = run_append_execute(
        conn,
        [{
            "group_id": "g1",
            "canonical_schema": ["Name", "Amount", "__row_id"],
            "per_table": [{
                "table_key": "SMC Data 2025.xlsx::Base",
                "column_mapping": {
                    "Name": "Name",
                    "Amount": "Amount",
                    "__row_id": "__row_id",
                },
            }],
        }],
        [],
    )

    assert read_physical_table_columns(conn, "appended__g1") == [
        "Name", "Amount", "__row_id",
    ]
    assert read_table(conn, "appended__g1") == [
        {"Name": "A", "Amount": "10", "__row_id": 1},
    ]
    assert result["groupSchema"][0]["cols"] == 2
    assert result["groupSchema"][0]["columns"] == ["Name", "Amount"]


def test_header_apply_ignores_stale_reserved_decision_and_validates_duplicates():
    conn = _conn()
    store_table(conn, "tbl__headers", [{"Original": "A", "Amount": "10"}])
    register_table(conn, "headers", "tbl__headers")

    applied = _hn_service.apply_header_norm(conn, {
        "headers": [
            {"source_col": "Original", "action": "KEEP"},
            {"source_col": "Amount", "action": "AUTO", "mapped_to": "Value"},
            {"source_col": "_source_table", "action": "KEEP"},
        ]
    })
    assert applied["appliedTables"][0]["kept"] == 1
    assert filter_data_columns(read_table_columns(conn, "hn__headers")) == [
        "Original", "Value",
    ]
    assert read_table(conn, "hn__headers")[0]["__row_id"] == 1

    with pytest.raises(ValueError, match="Duplicate output column"):
        _hn_service.apply_header_norm(conn, {
            "headers": [
                {"source_col": "Original", "action": "AUTO", "mapped_to": "Same"},
                {"source_col": "Amount", "action": "AUTO", "mapped_to": "same"},
            ]
        })


def test_header_apply_prevalidates_all_tables_before_mutation(monkeypatch):
    conn = _conn()
    store_table(conn, "tbl__one", [{"A": "old"}])
    store_table(conn, "tbl__two", [{"B": "value", "C": "required"}])
    register_table(conn, "one", "tbl__one")
    register_table(conn, "two", "tbl__two")
    conn.execute('CREATE TABLE "hn__one" ("A" VARCHAR)')
    conn.execute('INSERT INTO "hn__one" VALUES (\'existing\')')
    conn.commit()
    learned = []
    monkeypatch.setattr(_hn_service, "alias_add", lambda *args: learned.append(args))

    with pytest.raises(ValueError, match="Incomplete decisions"):
        _hn_service.apply_header_norm(conn, {
            "one": [{"source_col": "A", "action": "AUTO", "mapped_to": "Renamed"}],
            "two": [{"source_col": "B", "action": "KEEP"}],
        })

    assert read_table(conn, "hn__one") == [{"A": "existing"}]
    assert learned == []


@pytest.mark.parametrize(
    "reserved_output",
    ["__row_id", "_source_table", "Source-Table"],
)
def test_header_apply_rejects_reserved_outputs_atomically(
    monkeypatch,
    reserved_output,
):
    conn = _conn()
    store_table(conn, "tbl__reserved_output", [{"Original": "A", "Amount": "10"}])
    register_table(conn, "reserved-output", "tbl__reserved_output")
    conn.execute('CREATE TABLE "hn__reserved_output" ("Existing" VARCHAR)')
    conn.execute("INSERT INTO \"hn__reserved_output\" VALUES ('unchanged')")
    conn.commit()
    set_meta(conn, "headerNormApplied", {"sentinel": True})
    learned = []
    monkeypatch.setattr(_hn_service, "alias_add", lambda *args: learned.append(args))

    with pytest.raises(ValueError, match="Reserved output column"):
        _hn_service.apply_header_norm(conn, {
            "reserved-output": [
                {
                    "source_col": "Original",
                    "action": "AUTO",
                    "mapped_to": reserved_output,
                    "user_edited": True,
                },
                {"source_col": "Amount", "action": "KEEP"},
            ],
        })

    assert read_table(conn, "hn__reserved_output") == [{"Existing": "unchanged"}]
    assert get_meta(conn, "headerNormApplied") == {"sentinel": True}
    assert read_table(conn, "tbl__reserved_output") == [
        {"Original": "A", "Amount": "10"},
    ]
    assert learned == []


def test_append_validation_failure_preserves_existing_output():
    conn = _conn()
    store_table(conn, "tbl__source", [{"A": "new"}])
    register_table(conn, "source", "tbl__source")
    store_table(conn, "appended__g1", [{"A": "existing"}])

    with pytest.raises(ValueError, match="unique"):
        run_append_execute(conn, [{
            "group_id": "g1",
            "canonical_schema": ["A", "a"],
            "per_table": [{"table_key": "source", "column_mapping": {"A": "A"}}],
        }], [])

    assert read_table(conn, "appended__g1") == [{"A": "existing"}]


def test_append_swap_failure_rolls_back_existing_output(monkeypatch):
    conn = _conn()
    store_table(conn, "tbl__source", [{"A": "new"}])
    register_table(conn, "source", "tbl__source")
    store_table(conn, "appended__g1", [{"A": "existing"}])

    def fail_registration(*args, **kwargs):
        raise RuntimeError("injected registry failure")

    monkeypatch.setattr(append_service, "register_table", fail_registration)
    with pytest.raises(RuntimeError, match="injected"):
        run_append_execute(conn, [{
            "group_id": "g1", "canonical_schema": ["A"],
            "per_table": [{"table_key": "source", "column_mapping": {"A": "A"}}],
        }], [])

    assert read_table(conn, "appended__g1") == [{"A": "existing"}]


def test_append_second_group_swap_failure_rolls_back_every_output(monkeypatch):
    conn = _conn()
    for key, value in (("source1", "new-1"), ("source2", "new-2")):
        store_table(conn, f"tbl__{key}", [{"A": value}])
        register_table(conn, key, f"tbl__{key}")
    store_table(conn, "appended__g1", [{"A": "old-1"}])
    store_table(conn, "appended__g2", [{"A": "old-2"}])

    real_register = append_service.register_table
    calls = 0

    def fail_second_registration(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected late second-group failure")
        return real_register(*args, **kwargs)

    monkeypatch.setattr(append_service, "register_table", fail_second_registration)
    with pytest.raises(RuntimeError, match="late second-group"):
        run_append_execute(conn, [
            {"group_id": "g1", "canonical_schema": ["A"], "per_table": [
                {"table_key": "source1", "column_mapping": {"A": "A"}},
            ]},
            {"group_id": "g2", "canonical_schema": ["A"], "per_table": [
                {"table_key": "source2", "column_mapping": {"A": "A"}},
            ]},
        ], [])

    assert read_table(conn, "appended__g1") == [{"A": "old-1"}]
    assert read_table(conn, "appended__g2") == [{"A": "old-2"}]


def test_header_apply_is_repeatable_against_the_original_registered_source():
    conn = _conn()
    store_table(conn, "tbl__repeat", [{"Original": "A", "Amount": "10"}])
    register_table(conn, "repeat", "tbl__repeat")
    decisions = {
        "repeat": [
            {"source_col": "Original", "action": "KEEP"},
            {"source_col": "Amount", "action": "AUTO", "mapped_to": "Value"},
        ]
    }

    first = _hn_service.apply_header_norm(conn, decisions)
    second = _hn_service.apply_header_norm(conn, decisions)

    assert first["appliedTables"] == second["appliedTables"]
    assert read_table(conn, "hn__repeat") == [
        {"Original": "A", "Value": "10", "__row_id": 1},
    ]


def test_skip_merge_creates_public_version_and_final_tables():
    conn = _conn()
    conn.execute(
        'CREATE TABLE "appended__base" '
        '("Name" VARCHAR, "_source_table" VARCHAR, "Data Source System" VARCHAR)'
    )
    conn.execute("INSERT INTO \"appended__base\" VALUES ('A', 'file-a', 'SAP')")
    register_table(conn, "base", "appended__base")

    result = skip_merge(conn, "test-session", "base")

    assert result["columns"] == ["Name", "Data Source System"]
    assert read_physical_table_columns(conn, "final_merged_v1") == [
        "Name", "Data Source System", "__row_id",
    ]
    assert read_physical_table_columns(conn, "final_merged") == [
        "Name", "Data Source System", "__row_id",
    ]
