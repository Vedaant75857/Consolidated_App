from __future__ import annotations

from hashlib import sha256
import io
import zipfile

import openpyxl

from backend.module1.ingestion_adapter import (
    Module1AdapterConfig,
    Module1IngestionAdapter,
    AdapterParityEvidence,
    compare_parity,
    run_module1_shared,
)


def _connection():
    from shared.db import duckdb_connect

    conn = duckdb_connect(":memory:")
    conn.execute("CREATE TABLE meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
    conn.execute("CREATE TABLE table_registry (table_key VARCHAR PRIMARY KEY, sql_name VARCHAR NOT NULL)")
    conn.commit()
    return conn


def test_default_adapter_uses_legacy_only(monkeypatch):
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        if on_progress:
            on_progress({"stage": "legacy"})
        return ([], [{"file": "x"}])

    def shadow(payload):
        calls.append("shadow")
        return ([], [{"file": "x"}])

    events: list[dict] = []
    adapter = Module1IngestionAdapter(legacy_loader=legacy, shadow_loader=shadow)
    result = adapter.load_zip_to_session(object(), b"data", on_progress=events.append)

    assert result == ([], [{"file": "x"}])
    assert calls == ["legacy"]
    assert events == [{"stage": "legacy"}]
    assert adapter.last_report is None


def test_shadow_is_observational_and_order_independent():
    def legacy(conn, payload, on_progress=None):
        return {"warnings": [{"message": "ok", "file": "x"}], "inventory": {"b": 2, "a": 1}}

    def shadow(payload):
        return {"inventory": {"a": 1, "b": 2}, "warnings": [{"file": "x", "message": "ok"}]}

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, parity_gate_enabled=True),
    )
    result = adapter.load_zip_to_session(object(), b"data")

    assert result["inventory"] == {"b": 2, "a": 1}
    assert adapter.last_report is not None
    assert adapter.last_report.matched
    assert adapter.last_report.gate_passed


def test_shadow_mismatch_never_changes_legacy_result():
    def legacy(conn, payload, on_progress=None):
        return "legacy-result"

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=lambda payload: "shared-result",
        config=Module1AdapterConfig(shadow_enabled=True, parity_gate_enabled=True),
    )
    assert adapter.load_zip_to_session(object(), b"data") == "legacy-result"
    assert adapter.last_report is not None
    assert not adapter.last_report.matched
    assert not adapter.last_report.gate_passed


def test_parity_gate_requires_explicit_enablement():
    report = compare_parity({"x": 1}, {"x": 1})
    assert report.matched
    assert not report.gate_passed


def test_cutover_is_blocked_without_explicit_cutover_flag():
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ({}, [])

    def shadow(payload):
        calls.append("shadow")
        return ("same", [])

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"data")
    adapter.load_zip_to_session(object(), b"data")
    assert calls == ["shadow", "legacy", "shadow", "legacy"]


def test_cutover_requires_real_tuple_postconditions(monkeypatch):
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "all_registered_tables", lambda conn: [{"table_key": "x"}])
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ({}, [])

    def shared(payload):
        calls.append("shared")
        return AdapterParityEvidence(({}, []), sha256(payload).hexdigest(), "ingestion.v1")

    def typed(payload):
        calls.append("typed")
        return ({}, [])

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=shared,
        cutover_loader=typed,
        config=Module1AdapterConfig(
            shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True,
        ),
    )
    # Evidence is established by the first legacy invocation.
    assert adapter.load_zip_to_session(object(), b"data") == ({}, [])
    assert calls == ["shared", "legacy"]
    # A matching digest alone is insufficient: the live connection has no
    # published registry/rawArray postconditions, so cut-over stays blocked.
    assert adapter.load_zip_to_session(object(), b"data") == ({}, [])
    assert calls == ["shared", "legacy", "shared", "legacy"]


def test_connection_aware_cutover_requires_real_publication(monkeypatch):
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "all_registered_tables", lambda conn: [{"table_key": "x"}])
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ({}, [])

    def isolated_shadow(payload):
        calls.append("shadow")
        return AdapterParityEvidence(({}, []), sha256(payload).hexdigest(), "ingestion.v1")

    def typed_runner(conn, payload):
        calls.append("typed")
        return ("typed", [])

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=isolated_shadow,
        cutover_loader=typed_runner,
        config=Module1AdapterConfig(
            shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True,
        ),
    )
    adapter.load_zip_to_session(object(), b"data")
    assert adapter.load_zip_to_session(object(), b"data") == ({}, [])
    assert calls == ["shadow", "legacy", "shadow", "legacy"]


def test_shadow_runner_must_be_isolated_when_it_accepts_connection():
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return "legacy"

    def unsafe_shadow(conn, payload):
        calls.append("unsafe-shadow")
        return "legacy"

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy,
        shadow_loader=unsafe_shadow,
        config=Module1AdapterConfig(shadow_enabled=True),
    )
    assert adapter.load_zip_to_session(object(), b"data") == "legacy"
    assert calls == ["legacy"]
    assert adapter.last_report is None


def test_parity_evidence_is_bound_to_current_payload():
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return (payload, [])

    def shadow(payload):
        calls.append("shadow")
        return (payload, [])

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"file-a")
    # A different payload cannot reuse A's matching report for cut-over.
    adapter.load_zip_to_session(object(), b"file-b")
    assert calls == ["shadow", "legacy", "shadow", "legacy"]
    assert adapter.last_report is not None
    assert adapter.last_report.payload_sha256 == sha256(b"file-b").hexdigest()


def test_non_tuple_shared_result_cannot_unlock_legacy_route():
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ("legacy", [])

    def shared(payload):
        calls.append("shared")
        return {"status": "success", "tables": []}

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=shared,
        config=Module1AdapterConfig(shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"data")
    adapter.load_zip_to_session(object(), b"data")
    assert calls == ["shared", "legacy", "shared", "legacy"]
    assert adapter.last_report is not None
    assert not adapter.last_report.adapter_compatible


def test_explicit_adapter_evidence_carries_contract_version():
    payload = b"data"

    def legacy(conn, data, on_progress=None):
        return ("ok", [])

    def shadow(data):
        return AdapterParityEvidence(
            result=("ok", []),
            payload_sha256=sha256(data).hexdigest(),
            contract_version="ingestion.v1",
        )

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), payload)
    assert adapter.last_report is not None
    assert adapter.last_report.contract_version == "ingestion.v1"


def test_old_contract_evidence_cannot_unlock_cutover(monkeypatch):
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "all_registered_tables", lambda conn: [{"table_key": "x"}])
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ("ok", [])

    def shadow(payload):
        calls.append("shadow")
        return AdapterParityEvidence(("ok", []), sha256(payload).hexdigest(), "ingestion.v0")

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"data")
    adapter.load_zip_to_session(object(), b"data")
    assert calls == ["shadow", "legacy", "shadow", "legacy"]


def test_empty_session_postcondition_cannot_unlock_cutover(monkeypatch):
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "all_registered_tables", lambda conn: [])
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ("ok", [])

    def shadow(payload):
        calls.append("shadow")
        return AdapterParityEvidence(("ok", []), sha256(payload).hexdigest(), "ingestion.v1")

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=shadow,
        config=Module1AdapterConfig(shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"data")
    adapter.load_zip_to_session(object(), b"data")
    assert calls == ["shadow", "legacy", "shadow", "legacy"]


def test_fake_two_tuple_without_explicit_evidence_cannot_unlock_cutover(monkeypatch):
    import shared.db as shared_db
    monkeypatch.setattr(shared_db, "all_registered_tables", lambda conn: [{"table_key": "x"}])
    calls: list[str] = []

    def legacy(conn, payload, on_progress=None):
        calls.append("legacy")
        return ({}, [])

    def fake_shadow(payload):
        calls.append("shadow")
        return ({}, [])

    def typed(payload):
        calls.append("typed")
        return ({}, [])

    adapter = Module1IngestionAdapter(
        legacy_loader=legacy, shadow_loader=fake_shadow, cutover_loader=typed,
        config=Module1AdapterConfig(shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True),
    )
    adapter.load_zip_to_session(object(), b"data")
    adapter.load_zip_to_session(object(), b"data")
    assert calls == ["shadow", "legacy", "shadow", "legacy"]
    assert adapter.last_report is not None
    assert not adapter.last_report.adapter_compatible


def test_shared_csv_publishes_legacy_registry_raw_array_and_sse():
    from shared.db import get_meta

    conn = _connection()
    events: list[dict] = []
    result = run_module1_shared(conn, b"Name,Amount\nAlice,10\n", events.append)

    assert result == ({}, [])
    assert [event["stage"] for event in events] == ["zip_info", "file_loaded", "committed"]
    assert events[0]["total"] == 1
    assert events[-1]["file_count"] == 1
    assert conn.execute("SELECT table_key, sql_name FROM table_registry").fetchone()["table_key"] == "upload.csv::"
    assert [tuple(row) for row in conn.execute('SELECT * FROM "raw__upload_csv__"').fetchall()] == [("Name", "Amount"), ("Alice", "10")]
    assert get_meta(conn, "rawArray__upload.csv::") == [["Name", "Amount"], ["Alice", "10"]]


def test_shared_zip_and_direct_xlsx_preserve_file_and_sheet_keys():
    workbook = io.BytesIO()
    book = openpyxl.Workbook()
    book.active.title = "Sheet A"
    book.active.append(["Value"])
    book.active.append([7])
    book.save(workbook)

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("rows.csv", "A,B\n1,2\n")
        zipped.writestr("book.xlsx", workbook.getvalue())
    conn = _connection()
    events: list[dict] = []
    assert run_module1_shared(conn, archive.getvalue(), events.append) == ({}, [])
    keys = [row["table_key"] for row in conn.execute("SELECT table_key FROM table_registry ORDER BY table_key").fetchall()]
    assert keys == ["book.xlsx::Sheet A", "rows.csv::"]
    assert events[0] == {"stage": "zip_info", "total": 2}
    assert events[-1] == {"stage": "committed", "file_count": 2}

    # A bytes-only workbook has no filename for the detector.  OOXML markers
    # still route it through ExcelReader and produce the same Module 1 key.
    direct = _connection()
    assert run_module1_shared(direct, workbook.getvalue()) == ({}, [])
    assert direct.execute("SELECT table_key FROM table_registry").fetchone()["table_key"] == "upload.xlsx::Sheet A"


def test_nested_archive_identity_and_recursive_sse_total():
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zipped:
        zipped.writestr("rows.csv", "A\n1\n")
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("inner.zip", inner.getvalue())
        zipped.writestr("rows.csv", "A\n2\n")
    conn = _connection()
    events: list[dict] = []
    assert run_module1_shared(conn, archive.getvalue(), events.append) == ({}, [])
    keys = [row["table_key"] for row in conn.execute("SELECT table_key FROM table_registry ORDER BY table_key").fetchall()]
    assert keys == ["rows.csv::", "upload.zip/inner.zip/rows.csv::"]
    assert events[0] == {"stage": "zip_info", "total": 2}
    assert events[-1] == {"stage": "committed", "file_count": 2}


def test_raw_array_preview_bound_is_runtime_configurable(monkeypatch):
    from shared.db import get_meta

    monkeypatch.setenv("RAW_META_PREVIEW_ROWS", "1")
    conn = _connection()
    assert run_module1_shared(conn, b"A\n1\n2\n", None) == ({}, [])
    assert get_meta(conn, "rawArray__upload.csv::") == [["A"]]


def test_shared_publication_failure_rolls_back_registry_raw_and_meta(monkeypatch):
    import data_loading.file_loader as file_loader
    from shared.db import all_registered_tables

    def fail_rebuild(*args, **kwargs):
        raise RuntimeError("synthetic publication failure")

    monkeypatch.setattr(file_loader, "rebuild_table_from_raw_table", fail_rebuild)
    conn = _connection()
    events: list[dict] = []
    _raw, warnings = run_module1_shared(conn, b"A\n1\n", events.append)

    assert warnings and "synthetic publication failure" in warnings[-1]["message"]
    assert all_registered_tables(conn) == []
    assert conn.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'raw__%'").fetchall() == []
    assert events[-1] == {"stage": "committed", "file_count": 0}
