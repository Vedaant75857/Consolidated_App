import os
import sys
import importlib
import tempfile

import pytest
from flask import Flask

MODULE3_DIR = os.path.dirname(os.path.dirname(__file__))
BACKEND_DIR = os.path.dirname(MODULE3_DIR)
for path in (MODULE3_DIR, BACKEND_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

from ingestion_adapter import (  # noqa: E402
    CutoverGate,
    Module3AdapterConfig,
    Module3IngestionAdapter,
    compare_parity,
)


def test_adapter_defaults_to_legacy_and_preserves_upload_tuple_shape():
    calls = []

    def legacy(conn, filename, data):
        calls.append((filename, data))
        return (["spend.csv::"], [{"message": "legacy"}])

    adapter = Module3IngestionAdapter(legacy_single_loader=legacy)
    result = adapter.load_single_file(object(), "spend.csv", b"a,b\n1,2\n")

    assert result == (["spend.csv::"], [{"message": "legacy"}])
    assert calls == [("spend.csv", b"a,b\n1,2\n")]
    assert adapter.last_report is None


def test_cutover_requires_matching_parity_and_both_predecessor_gates():
    calls = []
    connections = []

    def legacy(conn, filename, data):
        calls.append("legacy")
        return {"tables": ["same"], "columns": [{"key": "COL_0"}]}

    def shared(conn, filename, data):
        calls.append("shared")
        connections.append(conn)
        return {"columns": [{"key": "COL_0"}], "tables": ["same"]}

    config = Module3AdapterConfig(
        shadow_enabled=True,
        cutover_enabled=True,
        parity_gate_enabled=True,
        predecessor_m1_gate=True,
        predecessor_m2_gate=True,
    )
    adapter = Module3IngestionAdapter(
        legacy_single_loader=legacy,
        shadow_loader=shared,
        config=config,
    )

    live_conn = object()
    result = adapter.load_single_file(live_conn, "spend.csv", b"x")

    assert result["tables"] == ["same"]
    assert calls == ["shared", "legacy"]
    assert connections == [None]
    assert adapter.last_report is not None
    assert adapter.last_report.matched is True
    assert adapter.last_report.cutover_allowed is True

    # Evidence is persisted from the prior request.  The next request can
    # cut over and receives the live connection exactly once; legacy is not
    # invoked for the gated request.
    calls.clear()
    result = adapter.load_single_file(live_conn, "spend.csv", b"x")
    assert result["tables"] == ["same"]
    assert calls == ["shared"]
    assert connections == [None, live_conn]


def test_m2_gate_cannot_bypass_missing_m1_evidence():
    report = compare_parity(
        {"stable": True},
        {"stable": True},
        gate_enabled=True,
        predecessor_gates=(False, True),
    )

    assert report.matched is True
    assert report.predecessor_gates_passed is False
    assert report.cutover_allowed is False


def test_zip_legacy_signature_is_preserved():
    seen = []

    def legacy(conn, data):
        seen.append(data)
        return (["archive.zip::spend.csv"], [])

    adapter = Module3IngestionAdapter(legacy_zip_loader=legacy)
    result = adapter.load_zip_to_session(object(), b"zip-bytes")

    assert result == (["archive.zip::spend.csv"], [])
    assert seen == [b"zip-bytes"]


def test_cutover_evidence_is_bound_to_payload_and_operation():
    calls = []

    def legacy(conn, filename, data):
        calls.append("legacy")
        return (["same"], [])

    def shared(conn, filename, data):
        calls.append("shared")
        return (["same"], [])

    config = Module3AdapterConfig(
        shadow_enabled=True, cutover_enabled=True, parity_gate_enabled=True,
        cutover_gate=CutoverGate(True, True, verified=True),
    )
    adapter = Module3IngestionAdapter(legacy_single_loader=legacy, shadow_loader=shared, config=config)
    conn = object()
    adapter.load_single_file(conn, "a.csv", b"A", session_id="s")
    adapter.load_single_file(conn, "b.csv", b"B", session_id="s")
    assert calls == ["shared", "legacy", "shared", "legacy"]

    # Repeating A with its exact evidence is the only request eligible for
    # shared-only cutover; B cannot unlock A (or vice versa).
    adapter.load_single_file(conn, "a.csv", b"A", session_id="s")
    assert calls[-1] == "shared"


def test_real_parquet_typed_import_publishes_stable_metadata():
    ingestion = importlib.import_module("backend.ingestion")
    if not all(hasattr(ingestion, name) for name in ("parse_transport_manifest", "import_duckdb_parquet")):
        pytest.skip("shared typed-artifact helpers not installed yet")
    duckdb = pytest.importorskip("duckdb")
    from backend.ingestion.models import OrderedColumnSchema, SourceAsset, TableArtifact, ValueType
    from backend.ingestion.transfer import TransferManifest
    from backend.ingestion.typed_artifact import serialize_transport
    from ingestion_adapter import import_typed_artifact_to_session

    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "typed.parquet")
        source = duckdb.connect()
        source.execute("COPY (SELECT 0 AS __procip_transfer_ordinal, 1 AS amount, 'A' AS supplier UNION ALL SELECT 1, 2, 'B') TO ? (FORMAT PARQUET)", [parquet_path])
        payload = open(parquet_path, "rb").read()
        table = TableArtifact("typed.csv::", "typed", (
            OrderedColumnSchema("col_1", "amount", 0, ValueType.INTEGER, physical_name="amount"),
            OrderedColumnSchema("col_2", "supplier", 1, ValueType.TEXT, physical_name="supplier"),
        ), 2)
        manifest = TransferManifest.from_payload("a1", "module1", "module3", [table], payload)
        conn = duckdb.connect()
        keys, _ = import_typed_artifact_to_session(conn, payload, serialize_transport(manifest))
        assert keys == ["typed.csv::"]
        metadata = conn.execute("SELECT column_key FROM _column_metadata ORDER BY column_order").fetchall()
        assert [row[0] for row in metadata] == ["RECORD_ID", "COL_0", "COL_1"]
        raw = conn.execute('SELECT * FROM "raw__typed_csv___7a959eb59127"').fetchall()
        assert raw[0][0] == "amount"


def test_tampered_typed_route_rejects_before_session_creation(monkeypatch):
    ingestion = importlib.import_module("backend.ingestion")
    if not all(hasattr(ingestion, name) for name in ("parse_transport_manifest", "import_duckdb_parquet")):
        pytest.skip("shared typed-artifact helpers not installed yet")
    duckdb = pytest.importorskip("duckdb")
    from backend.ingestion.models import OrderedColumnSchema, TableArtifact, ValueType
    from backend.ingestion.transfer import TransferManifest
    from backend.ingestion.typed_artifact import serialize_transport
    from routes import upload_routes
    import io

    with tempfile.TemporaryDirectory() as temp_dir:
        path = os.path.join(temp_dir, "typed.parquet")
        source = duckdb.connect()
        source.execute("COPY (SELECT 0 AS __procip_transfer_ordinal, 1 AS amount) TO ? (FORMAT PARQUET)", [path])
        payload = open(path, "rb").read()
    table = TableArtifact("typed.csv::", "typed", (OrderedColumnSchema("col_1", "amount", 0, ValueType.INTEGER, physical_name="amount"),), 1)
    manifest = TransferManifest.from_payload("a1", "module1", "module3", [table], payload)
    app = Flask(__name__)
    app.register_blueprint(upload_routes.upload_bp, url_prefix="/api")
    monkeypatch.setattr(upload_routes, "get_session_db", lambda *_: pytest.fail("session must not be created"))
    with app.test_client() as client:
        response = client.post("/api/import", data={
            "file": (io.BytesIO(payload + b"tampered"), "typed.parquet"),
            "manifest": (io.BytesIO(serialize_transport(manifest)), "manifest.json"),
        }, content_type="multipart/form-data")
    assert response.status_code == 400


def test_typed_transfer_rejects_unapproved_source_module():
    from ingestion_adapter import verify_typed_artifact
    with pytest.raises(ValueError):
        verify_typed_artifact({"source_module": "module3"}, b"not-a-parquet")
