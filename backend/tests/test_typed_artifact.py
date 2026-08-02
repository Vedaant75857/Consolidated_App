from __future__ import annotations

import duckdb
import pytest
from decimal import Decimal
from dataclasses import replace

from backend.ingestion.models import OrderedColumnSchema, TableArtifact, ValueType
from backend.ingestion.transfer import TransferManifest
from backend.ingestion.typed_artifact import (
    MANIFEST_HEADER,
    TypedArtifactLimits,
    export_duckdb_parquet,
    import_duckdb_parquet,
    parse_transport_manifest,
    serialize_transport,
    transport_headers,
)


@pytest.fixture
def source():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE source (id INTEGER, name VARCHAR, amount DECIMAL(10,2))")
    conn.execute("INSERT INTO source VALUES (1, 'a', 12.30), (2, 'b', 4.50)")
    try:
        yield conn
    finally:
        conn.close()


def _manifest() -> TransferManifest:
    columns = (
        OrderedColumnSchema("id", "id", 0, ValueType.INTEGER, False),
        OrderedColumnSchema("name", "name", 1, ValueType.TEXT),
        OrderedColumnSchema("amount", "amount", 2, ValueType.DECIMAL),
    )
    return TransferManifest.from_tables("artifact", "module1", "module2", [TableArtifact("source", "Source", columns, 2)])


def test_export_import_binds_and_verifies_real_parquet(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    assert manifest.verify_payload(payload)
    assert payload[:4] == payload[-4:] == b"PAR1"
    imported = import_duckdb_parquet(source, payload, manifest, "destination")
    assert imported.row_count == 2
    assert imported.raw_table == imported.typed_table == "destination"
    assert source.execute("SELECT * FROM destination ORDER BY id").fetchall() == [(1, "a", Decimal("12.30")), (2, "b", Decimal("4.50"))]


def test_import_rejects_tampered_payload_and_existing_target(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    with pytest.raises(ValueError, match="verification"):
        import_duckdb_parquet(source, payload + b"x", manifest, "destination")
    source.execute("CREATE TABLE destination (id INTEGER)")
    with pytest.raises(ValueError, match="already exists"):
        import_duckdb_parquet(source, payload, manifest, "destination")


def test_transport_multipart_and_headers_are_versioned_and_bound(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    encoded = serialize_transport(manifest)
    assert parse_transport_manifest(encoded, payload).digest() == manifest.digest()
    headers = transport_headers(manifest)
    assert MANIFEST_HEADER in headers
    assert parse_transport_manifest(None, payload, headers=headers).digest() == manifest.digest()
    with pytest.raises(ValueError, match="checksum or format"):
        parse_transport_manifest(encoded, payload + b"tamper")


def test_payload_and_manifest_limits_are_enforced(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    with pytest.raises(ValueError, match="size limit"):
        serialize_transport(manifest, limits=TypedArtifactLimits(max_manifest_bytes=1))
    with pytest.raises(ValueError, match="size limit"):
        import_duckdb_parquet(source, payload, manifest, "destination", limits=TypedArtifactLimits(max_payload_bytes=len(payload) - 1))
    with pytest.raises(ValueError, match="size limit"):
        parse_transport_manifest(manifest.to_dict(), payload, limits=TypedArtifactLimits(max_manifest_bytes=1))
    with pytest.raises(ValueError, match="size limit"):
        parse_transport_manifest(None, payload, headers={"X-ProcIP-Transfer-Manifest": "A" * 100}, limits=TypedArtifactLimits(max_manifest_bytes=8))


def test_duplicate_manifest_physical_columns_are_rejected(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    columns = tuple(replace(column, physical_name="id") for column in manifest.tables[0].columns)
    table = replace(manifest.tables[0], columns=columns)
    forged = replace(manifest, tables=(table,))
    forged = replace(forged, artifact_checksum=forged._compute_checksum())
    with pytest.raises(ValueError, match="duplicate physical/key"):
        import_duckdb_parquet(source, payload, forged, "duplicate_columns")


def test_import_preserves_source_row_order_and_rejects_type_tampering(source):
    expected_rows = source.execute("SELECT id, name, amount FROM source").fetchall()
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    import_duckdb_parquet(source, payload, manifest, "ordered")
    assert source.execute("SELECT id, name, amount FROM ordered").fetchall() == expected_rows

    first = replace(manifest.tables[0].columns[0], value_type=ValueType.TEXT)
    tampered_table = replace(manifest.tables[0], columns=(first, *manifest.tables[0].columns[1:]))
    tampered = replace(manifest, tables=(tampered_table,))
    tampered = replace(tampered, artifact_checksum=tampered._compute_checksum())
    with pytest.raises(ValueError, match="type mismatch"):
        import_duckdb_parquet(source, payload, tampered, "tampered_type")


def test_export_rejects_source_collision_with_reserved_ordinal(source):
    from backend.ingestion.typed_artifact import TRANSFER_ORDINAL_COLUMN

    source.execute(f'ALTER TABLE source ADD COLUMN "{TRANSFER_ORDINAL_COLUMN}" BIGINT')
    with pytest.raises(ValueError, match="reserved transfer column"):
        export_duckdb_parquet(source, "source", _manifest())


def test_future_contract_version_is_rejected(source):
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    future = replace(manifest, contract_version="ingestion.v2")
    future = replace(future, artifact_checksum=future._compute_checksum())
    with pytest.raises(ValueError, match="contract version"):
        serialize_transport(future)


def test_export_projection_does_not_leak_internal_columns(source):
    source.execute("ALTER TABLE source ADD COLUMN __row_id BIGINT")
    source.execute("UPDATE source SET __row_id = id")
    manifest, payload = export_duckdb_parquet(source, "source", _manifest())
    import_duckdb_parquet(source, payload, manifest, "projected")
    assert [row[0] for row in source.execute("DESCRIBE projected").fetchall()] == ["id", "name", "amount"]
