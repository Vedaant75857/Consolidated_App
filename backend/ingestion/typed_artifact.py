"""Bounded DuckDB Parquet artifacts and transport manifests.

This module is intentionally module-neutral.  It materializes one DuckDB table
to real Parquet bytes, binds the manifest to those bytes, and verifies the same
bytes before importing them.  No route, session, or HTTP framework is required;
adapters can carry :func:`serialize_transport` as a multipart part or headers.
"""
from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
import tempfile
from dataclasses import dataclass
from typing import Any, Mapping

from .duckdb_sink import quote_identifier
from .models import CONTRACT_VERSION, OrderedColumnSchema, ValueType
from .transfer import TransferManifest, TransferTable


logger = logging.getLogger(__name__)


TRANSPORT_VERSION = "typed-artifact.v1"
MANIFEST_HEADER = "X-ProcIP-Transfer-Manifest"
VERSION_HEADER = "X-ProcIP-Transfer-Version"
TRANSFER_ORDINAL_COLUMN = "__procip_transfer_ordinal"


@dataclass(frozen=True)
class TypedArtifactLimits:
    """Resource limits applied before and during typed transfer operations."""

    max_payload_bytes: int = 512 * 1024 * 1024
    max_manifest_bytes: int = 2 * 1024 * 1024
    max_tables: int = 128
    max_columns: int = 4096
    max_rows: int = 100_000_000

    def __post_init__(self) -> None:
        for name in ("max_payload_bytes", "max_manifest_bytes", "max_tables", "max_columns", "max_rows"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class TypedImportResult:
    """Materialization result shared by adapters with raw/typed table aliases."""

    artifact: TransferTable
    table_name: str
    raw_table: str
    typed_table: str

    @property
    def table_key(self) -> str:
        return self.artifact.table_key

    @property
    def columns(self) -> tuple[OrderedColumnSchema, ...]:
        return self.artifact.columns

    @property
    def row_count(self) -> int:
        return self.artifact.row_count


def _check_payload(payload: bytes | bytearray | memoryview, limits: TypedArtifactLimits) -> bytes:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("typed artifact payload must be bytes-like")
    raw = bytes(payload)
    if not raw:
        raise ValueError("typed artifact payload cannot be empty")
    if len(raw) > limits.max_payload_bytes:
        raise ValueError("typed artifact payload exceeds configured size limit")
    return raw


def _duck_value_type(type_name: str) -> ValueType:
    upper = type_name.upper()
    if "BOOL" in upper:
        return ValueType.BOOLEAN
    if "INT" in upper or upper.startswith("UBIGINT") or upper.startswith("UINTEGER"):
        return ValueType.INTEGER
    if any(token in upper for token in ("DECIMAL", "NUMERIC")):
        return ValueType.DECIMAL
    if any(token in upper for token in ("DOUBLE", "FLOAT", "REAL")):
        return ValueType.FLOAT
    if upper == "DATE":
        return ValueType.DATE
    if upper.startswith("TIME"):
        return ValueType.TIME
    if "TIMESTAMP" in upper or upper.startswith("DATETIME"):
        return ValueType.TIMESTAMP
    return ValueType.TEXT


def _table_info(connection: Any, table_name: str) -> tuple[tuple[str, str], ...]:
    identifier = quote_identifier(table_name)
    rows = connection.execute(f"PRAGMA table_info({identifier})").fetchall()
    return tuple((str(row[1]), str(row[2])) for row in rows)


def _manifest_projection(table: TransferTable) -> str:
    if not table.columns:
        raise ValueError("typed artifact manifest must declare columns")
    return ", ".join(
        quote_identifier(column.physical_name or column.display_name or column.key)
        for column in table.columns
    )


def _validate_manifest_table(
    table: TransferTable,
    info: tuple[tuple[str, str], ...],
    row_count: int,
    limits: TypedArtifactLimits,
) -> None:
    if len(info) > limits.max_columns:
        raise ValueError("typed artifact column count exceeds configured limit")
    if row_count > limits.max_rows:
        raise ValueError("typed artifact row count exceeds configured limit")
    if table.row_count != row_count:
        raise ValueError(f"row count mismatch: manifest={table.row_count}, payload={row_count}")
    if len(table.columns) != len(info):
        raise ValueError(f"column count mismatch: manifest={len(table.columns)}, payload={len(info)}")
    ordinals = [column.ordinal for column in table.columns]
    if ordinals != list(range(len(table.columns))):
        raise ValueError("manifest column ordinals must be contiguous and zero-based")
    physical_names = [column.physical_name or column.key for column in table.columns]
    if len(physical_names) != len(set(physical_names)):
        raise ValueError("manifest columns contain duplicate physical/key names")
    actual_names = [name for name, _ in info]
    if len(actual_names) != len(set(actual_names)):
        raise ValueError("payload columns contain duplicate names")
    for expected, (actual_name, actual_type) in zip(table.columns, info):
        expected_names = {expected.physical_name, expected.key, expected.display_name} - {None, ""}
        if actual_name not in expected_names:
            raise ValueError(f"column name mismatch at ordinal {expected.ordinal}: {sorted(expected_names)!r} != {actual_name!r}")
        actual_value_type = _duck_value_type(actual_type)
        if expected.value_type != actual_value_type:
            raise ValueError(
                f"column type mismatch for {actual_name!r}: {expected.value_type.value} != {actual_value_type.value}"
            )


def export_duckdb_parquet(
    connection: Any,
    table_name: str,
    manifest: TransferManifest,
    *,
    limits: TypedArtifactLimits | None = None,
) -> tuple[TransferManifest, bytes]:
    """Export one DuckDB table and return a manifest bound to real Parquet bytes."""
    limits = limits or TypedArtifactLimits()
    if len(manifest.tables) != 1:
        raise ValueError("a single-table Parquet payload requires exactly one manifest table")
    if len(manifest.tables) > limits.max_tables:
        raise ValueError("manifest table count exceeds configured limit")
    projection = _manifest_projection(manifest.tables[0])
    info_rows = connection.execute(
        f"DESCRIBE SELECT {projection} FROM {quote_identifier(table_name)}"
    ).fetchall()
    info = tuple((str(row[0]), str(row[1])) for row in info_rows)
    row_count = int(connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0])
    _validate_manifest_table(manifest.tables[0], info, row_count, limits)
    source_info = _table_info(connection, table_name)
    source_names = {name for name, _ in source_info}
    if TRANSFER_ORDINAL_COLUMN in source_names:
        raise ValueError(f"source table uses reserved transfer column: {TRANSFER_ORDINAL_COLUMN}")
    ordinal_order = f" ORDER BY {quote_identifier('__row_id')}" if "__row_id" in source_names else ""
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="procip-transfer-", suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        connection.execute(
            f"COPY (SELECT row_number() OVER ({ordinal_order}) - 1 AS {quote_identifier(TRANSFER_ORDINAL_COLUMN)}, {projection} FROM {quote_identifier(table_name)}) TO ? (FORMAT PARQUET)",
            [tmp_path],
        )
        payload_path = Path(tmp_path)
        if payload_path.stat().st_size > limits.max_payload_bytes:
            raise ValueError("typed artifact payload exceeds configured size limit")
        payload = _check_payload(payload_path.read_bytes(), limits)
        bound = manifest.bind_payload(payload, payload_format="parquet")
        return bound, payload
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning("Unable to remove temporary Parquet export %s", tmp_path, exc_info=True)


def import_duckdb_parquet(
    connection: Any,
    payload: bytes | bytearray | memoryview,
    manifest: TransferManifest,
    table_name: str,
    *,
    limits: TypedArtifactLimits | None = None,
) -> TypedImportResult:
    """Verify and atomically import one Parquet payload into DuckDB."""
    limits = limits or TypedArtifactLimits()
    raw = _check_payload(payload, limits)
    if manifest.payload_format != "parquet":
        raise ValueError("DuckDB typed import currently requires a Parquet payload")
    if not manifest.verify_payload(raw):
        raise ValueError("typed artifact payload failed manifest verification")
    if len(manifest.tables) != 1:
        raise ValueError("a single-table Parquet payload requires exactly one manifest table")
    if len(manifest.tables[0].columns) > limits.max_columns:
        raise ValueError("manifest column count exceeds configured limit")
    quoted_target = quote_identifier(table_name)
    exists = connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table_name]
    ).fetchone()[0]
    if exists:
        raise ValueError(f"destination table already exists: {table_name}")
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="procip-transfer-", suffix=".parquet", delete=False) as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        info_rows = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [tmp_path]).fetchall()
        all_info = tuple((str(row[0]), str(row[1])) for row in info_rows)
        if not all_info or all_info[0][0] != TRANSFER_ORDINAL_COLUMN:
            raise ValueError("typed Parquet payload is missing reserved transfer ordinal")
        if _duck_value_type(all_info[0][1]) != ValueType.INTEGER:
            raise ValueError("typed transfer ordinal has an invalid type")
        info = all_info[1:]
        row_count = int(connection.execute("SELECT COUNT(*) FROM read_parquet(?)", [tmp_path]).fetchone()[0])
        ordinal_stats = connection.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT {quote_identifier(TRANSFER_ORDINAL_COLUMN)}), "
            f"MIN({quote_identifier(TRANSFER_ORDINAL_COLUMN)}), MAX({quote_identifier(TRANSFER_ORDINAL_COLUMN)}), "
            f"COUNT(*) - COUNT({quote_identifier(TRANSFER_ORDINAL_COLUMN)}) "
            "FROM read_parquet(?)",
            [tmp_path],
        ).fetchone()
        ordinal_count, distinct_count, ordinal_min, ordinal_max, null_count = ordinal_stats
        if (
            int(ordinal_count) != row_count
            or int(distinct_count) != row_count
            or int(null_count) != 0
            or (row_count and (int(ordinal_min) != 0 or int(ordinal_max) != row_count - 1))
        ):
            raise ValueError("typed transfer ordinal is not a contiguous zero-based sequence")
        _validate_manifest_table(manifest.tables[0], info, row_count, limits)
        connection.execute("BEGIN TRANSACTION")
        try:
            public_projection = ", ".join(quote_identifier(name) for name, _ in info)
            connection.execute(
                f"CREATE TABLE {quoted_target} AS SELECT {public_projection} FROM read_parquet(?) ORDER BY {quote_identifier(TRANSFER_ORDINAL_COLUMN)}",
                [tmp_path],
            )
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
            raise
        return TypedImportResult(manifest.tables[0], table_name, table_name, table_name)
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            except OSError:
                logger.warning("Unable to remove temporary Parquet import %s", tmp_path, exc_info=True)


def serialize_transport(manifest: TransferManifest, *, limits: TypedArtifactLimits | None = None) -> bytes:
    """Serialize a versioned manifest envelope for a multipart body part."""
    limits = limits or TypedArtifactLimits()
    if manifest.contract_version != CONTRACT_VERSION:
        raise ValueError(f"unsupported typed artifact contract version: {manifest.contract_version}")
    if not manifest.payload_checksum or not manifest.verify():
        raise ValueError("only a payload-bound manifest can be transported")
    envelope = {"transport_version": TRANSPORT_VERSION, "manifest": manifest.to_dict()}
    encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > limits.max_manifest_bytes:
        raise ValueError("transport manifest exceeds configured size limit")
    return encoded


def transport_headers(manifest: TransferManifest, *, limits: TypedArtifactLimits | None = None) -> dict[str, str]:
    """Return headers carrying the same versioned manifest for non-multipart transport."""
    encoded = serialize_transport(manifest, limits=limits)
    return {
        VERSION_HEADER: TRANSPORT_VERSION,
        MANIFEST_HEADER: base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("="),
    }


def _manifest_from_dict(value: Mapping[str, Any], limits: TypedArtifactLimits) -> TransferManifest:
    required = ("artifact_id", "source_module", "destination_module", "tables")
    if any(not value.get(name) for name in required):
        raise ValueError("transport manifest is missing required fields")
    raw_tables = value["tables"]
    if not isinstance(raw_tables, list) or len(raw_tables) > limits.max_tables:
        raise ValueError("invalid or excessive transport table metadata")
    tables: list[TransferTable] = []
    for raw_table in raw_tables:
        if not isinstance(raw_table, Mapping):
            raise ValueError("invalid transport table metadata")
        raw_columns = raw_table.get("columns", [])
        if not isinstance(raw_columns, list) or len(raw_columns) > limits.max_columns:
            raise ValueError("invalid or excessive transport column metadata")
        columns = tuple(
            OrderedColumnSchema(
                key=str(column["key"]),
                display_name=str(column.get("display_name", column["key"])),
                ordinal=int(column["ordinal"]),
                value_type=ValueType(column.get("value_type", ValueType.TEXT.value)),
                nullable=bool(column.get("nullable", True)),
                physical_name=column.get("physical_name"),
                format_hint=column.get("format_hint"),
            )
            for column in raw_columns
        )
        tables.append(
            TransferTable(
                str(raw_table["table_key"]),
                columns,
                int(raw_table.get("row_count", 0)),
                raw_table.get("raw_hash"),
                raw_table.get("typed_hash"),
                raw_table.get("provenance", {}),
            )
        )
    manifest = TransferManifest(
        str(value["artifact_id"]),
        str(value["source_module"]),
        str(value["destination_module"]),
        tuple(tables),
        value.get("artifact_checksum"),
        str(value.get("contract_version", "ingestion.v1")),
        str(value.get("encoding", "typed-artifact")),
        value.get("payload_checksum"),
        value.get("payload_format"),
    )
    if not manifest.artifact_checksum or not manifest.payload_checksum:
        raise ValueError("transport manifest must include bound checksums")
    if manifest.contract_version != CONTRACT_VERSION:
        raise ValueError(f"unsupported typed artifact contract version: {manifest.contract_version}")
    return manifest


def parse_transport_manifest(
    envelope: bytes | bytearray | memoryview | str | Mapping[str, Any] | None,
    payload: bytes | bytearray | memoryview,
    *,
    headers: Mapping[str, str] | None = None,
    limits: TypedArtifactLimits | None = None,
) -> TransferManifest:
    """Parse a multipart/header envelope and verify it against payload bytes."""
    limits = limits or TypedArtifactLimits()
    source: Any = envelope
    headers = headers or {}
    if source is None:
        encoded = headers.get(MANIFEST_HEADER)
        if not encoded:
            raise ValueError("typed artifact manifest is missing")
        max_encoded = (limits.max_manifest_bytes * 4 + 2) // 3 + 4
        if len(encoded.encode("utf-8")) > max_encoded:
            raise ValueError("transport manifest exceeds configured size limit")
        if encoded.lstrip().startswith("{"):
            source = encoded
        else:
            try:
                padding = "=" * (-len(encoded) % 4)
                source = base64.urlsafe_b64decode(encoded + padding)
            except Exception:
                source = encoded
    if isinstance(source, Mapping):
        parsed = dict(source)
        try:
            if len(json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")) > limits.max_manifest_bytes:
                raise ValueError("transport manifest exceeds configured size limit")
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ValueError) and "size limit" in str(exc):
                raise
            raise ValueError("transport manifest is not JSON serializable") from exc
    else:
        raw = source.encode("utf-8") if isinstance(source, str) else bytes(source)
        if len(raw) > limits.max_manifest_bytes:
            raise ValueError("transport manifest exceeds configured size limit")
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("transport manifest is not valid JSON") from exc
    if isinstance(parsed, Mapping) and "transport_version" not in parsed and "artifact_id" in parsed:
        # Accept the pre-envelope ``TransferManifest.to_dict()`` shape during
        # migration.  It is still required to contain a payload checksum and
        # is verified against the actual bytes below.
        parsed = {"transport_version": TRANSPORT_VERSION, "manifest": parsed}
    if not isinstance(parsed, Mapping) or parsed.get("transport_version") != TRANSPORT_VERSION:
        raise ValueError("unsupported typed artifact transport version")
    if headers.get(VERSION_HEADER) not in (None, TRANSPORT_VERSION):
        raise ValueError("transport version header does not match manifest")
    manifest_value = parsed.get("manifest")
    if not isinstance(manifest_value, Mapping):
        raise ValueError("transport manifest envelope is missing manifest")
    manifest = _manifest_from_dict(manifest_value, limits)
    raw_payload = _check_payload(payload, limits)
    if not manifest.verify_payload(raw_payload):
        raise ValueError("transport payload does not match manifest checksum or format")
    return manifest
