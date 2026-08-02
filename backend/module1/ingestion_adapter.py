"""Module 1's compatibility adapter for the shared ingestion pipeline.

The adapter is intentionally conservative: the existing Module 1 loader is
the source of truth unless an operator explicitly enables shadowing.  A shadow
run is observational only and never receives the SSE progress callback.  This
keeps the public upload stream and the legacy tuple return value unchanged
while allowing a future shared reader/sink to prove parity before cut-over.

The shared implementation is injected (or discovered lazily) rather than
imported at module import time.  This keeps the adapter usable while the new
pipeline is unavailable and preserves test monkey-patching of the legacy
loader.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import math
import os
import re
import zipfile
import io
from hashlib import sha256
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

logger = logging.getLogger(__name__)

LegacyLoader = Callable[..., Any]
ShadowLoader = Callable[..., Any]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _env_bool_any(names: tuple[str, ...], default: bool = False) -> bool:
    """Read the first configured alias (keeps deployment flag names portable)."""
    for name in names:
        if os.getenv(name) is not None:
            return _env_bool(name, default)
    return default


@dataclass(frozen=True)
class Module1AdapterConfig:
    """Runtime flags for the adapter.

    Shadowing is off by default.  ``cutover_enabled`` is deliberately a
    separate switch: setting it without a passing parity report still cannot
    select the shared loader.
    """

    shadow_enabled: bool = False
    cutover_enabled: bool = False
    parity_gate_enabled: bool = False
    fail_on_mismatch: bool = False

    @classmethod
    def from_env(cls) -> "Module1AdapterConfig":
        mode = os.getenv("MODULE1_INGESTION_MODE", "legacy").strip().lower()
        return cls(
            shadow_enabled=_env_bool_any((
                "MODULE1_INGESTION_SHADOW", "MODULE1_INGESTION_SHADOW_ENABLED",
                "MODULE1_SHARED_INGESTION_SHADOW",
            ), mode == "shadow"),
            cutover_enabled=_env_bool_any((
                "MODULE1_INGESTION_CUTOVER", "MODULE1_INGESTION_CUTOVER_ENABLED",
            ), mode == "shared"),
            parity_gate_enabled=_env_bool_any((
                "MODULE1_INGESTION_PARITY_GATE", "MODULE1_INGESTION_PARITY_GATE_ENABLED",
            ), False),
            fail_on_mismatch=_env_bool_any((
                "MODULE1_INGESTION_SHADOW_STRICT", "MODULE1_INGESTION_SHADOW_FAIL_CLOSED",
            ), False),
        )

    @property
    def gates_enabled(self) -> bool:
        """Whether this process has explicitly opted into typed cut-over."""
        return bool(self.cutover_enabled and self.parity_gate_enabled)


@dataclass(frozen=True)
class ParityReport:
    """Deterministic comparison of legacy and shadow results."""

    legacy_digest: str
    shadow_digest: str
    matched: bool
    gate_passed: bool
    differences: tuple[str, ...] = ()
    payload_sha256: str | None = None
    contract_version: str | None = None
    adapter_compatible: bool = False
    postconditions_passed: bool = False
    canonical_matched: bool | None = None

    @property
    def cutover_allowed(self) -> bool:
        return bool(
            self.gate_passed
            and self.adapter_compatible
            and self.postconditions_passed
            and self.payload_sha256
        )


_shadow_loader: ShadowLoader | None = None
_cutover_loader: ShadowLoader | None = None
_last_report: ParityReport | None = None


@dataclass(frozen=True)
class AdapterParityEvidence:
    """Explicit shared-runner evidence compatible with Module 1's tuple DTO."""

    result: Any
    payload_sha256: str
    contract_version: str | None = None
    # Canonical session evidence is intentionally opaque to the public tuple
    # contract.  It is populated by the Module 1 shared bridge and lets the
    # adapter compare table keys/schema/rows/hashes rather than internal DTOs.
    canonical: Mapping[str, Any] | None = None
    # Public upload progress is part of the parity contract.  The shadow
    # bridge records these events without exposing them to the live session.
    events: tuple[Mapping[str, Any], ...] = ()


def _current_contract_version() -> str:
    try:
        from backend.ingestion.models import CONTRACT_VERSION
    except (ImportError, ModuleNotFoundError):
        try:
            from ingestion.models import CONTRACT_VERSION
        except (ImportError, ModuleNotFoundError):
            return "ingestion.v1"
    return str(CONTRACT_VERSION)


def _module1_postconditions(conn: Any, result: Any) -> bool:
    """Validate the legacy tuple and its session publication boundary."""
    if not isinstance(result, tuple) or len(result) != 2:
        return False
    inventory, warnings = result
    if not isinstance(inventory, (dict, list)) or not isinstance(warnings, list):
        return False
    if any(not isinstance(item, dict) for item in warnings):
        return False
    try:
        from shared.db import all_registered_tables, get_meta, safe_table_name
        registered = all_registered_tables(conn)
    except Exception:
        return False
    if not isinstance(registered, list) or not registered:
        return False
    for entry in registered:
        key = str(entry.get("table_key", ""))
        sql_name = str(entry.get("sql_name", ""))
        if not key or not sql_name:
            return False
        try:
            if not conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [sql_name]).fetchone()[0]:
                return False
            raw_name = safe_table_name("raw", key)
            if not conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [raw_name]).fetchone()[0]:
                return False
            raw_preview = get_meta(conn, f"rawArray__{key}")
            if not isinstance(raw_preview, list):
                return False
        except Exception:
            return False
    return True


def configure_shadow_loader(loader: ShadowLoader | None) -> None:
    """Install a shared-pipeline runner for the current process.

    Production startup can use this hook once the shared service is available;
    tests use it to provide a deterministic fake.  Passing ``None`` restores
    lazy discovery and is useful for test isolation.
    """

    global _shadow_loader
    _shadow_loader = loader


def configure_cutover_loader(loader: ShadowLoader | None) -> None:
    """Install the production shared runner used only after a parity gate."""
    global _cutover_loader
    _cutover_loader = loader


def get_last_parity_report() -> ParityReport | None:
    return _last_report


def _jsonable(value: Any) -> Any:
    """Convert arbitrary loader output into stable, JSON-safe data."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float) and not math.isfinite(value):
        return {"__float__": repr(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": bytes(value).hex()}
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (tuple, list)):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted((_jsonable(v) for v in value), key=lambda item: repr(item))
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable({name: getattr(value, name) for name in value.__dataclass_fields__})
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return repr(value)


def _digest(value: Any) -> str:
    encoded = json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compare_parity(legacy_result: Any, shadow_result: Any, *, gate_enabled: bool = False,
                   payload_sha256: str | None = None,
                   contract_version: str | None = None,
                   explicit_evidence: bool = False,
                   postconditions_passed: bool = False,
                   canonical_legacy: Mapping[str, Any] | None = None,
                   canonical_shadow: Mapping[str, Any] | None = None) -> ParityReport:
    """Compare loader results without depending on dictionary insertion order."""
    legacy_digest = _digest(legacy_result)
    shadow_digest = _digest(shadow_result)
    canonical_matched = None
    if canonical_legacy is not None and canonical_shadow is not None:
        canonical_matched = _jsonable(canonical_legacy) == _jsonable(canonical_shadow)
    matched = canonical_matched if canonical_matched is not None else legacy_digest == shadow_digest
    differences = () if matched else ("deterministic loader result digest differs",)
    # A tuple-shaped result alone is not enough to authorize cut-over.  The
    # shadow bridge must also provide canonical warnings, metadata and SSE
    # evidence so parity covers the public compatibility surface, not merely
    # table rows.
    evidence_complete = bool(
        isinstance(canonical_shadow, Mapping)
        and all(field in canonical_shadow for field in ("warnings", "metadata", "events"))
    )
    compatible = bool(explicit_evidence and evidence_complete and isinstance(shadow_result, tuple) and len(shadow_result) == 2)
    return ParityReport(
        legacy_digest=legacy_digest,
        shadow_digest=shadow_digest,
        matched=matched,
        gate_passed=bool(gate_enabled and matched),
        differences=differences,
        payload_sha256=payload_sha256,
        contract_version=contract_version,
        adapter_compatible=compatible,
        postconditions_passed=postconditions_passed,
        canonical_matched=canonical_matched,
    )


_EXCEL_EXTENSIONS = (".xls", ".xlsx", ".xlsm", ".xlsb", ".xltx", ".xltm", ".ods")


def _legacy_table_key(shared_key: str) -> str:
    """Map shared service source identities to Module 1's stable keys."""
    parts = str(shared_key).split(":")
    # Service child source IDs may repeat the root filename (for example a
    # direct workbook becomes ``upload.xlsx:upload.xlsx:Sheet``).  Prefer the
    # final Excel component before the sheet suffix; a sheet itself may legally
    # be named ``report.csv`` and must not replace its workbook identity.
    excel_candidates = [
        i for i, part in enumerate(parts[:-1])
        if part.lower().endswith(_EXCEL_EXTENSIONS)
    ]
    if excel_candidates:
        file_index = max(excel_candidates)
    else:
        csv_candidates = [
            i for i, part in enumerate(parts)
            if part.lower().endswith(".csv")
        ]
        file_index = max(csv_candidates) if csv_candidates else None
    if file_index is None:
        return str(shared_key)
    # Shared service identities carry a canonical archive path.  Preserve the
    # complete path for nested members so two ``data.csv`` files in different
    # inner archives cannot collide.  For a top-level archive member retain the
    # historical basename key used by Module 1's public DTOs.
    file_path = parts[file_index].replace("\\", "/").strip("/")
    path_parts = [part for part in file_path.split("/") if part]
    if len(path_parts) == 2 and path_parts[0].lower().endswith(".zip"):
        file_name = path_parts[-1]
    elif len(path_parts) > 2 and path_parts[0].lower().endswith(".zip"):
        file_name = "/".join(path_parts)
    else:
        file_name = file_path
    suffix = ":".join(parts[file_index + 1:])
    if file_name.lower().endswith(_EXCEL_EXTENSIONS):
        return f"{file_name}::{suffix}" if suffix else f"{file_name}::"
    return f"{file_name}::"


def _shared_table_names(table_key: str) -> tuple[str, str]:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(table_key)).strip("_") or "table"
    digest = sha256(str(table_key).encode("utf-8")).hexdigest()[:12]
    base = f"{safe[:48]}_{digest}"
    return f"{base}__raw", f"{base}__typed"


def _canonical_snapshot(conn: Any, *, result: Any = None, events: list[dict] | None = None) -> dict[str, Any]:
    """Hash the compatibility view, preserving ordered keys and schemas."""
    from shared.db import all_registered_tables, lookup_sql_name, read_table_columns, table_row_count, quote_id

    tables: list[dict[str, Any]] = []
    for entry in sorted(all_registered_tables(conn), key=lambda item: str(item.get("table_key", ""))):
        key = str(entry.get("table_key", ""))
        sql_name = lookup_sql_name(conn, key) or entry.get("sql_name")
        if not sql_name:
            continue
        columns = read_table_columns(conn, sql_name)
        rows = conn.execute(f"SELECT * FROM {quote_id(sql_name)} ORDER BY COALESCE(\"__row_id\", 0)").fetchall()
        raw_name = __import__("shared.db", fromlist=["safe_table_name"]).safe_table_name("raw", key)
        raw_rows = []
        if conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [raw_name]).fetchone()[0]:
            raw_rows = conn.execute(f"SELECT * FROM {quote_id(raw_name)}").fetchall()
        tables.append({
            "table_key": key,
            "columns": columns,
            "rows": len(rows),
            "raw_rows": len(raw_rows),
            "raw_hash": _digest(raw_rows),
            "typed_hash": _digest(rows),
        })
    # Public compatibility state is part of parity evidence.  A reader that
    # produces identical rows but drops warnings/raw previews must not unlock
    # cut-over.
    metadata: dict[str, Any] = {}
    try:
        from shared.db import get_all_meta_keys, get_meta
        for key in get_all_meta_keys(conn):
            if key == "inv" or key == "filesPayload" or str(key).startswith("rawArray__"):
                metadata[str(key)] = get_meta(conn, key)
    except Exception:
        metadata = {}
    warnings = result[1] if isinstance(result, tuple) and len(result) == 2 else []
    return {
        "tables": tables,
        "warnings": _jsonable(warnings),
        "metadata": _jsonable(metadata),
        "events": _jsonable(events or []),
    }


def _warning_dicts(result: Any) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for item in getattr(result, "issues", ()):
        code = getattr(getattr(item, "code", None), "value", getattr(item, "code", None))
        warnings.append({"file": getattr(item, "source", None), "message": getattr(item, "message", str(item)), "code": code, "severity": getattr(getattr(item, "severity", None), "value", getattr(item, "severity", "warning"))})
    return warnings


def _shared_source_name(file_data: bytes) -> str:
    """Choose a deterministic source extension for bytes-only Module 1 runs.

    The shared detector gives known Excel extensions precedence over ZIP magic,
    but this adapter's public entry point receives bytes without the uploaded
    filename.  Inspect the OOXML content marker so a direct workbook is sent
    through ``ExcelReader``; ordinary archives remain ZIPs.
    """
    payload = bytes(file_data)
    if not payload.startswith(b"PK\x03\x04"):
        return "upload.csv"
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = {name.lstrip("/").lower() for name in archive.namelist()}
            if "mimetype" in names:
                try:
                    mimetype = archive.read(next(name for name in archive.namelist() if name.lower() == "mimetype")).decode("ascii", "strict").strip()
                except (KeyError, UnicodeDecodeError):
                    mimetype = ""
                if mimetype == "application/vnd.oasis.opendocument.spreadsheet":
                    return "upload.ods"
            if "[content_types].xml" in names and "xl/workbook.bin" in names:
                return "upload.xlsb"
            if "[content_types].xml" in names and (
                "xl/workbook.xml" in names or any(name.startswith("xl/") for name in names)
            ):
                return "upload.xlsx"
    except (OSError, zipfile.BadZipFile):
        # Let the shared service report the structured invalid-archive issue.
        pass
    return "upload.zip"


def _archive_file_count(file_data: bytes, source_name: str) -> int:
    """Return the legacy SSE ``zip_info.total`` count where possible."""
    if source_name != "upload.zip":
        return 1
    try:
        def count_archive(payload: bytes, depth: int = 0) -> int:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                count = 0
                for entry in archive.infolist():
                    if entry.is_dir():
                        continue
                    lower = entry.filename.lower()
                    try:
                        member_payload = archive.read(entry)
                    except (OSError, zipfile.BadZipFile):
                        continue
                    # Inspect bytes before trusting the member suffix.  A
                    # nested archive called ``data.csv`` is still an archive,
                    # while an OOXML workbook called ``book.zip`` is a single
                    # loadable Excel source rather than a container to walk.
                    detected_name = _shared_source_name(member_payload)
                    is_container = detected_name == "upload.zip"
                    if is_container and depth < 2:
                        try:
                            count += count_archive(member_payload, depth + 1)
                        except (OSError, zipfile.BadZipFile):
                            pass
                    elif lower.endswith((".csv",) + _EXCEL_EXTENSIONS) or detected_name in {"upload.xlsx", "upload.xlsb", "upload.ods"}:
                        count += 1
                return count
        count = count_archive(file_data)
        return count or 1
    except (OSError, zipfile.BadZipFile):
        return 1


def _shared_sink_invariant(conn: Any, artifact: Any, raw_name: str, typed_name: str) -> tuple[bool, str]:
    """Verify the dual raw/typed publication before exposing Module 1 tables."""
    try:
        from shared.db import quote_id
        exists = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name IN (?, ?)",
            [raw_name, typed_name],
        ).fetchall()
        names = {str(row[0]) for row in exists}
        if raw_name not in names or typed_name not in names:
            return False, "shared raw and typed tables were not both published"
        raw_cols = [str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            [raw_name],
        ).fetchall()]
        typed_cols = [str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            [typed_name],
        ).fetchall()]
        if raw_cols != typed_cols:
            return False, "shared raw and typed schemas differ"
        raw_count = conn.execute(f"SELECT COUNT(*) FROM {quote_id(raw_name)}").fetchone()[0]
        typed_count = conn.execute(f"SELECT COUNT(*) FROM {quote_id(typed_name)}").fetchone()[0]
        if raw_count != typed_count or int(getattr(artifact, "row_count", raw_count)) != raw_count:
            return False, "shared raw and typed row counts differ"
        row_ids = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT __row_id FROM {quote_id(raw_name)} EXCEPT SELECT __row_id FROM {quote_id(typed_name)})"
        ).fetchone()[0]
        typed_row_ids = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT __row_id FROM {quote_id(typed_name)} EXCEPT SELECT __row_id FROM {quote_id(raw_name)})"
        ).fetchone()[0]
        if row_ids or typed_row_ids:
            return False, "shared raw and typed row identities differ"
        expected = ["__row_id", *[str(getattr(col, "physical_name", None) or getattr(col, "key", "")) for col in getattr(artifact, "columns", ())]]
        if expected != raw_cols:
            return False, "shared schema does not match artifact columns"
        artifact_raw_hash = str(getattr(artifact, "raw_hash", "") or "")
        artifact_typed_hash = str(getattr(artifact, "typed_hash", "") or "")
        if not artifact_raw_hash or not artifact_typed_hash:
            return False, "shared artifact hashes are missing"
        # Bind publication to the actual immutable table payloads.  Merely
        # carrying non-empty hashes in the artifact is insufficient because a
        # staged table could have been altered or only one side published.
        fields = ", ".join(quote_id(column) for column in raw_cols[1:]) or "NULL"
        for table_name, expected_hash, label in (
            (raw_name, artifact_raw_hash, "raw"),
            (typed_name, artifact_typed_hash, "typed"),
        ):
            actual_hash = str(conn.execute(
                f"SELECT sha256(coalesce(string_agg(json_array({fields}), '' ORDER BY \"__row_id\"), '')) "
                f"FROM {quote_id(table_name)}"
            ).fetchone()[0])
            if actual_hash != expected_hash:
                return False, f"shared {label} artifact hash does not match published table"
        return True, ""
    except Exception as exc:
        return False, f"shared sink invariant check failed: {exc}"


def run_module1_shared(conn: Any, file_data: bytes, on_progress: Callable[[dict], None] | None = None) -> tuple[dict, list[dict]]:
    """Run shared ingestion and publish Module 1's raw/data contracts.

    The shared service owns detection/readers; this bridge owns only the
    Module 1 table names, header rebuild, registry and raw-array metadata.
    """
    from backend.ingestion.models import IngestionRequest, SourceAsset
    from backend.ingestion.service import IngestionService
    from shared.db import get_meta, register_table, safe_table_name, set_meta, quote_id
    from data_loading.file_loader import (
        RAW_META_PREVIEW_ROWS,
        rebuild_table_from_raw_table,
        get_raw_array_from_table,
    )

    payload = bytes(file_data)
    name = _shared_source_name(payload)
    request = IngestionRequest(SourceAsset(name, size_bytes=len(file_data), source_id=None), module="module1")
    # Preserve the existing upload SSE contract.  Internal shared reader/sink
    # heartbeats are deliberately not forwarded; callers receive the same
    # zip_info/file_loaded/committed milestones as the legacy loader.
    if on_progress:
        on_progress({"stage": "zip_info", "total": _archive_file_count(payload, name)})
    result = IngestionService(sink_connection=conn).ingest(request, source=payload)
    warnings = _warning_dicts(result)
    published: list[tuple[str, str, str]] = []
    publication_states: list[dict[str, Any]] = []
    try:
        # Match the legacy loader's import-time configurable preview bound so
        # rawArray has identical shape across the two paths.
        # Read the environment at call time as the legacy loader does at
        # import time, while still honoring tests/operators that monkeypatch
        # the module constant.  Zero is a valid explicit bound (empty
        # rawArray) and must not silently become one.
        configured_rows = os.getenv("RAW_META_PREVIEW_ROWS")
        raw_preview_rows = int(configured_rows) if configured_rows is not None else int(RAW_META_PREVIEW_ROWS)
        raw_preview_rows = max(0, raw_preview_rows)
    except (TypeError, ValueError):
        raw_preview_rows = 20

    def _exists(name_: str) -> bool:
        return bool(conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name_]
        ).fetchone()[0])

    def _stage_previous(key_: str, raw_: str, tbl_: str) -> dict[str, Any]:
        digest = sha256(f"{key_}:{id(key_)}:{len(publication_states)}".encode()).hexdigest()[:12]
        state: dict[str, Any] = {
            "key": key_, "raw": raw_, "tbl": tbl_, "raw_backup": None,
            "tbl_backup": None, "old_registry": None, "old_meta": None,
            "meta_exists": False,
        }
        try:
            from shared.db import lookup_sql_name
            state["old_registry"] = lookup_sql_name(conn, key_)
        except Exception:
            state["old_registry"] = None
        meta_row = conn.execute("SELECT value FROM meta WHERE key = ?", [f"rawArray__{key_}"]).fetchone()
        if meta_row:
            state["meta_exists"] = True
            state["old_meta"] = get_meta(conn, f"rawArray__{key_}")
        for label, target in (("raw_backup", raw_), ("tbl_backup", tbl_)):
            if _exists(target):
                backup = f"__m1_prev_{digest}_{label}"
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(backup)}")
                conn.execute(f"ALTER TABLE {quote_id(target)} RENAME TO {quote_id(backup)}")
                state[label] = backup
        return state

    def _restore_previous(state: dict[str, Any]) -> None:
        key_ = state["key"]
        raw_, tbl_ = state["raw"], state["tbl"]
        for target in (tbl_, raw_):
            if _exists(target):
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(target)}")
        for label, target in (("raw_backup", raw_), ("tbl_backup", tbl_)):
            backup = state.get(label)
            if backup and _exists(backup):
                conn.execute(f"ALTER TABLE {quote_id(backup)} RENAME TO {quote_id(target)}")
        conn.execute("DELETE FROM table_registry WHERE table_key = ?", [key_])
        if state.get("old_registry"):
            register_table(conn, key_, state["old_registry"], commit=False)
        conn.execute("DELETE FROM meta WHERE key = ?", [f"rawArray__{key_}"])
        if state.get("meta_exists"):
            set_meta(conn, f"rawArray__{key_}", state.get("old_meta"), commit=False)

    try:
        for artifact in result.tables:
            shared_raw, shared_typed = _shared_table_names(artifact.table_key)
            invariant_ok, invariant_message = _shared_sink_invariant(conn, artifact, shared_raw, shared_typed)
            if not invariant_ok:
                raise RuntimeError(f"{invariant_message} for {artifact.table_key}")
            key = _legacy_table_key(artifact.table_key)
            legacy_raw = safe_table_name("raw", key)
            legacy_tbl = safe_table_name("tbl", key)
            state = _stage_previous(key, legacy_raw, legacy_tbl)
            publication_states.append(state)
            raw_cols = [
                row[0] for row in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
                    [shared_raw],
                ).fetchall() if row[0] != "__row_id"
            ]
            select = ", ".join(
                f"{quote_id(col)} AS {quote_id(f'RAW_{i + 1}')}" for i, col in enumerate(raw_cols)
            )
            if not select:
                raise RuntimeError(f"shared raw table has no columns for {artifact.table_key}")
            conn.execute(
                f"CREATE TABLE {quote_id(legacy_raw)} AS "
                f"SELECT {select} FROM {quote_id(shared_raw)} ORDER BY \"__row_id\""
            )
            # The existing rebuild operation preserves exact header-row and
            # data semantics, including uppercase/unique compatibility columns.
            rebuild_table_from_raw_table(conn, key, 0)
            if not _exists(legacy_tbl):
                raise RuntimeError(f"legacy table publication failed for {artifact.table_key}")
            set_meta(conn, f"rawArray__{key}", get_raw_array_from_table(conn, key, raw_preview_rows), commit=False)
            register_table(conn, key, legacy_tbl, commit=False)
            # Shared tables are implementation details and must not remain
            # visible once the compatibility publication is complete.
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_raw)}")
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_typed)}")
            published.append((key, legacy_raw, legacy_tbl))
            if on_progress:
                on_progress({
                    "stage": "file_loaded", "name": key,
                    "current": len(published), "elapsed": 0,
                })
        for state in publication_states:
            for backup in (state.get("raw_backup"), state.get("tbl_backup")):
                if backup and _exists(backup):
                    conn.execute(f"DROP TABLE IF EXISTS {quote_id(backup)}")
        conn.commit()
    except Exception as exc:
        logger.exception("Module 1 shared publication failed; rolling back")
        warnings.append({"file": getattr(result.source, "name", name), "message": str(exc)})
        # The shared sink commits its immutable tables before this bridge runs;
        # explicitly remove every artifact and compatibility row on failure so
        # a partial upload cannot become visible in the session inventory.
        for state in reversed(publication_states):
            try:
                _restore_previous(state)
            except Exception:
                logger.exception("failed to restore Module 1 publication for %s", state.get("key"))
        for artifact in result.tables:
            for table_name in _shared_table_names(artifact.table_key):
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
                except Exception:
                    pass
        conn.commit()
        published.clear()
    if on_progress:
        on_progress({"stage": "committed", "file_count": len(published)})
    return {}, warnings


def run_module1_shared_shadow(file_data: bytes) -> AdapterParityEvidence:
    """Isolated shadow execution with canonical evidence bound to the payload."""
    from shared.db import duckdb_connect
    conn = duckdb_connect(":memory:")
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        conn.execute("CREATE TABLE IF NOT EXISTS table_registry (table_key VARCHAR PRIMARY KEY, sql_name VARCHAR NOT NULL)")
        conn.commit()
        events: list[dict[str, Any]] = []
        result = run_module1_shared(conn, bytes(file_data), events.append)
        return AdapterParityEvidence(
            result,
            sha256(file_data).hexdigest(),
            _current_contract_version(),
            _canonical_snapshot(conn, result=result, events=events),
            tuple(events),
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _legacy_loader() -> LegacyLoader:
    # Import lazily so existing tests and callers can monkey-patch the legacy
    # function after importing this adapter.
    from data_loading import file_loader

    return file_loader.load_zip_to_session


def _discover_shadow_loader() -> ShadowLoader | None:
    """Find an optional shared runner without making it a hard dependency."""
    # Module 1's bridge is preferred over the generic bytes-only probe.  The
    # bridge owns the isolated DuckDB publication and canonical evidence needed
    # by the cutover gate.
    local_runner = globals().get("run_module1_shared_shadow")
    if callable(local_runner):
        return local_runner
    service = None
    for module_name in ("ingestion.service", "backend.ingestion.service"):
        try:
            service = __import__(module_name, fromlist=["service"])
            break
        except (ImportError, ModuleNotFoundError):
            continue
    if service is None:
        return None
    for name in ("run_module1_shared_shadow", "load_module1"):
        candidate = getattr(service, name, None)
        if callable(candidate):
            return candidate
    return None


def _call_shadow(loader: ShadowLoader, conn: Any, file_data: bytes) -> Any:
    """Call an isolated shadow runner.

    A bytes-only callable is inherently isolated from the production session.
    Connection-taking callables are rejected: passing Module 1's live DuckDB
    connection would make an observational run capable of mutating production
    state.  A future service can expose an isolated temporary connection while
    retaining a bytes-only adapter callable.
    """
    try:
        params = list(inspect.signature(loader).parameters.values())
    except (TypeError, ValueError):
        params = []
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    if len(positional) >= 2:
        raise RuntimeError("shadow loader must accept file bytes only; connection-taking runners are unsafe")
    return loader(file_data)


def _call_cutover(loader: ShadowLoader, conn: Any, file_data: bytes,
                  on_progress: Callable[[dict], None] | None) -> Any:
    """Invoke a shared loader after parity has unlocked cut-over."""
    try:
        params = list(inspect.signature(loader).parameters.values())
    except (TypeError, ValueError):
        params = []
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    if len(positional) >= 3:
        return loader(conn, file_data, on_progress)
    if len(positional) >= 2:
        return loader(conn, file_data)
    return loader(file_data)


class Module1IngestionAdapter:
    """Thin adapter preserving ``load_zip_to_session`` compatibility."""

    def __init__(self, *, legacy_loader: LegacyLoader | None = None,
                 shadow_loader: ShadowLoader | None = None,
                 cutover_loader: ShadowLoader | None = None,
                 config: Module1AdapterConfig | None = None) -> None:
        self.legacy_loader = legacy_loader
        self.shadow_loader = shadow_loader
        self.cutover_loader = cutover_loader
        self.config = config
        self.last_report: ParityReport | None = None

    def load_zip_to_session(self, conn: Any, file_data: bytes,
                            on_progress: Callable[[dict], None] | None = None) -> Any:
        config = self.config or Module1AdapterConfig.from_env()
        shadow_result: Any = None
        loader = self.shadow_loader or _shadow_loader
        # A cut-over request still needs a shadow run when no prior evidence is
        # present.  The first call therefore remains legacy by construction.
        shadow_available = config.shadow_enabled or config.gates_enabled
        if shadow_available and loader is None:
            loader = _discover_shadow_loader()
        cutover_loader = self.cutover_loader or _cutover_loader or loader
        if config.gates_enabled and self.last_report and self.last_report.matched and cutover_loader is not None:
            current_payload = sha256(file_data).hexdigest()
            evidence_matches = (
                self.last_report.payload_sha256 == current_payload
                and self.last_report.adapter_compatible
                and self.last_report.contract_version == _current_contract_version()
                and self.last_report.postconditions_passed
            )
            if evidence_matches:
                try:
                    before_publication = _canonical_snapshot(conn)
                    cutover_result = _call_cutover(cutover_loader, conn, file_data, on_progress)
                    # Never expose a shared IngestionResult through the legacy
                    # route.  Cut-over runners must return the tuple contract.
                    after_publication = _canonical_snapshot(conn, result=cutover_result)
                    publication_changed = before_publication.get("tables") != after_publication.get("tables")
                    if (
                        isinstance(cutover_result, tuple) and len(cutover_result) == 2
                        and _module1_postconditions(conn, cutover_result)
                        and publication_changed
                    ):
                        return cutover_result
                except Exception:
                    logger.exception("Module 1 shared ingestion cut-over failed; falling back to legacy")
                    if config.fail_on_mismatch:
                        raise

        if shadow_available and loader is not None:
            try:
                # No callback: shadow progress must never duplicate public SSE.
                shadow_result = _call_shadow(loader, conn, file_data)
            except Exception as exc:
                logger.exception("Module 1 ingestion shadow run failed")
                if config.fail_on_mismatch:
                    raise RuntimeError("Module 1 ingestion shadow run failed") from exc

        legacy = self.legacy_loader or _legacy_loader()
        legacy_events: list[dict[str, Any]] = []

        def _legacy_progress(event: dict[str, Any]) -> None:
            # Capture the exact public SSE milestones for parity while still
            # forwarding the same callback to the route.
            if isinstance(event, dict):
                legacy_events.append(dict(event))
            if on_progress is not None:
                on_progress(event)

        result = legacy(conn, file_data, on_progress=_legacy_progress)

        if shadow_available and loader is not None and shadow_result is not None:
            contract_version = None
            explicit_evidence = False
            shadow_canonical = None
            evidence_hash = sha256(file_data).hexdigest()
            if isinstance(shadow_result, AdapterParityEvidence):
                explicit_evidence = True
                contract_version = shadow_result.contract_version
                shadow_canonical = shadow_result.canonical
                if shadow_result.payload_sha256 != evidence_hash:
                    logger.warning("Module 1 shadow evidence payload hash does not match upload")
                    self.last_report = None
                    shadow_result = None
                else:
                    shadow_result = shadow_result.result
            if shadow_result is not None:
                self.last_report = compare_parity(
                    result, shadow_result, gate_enabled=config.parity_gate_enabled,
                    payload_sha256=evidence_hash, contract_version=contract_version,
                    explicit_evidence=explicit_evidence,
                    postconditions_passed=_module1_postconditions(conn, result),
                    canonical_legacy=(
                        _canonical_snapshot(conn, result=result, events=legacy_events)
                        if shadow_canonical is not None else None
                    ),
                    canonical_shadow=shadow_canonical,
                )
            global _last_report
            _last_report = self.last_report
            if not self.last_report.matched:
                logger.warning("Module 1 ingestion shadow parity mismatch: %s", self.last_report.differences)
                if config.fail_on_mismatch:
                    raise RuntimeError("Module 1 ingestion shadow parity gate failed")
        return result


_default_adapter = Module1IngestionAdapter()


def load_zip_to_session(conn: Any, file_data: bytes,
                        on_progress: Callable[[dict], None] | None = None) -> Any:
    """Route-compatible function used by Module 1's upload endpoint."""
    return _default_adapter.load_zip_to_session(conn, file_data, on_progress=on_progress)


__all__ = [
    "AdapterParityEvidence", "Module1AdapterConfig", "Module1IngestionAdapter", "ParityReport",
    "compare_parity", "configure_shadow_loader", "configure_cutover_loader",
    "get_last_parity_report",
    "load_zip_to_session",
]
