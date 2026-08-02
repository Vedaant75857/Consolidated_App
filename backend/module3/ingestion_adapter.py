"""Module 3 compatibility adapter for the shared ingestion pipeline.

The adapter deliberately leaves the existing upload loader as the default.  A
shared runner can be installed by startup code (or discovered lazily once the
shared service is available) and shadowed for parity checks.  Selecting that
runner requires all of the following explicit evidence:

* the Module 3 parity gate is enabled and the current legacy/shadow results
  match;
* the Module 1 gate has passed; and
* the Module 2 gate has passed (which, by definition, also requires Module 1).

This keeps the public Module 3 upload JSON and the legacy rollback path stable
while the shared implementation is still being brought online.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping

try:
    from backend.ingestion.models import CONTRACT_VERSION
    from backend.ingestion.transfer import CutoverMode as CentralCutoverMode
    from backend.ingestion.duckdb_sink import quote_identifier as _quote_identifier
except ImportError:  # pragma: no cover - standalone module3 launcher
    from ingestion.models import CONTRACT_VERSION
    from ingestion.transfer import CutoverMode as CentralCutoverMode
    from ingestion.duckdb_sink import quote_identifier as _quote_identifier

logger = logging.getLogger(__name__)

LegacyLoader = Callable[..., Any]
ShadowLoader = Callable[..., Any]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _env_bool_any(names: tuple[str, ...], default: bool = False) -> bool:
    """Read the first configured alias, keeping deployment names flexible."""
    for name in names:
        if os.getenv(name) is not None:
            return _env_bool(name, default)
    return default


@dataclass(frozen=True)
class Module3AdapterConfig:
    """Runtime flags for shadowing and parity-gated cutover.

    All flags default to ``False``.  In particular, setting cutover alone is
    insufficient: a current matching parity report and both predecessor gates
    are still required.
    """

    shadow_enabled: bool = False
    cutover_enabled: bool = False
    parity_gate_enabled: bool = False
    fail_on_mismatch: bool = False
    predecessor_m1_gate: bool = False
    predecessor_m2_gate: bool = False
    cutover_gate: "CutoverGate | None" = None

    @property
    def predecessor_gates_passed(self) -> bool:
        # M2 is only meaningful after M1; requiring both also makes ordering
        # explicit instead of allowing a direct M3 cutover.
        if self.cutover_gate is not None:
            if hasattr(self.cutover_gate, "mode") and hasattr(self.cutover_gate, "evidence"):
                try:
                    mode = self.cutover_gate.mode("module3")
                    evidence = self.cutover_gate.evidence.get("module3")
                    return mode is CentralCutoverMode.TYPED and bool(evidence and evidence.passed)
                except Exception:
                    return False
            return self.cutover_gate.verified and self.cutover_gate.m1_passed and self.cutover_gate.m2_passed
        # Boolean predecessor flags are retained for backwards-compatible
        # construction in tests, but are never populated from environment
        # variables (operator flags are not parity evidence).
        return self.predecessor_m1_gate and self.predecessor_m2_gate

    @classmethod
    def from_env(cls) -> "Module3AdapterConfig":
        return cls(
            shadow_enabled=_env_bool("MODULE3_INGESTION_SHADOW", False),
            cutover_enabled=_env_bool("MODULE3_INGESTION_CUTOVER", False),
            parity_gate_enabled=_env_bool("MODULE3_INGESTION_PARITY_GATE", False),
            fail_on_mismatch=_env_bool("MODULE3_INGESTION_SHADOW_STRICT", False),
            predecessor_m1_gate=False,
            predecessor_m2_gate=False,
        )


@dataclass(frozen=True)
class CutoverGate:
    """Verified predecessor evidence supplied by orchestration code."""

    m1_passed: bool
    m2_passed: bool
    verified: bool = False


@dataclass(frozen=True)
class ParityReport:
    """Deterministic comparison of legacy and shared loader results."""

    legacy_digest: str
    shadow_digest: str
    matched: bool
    gate_passed: bool
    differences: tuple[str, ...] = ()
    predecessor_gates_passed: bool = False
    source_sha256: str = ""
    operation: str = ""
    session_id: str | None = None
    contract_version: str = CONTRACT_VERSION

    @property
    def cutover_allowed(self) -> bool:
        # ``gate_passed`` records current legacy/shared parity (matching the
        # Module 1 adapter contract); predecessor evidence is an additional
        # cutover-only guard for Module 3.
        return self.gate_passed and self.predecessor_gates_passed


_shadow_loader: ShadowLoader | None = None
_last_report: ParityReport | None = None


def configure_shadow_loader(loader: ShadowLoader | None) -> None:
    """Install a shared runner for the current process (primarily startup/tests)."""
    global _shadow_loader
    _shadow_loader = loader


def get_last_parity_report() -> ParityReport | None:
    return _last_report


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": bytes(value).hex()}
    if isinstance(value, Mapping):
        return {
            str(k): _jsonable(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_jsonable(v) for v in value]
    if isinstance(value, set):
        return sorted((_jsonable(v) for v in value), key=repr)
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable({name: getattr(value, name) for name in value.__dataclass_fields__})
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return repr(value)


def _digest(value: Any) -> str:
    encoded = json.dumps(
        _jsonable(value), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compare_parity(
    legacy_result: Any,
    shadow_result: Any,
    *,
    gate_enabled: bool = False,
    predecessor_gates: tuple[bool, bool] = (False, False),
    source_sha256: str = "",
    operation: str = "",
    session_id: str | None = None,
) -> ParityReport:
    """Compare loader results and require ordered M1/M2 gates for cutover."""
    legacy_digest = _digest(legacy_result)
    shadow_digest = _digest(shadow_result)
    matched = legacy_digest == shadow_digest
    predecessor_passed = bool(predecessor_gates[0] and predecessor_gates[1])
    differences = () if matched else ("deterministic loader result digest differs",)
    return ParityReport(
        legacy_digest=legacy_digest,
        shadow_digest=shadow_digest,
        matched=matched,
        gate_passed=bool(gate_enabled and matched),
        differences=differences,
        predecessor_gates_passed=predecessor_passed,
        source_sha256=source_sha256,
        operation=operation,
        session_id=session_id,
    )


def _legacy_loader(name: str) -> LegacyLoader:
    # Lazy import preserves monkey-patching of services.upload.file_loader in
    # existing tests and keeps startup independent of shared-ingestion extras.
    from services.upload import file_loader

    return getattr(file_loader, name)


def _discover_shadow_loader() -> ShadowLoader | None:
    """Find an optional shared Module 3 runner without hard dependency."""
    try:
        from backend.ingestion import service  # type: ignore
    except (ImportError, ModuleNotFoundError):
        try:
            from ingestion import service  # type: ignore
        except (ImportError, ModuleNotFoundError):
            return None
    # ``ingestion.service.ingest(request)`` is a generic source reader and is
    # intentionally not discovered here: it has no Module 3 session writer
    # contract.  Only an explicit module adapter may be selected.
    for name in ("load_module3", "ingest_module3"):
        candidate = getattr(service, name, None)
        if callable(candidate):
            return candidate
    # The Module 3 adapter owns the compatibility publication bridge.  Keep
    # discovery lazy so importing this module never makes the shared pipeline
    # a hard dependency of the legacy-default path.
    return run_module3_shared


def _call_shadow(loader: ShadowLoader, operation: str, conn: Any,
                 filename: str | bytes, file_data: bytes | None = None,
                 *, isolated: bool = True) -> Any:
    """Call compatible shared runner shapes in an isolated context.

    In isolated mode the live Module 3 DuckDB connection is never passed to a
    shadow runner. Two-argument runners receive ``(filename, bytes)`` and
    three-argument runners receive ``(None, filename, bytes)``. A gated
    cutover may explicitly receive the live connection to commit shared data.
    """
    try:
        params = list(inspect.signature(loader).parameters.values())
    except (TypeError, ValueError):
        params = []
    positional = [
        p for p in params
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    # A future service may accept an operation keyword; support it only when
    # declared so old two-argument test doubles remain valid.
    names = {p.name for p in params}
    kwargs: dict[str, Any] = {"operation": operation} if "operation" in names else {}
    if file_data is None:
        args = (filename,) if len(positional) <= 1 else (filename,)
    elif len(positional) >= 3:
        args = ((None if isolated else conn), filename, file_data)
    elif len(positional) == 2:
        args = (filename, file_data) if isolated else (conn, file_data)
    else:
        args = (file_data,)
    return loader(*args, **kwargs)


def _shared_postconditions(result: Any, conn: Any) -> bool:
    """Require stable table keys and Module 3 column metadata after cutover."""
    if isinstance(result, Mapping):
        keys = result.get("table_keys") or result.get("tableKeys") or result.get("tables")
    elif isinstance(result, (tuple, list)) and result:
        keys = result[0]
    else:
        return False
    if not isinstance(keys, (tuple, list)) or any(not isinstance(k, str) or not k for k in keys):
        return False
    # If the connection exposes SQL, verify metadata exists and uses stable
    # COL_n keys. Lightweight test doubles need only satisfy the result shape.
    if hasattr(conn, "execute"):
        try:
            registry = conn.execute("SELECT table_key, data_table, raw_table FROM _table_registry").fetchall()
            registry_by_key = {str(row[0]): (str(row[1]), str(row[2])) for row in registry}
            if any(key not in registry_by_key for key in keys):
                return False
            for key in keys:
                data_table, raw_table = registry_by_key[key]
                exists = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_name IN (?, ?)",
                    (data_table, raw_table),
                ).fetchall()
                if len(exists) != 2:
                    return False
                data_count = int(conn.execute(f'SELECT COUNT(*) FROM {_quote_identifier(data_table)}').fetchone()[0])
                raw_count = int(conn.execute(f'SELECT COUNT(*) FROM {_quote_identifier(raw_table)}').fetchone()[0])
                # Raw/typed shared tables retain the source header and blank
                # rows; the Module 3 working table intentionally omits blank
                # rows.  Therefore raw count is a lower-bound relationship,
                # not an exact ``data + 1`` equality.
                if raw_count < data_count + 1:
                    return False
                metadata = conn.execute(
                    "SELECT column_key, column_order, display_name, data_type FROM _column_metadata WHERE table_key = ? ORDER BY column_order",
                    (key,),
                ).fetchall()
                if not metadata or metadata[0][0] != "RECORD_ID":
                    return False
                for idx, row in enumerate(metadata[1:]):
                    if row[0] != f"COL_{idx}" or row[1] != idx or not row[2] or not row[3]:
                        return False
        except Exception:
            return False
    return True


def verify_typed_artifact(raw_manifest: Any, payload: bytes) -> Any:
    try:
        from backend.ingestion import parse_transport_manifest
    except ImportError:  # pragma: no cover
        from ingestion import parse_transport_manifest
    manifest = parse_transport_manifest(raw_manifest, payload)
    if manifest.source_module not in {"module1", "module2"}:
        raise ValueError("typed transfer source module is not allowed for Module 3")
    if manifest.destination_module != "module3" or not manifest.verify_payload(payload):
        raise ValueError("typed transfer manifest/payload verification failed")
    return manifest


def import_typed_artifact_to_session(conn: Any, payload: bytes, raw_manifest: Any) -> tuple[list[str], list[dict[str, str]]]:
    """Verify and materialize a typed Parquet transfer into Module 3 tables.

    The shared helper owns payload decoding and checksum validation.  This
    adapter owns Module 3's registry, raw-preview compatibility, and stable
    ``COL_n`` metadata.  No session publication occurs until all tables load.
    """
    try:
        from backend.ingestion import parse_transport_manifest, import_duckdb_parquet
    except ImportError:  # pragma: no cover - standalone launcher
        from ingestion import parse_transport_manifest, import_duckdb_parquet
    manifest = verify_typed_artifact(raw_manifest, payload)
    if len(manifest.tables) != 1:
        raise ValueError("Module 3 typed import requires exactly one table per payload")
    from services.upload.file_loader import _register_table, _store_column_metadata, _ensure_registry
    try:
        from backend.ingestion.duckdb_sink import quote_identifier
    except ImportError:  # pragma: no cover
        from ingestion.duckdb_sink import quote_identifier
    _ensure_registry(conn)
    table_keys: list[str] = []
    warnings: list[dict[str, str]] = []
    for index, transfer_table in enumerate(manifest.tables):
        key = transfer_table.table_key
        safe = re.sub(r"[^A-Za-z0-9_]", "_", key)[:80] or f"table_{index}"
        suffix = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
        stage = f"_typed_stage_{index}_{safe}_{suffix}"
        data_table = f"data__{safe}_{suffix}"
        raw_table = f"raw__{safe}_{suffix}"
        existing = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name IN (?, ?, ?)",
            (stage, data_table, raw_table),
        ).fetchall()
        if existing:
            raise ValueError("typed destination table already exists")
        imported = import_duckdb_parquet(conn, payload, manifest, stage)
        if imported is None:
            raise ValueError(f"typed table {key!r} was not materialized")
        columns = [row[0] for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position", (stage,)
        ).fetchall()]
        if not columns:
            raise ValueError(f"typed table {key!r} has no columns")
        if len(columns) != len(set(columns)):
            raise ValueError("typed transfer contains duplicate columns")
        if any(str(column).upper() == "RECORD_ID" for column in columns):
            raise ValueError("typed transfer must not provide reserved RECORD_ID column")
        qstage, qdata, qraw = map(quote_identifier, (stage, data_table, raw_table))
        quoted = ", ".join(quote_identifier(column) for column in columns)
        conn.execute(f'CREATE TABLE {qdata} AS SELECT CAST(ROW_NUMBER() OVER () AS VARCHAR) AS RECORD_ID, {quoted} FROM {qstage}')
        conn.execute(f'DROP TABLE {qstage}')
        headers = [column.display_name for column in transfer_table.columns]
        if len(headers) != len(columns):
            headers = columns
        raw_values = ", ".join(f'? AS "RAW_{idx}"' for idx in range(len(columns)))
        conn.execute(f'CREATE TABLE {qraw} AS SELECT {raw_values} WHERE FALSE', headers)
        conn.execute(f'INSERT INTO {qraw} SELECT {raw_values}', headers)
        conn.execute(f'INSERT INTO {qraw} SELECT {", ".join(f'CAST({quote_identifier(column)} AS VARCHAR) AS "RAW_{idx}"' for idx, column in enumerate(columns))} FROM {qdata}')
        try:
            _register_table(conn, key, data_table, raw_table)
            _store_column_metadata(conn, key, data_table, headers)
        except Exception:
            conn.execute(f'DROP TABLE IF EXISTS {qdata}')
            conn.execute(f'DROP TABLE IF EXISTS {qraw}')
            try:
                conn.execute("DELETE FROM _table_registry WHERE table_key = ?", (key,))
                conn.execute("DELETE FROM _column_metadata WHERE table_key = ?", (key,))
            except Exception:
                pass
            conn.commit()
            raise
        table_keys.append(key)
    conn.commit()
    return table_keys, warnings


def _module3_legacy_key(shared_key: str, root_name: str) -> str:
    """Map a shared source identity to Module 3's stable upload key.

    The shared service keeps complete archive paths in ``table_key`` values;
    Module 3 historically exposed only the member basename (plus a sheet
    suffix).  Display headers remain metadata and never participate in this
    mapping.
    """
    value = str(shared_key)
    # Excel readers append ``:<sheet>``.  A sheet name may contain dots, so
    # identify the last path component that has a known workbook extension.
    excel_exts = (".xls", ".xlsx", ".xlsm", ".xlsb", ".xltx", ".xltm", ".ods")
    pieces = value.split(":")
    file_index = None
    for index, piece in enumerate(pieces):
        lowered = piece.lower()
        if any(lowered.endswith(ext) or f"/" in lowered and lowered.rsplit("/", 1)[-1].endswith(ext) for ext in excel_exts):
            file_index = index
    if file_index is not None:
        filename = pieces[file_index].replace("\\", "/").rsplit("/", 1)[-1]
        suffix = ":".join(pieces[file_index + 1:])
        return f"{filename}::{suffix}" if suffix else f"{filename}::"
    # Delimited members have no sheet identity.
    candidate = value.replace("\\", "/").rsplit("/", 1)[-1]
    if candidate and "." in candidate:
        return f"{candidate}::"
    base = os.path.basename(root_name or "upload.csv")
    return f"{base}::"


def _module3_shared_table_names(table_key: str) -> tuple[str, str]:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(table_key)).strip("_") or "table"
    digest = hashlib.sha256(str(table_key).encode("utf-8")).hexdigest()[:12]
    base = f"{safe[:48]}_{digest}"
    return f"{base}__raw", f"{base}__typed"


def _module3_warning_dicts(result: Any) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for item in getattr(result, "issues", ()):
        code = getattr(getattr(item, "code", None), "value", getattr(item, "code", None))
        severity = getattr(getattr(item, "severity", None), "value", getattr(item, "severity", "warning"))
        warnings.append({
            "file": getattr(item, "source", None),
            "message": getattr(item, "message", str(item)),
            "code": code,
            "severity": severity,
        })
    return warnings


def run_module3_shared(conn: Any, filename: str, file_data: bytes, *, operation: str | None = None) -> tuple[list[str], list[dict[str, Any]]]:
    """Run shared ingestion and publish Module 3's legacy-compatible tables.

    Shared raw/typed sink tables are staged internally, then translated into
    Module 3's ``_table_registry`` and ``_column_metadata`` contracts.  The
    bridge intentionally returns the existing ``(table_keys, warnings)`` DTO;
    callers may keep the legacy route and playground code unchanged.
    """
    try:
        from backend.ingestion.models import IngestionRequest, SourceAsset
        from backend.ingestion.service import IngestionService
    except ImportError:  # pragma: no cover - standalone module3 launcher
        from ingestion.models import IngestionRequest, SourceAsset
        from ingestion.service import IngestionService
    try:
        from backend.module3.services.upload.file_loader import (
            _clean_header, _dedupe_headers, _ensure_registry, _register_table,
            _store_column_metadata,
        )
    except ImportError:  # pragma: no cover - standalone module3 launcher
        from services.upload.file_loader import (
            _clean_header, _dedupe_headers, _ensure_registry, _register_table,
            _store_column_metadata,
        )
    try:
        from backend.ingestion.duckdb_sink import quote_identifier
    except ImportError:  # pragma: no cover
        from ingestion.duckdb_sink import quote_identifier

    owned_connection = conn is None
    if owned_connection:
        try:
            import duckdb
            conn = duckdb.connect(":memory:")
        except ImportError as exc:  # pragma: no cover - deployment issue
            raise RuntimeError("duckdb is required for shared Module 3 ingestion") from exc
    payload = bytes(file_data)
    request = IngestionRequest(
        SourceAsset(
            str(filename or "upload.csv"),
            size_bytes=len(payload),
            # Keep the uploaded basename in shared child identities so the
            # adapter can reconstruct Module 3's historical ``file::sheet``
            # keys for workbooks and archive members.
            source_id=os.path.basename(str(filename or "upload.csv")),
        ),
        module="module3",
    )
    result = IngestionService(sink_connection=conn).ingest(request, source=payload)
    warnings = _module3_warning_dicts(result)
    _ensure_registry(conn)
    published: list[str] = []
    created: list[str] = []
    try:
        for artifact in result.tables:
            shared_raw, shared_typed = _module3_shared_table_names(artifact.table_key)
            for table_name in (shared_raw, shared_typed):
                exists = conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", (table_name,)
                ).fetchone()[0]
                if not exists:
                    raise RuntimeError(f"shared ingestion table missing: {table_name}")
            key = _module3_legacy_key(artifact.table_key, filename)
            # Legacy names are retained for downstream SQL and playground
            # operations.  The shared key is only used for staging identity.
            key_file, _, key_sheet = key.partition("::")
            safe_identity = os.path.splitext(os.path.basename(key_file))[0]
            if key_sheet:
                safe_identity = f"{safe_identity}__{key_sheet}"
            safe = re.sub(r"[^A-Za-z0-9_]", "_", safe_identity)[:120]
            safe = safe or "table"
            data_table, raw_table = f"data__{safe}", f"raw__{safe}"
            for table_name in (data_table, raw_table):
                if conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", (table_name,)
                ).fetchone()[0]:
                    raise ValueError(f"destination table already exists: {table_name}")
            physical = [str(column.physical_name or column.key) for column in artifact.columns]
            displays = [str(column.display_name or f"COL_{idx + 1}") for idx, column in enumerate(artifact.columns)]
            if not physical:
                raise ValueError(f"shared table {artifact.table_key!r} has no columns")
            qraw = quote_identifier(shared_raw)
            qtyped = quote_identifier(shared_typed)
            # Raw compatibility table preserves the complete source including
            # the first/header row, with the historical RAW_n column names.
            raw_select = ", ".join(
                f"{quote_identifier(column)} AS {quote_identifier(f'RAW_{idx}')}"
                for idx, column in enumerate(physical)
            )
            conn.execute(
                f"CREATE TABLE {quote_identifier(raw_table)} AS "
                f"SELECT {raw_select} FROM {qraw} ORDER BY {quote_identifier('__row_id')}"
            )
            created.append(raw_table)
            # Keep typed values in the working table, but apply Module 3's
            # explicit header semantics and omit the source header/all-blank
            # rows just as the legacy loader does.
            headers = _dedupe_headers([_clean_header(value) for value in displays])
            keep = [(idx, header) for idx, header in enumerate(headers) if header != "RECORD_ID"]
            if not keep:
                raise ValueError(f"shared table {artifact.table_key!r} has no visible columns")
            nonempty = " AND ".join(
                f"({quote_identifier(physical[idx])} IS NULL OR TRIM(CAST({quote_identifier(physical[idx])} AS VARCHAR)) = '')"
                for idx, _ in keep
            )
            data_select = ", ".join(
                f"{quote_identifier(physical[idx])} AS {quote_identifier(header)}"
                for idx, header in keep
            )
            conn.execute(
                f"CREATE TABLE {quote_identifier(data_table)} AS SELECT "
                f"CAST(ROW_NUMBER() OVER () AS VARCHAR) AS {quote_identifier('RECORD_ID')}, {data_select} "
                f"FROM {qtyped} WHERE {quote_identifier('__row_id')} > 0 AND NOT ({nonempty}) "
                f"ORDER BY {quote_identifier('__row_id')}"
            )
            created.append(data_table)
            display_headers = [displays[idx] for idx, _ in keep]
            _register_table(conn, key, data_table, raw_table)
            _store_column_metadata(conn, key, data_table, display_headers)
            published.append(key)
            conn.execute(f"DROP TABLE IF EXISTS {qraw}")
            conn.execute(f"DROP TABLE IF EXISTS {qtyped}")
        conn.commit()
    except Exception:
        logger.exception("Module 3 shared publication failed; rolling back")
        for table_name in created:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")
            except Exception:
                pass
        for artifact in result.tables:
            for table_name in _module3_shared_table_names(artifact.table_key):
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)}")
                except Exception:
                    pass
        cleanup_keys = set(published)
        cleanup_keys.update(_module3_legacy_key(artifact.table_key, filename) for artifact in result.tables)
        for key in cleanup_keys:
            try:
                conn.execute("DELETE FROM _table_registry WHERE table_key = ?", (key,))
                conn.execute("DELETE FROM _column_metadata WHERE table_key = ?", (key,))
            except Exception:
                pass
        conn.commit()
        raise
    if owned_connection:
        try:
            conn.close()
        except Exception:
            pass
    return published, warnings


class Module3IngestionAdapter:
    """Thin adapter preserving Module 3 loader tuple contracts."""

    def __init__(
        self,
        *,
        legacy_zip_loader: LegacyLoader | None = None,
        legacy_single_loader: LegacyLoader | None = None,
        shadow_loader: ShadowLoader | None = None,
        config: Module3AdapterConfig | None = None,
    ) -> None:
        self.legacy_zip_loader = legacy_zip_loader
        self.legacy_single_loader = legacy_single_loader
        self.shadow_loader = shadow_loader
        self.config = config
        self.last_report: ParityReport | None = None
        self._reports: dict[tuple[str, str, str | None], ParityReport] = {}

    def _dispatch(self, operation: str, conn: Any, filename: str,
                  file_data: bytes, legacy: LegacyLoader,
                  session_id: str | None = None) -> Any:
        config = self.config or Module3AdapterConfig.from_env()
        loader = self.shadow_loader or _shadow_loader
        source_sha256 = hashlib.sha256(file_data).hexdigest()
        # A cutover is only allowed from parity evidence collected on a prior
        # request.  The first request always executes legacy after its isolated
        # shadow comparison, preventing a same-request double write.
        evidence = self._reports.get((source_sha256, operation, session_id))
        prior_cutover_evidence = bool(
            config.cutover_enabled
            and config.parity_gate_enabled
            and config.predecessor_gates_passed
            and evidence is not None
            and evidence.cutover_allowed
        )
        shadow_result: Any = None
        shadow_available = config.shadow_enabled or config.cutover_enabled
        if shadow_available and loader is None:
            loader = _discover_shadow_loader()
        if prior_cutover_evidence and loader is not None:
            try:
                # Shared module runners must stage/commit their own isolated
                # destination writes; no legacy write is performed on cutover.
                shared_result = _call_shadow(loader, operation, conn, filename, file_data, isolated=False)
                if not _shared_postconditions(shared_result, conn):
                    raise RuntimeError("shared Module 3 postcondition check failed")
                return shared_result
            except Exception:
                logger.exception("Module 3 ingestion cutover failed; using legacy loader")
                return legacy(conn, file_data) if operation == "zip" else legacy(conn, filename, file_data)

        if shadow_available and loader is not None:
            try:
                # Shadow runs do not receive a progress callback and are
                # observational until every gate passes.
                shadow_result = _call_shadow(loader, operation, conn, filename, file_data)
            except Exception:
                logger.exception("Module 3 ingestion shadow run failed")
                if config.cutover_enabled:
                    loader = None

        if operation == "zip":
            legacy_result = legacy(conn, file_data)
        else:
            legacy_result = legacy(conn, filename, file_data)
        if shadow_result is not None:
            self.last_report = compare_parity(
                legacy_result,
                shadow_result,
                gate_enabled=config.parity_gate_enabled,
                predecessor_gates=(
                    (config.predecessor_m1_gate, config.predecessor_m2_gate)
                    if config.cutover_gate is None
                    else (config.predecessor_gates_passed, config.predecessor_gates_passed)
                ),
                source_sha256=source_sha256,
                operation=operation,
                session_id=session_id,
            )
            global _last_report
            _last_report = self.last_report
            self._reports[(source_sha256, operation, session_id)] = self.last_report
            if not self.last_report.matched:
                logger.warning("Module 3 ingestion shadow parity mismatch: %s", self.last_report.differences)
                if config.fail_on_mismatch:
                    raise RuntimeError("Module 3 ingestion shadow parity gate failed")
        return legacy_result

    def load_zip_to_session(self, conn: Any, file_data: bytes, session_id: str | None = None) -> Any:
        return self._dispatch(
            "zip", conn, "upload.zip", file_data,
            self.legacy_zip_loader or _legacy_loader("load_zip_to_session"), session_id,
        )

    def load_single_file(self, conn: Any, filename: str, file_data: bytes, session_id: str | None = None) -> Any:
        return self._dispatch(
            "single", conn, filename, file_data,
            self.legacy_single_loader or _legacy_loader("load_single_file"), session_id,
        )


_default_adapter = Module3IngestionAdapter()


def load_zip_to_session(conn: Any, file_data: bytes, session_id: str | None = None) -> Any:
    return _default_adapter.load_zip_to_session(conn, file_data, session_id=session_id)


def load_single_file(conn: Any, filename: str, file_data: bytes, session_id: str | None = None) -> Any:
    return _default_adapter.load_single_file(conn, filename, file_data, session_id=session_id)


__all__ = [
    "Module3AdapterConfig", "CutoverGate", "Module3IngestionAdapter", "ParityReport",
    "compare_parity", "configure_shadow_loader", "get_last_parity_report",
    "load_zip_to_session", "load_single_file", "run_module3_shared",
]
