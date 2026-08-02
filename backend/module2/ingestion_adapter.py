"""Compatibility boundary for Module 2 shared ingestion.

Module 2 deliberately keeps its existing loader and upload DTO as the source
of truth until the shared pipeline has demonstrated parity.  The adapter is
small enough to be used by the route without moving session ownership or
normalisation logic into ``backend/ingestion``.

There are two independent safety requirements for a cut-over:

* Module 2 must have an explicit, persisted parity-evidence flag; and
* Module 1 (the predecessor in the migration order) must have passed its own
  parity gate.

Both are disabled by default.  A missing shared reader, an unsupported source,
or a shared-runner exception always falls back to the caller-supplied legacy
loader unless strict shadow mode is requested.
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

import pandas as pd

logger = logging.getLogger(__name__)

LegacyLoader = Callable[[Any, str, bytes, list[dict]], Any]
SharedLoader = Callable[..., Any]
CONTRACT_VERSION = "ingestion.v1"


def _shared_contract_version() -> str:
    for module_name in ("backend.ingestion.models", "ingestion.models"):
        try:
            return str(getattr(__import__(module_name, fromlist=["CONTRACT_VERSION"]), "CONTRACT_VERSION"))
        except (ImportError, ModuleNotFoundError, AttributeError):
            continue
    return CONTRACT_VERSION


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "enabled", "passed"}


@dataclass(frozen=True)
class Module2AdapterConfig:
    """Runtime controls for shadowing and cut-over.

    ``predecessor_gate_enabled`` and ``parity_evidence`` intentionally do not
    default from the cut-over flag.  Operators must opt into each gate
    explicitly, preventing an accidental environment-only migration.
    """

    shadow_enabled: bool = False
    cutover_enabled: bool = False
    parity_gate_enabled: bool = False
    predecessor_gate_enabled: bool = False
    parity_evidence: bool = False
    fail_on_mismatch: bool = False
    # Shared CutoverGate state is supplied by application startup.  A missing
    # gate deliberately fails closed; environment flags cannot represent a
    # predecessor's typed state.
    cutover_gate: Any | None = None

    @classmethod
    def from_env(cls) -> "Module2AdapterConfig":
        return cls(
            shadow_enabled=_env_bool("MODULE2_INGESTION_SHADOW", False),
            cutover_enabled=_env_bool("MODULE2_INGESTION_CUTOVER", False),
            parity_gate_enabled=_env_bool("MODULE2_INGESTION_PARITY_GATE", False),
            predecessor_gate_enabled=(
                _env_bool("MODULE2_INGESTION_PREDECESSOR_GATE", False)
            ),
            parity_evidence=_env_bool("MODULE2_INGESTION_PARITY_EVIDENCE", False),
            fail_on_mismatch=_env_bool("MODULE2_INGESTION_SHADOW_STRICT", False),
        )

    @property
    def gates_enabled(self) -> bool:
        flags = bool(
            self.cutover_enabled
            and self.parity_gate_enabled
            and self.predecessor_gate_enabled
            and self.parity_evidence
        )
        if not flags or self.cutover_gate is None:
            return False
        try:
            from backend.ingestion import CutoverMode
        except (ImportError, ModuleNotFoundError):
            try:
                from ingestion import CutoverMode
            except (ImportError, ModuleNotFoundError):
                return False
        try:
            # Module 2 may only follow a predecessor that is actually typed,
            # with passing evidence recorded in the shared gate.
            predecessor_mode = self.cutover_gate.mode("module1")
            return bool(
                getattr(predecessor_mode, "value", predecessor_mode) == CutoverMode.TYPED.value
                and self.cutover_gate.evidence.get("module1") is not None
                and self.cutover_gate.evidence["module1"].passed
            )
        except (AttributeError, KeyError, ValueError):
            return False


@dataclass(frozen=True)
class ParityReport:
    legacy_digest: str
    shared_digest: str
    matched: bool
    gate_passed: bool
    differences: tuple[str, ...] = ()
    source_digest: str | None = None
    session_id: str | None = None
    contract_version: str | None = None

    @property
    def cutover_allowed(self) -> bool:
        return bool(
            self.gate_passed and self.source_digest and self.session_id
            and self.contract_version
        )

    def matches_context(self, data: bytes, session_id: str | None,
                        contract_version: str) -> bool:
        return bool(
            self.cutover_allowed
            and self.source_digest == hashlib.sha256(data).hexdigest()
            and self.session_id == session_id
            and self.contract_version == contract_version
        )


_shared_loader: SharedLoader | None = None
_cutover_loader: SharedLoader | None = None
_last_report: ParityReport | None = None


def configure_shared_loader(loader: SharedLoader | None) -> None:
    """Install a shared runner (primarily for startup wiring and tests)."""
    global _shared_loader
    _shared_loader = loader


def configure_cutover_loader(loader: SharedLoader | None) -> None:
    """Install the explicit production runner used after parity unlock."""
    global _cutover_loader
    _cutover_loader = loader


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
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
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


def compare_parity(legacy_result: Any, shared_result: Any, *, gate_enabled: bool = False,
                   source_digest: str | None = None, session_id: str | None = None,
                   contract_version: str | None = None) -> ParityReport:
    legacy_digest = _digest(legacy_result)
    shared_digest = _digest(shared_result)
    matched = legacy_digest == shared_digest
    return ParityReport(
        legacy_digest=legacy_digest,
        shared_digest=shared_digest,
        matched=matched,
        gate_passed=bool(gate_enabled and matched),
        differences=() if matched else ("deterministic loader result digest differs",),
        source_digest=source_digest,
        session_id=session_id,
        contract_version=contract_version,
    )


def _inventory_items(result: Any) -> list[dict]:
    """Translate common shared-result shapes into Module 2's inventory DTO."""
    if getattr(result, "ok", True) is False:
        return []
    if isinstance(result, Mapping):
        if result.get("ok") is False:
            return []
        value = result.get("inventory")
        if isinstance(value, list):
            return [dict(item) for item in value if isinstance(item, Mapping)]
    if isinstance(result, (list, tuple)):
        return [dict(item) for item in result if isinstance(item, Mapping) and "table_key" in item]
    tables = getattr(result, "tables", None)
    if tables:
        items: list[dict] = []
        for table in tables:
            table_key = getattr(table, "table_key", None)
            if table_key:
                items.append({
                    "table_key": table_key,
                    "rows": int(getattr(table, "row_count", 0) or 0),
                    "cols": len(getattr(table, "columns", ()) or ()),
                })
        return items
    return []


def _discover_shared_loader() -> SharedLoader | None:
    """Discover an optional service without making shared ingestion required."""
    for module_name in ("backend.ingestion.service", "ingestion.service"):
        try:
            module = __import__(module_name, fromlist=["*"])
        except (ImportError, ModuleNotFoundError):
            continue
        # A generic ``ingest(request)`` is intentionally not discovered here:
        # Module 2 needs a session-aware compatibility wrapper and a generic
        # callable has no guarantee of writing its session tables/DTO.
        for name in ("load_module2", "ingest_module2", "module2_ingest"):
            candidate = getattr(module, name, None)
            if callable(candidate):
                return candidate
    return None


def _build_request(name: str, data: bytes, session_id: str | None = None) -> Any:
    """Build the shared service request lazily when its one-argument API is used."""
    for module_name in ("backend.ingestion.models", "ingestion.models"):
        try:
            module = __import__(module_name, fromlist=["IngestionRequest", "SourceAsset"])
            return module.IngestionRequest(
                source=module.SourceAsset(name=name, size_bytes=len(data), source_id=f"module2:{name}"),
                module="module2", session_id=session_id,
            )
        except (ImportError, ModuleNotFoundError):
            continue
    return data


def _call_loader(loader: SharedLoader, conn: Any, name: str, data: bytes,
                 session_id: str | None = None, *, allow_conn: bool = True) -> Any:
    """Call supported injected runner shapes without imposing one API."""
    try:
        params = list(inspect.signature(loader).parameters.values())
    except (TypeError, ValueError):
        params = []
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    if len(positional) >= 3:
        if not allow_conn:
            raise TypeError("shadow runner requiring a live connection is not observational")
        # Preserve the caller-owned session identity when the shared bridge
        # accepts it; older injected runners with a strict three-argument
        # signature continue to work unchanged.
        if any(p.name == "session_id" for p in params):
            return loader(conn, name, data, session_id=session_id)
        return loader(conn, name, data)
    if len(positional) == 2:
        if positional[0].name in {"request", "ingestion_request"}:
            return loader(_build_request(name, data, session_id), data)
        return loader(name, data)
    if len(positional) == 1:
        return loader(_build_request(name, data, session_id))
    return loader()


def _shared_csv_loader(conn: Any, name: str, data: bytes, inventory: list[dict]) -> Any:
    """Use the shared delimited reader for a direct CSV when available.

    The shared reader owns row/header fidelity.  Module 2 intentionally stores
    all source rows with ``header=None``; no adapter-side header inference or
    reconstruction is performed.
    """
    try:
        from backend.ingestion.models import IngestionRequest, SourceAsset
        from backend.ingestion.readers.csv_reader import DelimitedReader
    except (ImportError, ModuleNotFoundError):
        try:
            from ingestion.models import IngestionRequest, SourceAsset
            from ingestion.readers.csv_reader import DelimitedReader
        except (ImportError, ModuleNotFoundError):
            return None

    if not name.lower().endswith((".csv", ".tsv", ".psv", ".tab")):
        return None

    request = IngestionRequest(
        source=SourceAsset(name=name, size_bytes=len(data), source_id=f"module2:{name}"),
        module="module2",
    )
    parsed = DelimitedReader().parse(request, data)
    # The shared reader owns row/header fidelity.  Do not reconstruct or
    # prepend row zero here; doing so would duplicate a header once the reader
    # returns Module 2's header=None-compatible raw rows.
    rows = list(parsed.raw_rows)
    frame = pd.DataFrame(rows)
    key = f"{name}::"
    # Import lazily to keep this module independent from Module 2's top-level
    # package aliases during unified-host startup.
    from db import df_to_sqlite, register_table, safe_table_name, quote_id

    raw_sql = safe_table_name("raw", key)
    data_sql = safe_table_name("data", key)
    df_to_sqlite(conn, raw_sql, frame, commit=False)
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(data_sql)}")
    conn.execute(f"CREATE TABLE {quote_id(data_sql)} AS SELECT * FROM {quote_id(raw_sql)}")
    register_table(conn, key, data_sql, commit=False)
    inventory.append({"table_key": key, "rows": len(frame), "cols": len(frame.columns)})
    return inventory[-1]


def _shared_table_names(table_key: str) -> tuple[str, str]:
    """Return the deterministic names used by :class:`DuckDBSink`.

    This is deliberately kept local to the adapter: the sink tables are an
    implementation detail and are never registered as Module 2 tables.
    """
    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(table_key)).strip("_") or "table"
    digest = hashlib.sha256(str(table_key).encode("utf-8")).hexdigest()[:12]
    base = f"{safe[:48]}_{digest}"
    return f"{base}__raw", f"{base}__typed"


def _module2_table_key(name: str, artifact_key: str, *, workbook: bool) -> str:
    """Map a shared artifact identity to Module 2's ``name::sheet`` key."""
    key = str(artifact_key)
    if workbook and ":" in key:
        sheet = key.rsplit(":", 1)[-1]
        return f"{name}::{sheet}"
    return f"{name}::"


def run_module2_shared(
    conn: Any,
    name: str,
    data: bytes,
    inventory: list[dict] | None = None,
    *,
    session_id: str | None = None,
    on_progress: Callable[[dict], None] | None = None,
) -> list[dict]:
    """Ingest one upload member through the shared readers and publish M2 views.

    The shared sink preserves typed/raw artifacts while Module 2 continues to
    expose its historical VARCHAR ``raw``/``data`` tables.  Header selection,
    normalization, reset, and table selection therefore remain owned by the
    existing routes.  No session id is generated or changed here.
    """
    from backend.ingestion.models import IngestionRequest, SourceAsset
    from backend.ingestion.service import IngestionService
    from db import df_to_sqlite, register_table, safe_table_name, quote_id

    payload = bytes(data)
    lower = str(name).lower()
    workbook = lower.endswith((".xls", ".xlsx", ".xlsm", ".xlsb", ".xltx", ".xltm", ".ods"))
    # CSV artifacts use the legacy trailing ``::`` identity; workbook sheets
    # are mapped below after the reader appends ``:<sheet>``.
    source_id = str(name) if workbook else f"{name}::"
    request = IngestionRequest(
        source=SourceAsset(str(name), size_bytes=len(payload), source_id=source_id),
        module="module2", session_id=session_id,
    )
    result = IngestionService(sink_connection=conn, progress=on_progress).ingest(
        request, source=payload,
    )
    if not result.ok or not result.tables:
        return []

    published: list[dict] = []
    created_compat: list[str] = []
    try:
        for artifact in result.tables:
            shared_raw, shared_typed = _shared_table_names(artifact.table_key)
            # Sink publication is an invariant: never expose an incomplete
            # compatibility table if one side is missing.
            names = {
                str(row[0]) for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_name IN (?, ?)",
                    [shared_raw, shared_typed],
                ).fetchall()
            }
            if shared_raw not in names or shared_typed not in names:
                raise RuntimeError(f"shared raw/typed tables missing for {artifact.table_key}")

            key = _module2_table_key(str(name), artifact.table_key, workbook=workbook)
            raw_sql = safe_table_name("raw", key)
            data_sql = safe_table_name("data", key)
            # Materialise the immutable raw source (including row zero) as the
            # VARCHAR compatibility view expected by Module 2's header route.
            raw_cols = [str(row[0]) for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND column_name <> '__row_id' ORDER BY ordinal_position",
                [shared_raw],
            ).fetchall()]
            projection = ", ".join(quote_id(column) for column in raw_cols)
            if not projection:
                raise RuntimeError(f"shared table has no columns for {artifact.table_key}")
            frame = conn._conn.execute(
                f"SELECT {projection} FROM {quote_id(shared_raw)} ORDER BY \"__row_id\""
            ).df()
            if frame.empty:
                # Match the legacy upload contract: zero-row members are not
                # published as inventory tables.
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_raw)}")
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_typed)}")
                continue
            df_to_sqlite(conn, raw_sql, frame, commit=False)
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(data_sql)}")
            conn.execute(f"CREATE TABLE {quote_id(data_sql)} AS SELECT * FROM {quote_id(raw_sql)}")
            register_table(conn, key, data_sql, commit=False)
            item = {"table_key": key, "rows": int(len(frame)), "cols": int(len(frame.columns))}
            published.append(item)
            if inventory is not None:
                inventory.append(item)
            created_compat.extend((raw_sql, data_sql))
            # Shared tables are temporary implementation details.  The typed
            # values remain available to the typed-transfer boundary before
            # normalization; M2's active/data routes intentionally stay text.
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_raw)}")
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(shared_typed)}")
        conn.commit()
        return published
    except Exception:
        logger.exception("Module 2 shared publication failed; rolling back")
        if inventory is not None and published:
            # The caller owns the upload DTO list; keep it consistent with the
            # all-or-nothing table publication boundary.
            del inventory[-len(published):]
        try:
            conn.rollback()
        except Exception:
            pass
        for table_name in created_compat:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
            except Exception:
                pass
        for artifact in result.tables:
            for table_name in _shared_table_names(artifact.table_key):
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
                except Exception:
                    pass
        try:
            conn.commit()
        except Exception:
            pass
        raise


class Module2IngestionAdapter:
    """Preserve Module 2's loader signature and legacy rollback behavior."""

    def __init__(self, *, shared_loader: SharedLoader | None = None,
                 cutover_loader: SharedLoader | None = None,
                 config: Module2AdapterConfig | None = None) -> None:
        self.shared_loader = shared_loader
        self.cutover_loader = cutover_loader
        self.config = config
        self.last_report: ParityReport | None = None

    def load_source_file_to_session(
        self,
        conn: Any,
        name: str,
        data: bytes,
        inventory: list[dict],
        *,
        legacy_loader: LegacyLoader,
        session_id: str | None = None,
    ) -> Any:
        config = self.config or Module2AdapterConfig.from_env()
        loader = self.shared_loader or _shared_loader
        if (config.shadow_enabled or config.gates_enabled) and loader is None:
            loader = _discover_shared_loader()

        # A cut-over is allowed only after a prior, matching shadow report and
        # all explicit gates.  The first invocation always remains legacy.
        cutover_loader = self.cutover_loader or _cutover_loader
        if (
            config.gates_enabled and self.last_report
            and self.last_report.matches_context(data, session_id, _shared_contract_version())
            and cutover_loader
        ):
            try:
                before = len(inventory)
                result = _call_loader(cutover_loader, conn, name, data, session_id)
                result_items = _inventory_items(result)
                appended_items = inventory[before:]
                if len(inventory) == before and result_items:
                    inventory.extend(result_items)
                    appended_items = result_items
                if result is not None and appended_items and all(
                    isinstance(item.get("table_key"), str)
                    and isinstance(item.get("rows"), int)
                    and isinstance(item.get("cols"), int)
                    for item in appended_items
                ):
                    return result
            except Exception:
                logger.exception("Module 2 shared ingestion cut-over failed; rolling back to legacy")
                if config.fail_on_mismatch:
                    raise

        before = len(inventory)
        legacy_result = legacy_loader(conn, name, data, inventory)
        if config.shadow_enabled and loader:
            try:
                # Shadow runners are observational and must not mutate the
                # session.  They may return an inventory item/result directly.
                shared_result = _call_loader(
                    loader, conn, name, data, session_id, allow_conn=False,
                )
                if shared_result is None:
                    shared_result = inventory[before:]
                self.last_report = compare_parity(
                    inventory[before:], shared_result,
                    gate_enabled=(config.parity_gate_enabled and config.parity_evidence and config.predecessor_gate_enabled),
                    source_digest=hashlib.sha256(data).hexdigest(),
                    session_id=session_id,
                    contract_version=_shared_contract_version(),
                )
                global _last_report
                _last_report = self.last_report
                if not self.last_report.matched:
                    logger.warning("Module 2 ingestion shadow parity mismatch: %s", self.last_report.differences)
                    if config.fail_on_mismatch:
                        raise RuntimeError("Module 2 ingestion shadow parity gate failed")
            except Exception:
                logger.exception("Module 2 ingestion shadow run failed")
                if config.fail_on_mismatch:
                    raise
        return legacy_result


# The runner is installed as a capability, but the adapter still requires the
# explicit predecessor/parity/cutover gates before it can ever select it.
# Keeping this registration separate preserves the legacy-default fail-closed
# startup behavior while making the real bridge available to controlled hosts.
configure_cutover_loader(run_module2_shared)
_default_adapter = Module2IngestionAdapter()


def load_source_file_to_session(
    conn: Any,
    name: str,
    data: bytes,
    inventory: list[dict],
    *,
    legacy_loader: LegacyLoader,
    session_id: str | None = None,
) -> Any:
    return _default_adapter.load_source_file_to_session(
        conn, name, data, inventory, legacy_loader=legacy_loader,
        session_id=session_id,
    )


__all__ = [
    "Module2AdapterConfig", "Module2IngestionAdapter", "ParityReport",
    "compare_parity", "configure_shared_loader", "configure_cutover_loader",
    "get_last_parity_report", "run_module2_shared",
    "load_source_file_to_session",
]
