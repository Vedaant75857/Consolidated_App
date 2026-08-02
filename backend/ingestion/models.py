"""Stable, JSON-safe contracts for the shared ingestion pipeline.

These contracts deliberately contain no framework or storage objects.  They are
safe to persist in a manifest and can therefore be shared by all module
adapters without making Module 1 the upload gateway.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
import base64
from typing import Any, Mapping, Sequence

CONTRACT_VERSION = "ingestion.v1"


class SourceKind(str, Enum):
    CSV = "csv"
    TSV = "tsv"
    PSV = "psv"
    DELIMITED = "delimited"
    XLS = "xls"
    XLSX = "xlsx"
    XLSM = "xlsm"
    XLSB = "xlsb"
    XLTX = "xltx"
    XLTM = "xltm"
    ODS = "ods"
    ZIP = "zip"
    PARQUET = "parquet"
    UNKNOWN = "unknown"


class IssueSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class IssueCode(str, Enum):
    UNSUPPORTED_FORMAT = "INGEST_UNSUPPORTED_FORMAT"
    INVALID_ARCHIVE = "INGEST_INVALID_ARCHIVE"
    ARCHIVE_LIMIT = "INGEST_ARCHIVE_LIMIT"
    ARCHIVE_PATH_TRAVERSAL = "INGEST_ARCHIVE_PATH_TRAVERSAL"
    ARCHIVE_ENCRYPTED = "INGEST_ARCHIVE_ENCRYPTED"
    ARCHIVE_CORRUPT_ENTRY = "INGEST_ARCHIVE_CORRUPT_ENTRY"
    AMBIGUOUS_TYPE = "INGEST_AMBIGUOUS_TYPE"
    FORMULA_NOT_RECALCULATED = "INGEST_FORMULA_NOT_RECALCULATED"
    HIDDEN_DATA_INCLUDED = "INGEST_HIDDEN_DATA_INCLUDED"
    PARTIAL_RESULT = "INGEST_PARTIAL_RESULT"
    DECODING_ERROR = "INGEST_DECODING_ERROR"


class ValueType(str, Enum):
    TEXT = "text"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    NULL = "null"
    ERROR = "error"


class ResultStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(frozen=True)
class IngestionPolicy:
    include_hidden_rows: bool = True
    include_hidden_sheets: bool = True
    recalculate_formulas: bool = False
    accept_formula_cache: bool = True
    allow_partial_files: bool = True
    max_archive_entries: int = 10000
    max_expanded_bytes: int = 2 * 1024 * 1024 * 1024
    max_compression_ratio: float = 100.0
    max_archive_depth: int = 2


@dataclass(frozen=True)
class IngestionRequest:
    source: "SourceAsset"
    module: str | None = None
    session_id: str | None = None
    policy: IngestionPolicy = field(default_factory=IngestionPolicy)
    type_overrides: Mapping[str, ValueType] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceAsset:
    name: str
    path: str | None = None
    media_type: str | None = None
    size_bytes: int | None = None
    # A stream/path is intentionally not serialized.  Adapters may attach one
    # out of band while the manifest retains only these identifying fields.
    source_id: str | None = None


@dataclass(frozen=True)
class OrderedColumnSchema:
    key: str
    display_name: str
    ordinal: int
    value_type: ValueType = ValueType.TEXT
    nullable: bool = True
    physical_name: str | None = None
    format_hint: str | None = None


@dataclass(frozen=True)
class ColumnProvenance:
    column_key: str
    source_name: str
    source_ordinal: int
    header_row: int | None = None
    workbook_sheet: str | None = None
    original_header: str | None = None
    source_type: str | None = None
    date_system: str | None = None


@dataclass(frozen=True)
class TableArtifact:
    table_key: str
    display_name: str
    columns: tuple[OrderedColumnSchema, ...] = ()
    row_count: int = 0
    provenance: tuple[ColumnProvenance, ...] = ()
    raw_hash: str | None = None
    typed_hash: str | None = None


@dataclass(frozen=True)
class Issue:
    code: IssueCode
    severity: IssueSeverity
    message: str
    source: str | None = None
    location: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)
    recoverable: bool = True


@dataclass(frozen=True)
class IngestionStats:
    source_bytes: int = 0
    tables_seen: int = 0
    tables_loaded: int = 0
    rows_loaded: int = 0
    columns_loaded: int = 0
    files_succeeded: int = 0
    files_failed: int = 0


@dataclass(frozen=True)
class IngestionResult:
    status: ResultStatus
    request_id: str
    source: SourceAsset
    tables: tuple[TableArtifact, ...] = ()
    issues: tuple[Issue, ...] = ()
    stats: IngestionStats = field(default_factory=IngestionStats)
    contract_version: str = CONTRACT_VERSION

    @property
    def ok(self) -> bool:
        return self.status in (ResultStatus.SUCCESS, ResultStatus.PARTIAL)


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        # Decimal text is lossless and avoids a binary float round-trip.
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    if is_dataclass(value):
        return {k: _json_value(v) for k, v in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, set):
        return [_json_value(v) for v in sorted(value, key=lambda item: repr(item))]
    if isinstance(value, (tuple, list)):
        return [_json_value(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"value of type {type(value).__name__} is not JSON-safe")


def to_json_dict(value: Any) -> dict[str, Any]:
    """Return deterministic JSON-compatible data for a contract object."""
    result = _json_value(value)
    if not isinstance(result, dict):
        raise TypeError("contract value must serialize to an object")
    return result
