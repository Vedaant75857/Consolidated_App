"""Shared, module-neutral ingestion contracts and safety primitives."""
from .models import *
from .detect import Detection, detect_source
from .archives import ArchiveEntry, ArchiveLimits, ArchiveMember, ArchiveSafetyError, iter_archive_entries, iter_archive_members, read_archive_entry
from .transfer import (
    CutoverGate,
    CutoverMode,
    MODULE_ORDER,
    ParityEvidence,
    TransferManifest,
    TransferTable,
)
from .readers.registry import Reader, ReaderDescriptor, ReaderRegistry, UnsupportedFormatError, default_registry
from .readers import CSVReader, DelimitedReader, DelimitedScan, ExcelReader, LegacyExcelReader, ParsedTable
from .duckdb_sink import DuckDBSink, SinkResult, write_table
from .service import IngestionService, ingest, load_module1
from .typed_artifact import (
    MANIFEST_HEADER,
    TRANSPORT_VERSION,
    TRANSFER_ORDINAL_COLUMN,
    VERSION_HEADER,
    TypedArtifactLimits,
    TypedImportResult,
    export_duckdb_parquet,
    import_duckdb_parquet,
    parse_transport_manifest,
    serialize_transport,
    transport_headers,
)

__all__ = [
    "CONTRACT_VERSION", "SourceKind", "IssueSeverity", "IssueCode", "ValueType", "ResultStatus",
    "IngestionPolicy", "IngestionRequest", "SourceAsset", "OrderedColumnSchema",
    "ColumnProvenance", "TableArtifact", "Issue", "IngestionStats", "IngestionResult", "to_json_dict",
    "Detection", "detect_source", "ArchiveEntry", "ArchiveLimits", "ArchiveMember", "ArchiveSafetyError",
    "iter_archive_entries", "iter_archive_members", "read_archive_entry", "TransferManifest", "TransferTable",
    "CutoverGate", "CutoverMode", "MODULE_ORDER", "ParityEvidence",
    "Reader", "ReaderDescriptor", "ReaderRegistry", "UnsupportedFormatError", "default_registry",
    "CSVReader", "DelimitedReader", "DelimitedScan", "ExcelReader", "LegacyExcelReader", "ParsedTable", "DuckDBSink", "SinkResult", "write_table",
    "IngestionService", "ingest", "load_module1",
    "TRANSPORT_VERSION", "TRANSFER_ORDINAL_COLUMN", "MANIFEST_HEADER", "VERSION_HEADER", "TypedArtifactLimits", "TypedImportResult",
    "export_duckdb_parquet", "import_duckdb_parquet", "serialize_transport",
    "parse_transport_manifest", "transport_headers",
]
