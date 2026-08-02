from __future__ import annotations

import io
import json
import math
from dataclasses import replace
from decimal import Decimal
import stat
import zipfile

import pytest

from backend.ingestion import (
    ArchiveLimits,
    ArchiveSafetyError,
    IngestionResult,
    IngestionStats,
    Issue,
    IssueCode,
    IssueSeverity,
    ResultStatus,
    SourceAsset,
    SourceKind,
    TableArtifact,
    TransferManifest,
    UnsupportedFormatError,
    default_registry,
    detect_source,
    iter_archive_entries,
    read_archive_entry,
    to_json_dict,
)


def _zip(name: str = "data.csv", payload: bytes = b"a,b\n1,2\n") -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, payload)
    return out.getvalue()


@pytest.mark.parametrize("extension", ["xlsx", "xlsm", "xltx", "xltm"])
def test_ooxml_extension_precedes_zip_magic(extension: str) -> None:
    detection = detect_source(f"workbook.{extension}", _zip())
    assert detection.kind.value == extension
    assert detection.magic == "zip"
    assert detection.confidence == "extension"


def test_magic_classifies_unknown_zip_and_parquet() -> None:
    assert detect_source("upload.bin", _zip()).kind is SourceKind.ZIP
    assert detect_source("upload.bin", b"PAR1\x00\x00").kind is SourceKind.PARQUET


def test_archive_rejects_traversal_and_enforces_limits() -> None:
    with pytest.raises(ArchiveSafetyError) as traversal:
        list(iter_archive_entries(_zip("../evil.csv")))
    assert traversal.value.code is IssueCode.ARCHIVE_PATH_TRAVERSAL

    with pytest.raises(ArchiveSafetyError) as count:
        list(iter_archive_entries(_zip(), ArchiveLimits(max_entries=0)))
    assert count.value.code is IssueCode.ARCHIVE_LIMIT


def test_archive_enforces_ratio_size_and_depth_limits() -> None:
    # A highly compressible member trips the zip-bomb ratio guard before any
    # reader can materialize it.
    with pytest.raises(ArchiveSafetyError) as ratio:
        list(iter_archive_entries(_zip(payload=b"x" * 4096), ArchiveLimits(max_compression_ratio=2)))
    assert ratio.value.code is IssueCode.ARCHIVE_LIMIT

    with pytest.raises(ArchiveSafetyError) as expanded:
        list(iter_archive_entries(_zip(payload=b"123456"), ArchiveLimits(max_expanded_bytes=2, max_compression_ratio=10_000)))
    assert expanded.value.code is IssueCode.ARCHIVE_LIMIT

    with pytest.raises(ArchiveSafetyError) as depth:
        list(iter_archive_entries(_zip("a/b/c.csv"), ArchiveLimits(max_depth=1)))
    assert depth.value.code is IssueCode.ARCHIVE_LIMIT


@pytest.mark.parametrize("kwargs", [
    {"max_entries": 0}, {"max_entries": -1},
    {"max_expanded_bytes": 0}, {"max_expanded_bytes": -1},
    {"max_compression_ratio": 0}, {"max_compression_ratio": -1},
    {"max_compression_ratio": math.inf}, {"max_compression_ratio": math.nan},
    {"max_depth": -1}, {"chunk_size": 0}, {"chunk_size": -1},
])
def test_archive_limits_reject_invalid_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ArchiveLimits(**kwargs)  # type: ignore[arg-type]


def test_archive_entry_read_preserves_member_bytes() -> None:
    payload = b"a,b\n1,2\n"
    archive = _zip(payload=payload)
    assert read_archive_entry(archive, "data.csv") == payload

    with pytest.raises(ArchiveSafetyError) as missing:
        read_archive_entry(archive, "missing.csv")
    assert missing.value.code is IssueCode.ARCHIVE_CORRUPT_ENTRY


def test_archive_entry_read_applies_zip_bomb_limits() -> None:
    archive = _zip(payload=b"x" * 4096)
    with pytest.raises(ArchiveSafetyError) as error:
        read_archive_entry(archive, "data.csv", ArchiveLimits(max_compression_ratio=2))
    assert error.value.code is IssueCode.ARCHIVE_LIMIT


@pytest.mark.parametrize("name", [r"C:\evil.csv", r"\\server\share\evil.csv", "a\\..\\evil.csv", "a:evil.csv"])
def test_archive_rejects_windows_paths(name: str) -> None:
    with pytest.raises(ArchiveSafetyError):
        list(iter_archive_entries(_zip(name)))


def test_archive_rejects_symlink_and_normalized_duplicates() -> None:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        link = zipfile.ZipInfo("link.csv")
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, b"target")
    with pytest.raises(ArchiveSafetyError):
        list(iter_archive_entries(out.getvalue()))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("A.csv", b"1")
        archive.writestr("a.csv", b"2")
    with pytest.raises(ArchiveSafetyError):
        list(iter_archive_entries(out.getvalue()))


def test_nonseekable_detection_does_not_consume_upload() -> None:
    class NonSeekable:
        def __init__(self):
            self.data = b"PAR1\x00rest"
            self.read_called = False
        def seekable(self):
            return False
        def read(self, *_args):
            self.read_called = True
            raise AssertionError("detector consumed non-seekable stream")
    stream = NonSeekable()
    detection = detect_source("upload.xlsx", stream)
    assert detection.kind is SourceKind.XLSX
    assert detection.constraint == "NON_SEEKABLE_SOURCE_NOT_PEEKED"
    assert not stream.read_called


def test_registry_returns_structured_unsupported_error() -> None:
    registry = default_registry()
    with pytest.raises(UnsupportedFormatError) as error:
        registry.get(SourceKind.UNKNOWN)
    assert error.value.code == IssueCode.UNSUPPORTED_FORMAT.value
    assert registry.descriptor(SourceKind.XLSX).capabilities["formula_recalculation"] is False


def test_registry_registration_is_atomic_and_capability_read_is_structured() -> None:
    from backend.ingestion.readers import CapabilityReader, ReaderRegistry
    registry = ReaderRegistry()
    registry.register(CapabilityReader("one", [SourceKind.CSV]))
    with pytest.raises(ValueError):
        registry.register(CapabilityReader("mixed", [SourceKind.TSV, SourceKind.CSV]))
    assert registry.descriptor(SourceKind.TSV) is None
    with pytest.raises(UnsupportedFormatError):
        registry.get(SourceKind.CSV).read(None)  # type: ignore[arg-type]


def test_models_are_json_safe_and_diagnostics_deterministic() -> None:
    issue = Issue(IssueCode.AMBIGUOUS_TYPE, IssueSeverity.WARNING, "ambiguous", details={"column": "A"})
    result = IngestionResult(ResultStatus.PARTIAL, "r1", SourceAsset("a.csv"), issues=(issue,), stats=IngestionStats(files_succeeded=1))
    encoded = to_json_dict(result)
    assert encoded["contract_version"] == "ingestion.v1"
    assert json.dumps(encoded, sort_keys=True) == json.dumps(to_json_dict(result), sort_keys=True)
    assert encoded["issues"][0]["code"] == "INGEST_AMBIGUOUS_TYPE"
    encoded["extra"] = {"decimal": Decimal("1.20"), "bytes": b"x", "set": {"b", "a"}}
    assert to_json_dict(encoded)["extra"]["decimal"] == "1.20"
    with pytest.raises(TypeError):
        to_json_dict({"custom": object()})


def test_transfer_manifest_is_typed_and_digest_is_stable() -> None:
    table = TableArtifact("t1", "Table", row_count=2)
    first = TransferManifest.from_tables("a1", "module1", "module2", [table])
    second = TransferManifest.from_tables("a1", "module1", "module2", [table])
    assert first.encoding == "typed-artifact"
    assert first.to_dict()["tables"][0]["table_key"] == "t1"
    assert first.digest() == second.digest()
    assert first.verify()
    tampered = replace(first, tables=(replace(first.tables[0], row_count=9),))
    assert not tampered.verify()
    with pytest.raises(ValueError):
        TransferManifest.from_tables("", "module1", "module2", [table])
