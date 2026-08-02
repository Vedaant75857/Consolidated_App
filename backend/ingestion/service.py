"""Module-neutral ingestion orchestration and nested archive enforcement."""
from __future__ import annotations

from dataclasses import replace
import io
from pathlib import Path
import zipfile
from typing import Any, Callable
from uuid import uuid4

from .archives import ArchiveLimits, ArchiveSafetyError, iter_archive_members
from .detect import detect_source
from .errors import issue
from .models import IngestionRequest, IngestionResult, IngestionStats, IssueCode, ResultStatus, SourceAsset, SourceKind, TableArtifact
from .readers import DelimitedReader, ExcelReader, LegacyExcelReader, ParsedTable
from .duckdb_sink import DuckDBSink, SinkResult


class IngestionService:
    def __init__(self, *, sink_connection: Any | None = None, progress: Callable[[dict[str, Any]], None] | None = None):
        self.sink_connection = sink_connection
        self.progress = progress

    def ingest(self, request: IngestionRequest, *, source: bytes | bytearray | Any | None = None, cancel: Callable[[], bool] | None = None) -> IngestionResult:
        request_id = uuid4().hex
        payload = source if source is not None else request.source.path
        if payload is None:
            raise ValueError("source path or bytes are required")
        tables: list[TableArtifact] = []
        created_tables: list[str] = []
        issues = []
        files_ok = files_failed = 0
        rows_loaded = 0
        limits = ArchiveLimits(request.policy.max_archive_entries, request.policy.max_expanded_bytes, request.policy.max_compression_ratio, request.policy.max_archive_depth)
        self._expanded_bytes = 0
        try:
            # Large delimited uploads use the seekable/native path.  The
            # materialized ``parse`` API remains available for direct callers
            # and for Excel/archive compatibility paths.
            detection = detect_source(request.source.name, payload)
            # Publish a recursive file total once, before any table writes.
            # This is deliberately calculated by the same bounded traversal as
            # ingestion so nested ZIPs do not under-report progress.
            if self.progress:
                self.progress({
                    "request_id": request_id,
                    "phase": "zip_info",
                    "files_total": self._count_loadable_files(payload, request.source.name, 0, limits),
                })
            empty_payload = (isinstance(payload, (bytes, bytearray, memoryview)) and len(payload) == 0) or request.source.size_bytes == 0
            if self.sink_connection is not None and not empty_payload and detection.kind in {SourceKind.CSV, SourceKind.TSV, SourceKind.PSV, SourceKind.DELIMITED}:
                scan = DelimitedReader().prepare(request, payload)
                # DuckDB's CSV reader has no cp1252 decoder; retain the strict
                # compatibility parser for that uncommon legacy encoding.
                # DuckDB only accepts generic UTF-16 and cannot distinguish
                # byte order reliably; keep all UTF-16/legacy codecs on the
                # strict compatibility path.
                native_encoding = (scan.encoding.lower().replace("-", "") in {"utf8", "utf8sig"}) and not scan.ragged
                if native_encoding:
                    sink_result = DuckDBSink(self.sink_connection).write_delimited(
                        scan,
                        source_id=request.source.source_id or request.source.name,
                        display_name=Path(request.source.name).stem,
                        cancel=cancel,
                        progress=lambda event: self.progress({"request_id": request_id, **event}) if self.progress else None,
                    )
                    parsed = []
                    tables.append(sink_result.artifact)
                    created_tables.extend([sink_result.raw_table, sink_result.typed_table])
                    if any(col.display_name == f"COL_{col.ordinal + 1}" for col in sink_result.artifact.columns):
                        issues.append(issue(IssueCode.PARTIAL_RESULT, "ragged CSV rows introduced generated columns", source=request.source.name))
                    rows_loaded = sink_result.artifact.row_count
                    files_ok = 1
                    if self.progress:
                        self.progress({"request_id": request_id, "table": sink_result.artifact.table_key, "rows": rows_loaded, "phase": "committed"})
                    parsed_columns = len(sink_result.artifact.columns)
                else:
                    if scan.ragged:
                        issues.append(issue(IssueCode.PARTIAL_RESULT, "ragged CSV rows detected; compatibility parser used", source=request.source.name))
                    # Parse the retained spool path, not an exhausted upload
                    # stream. This also handles UTF-16BE and legacy codecs.
                    fallback_path = scan.path
                    try:
                        parsed = self._walk(request, fallback_path, request.source.name, 0, limits, cancel, issues, source_id=request.source.source_id or request.source.name)
                    finally:
                        scan.close()
                    parsed_columns = sum(len(t.artifact.columns) for t in parsed)
            else:
                parsed = self._walk(request, payload, request.source.name, 0, limits, cancel, issues, source_id=request.source.source_id or request.source.name)
                parsed_columns = sum(len(t.artifact.columns) for t in parsed)
            for table in parsed:
                if cancel and cancel():
                    break
                if self.sink_connection is not None:
                    sink_result = DuckDBSink(self.sink_connection).write(table)
                    created_tables.extend([sink_result.raw_table, sink_result.typed_table])
                    artifact_to_publish = sink_result.artifact
                else:
                    artifact_to_publish = table.artifact
                tables.append(artifact_to_publish)
                rows_loaded += artifact_to_publish.row_count
                if self.progress:
                    self.progress({"request_id": request_id, "table": artifact_to_publish.table_key, "rows": artifact_to_publish.row_count})
            if not files_ok:
                files_ok = len(tables)
        except ArchiveSafetyError as exc:
            self._rollback_tables(created_tables)
            tables.clear(); rows_loaded = 0; files_ok = 0
            issues.append(issue(exc.code, str(exc), source=request.source.name, location=exc.entry))
            files_failed = 1
        except RuntimeError as exc:
            self._rollback_tables(created_tables)
            tables.clear(); rows_loaded = 0; files_ok = 0
            issues.append(issue(IssueCode.UNSUPPORTED_FORMAT, str(exc), source=request.source.name))
            files_failed = 1
        except Exception as exc:
            self._rollback_tables(created_tables)
            tables.clear(); rows_loaded = 0; files_ok = 0
            issues.append(issue(IssueCode.PARTIAL_RESULT, f"ingestion failed: {exc}", source=request.source.name))
            files_failed = 1
        if cancel and cancel() and not any(i.code is IssueCode.PARTIAL_RESULT for i in issues):
            self._rollback_tables(created_tables)
            tables.clear()
            rows_loaded = 0
            files_ok = 0
            issues.append(issue(IssueCode.PARTIAL_RESULT, "ingestion cancelled", source=request.source.name))
        status = ResultStatus.FAILED if not tables and (files_failed or issues) else (ResultStatus.PARTIAL if issues else ResultStatus.SUCCESS)
        source_bytes = request.source.size_bytes or (len(payload) if isinstance(payload, (bytes, bytearray, memoryview)) else 0)
        stats = IngestionStats(source_bytes=source_bytes, tables_seen=len(tables), tables_loaded=len(tables), rows_loaded=rows_loaded, columns_loaded=locals().get("parsed_columns", sum(len(t.artifact.columns) for t in parsed) if 'parsed' in locals() else 0), files_succeeded=files_ok, files_failed=files_failed)
        return IngestionResult(status, request_id, request.source, tuple(tables), tuple(issues), stats)

    def _rollback_tables(self, names: list[str]) -> None:
        if self.sink_connection is None:
            return
        for name in reversed(names):
            try:
                self.sink_connection.execute(f'DROP TABLE IF EXISTS "{name.replace(chr(34), chr(34) * 2)}"')
            except Exception:
                pass

    def _walk(self, request: IngestionRequest, payload: Any, name: str, depth: int,
              limits: ArchiveLimits, cancel: Callable[[], bool] | None,
              issues: list[Any], *, source_id: str | None = None) -> list[ParsedTable]:
        if cancel and cancel():
            return []
        detection = detect_source(name, payload)
        # Uploaded archive bytes may have a misleading .csv/.txt name.  ZIP
        # magic is authoritative here except for known OOXML workbook names.
        if detection.kind not in {SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLTX, SourceKind.XLTM}:
            try:
                prefix = payload[:4] if isinstance(payload, (bytes, bytearray)) else b""
            except Exception:
                prefix = b""
            if prefix == b"PK\x03\x04":
                detection = replace(detection, kind=self._zip_payload_kind(payload), confidence="magic", magic="zip")
        if detection.kind is SourceKind.ZIP:
            if depth >= limits.max_depth:
                raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "nested archive depth exceeds limit", entry=name)
            result: list[ParsedTable] = []
            # The archive index is validated once.  Each member is decompressed
            # in bounded chunks and closed promptly before the next member.
            for member in iter_archive_members(payload, limits):
                if cancel and cancel():
                    break
                if member.entry.is_dir:
                    member.close()
                    continue
                self._expanded_bytes += member.entry.expanded_bytes
                if self._expanded_bytes > limits.max_expanded_bytes:
                    member.close()
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "cumulative expanded archive bytes exceed limit", entry=member.entry.name)
                try:
                    # ``name`` is the complete source path accumulated from
                    # the root upload.  Keep that path in the child identity;
                    # using only the immediate member name makes two nested
                    # archives with the same filename collide downstream.
                    member_name = f"{name}/{member.entry.name}"
                    # Keep a single, canonical source identity for each
                    # archive member.  ``name`` is the display/path identity
                    # while ``source_id`` is the stable operation identity;
                    # appending only the immediate member avoids the previous
                    # duplicated-prefix form (``root:root/inner:root/...``)
                    # and keeps same-named files in nested archives distinct.
                    member_source_id = f"{source_id or request.source.name}/{member.entry.name}"
                    result.extend(self._walk(request, member.read_bytes(), member_name, depth + 1, limits, cancel, issues, source_id=member_source_id))
                finally:
                    member.close()
            return result
        child = replace(request, source=SourceAsset(name=name, path=None, media_type=request.source.media_type, size_bytes=len(payload) if isinstance(payload, (bytes, bytearray)) else request.source.size_bytes, source_id=source_id or name))
        if detection.kind in {SourceKind.CSV, SourceKind.TSV, SourceKind.PSV, SourceKind.DELIMITED}:
            parsed = DelimitedReader().parse(child, payload)
            issues.extend(parsed.issues)
            return [parsed]
        if detection.kind in {SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLTX, SourceKind.XLTM}:
            parsed = list(ExcelReader().parse(child, payload))
            for table in parsed:
                issues.extend(table.issues)
            return parsed
        if detection.kind in {SourceKind.XLS, SourceKind.XLSB, SourceKind.ODS}:
            parsed = list(LegacyExcelReader().parse(child, payload))
            for table in parsed:
                issues.extend(table.issues)
            return parsed
        issues.append(issue(IssueCode.UNSUPPORTED_FORMAT, f"unsupported source format: {name}", source=name))
        return []

    def _count_loadable_files(self, payload: Any, name: str, depth: int, limits: ArchiveLimits) -> int:
        """Count recursively loadable files without trusting archive suffixes."""
        # Counting must never consume an upload stream before the reader gets
        # it.  Seekable paths/bytes are safe to inspect; an opaque stream is
        # conservatively reported as one source file.
        if hasattr(payload, "read"):
            return 1
        detection = detect_source(name, payload)
        # ZIP magic is authoritative for uploaded members whose suffix is
        # misleading (for example an inner archive named ``data.csv``), while
        # explicit OOXML workbook extensions remain handled by ``detect_source``.
        if detection.kind not in {SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLTX, SourceKind.XLTM}:
            try:
                prefix = payload[:4] if isinstance(payload, (bytes, bytearray, memoryview)) else b""
            except Exception:
                prefix = b""
            if prefix == b"PK\x03\x04":
                detection = replace(detection, kind=self._zip_payload_kind(payload), confidence="magic", magic="zip")
        if detection.kind is not SourceKind.ZIP:
            return int(detection.kind in {
                SourceKind.CSV, SourceKind.TSV, SourceKind.PSV, SourceKind.DELIMITED,
                SourceKind.XLS, SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLSB,
                SourceKind.XLTX, SourceKind.XLTM, SourceKind.ODS,
            })
        if depth >= limits.max_depth:
            return 0
        total = 0
        for member in iter_archive_members(payload, limits):
            try:
                if member.entry.is_dir:
                    continue
                member_name = f"{name}/{member.entry.name}"
                total += self._count_loadable_files(member.read_bytes(), member_name, depth + 1, limits)
            finally:
                member.close()
        return total

    @staticmethod
    def _zip_payload_kind(payload: bytes | bytearray | memoryview) -> SourceKind:
        """Classify ZIP-shaped bytes before treating them as an archive.

        OOXML and ODS workbooks are ZIP containers too.  Content markers are
        authoritative when a caller supplied an unhelpful extension (common
        for archive members); otherwise the bytes are a regular ZIP archive.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(bytes(payload))) as archive:
                names = {name.lstrip("/").lower() for name in archive.namelist()}
                if "mimetype" in names:
                    try:
                        mimetype_name = next(name for name in archive.namelist() if name.lower().lstrip("/") == "mimetype")
                        mimetype = archive.read(mimetype_name).decode("ascii", "strict").strip()
                    except (KeyError, UnicodeDecodeError):
                        mimetype = ""
                    if mimetype == "application/vnd.oasis.opendocument.spreadsheet":
                        return SourceKind.ODS
                if "xl/workbook.bin" in names:
                    return SourceKind.XLSB
                if "xl/workbook.xml" in names or ("[content_types].xml" in names and any(name.startswith("xl/") for name in names)):
                    return SourceKind.XLSX
        except (OSError, zipfile.BadZipFile):
            pass
        return SourceKind.ZIP


def ingest(request: IngestionRequest, *, source: Any | None = None, sink_connection: Any | None = None, cancel: Callable[[], bool] | None = None) -> IngestionResult:
    return IngestionService(sink_connection=sink_connection).ingest(request, source=source, cancel=cancel)


def load_module1(source: bytes | bytearray) -> IngestionResult:
    """Bytes-only shadow entry point used by Module 1's opt-in adapter."""
    payload = bytes(source)
    suffix = ".zip" if payload.startswith(b"PK\x03\x04") else ".csv"
    request = IngestionRequest(SourceAsset(f"upload{suffix}", size_bytes=len(payload), source_id="shadow"), module="module1")
    return IngestionService().ingest(request, source=payload)
