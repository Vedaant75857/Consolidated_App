"""Streaming, lossless delimited text reader.

The reader keeps the lexical cell values in ``raw_rows`` and exposes a typed
view in ``rows``.  Type inference is deliberately conservative: mixed or
ambiguous columns stay text instead of silently changing source values.
"""
from __future__ import annotations

import csv
import codecs
import io
import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator

from ..errors import issue
from ..models import (
    IngestionRequest, Issue, IssueCode, OrderedColumnSchema, SourceKind,
    TableArtifact, ValueType,
)


@dataclass(frozen=True)
class ParsedTable:
    artifact: TableArtifact
    raw_rows: tuple[tuple[str | None, ...], ...]
    rows: tuple[tuple[Any, ...], ...]
    issues: tuple[Issue, ...] = ()

    @property
    def columns(self) -> tuple[OrderedColumnSchema, ...]:
        return self.artifact.columns


@dataclass(frozen=True)
class DelimitedScan:
    """A seekable, bounded source description for the native DuckDB path.

    ``parse`` remains the compatibility/materialized API.  Services handling
    large uploads use ``prepare`` and pass this object directly to the sink;
    no Python row or cell collection is created in that path.
    """

    path: str
    delimiter: str
    encoding: str
    source_name: str
    temporary: bool = False
    ragged: bool = False

    def close(self) -> None:
        if self.temporary:
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass


def _source_bytes(request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None = None) -> bytes:
    value = source if source is not None else request.source.path
    if value is None:
        raise ValueError("IngestionRequest.source.path or source bytes are required")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if hasattr(value, "read"):
        return value.read()  # type: ignore[union-attr]
    return Path(value).read_bytes()


def _decode(data: bytes, encoding: str | None = None) -> tuple[str, str]:
    # BOMs are authoritative and preserve UTF-16/UTF-8 input without replacing
    # invalid bytes.  For ordinary files use strict UTF-8, then common legacy
    # encodings with a diagnostic emitted by the caller.
    candidates = [encoding] if encoding else []
    utf16_kind: str | None = None
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        utf16_kind = "utf-16-le" if data.startswith(b"\xff\xfe") else "utf-16-be"
        candidates.insert(0, "utf-16")
    elif data.startswith(b"\xef\xbb\xbf"):
        candidates.insert(0, "utf-8-sig")
    candidates.extend(["utf-8-sig", "utf-8", "cp1252", "latin-1"])
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return data.decode(candidate), (utf16_kind or candidate)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", data, 0, len(data), "unable to decode delimited source")


def _infer(values: list[str | None]) -> tuple[ValueType, list[Any], bool]:
    nonblank = [v for v in values if v not in (None, "")]
    if not nonblank:
        return ValueType.NULL, [None for _ in values], True
    lowered = {v.casefold() for v in nonblank}
    if lowered <= {"true", "false"}:
        return ValueType.BOOLEAN, [None if v in (None, "") else v.casefold() == "true" for v in values], True
    ints: list[int] = []
    int_ok = True
    for value in nonblank:
        if not value or value.strip() != value or (value.startswith("0") and len(value) > 1) or (value.startswith("-") and len(value) > 2 and value[1] == "0"):
            int_ok = False
            break
        try:
            parsed = int(value, 10)
            if parsed < -(2**63) or parsed > 2**63 - 1:
                int_ok = False
                break
            ints.append(parsed)
        except ValueError:
            int_ok = False
            break
    if int_ok and len(ints) == len(nonblank):
        iterator = iter(ints)
        return ValueType.INTEGER, [None if v in (None, "") else next(iterator) for v in values], True
    dec_ok = True
    decimals: list[Decimal] = []
    for value in nonblank:
        try:
            parsed = Decimal(value)
            if not parsed.is_finite():
                dec_ok = False
                break
            digits = len(parsed.as_tuple().digits)
            scale = max(0, -parsed.as_tuple().exponent)
            if digits > 38 or scale > 18:
                dec_ok = False
                break
            decimals.append(parsed)
        except (InvalidOperation, ValueError):
            dec_ok = False
            break
    if dec_ok and len(decimals) == len(nonblank) and any("." in v or "e" in v.lower() for v in nonblank):
        iterator = iter(decimals)
        return ValueType.DECIMAL, [None if v in (None, "") else next(iterator) for v in values], True
    return ValueType.TEXT, values[:], False


class DelimitedReader:
    name = "delimited"
    kinds = frozenset({SourceKind.CSV, SourceKind.TSV, SourceKind.PSV, SourceKind.DELIMITED})
    capabilities = {"typed_values": True, "lexical_raw": True, "streaming": True}

    def __init__(self, *, delimiter: str | None = None, encoding: str | None = None):
        self.delimiter = delimiter
        self.encoding = encoding

    def prepare(self, request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None = None) -> DelimitedScan:
        """Spool a source once and return metadata for native batched loading.

        Reads from file-like objects are explicitly chunked.  Detection and
        delimiter sniffing are bounded to the first 64 KiB; the complete file
        is never decoded into a second in-memory representation.
        """
        value = source if source is not None else request.source.path
        if value is None:
            raise ValueError("IngestionRequest.source.path or source bytes are required")
        temporary = False
        if isinstance(value, (str, Path)):
            path = str(value)
        else:
            fd, path = tempfile.mkstemp(prefix="ingestion_", suffix=".csv")
            temporary = True
            try:
                with os.fdopen(fd, "wb") as target:
                    if isinstance(value, (bytes, bytearray, memoryview)):
                        view = memoryview(value)
                        for offset in range(0, len(view), 1024 * 1024):
                            target.write(view[offset : offset + 1024 * 1024])
                    else:
                        stream = value  # type: ignore[assignment]
                        while True:
                            chunk = stream.read(1024 * 1024)
                            if not chunk:
                                break
                            target.write(chunk if isinstance(chunk, bytes) else bytes(chunk))
            except Exception:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
                raise
        try:
            with open(path, "rb") as handle:
                prefix = handle.read(65536)
            text, used_encoding = _decode(prefix, self.encoding)
            # Validate the complete byte stream incrementally. This prevents a
            # late non-UTF8 byte from being misclassified after prefix sniffing.
            if used_encoding in {"utf-8", "utf-8-sig"}:
                decoder = codecs.getincrementaldecoder("utf-8-sig")()
                try:
                    with open(path, "rb") as stream:
                        while chunk := stream.read(1024 * 1024):
                            decoder.decode(chunk)
                        decoder.decode(b"", final=True)
                except UnicodeDecodeError:
                    used_encoding = "cp1252"
            delimiter = self.delimiter
            if delimiter is None:
                suffix = Path(request.source.name).suffix.lower()
                delimiter = {".tsv": "\t", ".tab": "\t", ".psv": "|"}.get(suffix)
            if delimiter is None:
                try:
                    delimiter = csv.Sniffer().sniff(text, delimiters=",;\t|").delimiter
                except csv.Error:
                    delimiter = ","
            # Strict parsing is a streaming validation pass; it does not retain
            # rows and surfaces unmatched quotes as a structured ingestion error.
            ragged = False
            if used_encoding in {"utf-8", "utf-8-sig"}:
                with open(path, "r", encoding="utf-8-sig", newline="") as stream:
                    strict_reader = csv.reader(stream, delimiter=delimiter, strict=True)
                    width = None
                    for row in strict_reader:
                        if width is None: width = len(row)
                        elif len(row) != width: ragged = True
            return DelimitedScan(path, delimiter, used_encoding, request.source.name, temporary, ragged)
        except Exception:
            if temporary:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
            raise

    def parse(self, request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None = None) -> ParsedTable:
        data = _source_bytes(request, source)
        text, used_encoding = _decode(data, self.encoding)
        delimiter = self.delimiter
        if delimiter is None:
            kind = Path(request.source.name).suffix.lower()
            delimiter = {".tsv": "\t", ".tab": "\t", ".psv": "|"}.get(kind)
        if delimiter is None:
            try:
                delimiter = csv.Sniffer().sniff(text[:65536], delimiters=",;\t|").delimiter
            except csv.Error:
                delimiter = ","
        reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter)
        rows = [tuple(cell for cell in row) for row in reader]
        if not rows:
            headers: list[str] = []
            body: list[tuple[str | None, ...]] = []
            all_rows: list[tuple[str | None, ...]] = []
        else:
            width = max(len(row) for row in rows)
            headers = list(rows[0]) + [f"COL_{i + 1}" for i in range(len(rows[0]), width)]
            body = [tuple(row) + (None,) * (width - len(row)) for row in rows[1:]]
            all_rows = [tuple(row) + (None,) * (width - len(row)) for row in rows]
        # Stable, opaque keys.  Display headers are not used as SQL identity.
        columns: list[OrderedColumnSchema] = []
        typed_columns: list[list[Any]] = []
        issues: list[Issue] = []
        for index, header in enumerate(headers):
            values = [row[index] for row in body]
            value_type, typed, unambiguous = _infer(values)
            if not unambiguous and value_type is ValueType.TEXT and any(v not in (None, "") for v in values):
                # Text is a safe fallback; report only columns that had values
                # that looked numeric but could not be represented safely.
                if any(v and (v.lstrip("+-").isdigit() or "." in v) for v in values):
                    issues.append(issue(IssueCode.AMBIGUOUS_TYPE, f"column {header!r} contains mixed or ambiguous values", source=request.source.name, location=f"column:{index}"))
            typed_columns.append(typed)
            columns.append(OrderedColumnSchema(f"col_{index + 1}", header, index, value_type, nullable=True, physical_name=f"col_{index + 1}"))
        # The first row is retained as data.  Header selection is an explicit
        # adapter operation; ingestion never silently removes source rows.
        typed_rows = (tuple(all_rows[0][col] if columns[col].value_type is ValueType.TEXT else None for col in range(len(columns))),) if all_rows else ()
        typed_rows += tuple(tuple(typed_columns[col][row] for col in range(len(columns))) for row in range(len(body)))
        artifact = TableArtifact(request.source.source_id or Path(request.source.name).stem, Path(request.source.name).stem, tuple(columns), len(all_rows))
        return ParsedTable(artifact, tuple(all_rows), typed_rows, tuple(issues))

    def read(self, request: IngestionRequest) -> Iterable[TableArtifact]:
        yield self.parse(request).artifact


CSVReader = DelimitedReader
