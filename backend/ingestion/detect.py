"""Non-destructive source format detection.

Known Excel extensions are checked before ZIP magic because every OOXML
workbook is also a ZIP container.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .models import SourceKind

_EXTENSIONS = {
    ".csv": SourceKind.CSV, ".tsv": SourceKind.TSV, ".tab": SourceKind.TSV,
    ".psv": SourceKind.PSV, ".txt": SourceKind.DELIMITED,
    ".xls": SourceKind.XLS, ".xlsx": SourceKind.XLSX, ".xlsm": SourceKind.XLSM,
    ".xlsb": SourceKind.XLSB, ".xltx": SourceKind.XLTX, ".xltm": SourceKind.XLTM,
    ".ods": SourceKind.ODS, ".zip": SourceKind.ZIP, ".parquet": SourceKind.PARQUET,
}
_EXCEL = {SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLTX, SourceKind.XLTM}


@dataclass(frozen=True)
class Detection:
    kind: SourceKind
    extension: str | None = None
    media_type: str | None = None
    magic: str | None = None
    confidence: str = "extension"
    constraint: str | None = None


def _read_prefix(source: bytes | bytearray | memoryview | BinaryIO | str | Path) -> bytes:
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source[:4096])
    if hasattr(source, "read"):
        stream = source  # type: ignore[assignment]
        # A seekable stream can be peeked and restored.  Never consume a
        # non-seekable upload: the reader must receive its complete stream.
        try:
            if hasattr(stream, "seekable") and not stream.seekable():
                peek = getattr(stream, "peek", None)
                return bytes(peek(4096)) if callable(peek) else b""
            position = stream.tell()
            data = stream.read(4096)
            stream.seek(position)
            return data
        except (AttributeError, OSError, IOError):
            peek = getattr(stream, "peek", None)
            if callable(peek):
                return bytes(peek(4096))
            return b""
    with open(source, "rb") as handle:
        return handle.read(4096)


def detect_source(name: str, source: bytes | bytearray | memoryview | BinaryIO | str | Path,
                  media_type: str | None = None) -> Detection:
    suffix = Path(name).suffix.lower() or None
    ext_kind = _EXTENSIONS.get(suffix or "")
    try:
        seekable = bool(source.seekable()) if hasattr(source, "read") and hasattr(source, "seekable") else False  # type: ignore[union-attr]
    except (OSError, IOError):
        seekable = False
    non_seekable = hasattr(source, "read") and not seekable
    prefix = _read_prefix(source)
    constraint = "NON_SEEKABLE_SOURCE_NOT_PEEKED" if non_seekable and not prefix else None
    magic = None
    if prefix.startswith(b"PAR1"):
        magic, magic_kind = "parquet", SourceKind.PARQUET
    elif prefix.startswith(b"PK\x03\x04"):
        magic, magic_kind = "zip", SourceKind.ZIP
    elif prefix.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        magic, magic_kind = "ole", SourceKind.XLS
    else:
        magic_kind = None

    # Explicit Excel OOXML extension always wins over generic ZIP magic.
    if ext_kind in _EXCEL:
        return Detection(ext_kind, suffix, media_type, magic, "extension", constraint)
    if ext_kind is not None:
        # Parquet/ZIP magic can upgrade an ambiguous text extension, but a
        # declared legacy Excel extension remains authoritative.
        if ext_kind in (SourceKind.UNKNOWN, SourceKind.DELIMITED) and magic_kind:
            return Detection(magic_kind, suffix, media_type, magic, "magic", constraint)
        return Detection(ext_kind, suffix, media_type, magic, "extension", constraint)
    if magic_kind:
        return Detection(magic_kind, suffix, media_type, magic, "magic", constraint)
    if media_type:
        lowered = media_type.lower()
        if "parquet" in lowered:
            return Detection(SourceKind.PARQUET, suffix, media_type, magic, "media_type", constraint)
        if "spreadsheetml" in lowered:
            return Detection(SourceKind.XLSX, suffix, media_type, magic, "media_type", constraint)
        if "csv" in lowered:
            return Detection(SourceKind.CSV, suffix, media_type, magic, "media_type", constraint)
    return Detection(SourceKind.UNKNOWN, suffix, media_type, magic, "unknown", constraint)
