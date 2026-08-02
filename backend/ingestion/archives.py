"""Bounded ZIP traversal helpers used by readers and adapters."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import stat
import io
import math
import tempfile
import zipfile
from contextlib import suppress
from typing import BinaryIO, Iterator

from .models import IssueCode


class ArchiveSafetyError(ValueError):
    def __init__(self, code: IssueCode, message: str, *, entry: str | None = None):
        super().__init__(message)
        self.code, self.entry = code, entry


@dataclass(frozen=True)
class ArchiveLimits:
    max_entries: int = 10_000
    max_expanded_bytes: int = 2 * 1024 * 1024 * 1024
    max_compression_ratio: float = 100.0
    max_depth: int = 2
    chunk_size: int = 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_entries <= 0:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "max_entries must be positive")
        if self.max_expanded_bytes <= 0:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "max_expanded_bytes must be positive")
        if not math.isfinite(self.max_compression_ratio) or self.max_compression_ratio <= 0:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "max_compression_ratio must be finite and positive")
        # A depth of zero permits only root-level members; negative is invalid.
        if self.max_depth < 0:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "max_depth cannot be negative")
        if self.chunk_size <= 0:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "chunk_size must be positive")


@dataclass(frozen=True)
class ArchiveEntry:
    name: str
    compressed_bytes: int
    expanded_bytes: int
    is_dir: bool
    depth: int


@dataclass
class ArchiveMember:
    """A validated archive member backed by a bounded spool.

    ``iter_archive_members`` validates the complete ZIP index once and then
    yields members in archive order.  The member payload is decompressed in
    ``ArchiveLimits.chunk_size`` chunks into a ``SpooledTemporaryFile``; small
    entries stay in memory while larger entries roll to disk.  Callers that
    need bytes can use :meth:`read_bytes`, while readers that support streams
    should prefer :meth:`iter_chunks` to avoid an additional allocation.
    """

    entry: ArchiveEntry
    _spool: BinaryIO
    _chunk_size: int = 1024 * 1024

    def open(self) -> BinaryIO:
        """Return the seekable spooled payload positioned at byte zero."""
        self._spool.seek(0)
        return self._spool

    def iter_chunks(self, chunk_size: int | None = None) -> Iterator[bytes]:
        """Yield payload chunks without reopening or rescanning the archive."""
        size = chunk_size or self._chunk_size
        if size <= 0:
            raise ValueError("chunk_size must be positive")
        self._spool.seek(0)
        while True:
            chunk = self._spool.read(size)
            if not chunk:
                break
            yield chunk

    def read_bytes(self) -> bytes:
        """Materialize this member for legacy byte-oriented readers."""
        return b"".join(self.iter_chunks())

    def close(self) -> None:
        with suppress(Exception):
            self._spool.close()

    def __enter__(self) -> "ArchiveMember":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@dataclass(frozen=True)
class _ValidatedRecord:
    info: zipfile.ZipInfo
    entry: ArchiveEntry


class _ValidatedArchive:
    """One open ZIP plus its validated member index.

    Building this object performs all path, duplicate, encryption and bomb
    checks exactly once.  Consumers can then traverse records or read one
    indexed member without the old per-entry ``ZipFile``/``infolist`` scan.
    """

    def __init__(self, source: str | bytes | bytearray | BinaryIO, limits: ArchiveLimits):
        self._owned_handle: io.BytesIO | None = io.BytesIO(source) if isinstance(source, (bytes, bytearray)) else None
        handle = self._owned_handle if self._owned_handle is not None else source
        try:
            self.archive = zipfile.ZipFile(handle)
        except (zipfile.BadZipFile, OSError) as exc:
            if self._owned_handle is not None:
                self._owned_handle.close()
            raise ArchiveSafetyError(IssueCode.INVALID_ARCHIVE, "invalid ZIP archive") from exc
        self.limits = limits
        try:
            infos = self.archive.infolist()
            if len(infos) > limits.max_entries:
                raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "archive entry count exceeds limit")
            expanded = 0
            normalized_names: set[str] = set()
            records: list[_ValidatedRecord] = []
            by_name: dict[str, _ValidatedRecord] = {}
            by_exact_name: dict[str, _ValidatedRecord] = {}
            for info in infos:
                safe_name, depth = _validate_name(info.filename, limits)
                normalized_key = safe_name.casefold()
                if normalized_key in normalized_names:
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_PATH_TRAVERSAL, "duplicate archive member name", entry=safe_name)
                normalized_names.add(normalized_key)
                mode = (info.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_PATH_TRAVERSAL, "symlink archive members are not allowed", entry=safe_name)
                if info.flag_bits & 0x1:
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_ENCRYPTED, "encrypted archive entries are not supported", entry=safe_name)
                compressed = max(info.compress_size, 1)
                ratio = info.file_size / compressed
                if ratio > limits.max_compression_ratio:
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "archive compression ratio exceeds limit", entry=safe_name)
                expanded += info.file_size
                if expanded > limits.max_expanded_bytes:
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "archive expanded size exceeds limit", entry=safe_name)
                record = _ValidatedRecord(info, ArchiveEntry(safe_name, info.compress_size, info.file_size, info.is_dir(), depth))
                records.append(record)
                by_name[normalized_key] = record
                by_exact_name[safe_name] = record
            self.records = tuple(records)
            self.by_name = by_name
            self.by_exact_name = by_exact_name
        except ArchiveSafetyError:
            self.close()
            raise
        except (zipfile.BadZipFile, OSError) as exc:
            self.close()
            raise ArchiveSafetyError(IssueCode.INVALID_ARCHIVE, "invalid ZIP archive") from exc
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        with suppress(Exception):
            self.archive.close()
        if self._owned_handle is not None:
            with suppress(Exception):
                self._owned_handle.close()

    def __enter__(self) -> "_ValidatedArchive":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _spool(self, record: _ValidatedRecord) -> ArchiveMember:
        # Keep at most 8 MiB per active member in memory; larger payloads roll
        # to the platform temporary directory while preserving chunked reads.
        max_memory = min(max(self.limits.chunk_size * 4, 1024 * 1024), 8 * 1024 * 1024)
        spool = tempfile.SpooledTemporaryFile(max_size=max_memory, mode="w+b")
        read_total = 0
        try:
            if not record.entry.is_dir:
                with self.archive.open(record.info, "r") as member:
                    while True:
                        chunk = member.read(self.limits.chunk_size)
                        if not chunk:
                            break
                        read_total += len(chunk)
                        if read_total > record.entry.expanded_bytes:
                            raise ArchiveSafetyError(IssueCode.ARCHIVE_CORRUPT_ENTRY, "archive entry exceeds declared size", entry=record.entry.name)
                        spool.write(chunk)
                if read_total != record.entry.expanded_bytes:
                    raise ArchiveSafetyError(IssueCode.ARCHIVE_CORRUPT_ENTRY, "archive entry size does not match declaration", entry=record.entry.name)
            spool.seek(0)
            return ArchiveMember(record.entry, spool, self.limits.chunk_size)
        except (ArchiveSafetyError, zipfile.BadZipFile, RuntimeError, OSError) as exc:
            spool.close()
            if isinstance(exc, ArchiveSafetyError):
                raise
            raise ArchiveSafetyError(IssueCode.ARCHIVE_CORRUPT_ENTRY, "could not read archive entry", entry=record.entry.name) from exc

    def read_member(self, name: str) -> bytes:
        safe_name, _ = _validate_name(name, self.limits)
        # Preserve the legacy helper's case-sensitive lookup semantics while
        # retaining case-folded indexing for duplicate/collision validation.
        record = self.by_exact_name.get(safe_name)
        if record is None:
            raise ArchiveSafetyError(IssueCode.ARCHIVE_CORRUPT_ENTRY, "archive entry not found", entry=safe_name)
        member = self._spool(record)
        try:
            return member.read_bytes()
        finally:
            member.close()


def _validate_name(name: str, limits: ArchiveLimits) -> tuple[str, int]:
    if not name or "\x00" in name:
        raise ArchiveSafetyError(IssueCode.ARCHIVE_PATH_TRAVERSAL, "invalid archive path", entry=name)
    # ZIP names are POSIX-like, but accepting Windows syntax here is a common
    # extraction escape.  Reject it rather than trying to normalize it.
    if "\\" in name or ":" in name or name.startswith(("/", "//")):
        raise ArchiveSafetyError(IssueCode.ARCHIVE_PATH_TRAVERSAL, "Windows/absolute archive path is not allowed", entry=name)
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise ArchiveSafetyError(IssueCode.ARCHIVE_PATH_TRAVERSAL, "archive path escapes extraction root", entry=name)
    depth = len(path.parts) - 1
    if depth > limits.max_depth:
        raise ArchiveSafetyError(IssueCode.ARCHIVE_LIMIT, "archive nesting depth exceeds limit", entry=name)
    return "/".join(path.parts), depth


def iter_archive_entries(source: str | bytes | bytearray | BinaryIO,
                         limits: ArchiveLimits | None = None) -> Iterator[ArchiveEntry]:
    limits = limits or ArchiveLimits()
    with _ValidatedArchive(source, limits) as validated:
        for record in validated.records:
            yield record.entry


def iter_archive_members(source: str | bytes | bytearray | BinaryIO,
                         limits: ArchiveLimits | None = None) -> Iterator[ArchiveMember]:
    """Yield validated ZIP members using one index traversal and bounded spooling.

    All metadata safety checks run once before the first member is yielded.  A
    member's compressed stream is opened at most once; callers can consume its
    spooled stream repeatedly without reopening the archive or rescanning its
    central directory.  Members should be closed after use (or used as a
    context manager) to release temporary files promptly.
    """
    limits = limits or ArchiveLimits()
    with _ValidatedArchive(source, limits) as validated:
        for record in validated.records:
            yield validated._spool(record)


def read_archive_entry(source: str | bytes | bytearray | BinaryIO, name: str,
                       limits: ArchiveLimits | None = None) -> bytes:
    """Read one validated member; callers should spool large members."""
    limits = limits or ArchiveLimits()
    with _ValidatedArchive(source, limits) as validated:
        return validated.read_member(name)
