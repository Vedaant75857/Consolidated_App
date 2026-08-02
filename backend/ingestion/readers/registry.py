from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from ..models import IngestionRequest, SourceKind, TableArtifact


class UnsupportedFormatError(ValueError):
    code = "INGEST_UNSUPPORTED_FORMAT"


class Reader(Protocol):
    kinds: frozenset[SourceKind]
    name: str
    capabilities: dict[str, Any]

    def read(self, request: IngestionRequest) -> Iterable[TableArtifact]: ...


@dataclass(frozen=True)
class ReaderDescriptor:
    name: str
    kinds: frozenset[SourceKind]
    capabilities: dict[str, Any]


class ReaderRegistry:
    def __init__(self, readers: Iterable[Reader] = ()):
        self._readers: dict[SourceKind, Reader] = {}
        for reader in readers:
            self.register(reader)

    def register(self, reader: Reader) -> None:
        kinds = frozenset(reader.kinds)
        conflicts = kinds.intersection(self._readers)
        if conflicts:
            names = ", ".join(sorted(kind.value for kind in conflicts))
            raise ValueError(f"reader already registered for {names}")
        # Validate all kinds before mutating the registry (atomic registration).
        self._readers.update({kind: reader for kind in kinds})

    def descriptor(self, kind: SourceKind) -> ReaderDescriptor | None:
        reader = self._readers.get(kind)
        if reader is None:
            return None
        return ReaderDescriptor(reader.name, reader.kinds, dict(reader.capabilities))

    def get(self, kind: SourceKind) -> Reader:
        try:
            return self._readers[kind]
        except KeyError as exc:
            raise UnsupportedFormatError(f"no reader registered for {kind.value}") from exc

    def read(self, request: IngestionRequest, kind: SourceKind) -> Iterable[TableArtifact]:
        return self.get(kind).read(request)


class CapabilityReader:
    """Reader registration boundary; parsing is supplied in later phases."""
    def __init__(self, name: str, kinds: Iterable[SourceKind], **capabilities: Any):
        self.name = name
        self.kinds = frozenset(kinds)
        self.capabilities = capabilities

    def read(self, request: IngestionRequest) -> Iterable[TableArtifact]:
        raise UnsupportedFormatError(
            f"{self.name} parser is capability-only and is not enabled in this phase"
        )


def default_registry() -> ReaderRegistry:
    from .csv_reader import DelimitedReader
    from .excel_reader import ExcelReader, LegacyExcelReader
    registry = ReaderRegistry()
    registry.register(DelimitedReader())
    registry.register(ExcelReader())
    registry.register(LegacyExcelReader())
    registry.register(CapabilityReader("zip", [SourceKind.ZIP], archive_traversal=True))
    registry.register(CapabilityReader("parquet", [SourceKind.PARQUET], typed_transfer=True))
    return registry
