"""Typed transfer contracts and the staged module cutover gate.

The manifest is deliberately independent of HTTP and module route code.  A
manifest can still be used as a metadata-only description by legacy callers,
but a typed transfer is only *verified* when the actual Arrow/Parquet bytes
are supplied.  This distinction prevents a producer from claiming a checksum
for bytes that were never inspected by the receiver.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, ClassVar

from .models import CONTRACT_VERSION, OrderedColumnSchema, TableArtifact, to_json_dict


@dataclass(frozen=True)
class TransferTable:
    table_key: str
    columns: tuple[OrderedColumnSchema, ...]
    row_count: int
    raw_hash: str | None = None
    typed_hash: str | None = None
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.table_key:
            raise ValueError("table_key is required")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        keys = [column.key for column in self.columns]
        if len(keys) != len(set(keys)):
            raise ValueError("transfer columns must have unique keys")
        ordinals = [column.ordinal for column in self.columns]
        if ordinals != sorted(ordinals) or len(ordinals) != len(set(ordinals)):
            raise ValueError("transfer columns must have unique ordered ordinals")


@dataclass(frozen=True)
class TransferManifest:
    artifact_id: str
    source_module: str
    destination_module: str
    tables: tuple[TransferTable, ...]
    artifact_checksum: str | None = None
    contract_version: str = CONTRACT_VERSION
    encoding: str = "typed-artifact"
    # These fields are populated only after the producer has materialized the
    # typed artifact.  ``artifact_checksum`` remains the backwards-compatible
    # metadata checksum; ``payload_checksum`` is the checksum of real bytes.
    payload_checksum: str | None = None
    payload_format: str | None = None

    _PARQUET_MAGIC: ClassVar[bytes] = b"PAR1"
    _ARROW_FILE_MAGIC: ClassVar[bytes] = b"ARROW1"

    def __post_init__(self) -> None:
        if not self.artifact_id or not self.source_module or not self.destination_module:
            raise ValueError("artifact_id, source_module and destination_module are required")
        keys = [table.table_key for table in self.tables]
        if len(keys) != len(set(keys)):
            raise ValueError("transfer tables must have unique keys")
        if self.artifact_checksum is None:
            object.__setattr__(self, "artifact_checksum", self._compute_checksum())

    @classmethod
    def from_tables(cls, artifact_id: str, source_module: str,
                    destination_module: str, tables: tuple[TableArtifact, ...] | list[TableArtifact], *,
                    payload: bytes | bytearray | memoryview | str | Path | None = None,
                    payload_format: str | None = None) -> "TransferManifest":
        manifest = cls(artifact_id, source_module, destination_module, tuple(
            TransferTable(t.table_key, t.columns, t.row_count, t.raw_hash, t.typed_hash,
                          {"columns": [to_json_dict(p) for p in t.provenance]}) for t in tables
        ))
        return manifest if payload is None else manifest.bind_payload(payload, payload_format=payload_format)

    def to_dict(self) -> dict[str, Any]:
        return to_json_dict(self)

    def _compute_checksum(self) -> str:
        payload = json.dumps({
            "artifact_id": self.artifact_id,
            "source_module": self.source_module,
            "contract_version": self.contract_version,
            "encoding": self.encoding,
            "tables": [to_json_dict(table) for table in self.tables],
            **({"payload_checksum": self.payload_checksum} if self.payload_checksum else {}),
            **({"payload_format": self.payload_format} if self.payload_format else {}),
        }, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    def _compute_legacy_checksum(self) -> str:
        """Checksum shape used before contract version/encoding were bound.

        This is intentionally private; callers that must import an old
        manifest should use :meth:`verify_legacy` explicitly so a downgrade is
        visible rather than silently accepted by the normal verifier.
        """
        payload = json.dumps({
            "artifact_id": self.artifact_id,
            "source_module": self.source_module,
            "tables": [to_json_dict(table) for table in self.tables],
        }, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _coerce_payload(payload: bytes | bytearray | memoryview | str | Path) -> bytes:
        """Return payload bytes without trusting a caller-provided digest.

        Paths are accepted for adapter convenience, but are read exactly once
        and are never treated as checksums.  File-like streams are intentionally
        not accepted: consuming a non-seekable upload here would make a later
        reader see a truncated artifact.
        """
        if isinstance(payload, (bytes, bytearray, memoryview)):
            return bytes(payload)
        if isinstance(payload, (str, Path)):
            return Path(payload).read_bytes()
        raise TypeError("payload must be bytes-like or a filesystem path")

    @classmethod
    def _detect_payload_format(cls, payload: bytes) -> str | None:
        if len(payload) >= 8 and payload.startswith(cls._PARQUET_MAGIC) and payload.endswith(cls._PARQUET_MAGIC):
            return "parquet"
        if len(payload) >= 12 and payload.startswith(cls._ARROW_FILE_MAGIC) and payload.endswith(cls._ARROW_FILE_MAGIC):
            return "arrow"
        # Arrow IPC stream files have no trailing ``ARROW1`` marker.  They
        # begin with the continuation token and a little-endian metadata
        # length; require a bounded first message before accepting the format.
        if len(payload) >= 8 and payload[:4] == b"\xff\xff\xff\xff":
            metadata_length = int.from_bytes(payload[4:8], "little", signed=False)
            if 0 < metadata_length <= len(payload) - 8:
                return "arrow"
        return None

    def bind_payload(
        self,
        payload: bytes | bytearray | memoryview | str | Path,
        *,
        payload_format: str | None = None,
    ) -> "TransferManifest":
        """Return a manifest bound to the digest of materialized payload bytes.

        ``payload_format`` may be omitted when Parquet or Arrow file magic is
        present.  Unknown formats are rejected rather than allowing a CSV or a
        caller-generated digest to masquerade as a typed artifact.
        """
        raw = self._coerce_payload(payload)
        detected = self._detect_payload_format(raw)
        fmt = (payload_format or detected or "").lower()
        if fmt not in {"parquet", "arrow"}:
            raise ValueError("typed transfer payload must be an Arrow or Parquet file")
        # Explicit format labels do not waive byte-level validation.  A
        # producer cannot label arbitrary bytes as Parquet/Arrow to satisfy a
        # checksum-only contract.
        if detected != fmt:
            raise ValueError(f"payload format {fmt!r} does not match detected {detected!r} bytes")
        if not raw:
            raise ValueError("typed transfer payload cannot be empty")
        bound = dataclass_replace(
            self,
            payload_checksum=hashlib.sha256(raw).hexdigest(),
            payload_format=fmt,
        )
        # The metadata checksum covers the payload binding fields as well.  A
        # stale checksum from the unbound manifest must not survive cutover.
        return dataclass_replace(bound, artifact_checksum=bound._compute_checksum())

    # A descriptive alias for adapters and callers that prefer ``with_*``.
    with_payload = bind_payload

    @classmethod
    def from_payload(
        cls,
        artifact_id: str,
        source_module: str,
        destination_module: str,
        tables: tuple[TableArtifact, ...] | list[TableArtifact],
        payload: bytes | bytearray | memoryview | str | Path,
        *,
        payload_format: str | None = None,
    ) -> "TransferManifest":
        return cls.from_tables(artifact_id, source_module, destination_module, tables).bind_payload(
            payload, payload_format=payload_format
        )

    def verify_payload(self, payload: bytes | bytearray | memoryview | str | Path) -> bool:
        """Verify metadata *and* the checksum of actual Arrow/Parquet bytes."""
        if not self.payload_checksum or self.payload_format not in {"arrow", "parquet"}:
            return False
        try:
            raw = self._coerce_payload(payload)
            detected = self._detect_payload_format(raw)
        except (OSError, TypeError, ValueError):
            return False
        if detected != self.payload_format:
            return False
        return (
            self.verify()
            and hashlib.sha256(raw).hexdigest() == self.payload_checksum
        )

    def verify(
        self,
        expected_artifact_checksum: str | bytes | bytearray | memoryview | Path | None = None,
        payload: bytes | bytearray | memoryview | str | Path | None = None,
    ) -> bool:
        """Verify manifest metadata, optionally binding it to actual bytes.

        Existing callers may pass a metadata checksum or call ``verify()`` as
        before.  Passing payload bytes positionally is also supported for the
        new strict path (``verify(payload_bytes)``); a bytes-like first argument
        is never interpreted as a checksum string.
        """
        if payload is None and isinstance(expected_artifact_checksum, (bytes, bytearray, memoryview, Path)):
            payload = expected_artifact_checksum
            expected_artifact_checksum = None
        if payload is not None:
            return self.verify_payload(payload)
        expected = expected_artifact_checksum or self.artifact_checksum
        return bool(isinstance(expected, str) and expected == self._compute_checksum())

    def verify_legacy(self, expected_artifact_checksum: str | None = None) -> bool:
        """Explicitly verify a pre-v1 metadata checksum.

        Legacy verification never validates a typed payload and must not be
        used to enable a cutover.  It exists solely for controlled migration
        of persisted manifests created before checksum fields were bound.
        """
        expected = expected_artifact_checksum or self.artifact_checksum
        return bool(isinstance(expected, str) and expected == self._compute_legacy_checksum())

    def digest(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(payload).hexdigest()


def dataclass_replace(instance: Any, **changes: Any) -> Any:
    """Local alias kept tiny to avoid exposing dataclasses.replace in the API."""
    from dataclasses import replace

    return replace(instance, **changes)


class CutoverMode(str, Enum):
    """Operational mode for one module's ingestion path."""

    LEGACY = "legacy"
    SHADOW = "shadow"
    TYPED = "typed"


MODULE_ORDER: tuple[str, ...] = ("module1", "module2", "module3")


@dataclass(frozen=True)
class ParityEvidence:
    """Evidence required before a module can leave its legacy loader.

    Every gate is explicit.  This avoids treating a row-count-only comparison
    or a caller-provided checksum as parity.  ``transfer_passed`` is where the
    payload-bound Arrow/Parquet verification is recorded.
    """

    module: str
    raw_hash_match: bool = False
    typed_schema_match: bool = False
    row_count_match: bool = False
    workflow_passed: bool = False
    transfer_passed: bool = False
    predecessor: str | None = None

    @property
    def passed(self) -> bool:
        return all(
            (
                self.raw_hash_match,
                self.typed_schema_match,
                self.row_count_match,
                self.workflow_passed,
                self.transfer_passed,
            )
        )

    @property
    def complete(self) -> bool:
        return self.passed

    @classmethod
    def passing(cls, module: str, *, predecessor: str | None = None) -> "ParityEvidence":
        return cls(module, True, True, True, True, True, predecessor)

    @classmethod
    def from_transfer(
        cls,
        module: str,
        manifest: TransferManifest,
        payload: bytes | bytearray | memoryview | str | Path,
        *,
        raw_hash_match: bool,
        typed_schema_match: bool,
        row_count_match: bool,
        workflow_passed: bool,
        predecessor: str | None = None,
    ) -> "ParityEvidence":
        """Build evidence with transfer parity derived from real payload bytes."""
        return cls(
            module,
            raw_hash_match,
            typed_schema_match,
            row_count_match,
            workflow_passed,
            manifest.verify_payload(payload),
            predecessor,
        )


@dataclass
class CutoverGate:
    """Parity-gated, ordered cutover state for Modules 1 → 2 → 3.

    Construction is intentionally legacy-only.  A typed mode transition needs
    passing evidence for the module itself and, for Modules 2/3, a predecessor
    that has already completed typed cutover with passing evidence.  Failed
    evidence is retained for diagnostics but can never unlock a transition.
    """

    modes: dict[str, CutoverMode] = field(default_factory=lambda: {
        module: CutoverMode.LEGACY for module in MODULE_ORDER
    })
    evidence: dict[str, ParityEvidence] = field(default_factory=dict)

    def __post_init__(self) -> None:
        unknown = set(self.modes) - set(MODULE_ORDER)
        if unknown:
            raise ValueError(f"unknown cutover module(s): {sorted(unknown)}")
        for module in MODULE_ORDER:
            self.modes.setdefault(module, CutoverMode.LEGACY)
        for module, mode in list(self.modes.items()):
            self.modes[module] = CutoverMode(mode)

    def mode(self, module: str) -> CutoverMode:
        self._check_module(module)
        return self.modes[module]

    def record_evidence(self, evidence: ParityEvidence) -> bool:
        self._check_module(evidence.module)
        if evidence.predecessor is not None:
            expected = self.predecessor(evidence.module)
            if evidence.predecessor != expected:
                raise ValueError(f"{evidence.module} predecessor must be {expected}")
        self.evidence[evidence.module] = evidence
        return evidence.passed

    def can_enable(self, module: str, evidence: ParityEvidence | None = None) -> bool:
        self._check_module(module)
        candidate = evidence or self.evidence.get(module)
        if candidate is None or candidate.module != module or not candidate.passed:
            return False
        predecessor = self.predecessor(module)
        if predecessor is None:
            return True
        previous_evidence = self.evidence.get(predecessor)
        return (
            self.modes[predecessor] is CutoverMode.TYPED
            and previous_evidence is not None
            and previous_evidence.passed
        )

    def enable(self, module: str, evidence: ParityEvidence | None = None) -> CutoverMode:
        if not self.can_enable(module, evidence):
            raise ValueError(
                f"{module} typed cutover is parity-gated; pass module and predecessor evidence first"
            )
        if evidence is not None:
            self.record_evidence(evidence)
        self.modes[module] = CutoverMode.TYPED
        return self.modes[module]

    def enable_typed(self, module: str, evidence: ParityEvidence | None = None) -> CutoverMode:
        return self.enable(module, evidence)

    def predecessor(self, module: str) -> str | None:
        self._check_module(module)
        index = MODULE_ORDER.index(module)
        return MODULE_ORDER[index - 1] if index else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "modes": {module: mode.value for module, mode in self.modes.items()},
            "evidence": {module: to_json_dict(value) for module, value in self.evidence.items()},
        }

    @staticmethod
    def _check_module(module: str) -> None:
        if module not in MODULE_ORDER:
            raise ValueError(f"unknown cutover module: {module}")
