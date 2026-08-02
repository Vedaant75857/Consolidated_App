from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from backend.ingestion.models import TableArtifact
from backend.ingestion.transfer import (
    CutoverGate,
    CutoverMode,
    ParityEvidence,
    TransferManifest,
)


PARQUET = b"PAR1" + b"typed rows and schema" + b"PAR1"
ARROW_STREAM = b"\xff\xff\xff\xff" + (4).to_bytes(4, "little") + b"meta"


def _manifest(payload: bytes = PARQUET) -> TransferManifest:
    return TransferManifest.from_payload(
        "artifact-1",
        "module1",
        "module2",
        [TableArtifact("table-1", "Table 1", row_count=1)],
        payload,
    )


def test_transfer_verification_binds_to_actual_parquet_bytes() -> None:
    manifest = _manifest()
    assert manifest.payload_format == "parquet"
    assert manifest.verify()
    assert manifest.verify_payload(PARQUET)
    assert manifest.verify(PARQUET)
    assert not manifest.verify_payload(PARQUET.replace(b"rows", b"ROWS"))
    via_tables = TransferManifest.from_tables(
        "artifact-1", "module1", "module2", [TableArtifact("table-1", "Table 1", row_count=1)], payload=PARQUET
    )
    assert via_tables.verify_payload(PARQUET)


def test_arrow_ipc_stream_payload_is_supported() -> None:
    manifest = _manifest(ARROW_STREAM)
    assert manifest.payload_format == "arrow"
    assert manifest.verify_payload(ARROW_STREAM)


def test_transfer_rejects_non_typed_or_mislabeled_payload() -> None:
    manifest = TransferManifest.from_tables(
        "artifact-1", "module1", "module2", [TableArtifact("table-1", "Table 1")]
    )
    assert not manifest.verify_payload(PARQUET)
    with pytest.raises(ValueError):
        manifest.bind_payload(b"id,name\n1,A\n")
    with pytest.raises(ValueError):
        manifest.bind_payload(PARQUET, payload_format="arrow")
    with pytest.raises(ValueError):
        manifest.bind_payload(b"arbitrary bytes", payload_format="parquet")
    arbitrary = b"not really parquet"
    forged = replace(
        manifest,
        payload_checksum=hashlib.sha256(arbitrary).hexdigest(),
        payload_format="parquet",
    )
    forged = replace(forged, artifact_checksum=forged._compute_checksum())
    assert not forged.verify_payload(arbitrary)


def test_cutover_is_legacy_by_default_and_ordered() -> None:
    gate = CutoverGate()
    assert gate.mode("module1") is CutoverMode.LEGACY
    assert gate.mode("module2") is CutoverMode.LEGACY
    assert gate.mode("module3") is CutoverMode.LEGACY
    passing_m1 = ParityEvidence.passing("module1")
    passing_m2 = ParityEvidence.passing("module2", predecessor="module1")
    passing_m3 = ParityEvidence.passing("module3", predecessor="module2")

    with pytest.raises(ValueError):
        gate.enable("module2", passing_m2)
    gate.enable("module1", passing_m1)
    with pytest.raises(ValueError):
        gate.enable("module3", passing_m3)
    gate.enable("module2", passing_m2)
    gate.enable("module3", passing_m3)
    assert gate.to_dict()["modes"] == {
        "module1": "typed",
        "module2": "typed",
        "module3": "typed",
    }


def test_parity_evidence_derives_transfer_result_from_payload() -> None:
    manifest = _manifest()
    evidence = ParityEvidence.from_transfer(
        "module1",
        manifest,
        PARQUET,
        raw_hash_match=True,
        typed_schema_match=True,
        row_count_match=True,
        workflow_passed=True,
    )
    assert evidence.passed
    tampered = ParityEvidence.from_transfer(
        "module1",
        manifest,
        PARQUET + b"tampered",
        raw_hash_match=True,
        typed_schema_match=True,
        row_count_match=True,
        workflow_passed=True,
    )
    assert not tampered.passed


def test_failed_predecessor_evidence_never_unlocks_next_module() -> None:
    gate = CutoverGate()
    failed = ParityEvidence("module1", raw_hash_match=True)
    assert gate.record_evidence(failed) is False
    assert not gate.can_enable("module1")
    with pytest.raises(ValueError):
        gate.enable("module2", ParityEvidence.passing("module2", predecessor="module1"))
    assert gate.mode("module1") is CutoverMode.LEGACY


def test_tampered_metadata_cannot_be_repaired_by_external_checksum() -> None:
    manifest = _manifest()
    tampered = replace(manifest, tables=())
    assert not tampered.verify_payload(PARQUET)
    assert not tampered.verify(manifest.artifact_checksum)


def test_contract_version_and_encoding_are_checksum_bound() -> None:
    manifest = _manifest()
    for changes in ({"contract_version": "ingestion.v0"}, {"encoding": "legacy-csv"}):
        tampered = replace(manifest, **changes)
        assert not tampered.verify()
        assert not tampered.verify_payload(PARQUET)

    # Old metadata can only be accepted through an explicit migration check.
    legacy_checksum = manifest._compute_legacy_checksum()
    assert not manifest.verify(legacy_checksum)
    assert manifest.verify_legacy(legacy_checksum)
