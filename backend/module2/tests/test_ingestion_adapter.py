from __future__ import annotations

from backend.module2.ingestion_adapter import (
    Module2AdapterConfig,
    Module2IngestionAdapter,
    _discover_shared_loader,
)
from backend.ingestion import CutoverGate, CutoverMode, ParityEvidence


def _module2_gate() -> CutoverGate:
    return CutoverGate(
        modes={"module1": CutoverMode.TYPED},
        evidence={"module1": ParityEvidence.passing("module1")},
    )


def test_adapter_is_legacy_by_default_and_preserves_inventory_contract():
    calls = []

    def legacy(conn, name, data, inventory):
        calls.append((conn, name, data))
        inventory.append({"table_key": f"{name}::", "rows": 1, "cols": 1})
        return None

    inventory = []
    adapter = Module2IngestionAdapter()
    result = adapter.load_source_file_to_session(
        "conn", "input.csv", b"a\n1\n", inventory, legacy_loader=legacy,
    )

    assert result is None
    assert calls == [("conn", "input.csv", b"a\n1\n")]
    assert inventory == [{"table_key": "input.csv::", "rows": 1, "cols": 1}]
    assert adapter.last_report is None


def test_cutover_requires_predecessor_and_explicit_parity_evidence():
    config = Module2AdapterConfig(
        shadow_enabled=True,
        cutover_enabled=True,
        parity_gate_enabled=True,
        predecessor_gate_enabled=True,
        parity_evidence=True,
        cutover_gate=_module2_gate(),
    )
    legacy_calls = []
    shared_calls = []

    def legacy(conn, name, data, inventory):
        legacy_calls.append(name)
        inventory.append({"table_key": f"{name}::", "rows": 1, "cols": 1})

    def shared(name, data):
        shared_calls.append((name, data))
        return [{"table_key": f"{name}::", "rows": 1, "cols": 1}]

    adapter = Module2IngestionAdapter(shared_loader=shared, cutover_loader=shared, config=config)
    inventory = []
    adapter.load_source_file_to_session(
        None, "input.csv", b"a\n1\n", inventory, legacy_loader=legacy,
        session_id="session-a",
    )
    assert legacy_calls == ["input.csv"]
    assert adapter.last_report is not None
    assert adapter.last_report.cutover_allowed

    # Once a matching report exists, the shared runner may be selected.  The
    # caller's session id/DTO remains outside this adapter and is untouched.
    adapter.load_source_file_to_session(
        None, "input.csv", b"a\n1\n", inventory, legacy_loader=legacy,
        session_id="session-a",
    )
    assert legacy_calls == ["input.csv"]
    assert shared_calls[-1] == ("input.csv", b"a\n1\n")


def test_cutover_is_blocked_when_any_gate_is_missing():
    config = Module2AdapterConfig(
        cutover_enabled=True,
        parity_gate_enabled=True,
        predecessor_gate_enabled=True,
        parity_evidence=False,
        cutover_gate=_module2_gate(),
    )
    calls = []

    def legacy(conn, name, data, inventory):
        calls.append(name)

    def shared(name, data):
        raise AssertionError("shared loader must not run without evidence")

    adapter = Module2IngestionAdapter(shared_loader=shared, config=config)
    adapter.load_source_file_to_session(None, "input.csv", b"", [], legacy_loader=legacy)
    assert calls == ["input.csv"]


def test_parity_for_file_a_never_unlocks_file_b():
    config = Module2AdapterConfig(
        shadow_enabled=True,
        cutover_enabled=True,
        parity_gate_enabled=True,
        predecessor_gate_enabled=True,
        parity_evidence=True,
        cutover_gate=_module2_gate(),
    )
    legacy_calls = []

    def legacy(conn, name, data, inventory):
        legacy_calls.append(name)
        inventory.append({"table_key": f"{name}::", "rows": 1, "cols": 1})

    def shared(name, data):
        return [{"table_key": f"{name}::", "rows": 1, "cols": 1}]

    adapter = Module2IngestionAdapter(
        shared_loader=shared, cutover_loader=shared, config=config,
    )
    inventory = []
    adapter.load_source_file_to_session(
        None, "a.csv", b"a\n1\n", inventory, legacy_loader=legacy,
        session_id="session-a",
    )
    adapter.load_source_file_to_session(
        None, "b.csv", b"b\n2\n", inventory, legacy_loader=legacy,
        session_id="session-a",
    )
    assert legacy_calls == ["a.csv", "b.csv"]


def test_shadow_runner_requiring_live_connection_is_not_called():
    config = Module2AdapterConfig(shadow_enabled=True)
    calls = []

    def legacy(conn, name, data, inventory):
        calls.append("legacy")

    def unsafe(conn, name, data):
        raise AssertionError("the production connection must not reach shadow code")

    adapter = Module2IngestionAdapter(shared_loader=unsafe, config=config)
    adapter.load_source_file_to_session(None, "input.csv", b"", [], legacy_loader=legacy)
    assert calls == ["legacy"]
    assert adapter.last_report is None


def test_generic_ingest_callable_is_not_discovered(monkeypatch):
    import sys
    import types

    service = types.ModuleType("ingestion.service")
    service.ingest = lambda request: request
    monkeypatch.setitem(sys.modules, "ingestion.service", service)
    assert _discover_shared_loader() is None


def test_request_runner_receives_caller_session_id():
    config = Module2AdapterConfig(shadow_enabled=True)
    seen = []

    def legacy(conn, name, data, inventory):
        inventory.append({"table_key": f"{name}::", "rows": 1, "cols": 1})

    def shared(request):
        seen.append(request.session_id)
        return [{"table_key": "input.csv::", "rows": 1, "cols": 1}]

    adapter = Module2IngestionAdapter(shared_loader=shared, config=config)
    adapter.load_source_file_to_session(
        None, "input.csv", b"a\n1\n", [], legacy_loader=legacy,
        session_id="caller-session",
    )
    assert seen == ["caller-session"]
