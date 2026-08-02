from __future__ import annotations

import io
import threading
import zipfile
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from backend.ingestion import (
    ColumnProvenance,
    DelimitedReader,
    DuckDBSink,
    IngestionPolicy,
    IngestionRequest,
    IngestionService,
    IssueCode,
    OrderedColumnSchema,
    SourceAsset,
    TableArtifact,
    ValueType,
)


def _table_rows(connection: duckdb.DuckDBPyConnection, suffix: str = "__raw") -> list[tuple[object, ...]]:
    name = next(row[0] for row in connection.execute("SHOW TABLES").fetchall() if row[0].endswith(suffix))
    return connection.execute(f'SELECT * FROM "{name}"').fetchall()


def test_late_ragged_row_uses_compatibility_path_and_reports_issue() -> None:
    rows = ["a,b\n"]
    rows.extend(f"{index},value-{index}\n" for index in range(20_050))
    rows.append("late,value,extra\n")
    connection = duckdb.connect(":memory:")

    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("large.csv", source_id="late-ragged")),
        source="".join(rows).encode(),
    )

    assert result.ok
    assert len(result.tables) == 1
    assert len(result.tables[0].columns) == 3
    assert result.tables[0].columns[2].display_name == "COL_3"
    assert any(item.code is IssueCode.PARTIAL_RESULT for item in result.issues)
    assert _table_rows(connection)[-1] == (20_051, "late", "value", "extra")


def test_cp1252_byte_after_prefix_window_is_preserved() -> None:
    # Keep the non-UTF8 byte beyond the 64 KiB sniff window without making
    # this fallback test materialize thousands of sink rows.
    payload = b"text\n" + (b"ordinary" * 9_000) + b"\ncaf\xe9\n"
    assert len(payload) > 65_536
    connection = duckdb.connect(":memory:")

    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("legacy.csv", source_id="cp1252-late")), source=payload
    )

    assert result.ok
    assert (2, "café") in _table_rows(connection)
    assert "�" not in "".join(str(value) for row in _table_rows(connection) for value in row)


def test_utf16be_falls_back_without_losing_byte_order_or_bom() -> None:
    payload = b"\xfe\xff" + "name,amount\nCafé,2\n".encode("utf-16-be")
    connection = duckdb.connect(":memory:")

    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("utf16.csv", source_id="utf16be")), source=payload
    )

    assert result.ok
    assert _table_rows(connection) == [(0, "name", "amount"), (1, "Café", "2")]


def test_unmatched_quote_fails_and_publishes_no_table() -> None:
    connection = duckdb.connect(":memory:")
    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("broken.csv", source_id="unmatched-quote")),
        source=b"a,b\n\"unterminated,value\n",
    )

    assert not result.ok
    assert not result.tables
    assert any(item.code is IssueCode.PARTIAL_RESULT for item in result.issues)
    assert connection.execute("SHOW TABLES").fetchall() == []


def test_blank_and_quoted_empty_cells_match_native_and_materialized_hashes() -> None:
    payload = b'a,b\n,\n"",\n" ",x\n'
    request = IngestionRequest(SourceAsset("empty-values.csv", source_id="native-empty"))
    native_connection = duckdb.connect(":memory:")
    native = IngestionService(sink_connection=native_connection).ingest(request, source=payload)
    assert native.ok

    materialized = DelimitedReader().parse(
        replace(request, source=SourceAsset("empty-values.csv", source_id="materialized-empty")), payload
    )
    materialized_connection = duckdb.connect(":memory:")
    materialized_sink = DuckDBSink(materialized_connection).write(materialized)

    assert native.tables[0].columns == materialized_sink.artifact.columns
    assert native.tables[0].raw_hash == materialized_sink.raw_hash
    assert native.tables[0].typed_hash == materialized_sink.typed_hash
    assert _table_rows(native_connection) == _table_rows(materialized_connection)
    assert _table_rows(native_connection, "__typed") == _table_rows(materialized_connection, "__typed")


def test_decimal_with_more_than_eighteen_fractional_digits_stays_text() -> None:
    payload = b"amount\n1.1234567890123456789\n"
    connection = duckdb.connect(":memory:")
    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("precision.csv", source_id="precision")), source=payload
    )

    assert result.ok
    assert result.tables[0].columns[0].value_type is ValueType.TEXT
    assert _table_rows(connection, "__typed") == [(0, "amount"), (1, "1.1234567890123456789")]


def test_fallback_spool_is_removed_when_compatibility_parser_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Path] = {}

    def fail_parse(_reader: DelimitedReader, _request: IngestionRequest, source: object = None):
        captured["path"] = Path(str(source))
        raise RuntimeError("compatibility parser failure")

    monkeypatch.setattr(DelimitedReader, "parse", fail_parse)
    connection = duckdb.connect(":memory:")
    result = IngestionService(sink_connection=connection).ingest(
        IngestionRequest(SourceAsset("fallback.csv", source_id="spool-cleanup")),
        source=b"text\nlate\xe9\n",
    )

    assert not result.ok
    assert captured["path"]
    assert not captured["path"].exists()
    assert connection.execute("SHOW TABLES").fetchall() == []


def test_progress_callback_exception_leaves_no_worker_or_table() -> None:
    # Enough data to ensure the native CREATE TABLE query crosses a heartbeat.
    payload = ("value\n" + "\n".join(str(index) for index in range(300_000)) + "\n").encode()
    events: list[dict[str, object]] = []

    def progress(event: dict[str, object]) -> None:
        events.append(event)
        if "heartbeat_seconds" in event:
            raise RuntimeError("progress sink failed")

    connection = duckdb.connect(":memory:")
    before = {thread.ident for thread in threading.enumerate()}
    result = IngestionService(sink_connection=connection, progress=progress).ingest(
        IngestionRequest(SourceAsset("progress.csv", source_id="progress-failure")), source=payload
    )

    assert not result.ok
    assert any("heartbeat_seconds" in event for event in events)
    assert connection.execute("SHOW TABLES").fetchall() == []
    assert not ({thread.ident for thread in threading.enumerate()} - before)


def test_nested_archive_budget_is_cumulative_across_members() -> None:
    csv_payload = b"value\n" + (b"x" * 600) + b"\n"
    inner_stream = io.BytesIO()
    with zipfile.ZipFile(inner_stream, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("rows.csv", csv_payload)
    inner_payload = inner_stream.getvalue()
    outer_stream = io.BytesIO()
    with zipfile.ZipFile(outer_stream, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("nested.zip", inner_payload)
    limit = max(len(inner_payload), len(csv_payload)) + 1
    request = IngestionRequest(
        SourceAsset("outer.zip", source_id="nested-budget"),
        policy=replace(IngestionPolicy(), max_expanded_bytes=limit, max_compression_ratio=1_000),
    )

    result = IngestionService().ingest(request, source=outer_stream.getvalue())

    assert not result.ok
    assert any(item.code is IssueCode.ARCHIVE_LIMIT for item in result.issues)


def test_materialized_sink_exposes_hash_metadata_and_rolls_back_on_mismatch() -> None:
    columns = (OrderedColumnSchema("col_1", "name", 0, ValueType.TEXT, physical_name="col_1"),)
    provenance = (ColumnProvenance("col_1", "rows.csv", 0, original_header="name"),)
    artifact = TableArtifact("materialized", "rows", columns, provenance=provenance)
    connection = duckdb.connect(":memory:")
    result = DuckDBSink(connection).write_table(artifact, [("name",), ("Alice",)], [("name",), ("Alice",)])

    assert result.artifact.provenance == provenance
    assert result.artifact.raw_hash == result.raw_hash
    assert result.artifact.typed_hash == result.typed_hash
    assert result.artifact.row_count == 2

    with pytest.raises(ValueError, match="row counts differ"):
        DuckDBSink(connection).write_table(
            TableArtifact("rollback", "rows", columns), [("name",)], []
        )
    assert not any("rollback" in row[0] for row in connection.execute("SHOW TABLES").fetchall())
