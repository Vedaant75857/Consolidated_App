from __future__ import annotations

import duckdb

from backend.ingestion import DelimitedReader, IngestionRequest, IngestionService, SourceAsset, ValueType


def test_service_uses_native_csv_path_without_materializing_parse(monkeypatch) -> None:
    def fail_parse(*args, **kwargs):
        raise AssertionError("optimized service path must not call parse")

    monkeypatch.setattr(DelimitedReader, "parse", fail_parse)
    connection = duckdb.connect(":memory:")
    request = IngestionRequest(SourceAsset("rows.csv", source_id="stream"))
    result = IngestionService(sink_connection=connection).ingest(request, source=b"id,n\n001,2\n002,3\n")
    assert result.ok
    assert result.tables[0].columns[1].value_type is ValueType.INTEGER
    assert result.stats.rows_loaded == 3  # source row zero is intentionally retained


def test_native_csv_progress_and_early_cancellation_leave_no_tables() -> None:
    events: list[dict] = []
    connection = duckdb.connect(":memory:")
    request = IngestionRequest(SourceAsset("rows.csv", source_id="cancel"))
    result = IngestionService(sink_connection=connection, progress=events.append).ingest(
        request, source=b"id,n\n001,2\n", cancel=lambda: True
    )
    assert not result.ok
    assert not events or any(event.get("phase") == "reading" for event in events)
    assert connection.execute("show tables").fetchall() == []
