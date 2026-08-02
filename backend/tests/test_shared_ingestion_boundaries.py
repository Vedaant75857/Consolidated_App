from __future__ import annotations

import io

import duckdb
from openpyxl import Workbook
import pytest

from backend.ingestion import DelimitedReader, DuckDBSink, ExcelReader, IngestionRequest, SourceAsset


def test_excel_reader_preserves_source_first_row_in_raw_representation() -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["Original header", "Second header"])
    sheet.append(["value", 7])
    payload = io.BytesIO()
    workbook.save(payload)

    parsed = ExcelReader().parse(IngestionRequest(SourceAsset("source.xlsx")), payload.getvalue())[0]

    assert parsed.artifact.row_count == 2
    assert parsed.raw_rows[0] == ("Original header", "Second header")
    assert parsed.raw_rows[1] == ("value", 7)


class _SpyConnection:
    def __init__(self) -> None:
        self.connection = duckdb.connect(":memory:")
        self.batch_lengths: list[int] = []

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def executemany(self, sql, parameters):
        rows = list(parameters)
        self.batch_lengths.append(len(rows))
        return self.connection.executemany(sql, rows)


def test_duckdb_sink_batches_writes_and_keeps_raw_table_immutable() -> None:
    request = IngestionRequest(SourceAsset("rows.csv"))
    parsed = DelimitedReader().parse(request, b"id,n\n001,2\n002,3\n003,4\n")
    spy = _SpyConnection()
    sink = DuckDBSink(spy, batch_size=2)

    result = sink.write(parsed)
    assert spy.batch_lengths == [2, 2, 2, 2]
    assert spy.connection.execute(f'SELECT COUNT(*) FROM "{result.raw_table}"').fetchone()[0] == 4

    with pytest.raises(ValueError, match="immutable ingestion table already exists"):
        sink.write(parsed)
    assert spy.connection.execute(f'SELECT COUNT(*) FROM "{result.raw_table}"').fetchone()[0] == 4


def test_duckdb_sink_rolls_back_partial_batch_on_row_width_error() -> None:
    request = IngestionRequest(SourceAsset("rows.csv"))
    parsed = DelimitedReader().parse(request, b"id,n\n001,2\n")
    spy = _SpyConnection()

    with pytest.raises(ValueError, match="row width"):
        DuckDBSink(spy, batch_size=1).write_table(parsed.artifact, [("id", "n"), ("001", "2")], [("id",), ("001", "2")])

    assert spy.connection.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rows__%'"
    ).fetchone()[0] == 0
