from __future__ import annotations

import io
import zipfile

import duckdb
from openpyxl import Workbook

from backend.ingestion import (
    DelimitedReader,
    DuckDBSink,
    ExcelReader,
    LegacyExcelReader,
    IngestionRequest,
    IngestionService,
    SourceAsset,
    SourceKind,
    ValueType,
)


def _calamine_fixture() -> bytes:
    stream = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Legacy"
    sheet.append(["name", "amount"])
    sheet.append(["item", 3])
    workbook.save(stream)
    return stream.getvalue()


def test_delimited_reader_preserves_lexical_values_and_types() -> None:
    request = IngestionRequest(SourceAsset("rows.csv"))
    parsed = DelimitedReader().parse(request, b"id,amount,flag\n001,1.20,TRUE\n002,2.30,FALSE\n")
    assert parsed.raw_rows[0] == ("id", "amount", "flag")
    assert parsed.raw_rows[1] == ("001", "1.20", "TRUE")
    assert parsed.rows[1] == ("001", parsed.rows[1][1], True)
    assert parsed.columns[0].value_type is ValueType.TEXT
    assert parsed.columns[1].value_type is ValueType.DECIMAL
    assert parsed.columns[2].value_type is ValueType.BOOLEAN


def test_excel_reader_handles_dates_and_formula_cache_boundary() -> None:
    stream = io.BytesIO()
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Data"
    sheet.append(["when", "value"])
    sheet.append([__import__("datetime").date(2024, 1, 2), 4])
    workbook.save(stream)
    parsed = ExcelReader().parse(IngestionRequest(SourceAsset("book.xlsx")), stream.getvalue())
    assert len(parsed) == 1
    assert parsed[0].columns[0].value_type in {ValueType.DATE, ValueType.TIMESTAMP}
    assert parsed[0].rows[1][1] == 4


def test_calamine_legacy_reader_preserves_rows_and_sheet_identity() -> None:
    # The installed calamine engine can read this OOXML fixture; naming it as
    # each legacy family exercises the family dispatch/contract without
    # requiring binary fixture downloads.
    for name in ("legacy.xls", "legacy.xlsb", "legacy.ods"):
        parsed = LegacyExcelReader().parse(IngestionRequest(SourceAsset(name)), _calamine_fixture())
        assert parsed[0].artifact.display_name == "Legacy"
        assert parsed[0].raw_rows[0] == ("name", "amount")
        assert parsed[0].rows[1][1] == 3.0
        assert any("calamine" in item.message for item in parsed[0].issues)


def test_duckdb_sink_publishes_raw_and_typed_tables_atomically() -> None:
    request = IngestionRequest(SourceAsset("rows.csv"))
    parsed = DelimitedReader().parse(request, b"id,n\n001,2\n")
    connection = duckdb.connect(":memory:")
    result = DuckDBSink(connection).write(parsed)
    assert connection.execute(f'SELECT * FROM "{result.raw_table}"').fetchall() == [(0, "id", "n"), (1, "001", "2")]
    assert connection.execute(f'SELECT * FROM "{result.typed_table}"').fetchall() == [(0, "id", None), (1, "001", 2)]
    assert result.raw_hash and result.typed_hash


def test_service_enforces_nested_archive_depth_at_open_boundary() -> None:
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as archive:
        archive.writestr("rows.csv", b"a\n1\n")
    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("nested.zip", inner.getvalue())
    request = IngestionRequest(SourceAsset("outer.zip"))
    result = IngestionService().ingest(request, source=outer.getvalue())
    assert result.ok
    constrained = IngestionRequest(request.source, policy=__import__("dataclasses").replace(request.policy, max_archive_depth=1))
    rejected = IngestionService().ingest(constrained, source=outer.getvalue())
    assert not rejected.ok
    assert any("ARCHIVE_LIMIT" in issue.code.value for issue in rejected.issues)
