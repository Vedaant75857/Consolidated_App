"""OOXML Excel reader using openpyxl's read-only cell iterator.

Formula text is retained in ``raw_rows``.  Cached values are used only when a
second, ``data_only`` workbook exposes one; the backend never recalculates.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, BinaryIO, Iterable

from ..errors import issue
from ..models import ColumnProvenance, IngestionRequest, Issue, IssueCode, OrderedColumnSchema, SourceKind, TableArtifact, ValueType
from .csv_reader import ParsedTable


def _source(request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None) -> bytes | str | Path | BinaryIO:
    value = source if source is not None else request.source.path
    if value is None:
        raise ValueError("IngestionRequest.source.path or source bytes are required")
    return value


def _type_for(values: list[Any]) -> ValueType:
    nonnull = [v for v in values if v is not None]
    if not nonnull:
        return ValueType.NULL
    if all(isinstance(v, bool) for v in nonnull):
        return ValueType.BOOLEAN
    if all(isinstance(v, int) and not isinstance(v, bool) for v in nonnull):
        return ValueType.INTEGER
    if all(isinstance(v, float) for v in nonnull):
        return ValueType.FLOAT
    if all(isinstance(v, datetime) for v in nonnull):
        return ValueType.TIMESTAMP
    if all(isinstance(v, date) and not isinstance(v, datetime) for v in nonnull):
        return ValueType.DATE
    if all(isinstance(v, time) for v in nonnull):
        return ValueType.TIME
    return ValueType.TEXT


class ExcelReader:
    name = "excel-ooxml"
    kinds = frozenset({SourceKind.XLSX, SourceKind.XLSM, SourceKind.XLTX, SourceKind.XLTM})
    capabilities = {"typed_values": True, "formula_provenance": True, "merged_ranges": True, "streaming": True, "formula_recalculation": False}

    def parse(self, request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None = None) -> tuple[ParsedTable, ...]:
        try:
            import openpyxl
        except ImportError as exc:  # pragma: no cover - deployment issue
            raise RuntimeError("openpyxl is required for Excel ingestion") from exc
        value = _source(request, source)
        if isinstance(value, (bytes, bytearray, memoryview)):
            import io
            value = io.BytesIO(bytes(value))
        # Formula and cached workbooks are opened independently.  read_only
        # keeps memory bounded for large sheets.
        workbook = openpyxl.load_workbook(value, read_only=True, data_only=False, keep_vba=request.source.name.lower().endswith("m"))
        cached = None
        try:
            if hasattr(value, "seek"):
                value.seek(0)
                cached = openpyxl.load_workbook(value, read_only=True, data_only=True, keep_vba=request.source.name.lower().endswith("m"))
            elif isinstance(value, (str, Path)):
                cached = openpyxl.load_workbook(value, read_only=True, data_only=True, keep_vba=request.source.name.lower().endswith("m"))
        except Exception:
            cached = None
        result: list[ParsedTable] = []
        try:
            for sheet in workbook.worksheets:
                if sheet.sheet_state != "visible" and not request.policy.include_hidden_sheets:
                    continue
                cached_sheet = cached[sheet.title] if cached is not None and sheet.title in cached.sheetnames else None
                rows_raw: list[tuple[Any, ...]] = []
                rows_typed: list[tuple[Any, ...]] = []
                formula_issues: list[Issue] = []
                if sheet.sheet_state != "visible":
                    formula_issues.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, f"hidden worksheet included: {sheet.title}", source=request.source.name, location=sheet.title))
                merged = getattr(sheet, "merged_cells", None)
                if merged is not None and getattr(merged, "ranges", ()):
                    formula_issues.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, f"merged ranges preserved in {sheet.title}", source=request.source.name, location=sheet.title, details={"merged_ranges": [str(r) for r in merged.ranges]}))
                if not request.policy.include_hidden_rows and not hasattr(sheet, "row_dimensions"):
                    formula_issues.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, "read-only Excel engine does not expose hidden-row metadata; rows retained", source=request.source.name, location=sheet.title))
                for row in sheet.iter_rows():
                    row_dimensions = getattr(sheet, "row_dimensions", {})
                    if not request.policy.include_hidden_rows and getattr(getattr(row_dimensions, "get", lambda *_: None)(row[0].row), "hidden", False):
                        continue
                    raw_values: list[Any] = []
                    typed_values: list[Any] = []
                    for cell in row:
                        raw = cell.value
                        typed = raw
                        if cell.data_type == "f":
                            raw_values.append(raw)
                            cached_value = None
                            if cached_sheet is not None:
                                try:
                                    cached_value = cached_sheet[cell.coordinate].value
                                except Exception:
                                    cached_value = None
                            if request.policy.accept_formula_cache and cached_value is not None:
                                typed = cached_value
                            else:
                                typed = None
                                formula_issues.append(issue(IssueCode.FORMULA_NOT_RECALCULATED, f"formula cache unavailable for {sheet.title}!{cell.coordinate}", source=request.source.name, location=f"{sheet.title}!{cell.coordinate}"))
                        else:
                            raw_values.append(raw)
                        typed_values.append(typed)
                    rows_raw.append(tuple(raw_values))
                    rows_typed.append(tuple(typed_values))
                if not rows_raw:
                    continue
                width = max(len(row) for row in rows_raw)
                header = list(rows_raw[0]) + [f"COL_{i + 1}" for i in range(len(rows_raw[0]), width)]
                body_raw = tuple(tuple(row) + (None,) * (width - len(row)) for row in rows_raw)
                body_typed_data = tuple(tuple(row) + (None,) * (width - len(row)) for row in rows_typed[1:])
                columns = tuple(OrderedColumnSchema(f"col_{i + 1}", "" if name is None else str(name), i, _type_for([r[i] for r in body_typed_data]), physical_name=f"col_{i + 1}") for i, name in enumerate(header))
                body_typed = (tuple(rows_typed[0][i] if columns[i].value_type is ValueType.TEXT else None for i in range(width)),) + body_typed_data
                key_base = request.source.source_id or Path(request.source.name).stem
                provenance = tuple(ColumnProvenance(col.key, request.source.name, col.ordinal, workbook_sheet=sheet.title, original_header=col.display_name, source_type=col.value_type.value) for col in columns)
                artifact = TableArtifact(f"{key_base}:{sheet.title}", sheet.title, columns, len(body_typed), provenance=provenance)
                result.append(ParsedTable(artifact, body_raw, body_typed, tuple(formula_issues)))
        finally:
            workbook.close()
            if cached is not None:
                cached.close()
        return tuple(result)

    def read(self, request: IngestionRequest) -> Iterable[TableArtifact]:
        for table in self.parse(request):
            yield table.artifact


class LegacyExcelReader:
    """Reader for XLS/XLSB/ODS through python-calamine.

    Calamine intentionally exposes values rather than OOXML formula/style
    internals.  We retain all rows and sheet identity and emit a visible
    capability diagnostic for formula/date metadata that the engine cannot
    provide instead of pretending it was preserved.
    """

    name = "excel-calamine"
    kinds = frozenset({SourceKind.XLS, SourceKind.XLSB, SourceKind.ODS})
    capabilities = {"typed_values": True, "formula_recalculation": False, "formula_provenance": False, "merged_ranges": True, "streaming": True}

    def parse(self, request: IngestionRequest, source: bytes | bytearray | BinaryIO | str | Path | None = None) -> tuple[ParsedTable, ...]:
        try:
            from python_calamine import SheetVisibleEnum, load_workbook
        except ImportError as exc:
            raise RuntimeError("python-calamine is required for legacy Excel/ODS ingestion") from exc
        value = _source(request, source)
        if isinstance(value, (bytes, bytearray, memoryview)):
            import io
            value = io.BytesIO(bytes(value))
        workbook = load_workbook(value)
        result: list[ParsedTable] = []
        try:
            for metadata in workbook.sheets_metadata:
                if metadata.visible != SheetVisibleEnum.Visible and not request.policy.include_hidden_sheets:
                    continue
                sheet = workbook.get_sheet_by_name(metadata.name)
                rows_raw = [tuple(row) for row in sheet.iter_rows()]
                if not rows_raw:
                    continue
                width = max(len(row) for row in rows_raw)
                rows_raw = [tuple(row) + (None,) * (width - len(row)) for row in rows_raw]
                header = list(rows_raw[0])
                columns = tuple(OrderedColumnSchema(f"col_{i + 1}", "" if name is None else str(name), i, _type_for([r[i] for r in rows_raw[1:]]), physical_name=f"col_{i + 1}") for i, name in enumerate(header))
                rows_typed = (tuple(rows_raw[0][i] if columns[i].value_type is ValueType.TEXT else None for i in range(width)),) + tuple(tuple(row) for row in rows_raw[1:])
                diagnostics: list[Issue] = []
                if metadata.visible != SheetVisibleEnum.Visible:
                    diagnostics.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, f"hidden worksheet included: {metadata.name}", source=request.source.name, location=metadata.name))
                merged = getattr(sheet, "merged_cell_ranges", ())
                if merged:
                    diagnostics.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, f"merged ranges preserved in {metadata.name}", source=request.source.name, location=metadata.name, details={"merged_ranges": [str(r) for r in merged]}))
                if not request.policy.include_hidden_rows:
                    diagnostics.append(issue(IssueCode.HIDDEN_DATA_INCLUDED, "calamine does not expose hidden-row metadata; rows retained", source=request.source.name, location=metadata.name))
                diagnostics.append(issue(IssueCode.FORMULA_NOT_RECALCULATED, "calamine exposes values without formula/cache provenance", source=request.source.name, location=metadata.name, details={"engine": "python-calamine"}))
                key_base = request.source.source_id or Path(request.source.name).stem
                provenance = tuple(ColumnProvenance(col.key, request.source.name, col.ordinal, workbook_sheet=metadata.name, original_header=col.display_name, source_type=col.value_type.value) for col in columns)
                artifact = TableArtifact(f"{key_base}:{metadata.name}", metadata.name, columns, len(rows_raw), provenance=provenance)
                result.append(ParsedTable(artifact, tuple(rows_raw), rows_typed, tuple(diagnostics)))
        finally:
            close = getattr(workbook, "close", None)
            if callable(close):
                close()
        return tuple(result)

    def read(self, request: IngestionRequest) -> Iterable[TableArtifact]:
        for table in self.parse(request):
            yield table.artifact
