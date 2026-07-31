"""Create the consolidated Spend Quality Assessment and dashboard XLSX export."""

from __future__ import annotations

import io
import math
import re
from numbers import Number
from typing import Any, Iterable, Sequence

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLUMNS = 16_384
EXCEL_MAX_CELL_TEXT = 32_767

_INVALID_SHEET_CHARS = re.compile(r"[\\\\/*?:\[\]]")
_FORMULA_PREFIXES = ("=", "+", "-", "@")

_TITLE_FILL = PatternFill("solid", fgColor="A61B2B")
_HEADER_FILL = PatternFill("solid", fgColor="F4E6E8")
_TITLE_FONT = Font(color="FFFFFF", bold=True, size=14)
_HEADER_FONT = Font(bold=True, color="5E1520")


def build_complete_analysis_workbook(
    assessment: dict[str, Any],
    views: Sequence[dict[str, Any]],
) -> bytes:
    """Return an XLSX workbook for the saved assessment and generated views.

    The caller is responsible for snapshotting the supplied data while holding the
    session lock. This function only formats that snapshot; it never queries data
    or invokes AI.
    """
    workbook = Workbook()
    workbook.remove(workbook.active)

    _write_spend_summary(workbook, assessment)
    _write_fill_rate_summary(workbook, assessment.get("columnFillRate") or {})
    _write_spend_bifurcation(workbook, assessment.get("spendBifurcation") or {})
    _write_date_distribution(
        workbook,
        assessment.get("datePivot") or {},
        assessment.get("dateSource") or {},
    )
    _write_spend_cuts(workbook, assessment.get("paretoAnalysis") or {})

    for view in views:
        if isinstance(view, dict):
            _write_view(workbook, view)

    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


def _new_sheet(workbook: Workbook, requested_title: str):
    cleaned = _INVALID_SHEET_CHARS.sub(" ", requested_title).strip() or "Analysis"
    cleaned = cleaned[:31]
    existing = set(workbook.sheetnames)
    title = cleaned
    suffix = 2
    while title in existing:
        suffix_text = f" ({suffix})"
        title = f"{cleaned[:31 - len(suffix_text)]}{suffix_text}"
        suffix += 1
    return workbook.create_sheet(title)


def _set_title(worksheet, title: str) -> int:
    cell = worksheet.cell(row=1, column=1, value=_safe_text(title))
    cell.fill = _TITLE_FILL
    cell.font = _TITLE_FONT
    cell.alignment = Alignment(vertical="center")
    worksheet.row_dimensions[1].height = 24
    return 3


def _safe_text(value: Any) -> str:
    text = str(value)
    if len(text) > EXCEL_MAX_CELL_TEXT:
        text = f"{text[:EXCEL_MAX_CELL_TEXT - 1]}…"
    if text.startswith(_FORMULA_PREFIXES):
        return f"'{text}"
    return text


def _safe_cell_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Number):
        try:
            if not math.isfinite(float(value)):
                return None
        except (TypeError, ValueError):
            return None
        return value
    return _safe_text(value)


def _check_limits(row_count: int, column_count: int):
    if row_count > EXCEL_MAX_ROWS:
        raise ValueError("Export exceeds Excel's maximum row limit for one worksheet.")
    if column_count > EXCEL_MAX_COLUMNS:
        raise ValueError("Export exceeds Excel's maximum column limit for one worksheet.")


def _number_format(header: str) -> str | None:
    normalized = header.lower()
    if "%" in header or "percent" in normalized or "fill rate" in normalized or "coverage" in normalized:
        # The API already returns percentage points (for example, 80.5), not 0.805.
        return '0.0"%"'
    if any(token in normalized for token in ("spend", "amount", "cost", "value")):
        return '#,##0.00;[Red]-#,##0.00'
    return None


def _write_table(
    worksheet,
    start_row: int,
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> int:
    header_list = list(headers)
    row_list = [list(row) for row in rows]
    _check_limits(start_row + len(row_list), len(header_list))

    for column, header in enumerate(header_list, start=1):
        cell = worksheet.cell(row=start_row, column=column, value=_safe_text(header))
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.alignment = Alignment(vertical="center")

    for row_offset, values in enumerate(row_list, start=1):
        for column, value in enumerate(values, start=1):
            cell = worksheet.cell(
                row=start_row + row_offset,
                column=column,
                value=_safe_cell_value(value),
            )
            cell.alignment = Alignment(vertical="top", wrap_text=isinstance(cell.value, str))
            fmt = _number_format(header_list[column - 1])
            if fmt and isinstance(cell.value, Number) and not isinstance(cell.value, bool):
                cell.number_format = fmt

    if row_list:
        worksheet.auto_filter.ref = (
            f"A{start_row}:{get_column_letter(len(header_list))}{start_row + len(row_list)}"
        )

    for column, header in enumerate(header_list, start=1):
        sample_values = [header] + [row[column - 1] for row in row_list[:100] if len(row) >= column]
        width = max(len(str(value)) for value in sample_values if value is not None) if sample_values else len(header)
        worksheet.column_dimensions[get_column_letter(column)].width = min(max(width + 2, 12), 45)

    return start_row + len(row_list) + 2


def _write_unavailable(worksheet, row: int, message: str | None) -> int:
    worksheet.cell(row=row, column=1, value="Status").font = _HEADER_FONT
    worksheet.cell(
        row=row,
        column=2,
        value=_safe_text(message or "This analysis was not available for the uploaded data."),
    ).alignment = Alignment(wrap_text=True, vertical="top")
    worksheet.column_dimensions["A"].width = 18
    worksheet.column_dimensions["B"].width = 80
    return row + 3


def _write_spend_summary(workbook: Workbook, assessment: dict[str, Any]):
    worksheet = _new_sheet(workbook, "Spend Summary")
    row = _set_title(worksheet, "Spend Quality Assessment - Spend Summary")

    date_source = assessment.get("dateSource") or {}
    spend_breakdown = assessment.get("spendBreakdown") or {}
    supplier_breakdown = assessment.get("supplierBreakdown") or {}
    summary_rows = [
        ["Total rows", assessment.get("totalRows")],
        ["Date source", date_source.get("displayName")],
        ["Date source column", date_source.get("sourceColumn")],
        ["Date fallback used", "Yes" if date_source.get("fallback") else "No"],
        ["Last twelve months spend", spend_breakdown.get("ltmSpend")],
        ["Current fiscal year spend", spend_breakdown.get("currentFySpend")],
        ["Prior fiscal year spend", spend_breakdown.get("priorFySpend")],
        ["Year-on-year change", spend_breakdown.get("yoyAbs")],
        ["Year-on-year change (%)", spend_breakdown.get("yoyPct")],
        ["Total suppliers", supplier_breakdown.get("totalSuppliers")],
        ["Suppliers needed for 80% spend", supplier_breakdown.get("suppliersTo80Pct")],
    ]
    row = _write_table(worksheet, row, ["Metric", "Value"], summary_rows)

    executive_rows = (assessment.get("executiveSummary") or {}).get("rows") or []
    if executive_rows:
        row = _write_table(
            worksheet,
            row,
            ["Key Point", "Summary"],
            [
                [item.get("label") or item.get("key") or "Summary", item.get("text")]
                for item in executive_rows
                if isinstance(item, dict)
            ],
        )

    warnings = assessment.get("warnings") or []
    if warnings:
        _write_table(
            worksheet,
            row,
            ["Warning Code", "Severity", "Message"],
            [
                [warning.get("code"), warning.get("severity"), warning.get("message")]
                for warning in warnings
                if isinstance(warning, dict)
            ],
        )

    worksheet.freeze_panes = "A4"


def _write_fill_rate_summary(workbook: Workbook, fill_rate: dict[str, Any]):
    worksheet = _new_sheet(workbook, "Fill Rate Summary")
    row = _set_title(worksheet, "Column Fill Rate with Spend Coverage")
    columns = fill_rate.get("columns") or []
    if not fill_rate.get("feasible") or not columns:
        _write_unavailable(worksheet, row, fill_rate.get("message"))
        return
    _write_table(
        worksheet,
        row,
        ["Original Column", "Source Column", "Order", "Fill Rate (%)", "Spend Coverage (%)"],
        [
            [
                column.get("columnName"),
                column.get("sourceColumn"),
                column.get("order"),
                column.get("fillRate"),
                column.get("spendCoverage"),
            ]
            for column in columns
            if isinstance(column, dict)
        ],
    )
    worksheet.freeze_panes = "A4"


def _write_spend_bifurcation(workbook: Workbook, bifurcation: dict[str, Any]):
    worksheet = _new_sheet(workbook, "Spend Bifurcation")
    row = _set_title(worksheet, "Positive, Negative, and Net Spend")
    if not bifurcation.get("feasible"):
        _write_unavailable(worksheet, row, bifurcation.get("message"))
        return
    _write_table(
        worksheet,
        row,
        ["Spend Type", "Spend Amount", "% of Net Spend"],
        [
            ["Positive Spend", bifurcation.get("positiveSpend"), bifurcation.get("positivePctOfNet")],
            ["Negative Spend", bifurcation.get("negativeSpend"), bifurcation.get("negativePctOfNet")],
            ["Net Spend", bifurcation.get("netSpend"), None],
        ],
    )
    worksheet.freeze_panes = "A4"


def _write_date_distribution(
    workbook: Workbook,
    date_pivot: dict[str, Any],
    date_source: dict[str, Any],
):
    worksheet = _new_sheet(workbook, "Date-wise Distribution")
    row = _set_title(worksheet, "Date-wise Spend Distribution")
    if date_source.get("displayName"):
        worksheet.cell(row=row, column=1, value="Date source").font = _HEADER_FONT
        worksheet.cell(row=row, column=2, value=_safe_text(date_source.get("displayName")))
        row += 2

    if not date_pivot.get("feasible"):
        _write_unavailable(worksheet, row, date_pivot.get("message"))
        return

    years = date_pivot.get("years") or []
    months = date_pivot.get("months") or []
    cells = date_pivot.get("cells") or {}
    if not years or not months:
        _write_unavailable(worksheet, row, date_pivot.get("message"))
        return

    headers = ["Month", *[str(year) for year in years], "Annual Total"]
    records = []
    for month_number, month_name in enumerate(months, start=1):
        amounts = [
            (cells.get(str(year), {}) or {}).get(str(month_number))
            for year in years
        ]
        total = sum(value for value in amounts if isinstance(value, Number) and not isinstance(value, bool))
        records.append([month_name, *amounts, total])

    _write_table(worksheet, row, headers, records)
    worksheet.freeze_panes = f"B{row + 1}"


def _threshold_key(metrics: dict[str, Any], threshold: Any) -> str | None:
    candidates = [str(threshold)]
    try:
        numeric = float(threshold)
        candidates.extend([str(numeric), str(int(numeric))])
    except (TypeError, ValueError):
        pass
    return next((candidate for candidate in candidates if candidate in metrics), None)


def _format_threshold(threshold: Any) -> str:
    try:
        numeric = float(threshold)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)
    except (TypeError, ValueError):
        return str(threshold)


def _write_spend_cuts(workbook: Workbook, pareto: dict[str, Any]):
    thresholds = pareto.get("thresholds") or []
    metrics = pareto.get("metrics") or {}
    if not pareto.get("feasible") or not thresholds:
        worksheet = _new_sheet(workbook, "Spend Cuts")
        row = _set_title(worksheet, "Spend Cuts")
        _write_unavailable(worksheet, row, pareto.get("message"))
        return

    for threshold in thresholds:
        label = _format_threshold(threshold)
        worksheet = _new_sheet(workbook, f"Spend Cut {label}%")
        row = _set_title(worksheet, f"Spend Cut - {label}% of Spend")
        metric_key = _threshold_key(metrics, threshold)
        cut = metrics.get(metric_key) if metric_key else None
        if not isinstance(cut, dict):
            _write_unavailable(worksheet, row, "Metrics for this spend cut were not available.")
            continue
        _write_table(
            worksheet,
            row,
            ["Metric", "Value"],
            [
                ["Total Dataset Spend", pareto.get("totalDatasetSpend")],
                ["Spend in Cut", cut.get("totalSpend")],
                ["Transaction Count", cut.get("transactionCount")],
                ["Unique Transactions", cut.get("uniqueTransactions")],
                ["Supplier Count", cut.get("supplierCount")],
            ],
        )
        worksheet.freeze_panes = "A4"


def _flatten_tree(nodes: Sequence[dict[str, Any]], depth: int = 0) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        rows.append([
            node.get("level"),
            node.get("name"),
            node.get("totalSpend"),
            node.get("percentOfParent"),
            node.get("percentOfTotal"),
            depth,
        ])
        children = node.get("children") or []
        if isinstance(children, list):
            rows.extend(_flatten_tree(children, depth + 1))
    return rows


def _rows_from_records(records: Sequence[dict[str, Any]]) -> tuple[list[str], list[list[Any]]]:
    headers: list[str] = []
    for record in records:
        if isinstance(record, dict):
            for key in record:
                if key not in headers:
                    headers.append(str(key))
    return headers, [
        [record.get(header) for header in headers]
        for record in records
        if isinstance(record, dict)
    ]


def _write_view(workbook: Workbook, view: dict[str, Any]):
    title = str(view.get("title") or view.get("viewId") or "Dashboard View")
    worksheet = _new_sheet(workbook, f"View - {title}")
    row = _set_title(worksheet, title)

    if view.get("error"):
        _write_unavailable(worksheet, row, str(view.get("error")))
        return

    metadata = []
    if view.get("threshold") is not None:
        metadata.append(["Pareto Threshold (%)", view.get("threshold")])
    if view.get("totalSuppliers") is not None:
        metadata.append(["Total Suppliers", view.get("totalSuppliers")])
    if view.get("suppliersInGroup") is not None:
        metadata.append(["Suppliers in Group", view.get("suppliersInGroup")])
    if view.get("excludedRows") is not None:
        metadata.append(["Excluded Rows", view.get("excludedRows")])
    if view.get("aiSummary"):
        metadata.append(["AI Summary", view.get("aiSummary")])
    if metadata:
        row = _write_table(worksheet, row, ["View Detail", "Value"], metadata)

    if view.get("chartType") == "tree_pivot":
        tree_rows = _flatten_tree(view.get("treeData") or [])
        if not tree_rows:
            _write_unavailable(worksheet, row, "No category drill-down rows were generated.")
            return
        _write_table(
            worksheet,
            row,
            ["Level", "Category", "Total Spend (USD)", "% of Parent", "% of Total", "Depth"],
            tree_rows,
        )
        worksheet.freeze_panes = f"A{row + 1}"
        return

    table_data = view.get("tableData")
    if isinstance(table_data, list):
        headers, rows = _rows_from_records(table_data)
        if headers:
            _write_table(worksheet, row, headers, rows)
            worksheet.freeze_panes = f"A{row + 1}"
            return
    elif isinstance(table_data, dict):
        wrote_table = False
        for section_name, section_rows in table_data.items():
            if not isinstance(section_rows, list):
                continue
            headers, rows = _rows_from_records(section_rows)
            if not headers:
                continue
            worksheet.cell(row=row, column=1, value=_safe_text(str(section_name).replace("_", " ").title())).font = _HEADER_FONT
            row = _write_table(worksheet, row + 1, headers, rows)
            wrote_table = True
        if wrote_table:
            worksheet.freeze_panes = "A4"
            return

    _write_unavailable(worksheet, row, "No tabular data was generated for this view.")
