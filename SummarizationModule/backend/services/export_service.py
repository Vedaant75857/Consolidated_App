import base64
import csv
import io
from datetime import datetime
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch, mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    Image,
    PageBreak,
)

BAIN_RED = colors.HexColor("#CC0000")
BAIN_DARK = colors.HexColor("#1A1A1A")
BAIN_LIGHT_RED = colors.HexColor("#FEE2E2")


def generate_csv(view_result: dict[str, Any]) -> str:
    table_data = view_result.get("tableData")
    rows: list[dict] = []
    if isinstance(table_data, list):
        rows = table_data
    elif isinstance(table_data, dict):
        for key in ["monthly", "last12", "yearly"]:
            if key in table_data and isinstance(table_data[key], list):
                rows = table_data[key]
                break
        if not rows:
            first_key = next(iter(table_data), None)
            if first_key and isinstance(table_data[first_key], list):
                rows = table_data[first_key]

    if not rows:
        return ""

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def generate_pdf(views: list[dict[str, Any]]) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=25 * mm,
        bottomMargin=20 * mm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "BainTitle",
        parent=styles["Title"],
        fontSize=28,
        textColor=BAIN_RED,
        spaceAfter=12,
    )
    heading_style = ParagraphStyle(
        "BainHeading",
        parent=styles["Heading1"],
        fontSize=18,
        textColor=BAIN_RED,
        spaceBefore=20,
        spaceAfter=10,
    )
    body_style = ParagraphStyle(
        "BainBody",
        parent=styles["Normal"],
        fontSize=10,
        textColor=BAIN_DARK,
        spaceAfter=8,
        leading=14,
    )
    small_style = ParagraphStyle(
        "BainSmall",
        parent=styles["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#6B7280"),
    )

    elements = []

    # Title page
    elements.append(Spacer(1, 80))
    elements.append(Paragraph("Procurement Spend Analysis", title_style))
    elements.append(Spacer(1, 20))
    date_str = datetime.now().strftime("%B %d, %Y")
    elements.append(Paragraph(f"Generated on {date_str}", body_style))
    elements.append(Spacer(1, 10))
    elements.append(
        Paragraph(
            f"{len(views)} view(s) included in this report",
            small_style,
        )
    )
    elements.append(PageBreak())

    for view in views:
        elements.append(Paragraph(view.get("title", "View"), heading_style))

        summary = view.get("aiSummary", "")
        if summary:
            elements.append(Paragraph(f"<b>AI Analysis:</b> {summary}", body_style))
            elements.append(Spacer(1, 8))

        chart_b64 = view.get("chartImage")
        if chart_b64:
            try:
                img_data = base64.b64decode(chart_b64)
                img_buffer = io.BytesIO(img_data)
                img = Image(img_buffer, width=6 * inch, height=3 * inch)
                elements.append(img)
                elements.append(Spacer(1, 10))
            except Exception:
                pass

        table_data = view.get("tableData")
        rows: list[dict] = []
        if isinstance(table_data, list):
            rows = table_data[:30]
        elif isinstance(table_data, dict):
            for key in ["last12", "monthly", "yearly"]:
                if key in table_data and isinstance(table_data[key], list):
                    rows = table_data[key][:30]
                    break
            if not rows:
                first_key = next(iter(table_data), None)
                if first_key and isinstance(table_data[first_key], list):
                    rows = table_data[first_key][:30]

        if rows:
            headers = list(rows[0].keys())
            data = [headers]
            for r in rows:
                data.append([str(r.get(h, "")) for h in headers])

            col_count = len(headers)
            available_width = landscape(A4)[0] - 40 * mm
            col_width = available_width / col_count

            t = Table(data, colWidths=[col_width] * col_count)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), BAIN_RED),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("FONTSIZE", (0, 1), (-1, -1), 7),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, BAIN_LIGHT_RED]),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]))
            elements.append(t)

        elements.append(PageBreak())

    doc.build(elements)
    return buffer.getvalue()
