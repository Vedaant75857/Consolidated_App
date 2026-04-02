import csv
import io
from typing import Any


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
