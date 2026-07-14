"""Shared supplier Pareto calculations."""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd


def threshold_key(threshold: float | int) -> str:
    value = float(threshold)
    return str(int(value)) if value.is_integer() else str(value)


def _normalise_text(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    return text.replace({"nan": pd.NA, "NaN": pd.NA, "None": pd.NA})


def compute_supplier_pareto(
    df: pd.DataFrame,
    thresholds: Iterable[float | int],
    *,
    supplier_col: str = "supplier",
    spend_col: str = "total_spend",
    description_col: str | None = "description",
) -> dict[str, Any]:
    """Compute Excel-style supplier Pareto cuts over signed net spend.

    Vendors are grouped by trimmed supplier name, ranked by signed spend
    descending, and each cut includes the first vendor that breaches the
    requested cumulative threshold.
    """
    threshold_values = [float(t) for t in thresholds]

    if supplier_col not in df.columns or spend_col not in df.columns:
        return {
            "feasible": False,
            "message": "supplier or total_spend not mapped.",
            "excludedRows": len(df),
            "totalSuppliers": 0,
            "totalSpend": 0.0,
            "supplierTable": pd.DataFrame(),
            "cuts": {},
        }

    work = pd.DataFrame(index=df.index)
    work["_supplier"] = _normalise_text(df[supplier_col])
    work["_spend"] = pd.to_numeric(df[spend_col], errors="coerce")

    if description_col and description_col in df.columns:
        work["_description"] = _normalise_text(df[description_col]).fillna("")
    else:
        work["_description"] = ""

    vendor_rows = work[work["_supplier"].notna() & (work["_supplier"] != "")].copy()
    spend_rows = vendor_rows[vendor_rows["_spend"].notna()].copy()
    excluded_rows = len(df) - len(spend_rows)

    if spend_rows.empty:
        return {
            "feasible": False,
            "message": "No valid supplier-spend data.",
            "excludedRows": excluded_rows,
            "totalSuppliers": 0,
            "totalSpend": 0.0,
            "supplierTable": pd.DataFrame(),
            "cuts": {},
        }

    supplier_spend = (
        spend_rows.groupby("_supplier", as_index=False)["_spend"]
        .sum()
        .rename(columns={"_supplier": "supplier", "_spend": "spend"})
    )
    supplier_spend["supplier"] = supplier_spend["supplier"].astype(str)
    supplier_spend = supplier_spend.sort_values(
        ["spend", "supplier"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    total_spend = float(supplier_spend["spend"].sum())
    total_suppliers = len(supplier_spend)

    if total_spend <= 0:
        return {
            "feasible": False,
            "message": "Net supplier spend is zero or negative.",
            "excludedRows": excluded_rows,
            "totalSuppliers": total_suppliers,
            "totalSpend": total_spend,
            "supplierTable": supplier_spend,
            "cuts": {},
        }

    supplier_spend["percentOfTotal"] = supplier_spend["spend"] / total_spend * 100
    supplier_spend["cumulativePercent"] = supplier_spend["spend"].cumsum() / total_spend * 100
    supplier_spend["rank"] = range(1, total_suppliers + 1)

    cuts: dict[str, dict[str, Any]] = {}
    for threshold in threshold_values:
        crossed = supplier_spend.index[supplier_spend["cumulativePercent"] >= threshold]
        cutoff_idx = int(crossed[0]) if len(crossed) else total_suppliers - 1
        top_suppliers = supplier_spend.iloc[: cutoff_idx + 1].copy()
        supplier_names = top_suppliers["supplier"].tolist()

        bucket_rows = vendor_rows[vendor_rows["_supplier"].astype(str).isin(supplier_names)]
        unique_transactions = int(
            bucket_rows[["_supplier", "_description"]].drop_duplicates().shape[0]
        )

        cuts[threshold_key(threshold)] = {
            "threshold": threshold,
            "cutoffIndex": cutoff_idx,
            "suppliers": supplier_names,
            "totalSpend": float(top_suppliers["spend"].sum()),
            "transactionCount": int(len(bucket_rows)),
            "uniqueTransactions": unique_transactions,
            "supplierCount": int(len(top_suppliers)),
        }

    return {
        "feasible": True,
        "message": None,
        "excludedRows": excluded_rows,
        "totalSuppliers": total_suppliers,
        "totalSpend": total_spend,
        "supplierTable": supplier_spend,
        "cuts": cuts,
    }
