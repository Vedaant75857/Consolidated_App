import sqlite3
from typing import Any

import pandas as pd

VIEW_REGISTRY: list[dict[str, Any]] = [
    {
        "viewId": "spend_over_time",
        "title": "Spend Over Time",
        "description": "Monthly and yearly spend trends",
        "requiredFields": ["invoice_date", "total_spend"],
        "chartType": "bar",
    },
    {
        "viewId": "supplier_ranking",
        "title": "Supplier Spend Ranking",
        "description": "Top N suppliers by total spend",
        "requiredFields": ["supplier", "total_spend"],
        "chartType": "hbar",
    },
    {
        "viewId": "pareto_analysis",
        "title": "Pareto Analysis",
        "description": "Suppliers making up X% of spend",
        "requiredFields": ["supplier", "total_spend"],
        "chartType": "pareto",
    },
    {
        "viewId": "currency_spend",
        "title": "Currency vs Local Spend",
        "description": "Spend breakdown by invoice currency",
        "requiredFields": ["currency", "local_spend"],
        "chartType": "bar",
    },
    {
        "viewId": "country_spend",
        "title": "Country vs Total Spend",
        "description": "Spend breakdown by country",
        "requiredFields": ["country", "total_spend"],
        "chartType": "bar",
    },
    {
        "viewId": "l1_spend",
        "title": "Category L1 Spend",
        "description": "Spend by top-level category",
        "requiredFields": ["l1", "total_spend"],
        "chartType": "bar",
    },
    {
        "viewId": "l1_vs_l2_mekko",
        "title": "Category L1 vs L2 (Mekko)",
        "description": "Marimekko chart of L1 vs L2 spend",
        "requiredFields": ["l1", "l2", "total_spend"],
        "chartType": "mekko",
    },
    {
        "viewId": "l2_vs_l3_mekko",
        "title": "Category L2 vs L3 (Mekko per L1)",
        "description": "Marimekko chart of L2 vs L3 spend, per L1",
        "requiredFields": ["l1", "l2", "l3", "total_spend"],
        "chartType": "mekko",
    },
    {
        "viewId": "category_drilldown",
        "title": "Category Drill-Down",
        "description": "Expandable pivot: L1 > L2 > L3",
        "requiredFields": ["l1", "total_spend"],
        "chartType": "tree_pivot",
    },
]


def get_available_views(mapping: dict[str, str | None]) -> list[dict[str, Any]]:
    mapped_fields = {k for k, v in mapping.items() if v}
    result = []
    for view in VIEW_REGISTRY:
        available = all(f in mapped_fields for f in view["requiredFields"])
        result.append({**view, "available": available})
    return result


def _load_analysis_df(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM analysis_data", conn)
    if "invoice_date" in df.columns:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    for col in ["total_spend", "local_spend"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _to_records(df: pd.DataFrame) -> list[dict]:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == "datetime64[ns]":
            out[c] = out[c].dt.strftime("%Y-%m-%d").where(out[c].notna(), None)
    return out.where(out.notna(), None).to_dict(orient="records")


def compute_spend_over_time(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["invoice_date", "total_spend"]).copy()
    excluded = len(df) - len(work)

    work["_year"] = work["invoice_date"].dt.year
    work["_month"] = work["invoice_date"].dt.month
    work["_month_name"] = work["invoice_date"].dt.strftime("%b")

    monthly = (
        work.groupby(["_year", "_month", "_month_name"])["total_spend"]
        .sum()
        .reset_index()
        .sort_values(["_year", "_month"], ascending=[False, True])
        .rename(columns={"_year": "Year", "_month_name": "Month", "total_spend": "Total Spend (USD)"})
    )[["Year", "Month", "Total Spend (USD)"]]

    max_date = work["invoice_date"].max()
    cutoff = max_date - pd.DateOffset(months=11)
    mask = work["invoice_date"].dt.to_period("M") >= cutoff.to_period("M")
    last_12 = (
        work[mask]
        .groupby(["_year", "_month", "_month_name"])["total_spend"]
        .sum()
        .reset_index()
        .sort_values(["_year", "_month"])
        .rename(columns={"_year": "Year", "_month_name": "Month", "total_spend": "Total Spend (USD)"})
    )[["Year", "Month", "Total Spend (USD)"]]

    yearly = (
        work.groupby("_year")["total_spend"]
        .sum()
        .reset_index()
        .sort_values("_year", ascending=False)
        .rename(columns={"_year": "Year", "total_spend": "Total Spend (USD)"})
    )
    yearly["Year"] = yearly["Year"].astype(int).astype(str)

    chart_labels = (last_12["Month"].astype(str) + " " + last_12["Year"].astype(int).astype(str)).tolist()
    chart_values = last_12["Total Spend (USD)"].tolist()

    return {
        "tableData": {
            "monthly": _to_records(monthly),
            "yearly": _to_records(yearly),
            "last12": _to_records(last_12),
        },
        "chartData": {
            "labels": chart_labels,
            "values": chart_values,
        },
        "excludedRows": excluded,
    }


def compute_supplier_ranking(df: pd.DataFrame, top_n: int = 20) -> dict[str, Any]:
    work = df.dropna(subset=["supplier", "total_spend"]).copy()
    work = work[work["supplier"].str.strip() != ""]
    excluded = len(df) - len(work)

    supplier = (
        work.groupby("supplier")["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={"supplier": "Supplier Name", "total_spend": "Total Spend (USD)"})
        .sort_values("Total Spend (USD)", ascending=False)
        .reset_index(drop=True)
    )
    supplier.index += 1
    supplier.index.name = "Rank"
    total = supplier["Total Spend (USD)"].sum()
    supplier["% of Total"] = (supplier["Total Spend (USD)"] / total * 100).round(2)

    top = supplier.head(top_n).reset_index()

    return {
        "tableData": _to_records(supplier.reset_index()),
        "chartData": {
            "labels": top["Supplier Name"].tolist(),
            "values": top["Total Spend (USD)"].tolist(),
        },
        "excludedRows": excluded,
        "totalSuppliers": len(supplier),
    }


def compute_pareto(df: pd.DataFrame, threshold: float = 80.0) -> dict[str, Any]:
    work = df.dropna(subset=["supplier", "total_spend"]).copy()
    work = work[work["supplier"].str.strip() != ""]
    excluded = len(df) - len(work)

    supplier = (
        work.groupby("supplier")["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={"supplier": "Supplier Name", "total_spend": "Total Spend (USD)"})
        .sort_values("Total Spend (USD)", ascending=False)
        .reset_index(drop=True)
    )
    total = supplier["Total Spend (USD)"].sum()
    supplier["% of Total"] = (supplier["Total Spend (USD)"] / total * 100).round(2)
    supplier["Cumulative %"] = supplier["% of Total"].cumsum().round(2)
    supplier.insert(0, "Rank", range(1, len(supplier) + 1))

    cutoff_idx = supplier[supplier["Cumulative %"] >= threshold].index
    if len(cutoff_idx) > 0:
        pareto = supplier.loc[: cutoff_idx[0]].copy()
    else:
        pareto = supplier.copy()

    return {
        "tableData": _to_records(pareto),
        "chartData": {
            "labels": pareto["Supplier Name"].tolist(),
            "spendValues": pareto["Total Spend (USD)"].tolist(),
            "cumulativePercent": pareto["Cumulative %"].tolist(),
        },
        "excludedRows": excluded,
        "suppliersInGroup": len(pareto),
        "totalSuppliers": len(supplier),
        "threshold": threshold,
    }


def compute_currency_spend(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["currency", "local_spend"]).copy()
    work = work[work["currency"].str.strip() != ""]
    excluded = len(df) - len(work)

    currency = (
        work.groupby("currency")["local_spend"]
        .sum()
        .reset_index()
        .rename(columns={"currency": "Currency", "local_spend": "Total Local Spend"})
        .sort_values("Total Local Spend", ascending=False)
        .reset_index(drop=True)
    )
    total = currency["Total Local Spend"].sum()
    currency["% of Total"] = (currency["Total Local Spend"] / total * 100).round(2)

    return {
        "tableData": _to_records(currency),
        "chartData": {
            "labels": currency["Currency"].tolist(),
            "values": currency["Total Local Spend"].tolist(),
        },
        "excludedRows": excluded,
    }


def compute_country_spend(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["country", "total_spend"]).copy()
    work = work[work["country"].str.strip() != ""]
    excluded = len(df) - len(work)

    country = (
        work.groupby("country")["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={"country": "Country", "total_spend": "Total Spend (USD)"})
        .sort_values("Total Spend (USD)", ascending=False)
        .reset_index(drop=True)
    )
    total = country["Total Spend (USD)"].sum()
    country["% of Total"] = (country["Total Spend (USD)"] / total * 100).round(2)

    return {
        "tableData": _to_records(country),
        "chartData": {
            "labels": country["Country"].tolist(),
            "values": country["Total Spend (USD)"].tolist(),
        },
        "excludedRows": excluded,
    }


def compute_l1_spend(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["l1", "total_spend"]).copy()
    work = work[work["l1"].str.strip() != ""]
    excluded = len(df) - len(work)

    l1 = (
        work.groupby("l1")["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={"l1": "Category L1", "total_spend": "Total Spend (USD)"})
        .sort_values("Total Spend (USD)", ascending=False)
        .reset_index(drop=True)
    )
    total = l1["Total Spend (USD)"].sum()
    l1["% of Total"] = (l1["Total Spend (USD)"] / total * 100).round(2)

    return {
        "tableData": _to_records(l1),
        "chartData": {
            "labels": l1["Category L1"].tolist(),
            "values": l1["Total Spend (USD)"].tolist(),
        },
        "excludedRows": excluded,
    }


def _build_mekko_data(
    df: pd.DataFrame, col_field: str, seg_field: str, spend_field: str = "total_spend"
) -> dict[str, Any]:
    """Build Mekko chart data: columns = col_field, segments = seg_field."""
    grouped = df.groupby([col_field, seg_field])[spend_field].sum().reset_index()
    grand_total = grouped[spend_field].sum()
    if grand_total == 0:
        return {"columns": [], "grandTotal": 0}

    col_totals = grouped.groupby(col_field)[spend_field].sum().sort_values(ascending=False)
    columns = []
    for col_val in col_totals.index:
        col_spend = col_totals[col_val]
        col_width = col_spend / grand_total
        segs = grouped[grouped[col_field] == col_val].sort_values(spend_field, ascending=False)
        segments = []
        for _, row in segs.iterrows():
            segments.append({
                "label": str(row[seg_field]),
                "value": float(row[spend_field]),
                "share": float(row[spend_field] / col_spend) if col_spend > 0 else 0,
            })
        columns.append({
            "label": str(col_val),
            "totalSpend": float(col_spend),
            "width": float(col_width),
            "segments": segments,
        })

    return {"columns": columns, "grandTotal": float(grand_total)}


def compute_l1_vs_l2_mekko(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["l1", "l2", "total_spend"]).copy()
    work = work[(work["l1"].str.strip() != "") & (work["l2"].str.strip() != "")]
    excluded = len(df) - len(work)

    mekko = _build_mekko_data(work, "l1", "l2")
    table = (
        work.groupby(["l1", "l2"])["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={"l1": "Category L1", "l2": "Category L2", "total_spend": "Total Spend (USD)"})
        .sort_values("Total Spend (USD)", ascending=False)
    )

    return {
        "tableData": _to_records(table),
        "chartData": mekko,
        "excludedRows": excluded,
    }


def compute_l2_vs_l3_mekko(df: pd.DataFrame) -> dict[str, Any]:
    work = df.dropna(subset=["l1", "l2", "l3", "total_spend"]).copy()
    work = work[
        (work["l1"].str.strip() != "")
        & (work["l2"].str.strip() != "")
        & (work["l3"].str.strip() != "")
    ]
    excluded = len(df) - len(work)

    per_l1: dict[str, Any] = {}
    for l1_val in work["l1"].unique():
        subset = work[work["l1"] == l1_val]
        per_l1[str(l1_val)] = _build_mekko_data(subset, "l2", "l3")

    table = (
        work.groupby(["l1", "l2", "l3"])["total_spend"]
        .sum()
        .reset_index()
        .rename(columns={
            "l1": "Category L1", "l2": "Category L2",
            "l3": "Category L3", "total_spend": "Total Spend (USD)",
        })
        .sort_values("Total Spend (USD)", ascending=False)
    )

    return {
        "tableData": _to_records(table),
        "chartData": {"perL1": per_l1},
        "excludedRows": excluded,
    }


def compute_category_drilldown(df: pd.DataFrame, mapping: dict) -> dict[str, Any]:
    """Build hierarchical tree data for L1 > L2 > L3."""
    available_levels = []
    for lvl in ["l1", "l2", "l3"]:
        if mapping.get(lvl) and lvl in df.columns:
            col_data = df[lvl].dropna()
            if (col_data.str.strip() != "").any():
                available_levels.append(lvl)

    if not available_levels:
        return {"treeData": [], "excludedRows": len(df)}

    work = df.dropna(subset=["total_spend"] + available_levels).copy()
    for lvl in available_levels:
        work = work[work[lvl].str.strip() != ""]
    excluded = len(df) - len(work)
    grand_total = work["total_spend"].sum()

    def build_tree(data: pd.DataFrame, levels: list[str], depth: int = 0) -> list[dict]:
        if not levels:
            return []
        current_level = levels[0]
        remaining = levels[1:]
        grouped = data.groupby(current_level)["total_spend"].sum().sort_values(ascending=False)
        parent_total = data["total_spend"].sum()
        nodes = []
        for val, spend in grouped.items():
            node: dict[str, Any] = {
                "name": str(val),
                "level": current_level,
                "totalSpend": float(spend),
                "percentOfParent": round(float(spend / parent_total * 100), 2) if parent_total > 0 else 0,
                "percentOfTotal": round(float(spend / grand_total * 100), 2) if grand_total > 0 else 0,
            }
            if remaining:
                child_data = data[data[current_level] == val]
                node["children"] = build_tree(child_data, remaining, depth + 1)
            else:
                node["children"] = []
            nodes.append(node)
        return nodes

    tree = build_tree(work, available_levels)

    return {
        "treeData": tree,
        "excludedRows": excluded,
        "availableLevels": available_levels,
    }


COMPUTE_FUNCS = {
    "spend_over_time": lambda df, cfg: compute_spend_over_time(df),
    "supplier_ranking": lambda df, cfg: compute_supplier_ranking(df, cfg.get("topN", 20)),
    "pareto_analysis": lambda df, cfg: compute_pareto(df, cfg.get("paretoThreshold", 80.0)),
    "currency_spend": lambda df, cfg: compute_currency_spend(df),
    "country_spend": lambda df, cfg: compute_country_spend(df),
    "l1_spend": lambda df, cfg: compute_l1_spend(df),
    "l1_vs_l2_mekko": lambda df, cfg: compute_l1_vs_l2_mekko(df),
    "l2_vs_l3_mekko": lambda df, cfg: compute_l2_vs_l3_mekko(df),
    "category_drilldown": lambda df, cfg: compute_category_drilldown(df, cfg.get("mapping", {})),
}


def compute_views(
    conn: sqlite3.Connection,
    selected_views: list[str],
    config: dict[str, Any],
    mapping: dict[str, str | None],
) -> list[dict[str, Any]]:
    df = _load_analysis_df(conn)
    results = []

    for view_id in selected_views:
        view_def = next((v for v in VIEW_REGISTRY if v["viewId"] == view_id), None)
        if not view_def:
            continue
        func = COMPUTE_FUNCS.get(view_id)
        if not func:
            continue

        cfg = {**config, "mapping": mapping}
        try:
            data = func(df, cfg)
            results.append({
                "viewId": view_id,
                "title": view_def["title"],
                "chartType": view_def["chartType"],
                **data,
            })
        except Exception as exc:
            results.append({
                "viewId": view_id,
                "title": view_def["title"],
                "chartType": view_def["chartType"],
                "error": str(exc),
            })

    return results
