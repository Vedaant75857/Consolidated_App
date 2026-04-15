"""
FX Rates Module
Replaces external APIs with an Excel-based FX reference workbook.
Vectorised conversions using Pandas & NumPy arrays.
"""
import os
import pandas as pd
import numpy as np

MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun", "Jul","Aug","Sep","Oct","Nov","Dec"]
DEFAULT_TARGET_CURRENCY = "USD"

def _candidate_reference_paths():
    """Build an ordered list of paths to search for the FX rates workbook.

    When running inside a PyInstaller bundle, the _MEIPASS root and the
    directory next to the exe are checked FIRST because relative paths
    from ``__file__`` are unreliable in frozen mode.
    """
    import sys as _sys
    base_dir = os.path.dirname(__file__)
    env_path = os.getenv('MONTHLY_FX_REFERENCE_PATH')

    candidates: list[str | None] = [env_path]

    if getattr(_sys, "frozen", False):
        candidates.append(os.path.join(_sys._MEIPASS, "FX_rates_table.xlsx"))  # type: ignore[attr-defined]
        candidates.append(os.path.join(os.path.dirname(_sys.executable), "FX_rates_table.xlsx"))

    candidates.extend([
        os.path.join(base_dir, '..', '..', 'FX_rates_table.xlsx'),
        os.path.join(base_dir, '..', '..', '..', 'FX_rates_table.xlsx'),
        os.path.join(base_dir, '..', 'data', 'FX_rates_table.xlsx'),
        os.path.join(base_dir, '..', '..', '..', 'DataConsolidationAppV7', 'backend', 'data', 'FX_rates_table.xlsx'),
    ])
    return [os.path.abspath(path) for path in candidates if path]

def resolve_fx_reference_path(reference_path=None):
    candidates = [reference_path] if reference_path else []
    candidates.extend(_candidate_reference_paths())
    checked = []
    for path in candidates:
        if path is None:
            continue
        abs_path = os.path.abspath(path)
        checked.append(abs_path)
        if os.path.exists(abs_path):
            return abs_path
    raise FileNotFoundError("Monthly FX reference workbook not found. Checked: " + ", ".join(checked))

# Global cache so we don't open the Excel file on every single API request
_FX_CACHE = None

def load_fx_table(path: str = None):
    global _FX_CACHE
    if _FX_CACHE is not None:
        return _FX_CACHE

    actual_path = resolve_fx_reference_path(path)
    
    raw = pd.read_excel(actual_path, header=None)
    header_row = None
    for i, row in raw.iterrows():
        if str(row.iloc[0]).strip() == "Years":
            header_row = i
            break
    if header_row is None:
        raise ValueError("Could not find header row in FX table. Expected 'Years' in column 0.")

    currencies = [
        str(c).strip() for c in raw.iloc[header_row, 2:].tolist()
        if str(c).strip() not in ("nan", "")
    ]

    fx = {}
    sorted_keys = []
    for _, row in raw.iloc[header_row + 1:].iterrows():
        year_val  = str(row.iloc[0]).strip()
        month_val = str(row.iloc[1]).strip()
        if year_val in ("nan", "") or month_val in ("nan", ""): continue
        if not year_val.isdigit(): continue

        year = int(year_val)
        month = month_val
        month_idx = MONTH_ORDER.index(month) if month in MONTH_ORDER else -1

        for i, ccy in enumerate(currencies):
            val = row.iloc[2 + i]
            try: rate = float(val)
            except (ValueError, TypeError): continue
            fx[(ccy, year, month)] = rate

        sorted_keys.append((year, month_idx, month))

    if not sorted_keys:
        raise ValueError("FX Table had no usable rows.")

    sorted_keys = sorted(set(sorted_keys))
    latest_year, _, latest_month = sorted_keys[-1]

    latest_rate = {
        ccy: fx[(ccy, latest_year, latest_month)]
        for ccy in currencies
        if (ccy, latest_year, latest_month) in fx
    }

    all_years = sorted(set(y for (_, y, _) in fx.keys()))
    fx_yearly = {}
    fx_yearly_months = {}

    for ccy in currencies:
        for year in all_years:
            monthly_rates = [fx[(ccy, year, m)] for m in MONTH_ORDER if (ccy, year, m) in fx]
            if monthly_rates:
                fx_yearly[(ccy, year)] = sum(monthly_rates) / len(monthly_rates)
                fx_yearly_months[(ccy, year)] = len(monthly_rates)

    _FX_CACHE = (fx, latest_rate, currencies, (latest_year, latest_month), fx_yearly, fx_yearly_months)
    return _FX_CACHE

def run_conversion(
    df: pd.DataFrame,
    spend_col: str,
    currency_col: str,
    conversion_mode: str,
    date_col=None,
    scope_year=None,
    fx_data=None,
    fx_overrides=None,
    fx_override_mode: str = "flat",
    target_currency: str = DEFAULT_TARGET_CURRENCY,
):
    df = df.copy()
    if fx_data is None:
        fx_data = load_fx_table()
    FX, LATEST_RATE, SUPPORTED_CURRENCIES, LATEST_PERIOD, FX_YEARLY, FX_YEARLY_MONTHS = fx_data

    # Inject user-provided overrides into local copies — never mutate the global cache
    # Track which currencies have user overrides so we can disable fallback for them
    _override_ccys = set()

    if fx_overrides:
        FX = dict(FX)
        LATEST_RATE = dict(LATEST_RATE)
        FX_YEARLY = dict(FX_YEARLY)

        if fx_override_mode == "yearly":
            # fx_overrides = {"AED": {"2024": 3.67, "2025": 3.68}}
            for ccy_raw, year_rates in fx_overrides.items():
                ccy = str(ccy_raw).strip().upper()
                if not isinstance(year_rates, dict):
                    continue
                _override_ccys.add(ccy)
                for yr_raw, rate_raw in year_rates.items():
                    try:
                        yr = int(yr_raw)
                        rate = float(rate_raw)
                    except (TypeError, ValueError):
                        continue
                    FX_YEARLY[(ccy, yr)] = rate
                    # Populate monthly FX so monthly resolution also finds them
                    for mo in MONTH_ORDER:
                        FX[(ccy, yr, mo)] = rate

        elif fx_override_mode == "monthly":
            # fx_overrides = {"AED": {"2024": {"Jan": 3.67, "Mar": 3.70}}}
            for ccy_raw, year_dict in fx_overrides.items():
                ccy = str(ccy_raw).strip().upper()
                if not isinstance(year_dict, dict):
                    continue
                _override_ccys.add(ccy)
                for yr_raw, month_rates in year_dict.items():
                    try:
                        yr = int(yr_raw)
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(month_rates, dict):
                        continue
                    for mo_raw, rate_raw in month_rates.items():
                        try:
                            rate = float(rate_raw)
                        except (TypeError, ValueError):
                            continue
                        mo = str(mo_raw).strip()
                        FX[(ccy, yr, mo)] = rate

        else:
            # Legacy flat mode: fx_overrides = {"AED": 3.67}
            for raw_ccy, raw_rate in fx_overrides.items():
                ccy = str(raw_ccy).strip().upper()
                try:
                    rate = float(raw_rate)
                except (TypeError, ValueError):
                    continue
                LATEST_RATE[ccy] = rate
                for (c, yr, mo) in list(FX.keys()):
                    if c == ccy:
                        FX[(ccy, yr, mo)] = rate
                for (c, yr) in list(FX_YEARLY.keys()):
                    if c == ccy:
                        FX_YEARLY[(ccy, yr)] = rate

    target_currency = target_currency.upper().strip() if target_currency else DEFAULT_TARGET_CURRENCY

    fx_col = f"FX_rate_used_{spend_col}"
    out_col = f"{spend_col}_converted_in{target_currency}"
    _s = 1
    while fx_col in df.columns: fx_col = f"FX_rate_used_{spend_col}_{_s}"; _s += 1
    _s = 1
    while out_col in df.columns: out_col = f"{spend_col}_converted_in{target_currency}_{_s}"; _s += 1

    status_col = f"{spend_col}_conversion_status"
    _s = 1
    while status_col in df.columns:
        status_col = f"{spend_col}_conversion_status_{_s}"; _s += 1

    s = df[spend_col].astype(str).str.strip().str.replace(r"[,$€£]", "", regex=True)
    paren_mask = s.str.match(r"^\(.+\)$")
    s = s.where(~paren_mask, "-" + s.str[1:-1])
    spend_num = pd.to_numeric(s, errors="coerce")

    ccy_series = df[currency_col].astype(str).str.strip().str.upper().where(
        df[currency_col].notna() & (df[currency_col].astype(str).str.strip() != ""), other=pd.NA
    )

    if conversion_mode == "monthly":
        parsed_dates = pd.to_datetime(df[date_col], errors="coerce", dayfirst=False)
        mask_failed = parsed_dates.isna() & df[date_col].notna()
        if mask_failed.any():
            retry = pd.to_datetime(df.loc[mask_failed, date_col], errors="coerce", dayfirst=True)
            parsed_dates = parsed_dates.where(~mask_failed, retry)

        row_year = parsed_dates.dt.year
        row_month = parsed_dates.dt.strftime("%b").where(parsed_dates.notna(), other=pd.NA)

        lookup_keys = pd.DataFrame({"ccy": ccy_series.values, "year": row_year.values, "month": row_month.values})

        def _resolve_monthly(ccy, year, month):
            """Resolve the effective conversion rate for source -> target.

            All FX table rates are "local currency units per 1 USD".
            For non-USD targets we bridge: spend / source_rate * target_rate.
            Returns (effective_rate, used_fallback).  The caller divides spend
            by effective_rate to get the target amount.
            """
            if pd.isna(ccy): return np.nan, False
            if ccy == target_currency: return 1.0, False

            def _src_rate(c, yr, mo):
                if c == "USD": return 1.0, False
                if pd.notna(yr) and pd.notna(mo):
                    r = FX.get((c, int(yr), mo))
                    if r is not None: return r, False
                if c in _override_ccys: return np.nan, False
                r = LATEST_RATE.get(c)
                return (r, True) if r is not None else (np.nan, False)

            src, src_fb = _src_rate(ccy, year, month)
            if target_currency == "USD":
                return src, src_fb
            tgt, tgt_fb = _src_rate(target_currency, year, month)
            if np.isnan(src) or np.isnan(tgt) or tgt == 0:
                return np.nan, src_fb or tgt_fb
            # effective_rate: spend / effective = spend / src * tgt
            return src / tgt, src_fb or tgt_fb

        unique_combos = lookup_keys.drop_duplicates().copy()
        resolved = unique_combos.apply(lambda r: _resolve_monthly(r["ccy"], r["year"], r["month"]), axis=1)
        unique_combos["rate"] = [v[0] for v in resolved]
        unique_combos["fallback"] = [v[1] for v in resolved]

        rates_df = lookup_keys.merge(unique_combos, on=["ccy","year","month"], how="left")
        rate_values = rates_df["rate"].values.astype(float)
        fallback_flags = rates_df["fallback"].values
        use_string_labels = False

    elif conversion_mode == "scope_year":
        def _resolve_scope(ccy):
            if pd.isna(ccy): return np.nan, False
            if ccy == target_currency: return 1.0, False

            def _src_scope(c):
                if c == "USD": return 1.0, False
                if scope_year:
                    r = FX_YEARLY.get((c, int(scope_year)))
                    if r is not None: return r, False
                if c in _override_ccys: return np.nan, False
                r = LATEST_RATE.get(c)
                return (r, True) if r is not None else (np.nan, False)

            src, src_fb = _src_scope(ccy)
            if target_currency == "USD":
                return src, src_fb
            tgt, tgt_fb = _src_scope(target_currency)
            if np.isnan(src) or np.isnan(tgt) or tgt == 0:
                return np.nan, src_fb or tgt_fb
            return src / tgt, src_fb or tgt_fb

        unique_ccys = list(ccy_series.unique())
        resolved_map = {c: _resolve_scope(c) for c in unique_ccys}
        rate_values = np.array([resolved_map[c][0] if c in resolved_map else np.nan for c in ccy_series], dtype=float)
        fallback_flags = np.array([resolved_map.get(c, (None, False))[1] for c in ccy_series], dtype=bool)
        use_string_labels = False

    else:
        def _resolve_latest(ccy):
            if pd.isna(ccy): return np.nan
            if ccy == target_currency: return 1.0
            src = 1.0 if ccy == "USD" else LATEST_RATE.get(ccy, np.nan)
            if target_currency == "USD":
                return src
            tgt = LATEST_RATE.get(target_currency, np.nan)
            if np.isnan(src) or np.isnan(tgt) or tgt == 0:
                return np.nan
            return src / tgt

        unique_ccys = list(ccy_series.unique())
        resolved_map = {c: _resolve_latest(c) for c in unique_ccys}
        rate_values = np.array([resolved_map.get(c, np.nan) for c in ccy_series], dtype=float)
        fallback_flags = np.zeros(len(df), dtype=bool)
        use_string_labels = False

    converted = np.where(
        ccy_series == target_currency,
        spend_num,
        spend_num / rate_values,
    )
    converted = pd.array(converted, dtype="Float64")

    valid_spend = spend_num.notna()
    valid_ccy = ccy_series.notna()
    valid_rate = pd.Series(~np.isnan(rate_values))
    valid_all = valid_spend & valid_ccy & valid_rate

    # Build per-row status string
    status_values = pd.Series("converted", index=df.index, dtype=object)
    status_values[~valid_spend] = "spend_invalid"
    status_values[valid_spend & ~valid_ccy] = "currency_missing"
    status_values[valid_spend & valid_ccy & ~valid_rate] = "unsupported_currency"

    fallback_mask = pd.Series(fallback_flags.astype(bool), index=df.index)
    status_values[valid_all & fallback_mask] = "fallback"

    if conversion_mode == "monthly":
        date_parse_fail = parsed_dates.isna() & df[date_col].notna()
        # Mark rows where date failed to parse AND no rate was found even via fallback
        status_values[valid_spend & valid_ccy & date_parse_fail & ~valid_rate] = "date_unparseable"

    converted_series = pd.Series(converted, index=df.index)
    converted_series[~valid_all] = pd.NA

    if use_string_labels:
        fx_rate_series = pd.Series([f"AVG_{scope_year}"] * len(df), index=df.index, dtype=object)
    else:
        fx_rate_series = pd.Series(rate_values, index=df.index, dtype="Float64")
    fx_rate_series[~valid_all] = pd.NA

    # Convert nullable Float64 to standard float64 (pd.NA → np.nan)
    # so downstream .fillna("") in preview/download endpoints doesn't crash
    if fx_rate_series.dtype == "Float64":
        fx_rate_series = fx_rate_series.astype("float64")
    converted_series = converted_series.astype("float64")

    spend_idx = df.columns.get_loc(spend_col)
    df.insert(spend_idx + 1, fx_col, fx_rate_series)
    df.insert(spend_idx + 2, out_col, converted_series)
    df.insert(spend_idx + 3, status_col, status_values)

    metrics = {
        "n_converted":        int((status_values == "converted").sum()),
        "n_fallback":         int((status_values == "fallback").sum()),
        "n_spend_invalid":    int((status_values == "spend_invalid").sum()),
        "n_currency_missing": int((status_values == "currency_missing").sum()),
        "n_unsupported":      int((status_values == "unsupported_currency").sum()),
        "n_date_unparseable": int((status_values == "date_unparseable").sum()),
        # Legacy keys for backward compatibility
        "n_parse_err":        int((status_values == "spend_invalid").sum()),
        "n_no_ccy":           int((status_values == "currency_missing").sum()),
        "n_no_rate":          int((status_values == "unsupported_currency").sum()),
        "unsupported":        list(set(ccy_series[valid_spend & valid_ccy & ~valid_rate].dropna().unique())),
    }
    return df, metrics, fx_col, out_col, status_col
