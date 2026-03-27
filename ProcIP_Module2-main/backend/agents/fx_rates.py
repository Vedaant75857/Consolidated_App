"""
FX Rates Module
API-based historical exchange rate fetcher.

Uses date-based API calls grouped by source currency and unique dates.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta

# Frankfurter uses ECB reference data and supports date-specific queries.
_FX_API_URL = "https://api.frankfurter.dev/v1"
_DIRECT_RATE_CACHE = {}

# Known supported currencies
SUPPORTED_CURRENCIES = {
    'USD', 'EUR', 'GBP', 'JPY', 'INR', 'CHF', 'CAD', 'AUD', 'CNY',
    'SEK', 'NZD', 'MXN', 'SGD', 'HKD', 'NOK', 'KRW', 'TRY', 'RUB',
    'BRL', 'ZAR', 'BGN', 'HRK', 'CZK', 'DKK', 'HUF', 'PLN',
    'RON', 'ISK', 'CYP', 'EEK', 'LVL', 'LTL', 'MTL', 'SKK',
}


def _normalize_date(date_str):
    try:
        return datetime.strptime(str(date_str), '%Y-%m-%d').strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def _fetch_direct_rate_api(date_str, from_curr, to_curr, lookback_days=7):
    """Fetch direct historical FX rate from API for from->to on date (or nearest prior day)."""
    normalized_date = _normalize_date(date_str)
    if not normalized_date:
        return None

    from_curr = str(from_curr).upper().strip()
    to_curr = str(to_curr).upper().strip()
    if from_curr == to_curr:
        return 1.0

    cache_key = (normalized_date, from_curr, to_curr)
    if cache_key in _DIRECT_RATE_CACHE:
        return _DIRECT_RATE_CACHE[cache_key]

    target = datetime.strptime(normalized_date, '%Y-%m-%d')
    for step in range(0, max(lookback_days, 0) + 1):
        candidate = (target - timedelta(days=step)).strftime('%Y-%m-%d')
        try:
            url = f"{_FX_API_URL}/{candidate}"
            resp = requests.get(
                url,
                params={'from': from_curr, 'to': to_curr},
                timeout=12,
                headers={'User-Agent': 'Bain-Data-Cleaning-Tool/1.0'}
            )
            resp.raise_for_status()
            payload = resp.json() if resp.content else {}
            rates = payload.get('rates', {}) if isinstance(payload, dict) else {}
            raw_rate = rates.get(to_curr)
            if raw_rate is None:
                continue

            rate = float(raw_rate)
            _DIRECT_RATE_CACHE[cache_key] = rate
            if step > 0:
                print(f"[FX] API fallback: using {candidate} for {normalized_date} ({from_curr}->{to_curr})")
            return rate
        except Exception:
            continue

    _DIRECT_RATE_CACHE[cache_key] = None
    return None


def fetch_grouped_fx_rates(currency_to_dates, target_currency, lookback_days=7):
    """Fetch rates by grouped source currency and unique dates.

    Args:
        currency_to_dates: dict like {"EUR": {"2026-03-01", "2026-03-02"}, ...}
        target_currency: target 3-letter code
    Returns:
        dict[tuple[str, str], float]: {(from_curr, date_str): rate}
    """
    target_curr = str(target_currency).upper().strip()
    rate_map = {}
    if not currency_to_dates:
        return rate_map

    for from_curr, dates in currency_to_dates.items():
        source_curr = str(from_curr).upper().strip()
        if source_curr == target_curr:
            continue
        for d in sorted(set(dates or [])):
            norm_date = _normalize_date(d)
            if not norm_date:
                continue
            rate = _fetch_direct_rate_api(norm_date, source_curr, target_curr, lookback_days=lookback_days)
            if rate is not None:
                rate_map[(source_curr, norm_date)] = rate

    return rate_map


def get_fx_rate(date_str, from_currency, to_currency):
    """Get a single direct rate from source to target for a date."""
    from_curr = str(from_currency).upper().strip()
    to_curr = str(to_currency).upper().strip()
    if from_curr == to_curr:
        return 1.0
    return _fetch_direct_rate_api(date_str, from_curr, to_curr, lookback_days=7)


def detect_currency_columns(df, threshold=0.5):
    """Detect columns that are likely currency codes."""
    currency_codes = SUPPORTED_CURRENCIES
    candidates = []

    for col in df.columns:
        non_null = df[col].dropna().astype(str).unique()
        if len(non_null) == 0:
            continue

        match_count = sum(
            1 for val in non_null
            if val.upper().strip() in currency_codes
        )

        match_ratio = match_count / len(non_null)
        if match_ratio >= threshold:
            candidates.append(str(col))

    return candidates


def detect_spend_columns(df):
    """Detect columns that are likely spend/amount columns."""
    money_keywords = {
        'spend', 'amount', 'cost', 'price', 'total', 'value',
        'revenue', 'sales', 'invoice', 'sum', 'net', 'gross',
        'charge', 'fee', 'payment', 'pay'
    }

    candidates = []

    for col in df.columns:
        col_lower = str(col).lower()

        has_money_keyword = any(kw in col_lower for kw in money_keywords)
        if not has_money_keyword:
            continue

        try:
            numeric = pd.to_numeric(df[col], errors='coerce')
            non_null_numeric = numeric.notna().sum()
            total_non_null = df[col].notna().sum()

            if total_non_null > 0 and non_null_numeric / total_non_null >= 0.5:
                candidates.append(str(col))
        except Exception:
            pass

    return candidates
