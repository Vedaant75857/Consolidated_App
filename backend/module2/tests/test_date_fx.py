import pandas as pd
import numpy as np
import pytest

from agents.date_utils import (
    _profile_date_series,
    _parse_date_series,
    _find_normalized_date_col,
)
from agents.normalization import date_normalization_agent
from agents.fx_rates import load_fx_table, run_conversion


def _make_df(rows):
    return pd.DataFrame(rows)


class TestDateProfiling:
    """Verify the shared date profiler picks the right DMY/MDY order."""

    def test_profile_detects_dmy(self):
        s = pd.Series(["01-02-2023", "15-03-2023", "20-04-2023"])
        assert _profile_date_series(s) == "DMY"

    def test_profile_detects_mdy(self):
        s = pd.Series(["02-01-2023", "03-15-2023", "04-20-2023"])
        assert _profile_date_series(s) == "MDY"

    def test_parse_date_series_handles_mixed_dmy(self):
        s = pd.Series(["01-02-2023", "15-03-2023", "", "not-a-date"])
        parsed = _parse_date_series(s)
        assert parsed[0] == pd.Timestamp("2023-02-01")
        assert parsed[1] == pd.Timestamp("2023-03-15")
        assert pd.isna(parsed[2])
        assert pd.isna(parsed[3])


class TestDateNormalization:
    """Verify the normalization agent produces usable Norm_Date_* columns."""

    def test_normalizes_dmy_dates_to_target_format(self):
        df = _make_df({
            "Invoice Date": ["01-02-2023", "15-03-2023", "20-04-2023"],
            "Amount": [100, 200, 300],
        })
        df, msg, _ = date_normalization_agent(df, user_format="%d-%m-%Y")
        norm_col = "Norm_Date_Invoice Date_ddmmyyyy"
        assert norm_col in df.columns
        assert df[norm_col].tolist() == ["01-02-2023", "15-03-2023", "20-04-2023"]

    def test_find_normalized_date_col_prefers_populated(self):
        df = _make_df({
            "Invoice Date": ["01-02-2023", "15-03-2023"],
            "Norm_Date_Invoice Date_ddmmyyyy": ["01-02-2023", "15-03-2023"],
        })
        assert _find_normalized_date_col(df, "Invoice Date") == "Norm_Date_Invoice Date_ddmmyyyy"

    def test_find_normalized_date_col_ignores_empty(self):
        df = _make_df({
            "Invoice Date": ["01-02-2023", "15-03-2023"],
            "Norm_Date_Invoice Date_ddmmyyyy": ["", ""],
        })
        assert _find_normalized_date_col(df, "Invoice Date") is None


class TestFxRateLookup:
    """Verify embedded FX values and basic rate lookup behavior."""

    def test_embedded_eur_jan_2024_value(self):
        fx_data = load_fx_table()
        FX, LATEST_RATE, _, _, _, _ = fx_data
        assert FX[("EUR", 2024, "Jan")] == pytest.approx(0.916761)


class TestCurrencyConversion:
    """Verify currency conversion picks correct rates and uses normalized dates."""

    def _base_df(self):
        return _make_df({
            "Currency": ["EUR", "EUR", "EUR"],
            "Spend": [1000, 2000, 3000],
            "Invoice Date": ["01-02-2023", "15-03-2023", "20-04-2023"],
        })

    def test_monthly_conversion_with_original_dmy_date(self):
        """With the aligned parser, original DMY dates should resolve to monthly rates.

        Feb 2023 is before the embedded table start (Mar 2023), so that row
        falls back to the latest EUR rate.
        """
        df = self._base_df()
        df, metrics, _, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="USD",
        )
        assert metrics["n_converted"] == 2
        assert metrics["n_fallback"] == 1
        assert metrics["n_date_unparseable"] == 0
        assert "FX_rate_used_Spend" in df.columns
        assert "Spend_converted_inUSD" in df.columns

    def test_monthly_conversion_prefers_normalized_date_column(self):
        """When a Norm_Date_* column exists, conversion should use it."""
        df = self._base_df()
        df, _, _ = date_normalization_agent(df, user_format="%d-%m-%Y")

        df, metrics, _, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="USD",
        )
        assert metrics["n_converted"] == 2
        assert metrics["n_fallback"] == 1
        assert metrics["n_date_unparseable"] == 0

    def test_conversion_falls_back_for_future_month(self):
        """A date outside the embedded table should fall back to the latest rate."""
        df = _make_df({
            "Currency": ["EUR"],
            "Spend": [1000],
            "Invoice Date": ["15-08-2026"],  # Embedded table ends Jul 2026.
        })
        df, metrics, _, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="USD",
        )
        assert metrics["n_converted"] == 0
        assert metrics["n_fallback"] == 1

    def test_non_usd_target_bridging(self):
        """Converting EUR -> GBP should use EUR/USD and GBP/USD rates."""
        df = _make_df({
            "Currency": ["EUR"],
            "Spend": [1000],
            "Invoice Date": ["15-01-2024"],
        })
        df, metrics, _, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="GBP",
        )
        assert metrics["n_converted"] == 1
        assert metrics["n_fallback"] == 0
        # Approximate sanity check: 1000 EUR / 0.917 * 0.787 ~= 858 GBP.
        converted = df["Spend_converted_inGBP"].iloc[0]
        assert 800 < converted < 900


class TestOverrideFallback:
    """Verify partial monthly overrides fall back to the latest rate."""

    def test_monthly_override_is_used_for_that_month(self):
        """When the user provides a monthly override for the row's month, use it."""
        df = _make_df({
            "Currency": ["AED"],
            "Spend": [1000],
            "Invoice Date": ["15-03-2024"],
        })
        overrides = {"AED": {"2024": {"Mar": 3.70}}}
        df, metrics, fx_col, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="USD",
            fx_overrides=overrides, fx_override_mode="monthly",
        )
        assert metrics["n_converted"] == 1
        assert metrics["n_fallback"] == 0
        assert df[fx_col].iloc[0] == pytest.approx(3.70)

    def test_partial_monthly_override_falls_back_to_latest(self):
        """Missing monthly overrides fall back to the latest embedded rate."""
        df = _make_df({
            "Currency": ["AED"],
            "Spend": [1000],
            "Invoice Date": ["15-08-2026"],  # Outside embedded range.
        })
        # Override only March 2024; Aug 2026 should fall back to latest AED rate.
        overrides = {"AED": {"2024": {"Mar": 3.70}}}
        df, metrics, _, _, _ = run_conversion(
            df, "Spend", "Currency", conversion_mode="monthly",
            date_col="Invoice Date", target_currency="USD",
            fx_overrides=overrides, fx_override_mode="monthly",
        )
        assert metrics["n_converted"] == 0
        assert metrics["n_fallback"] == 1
