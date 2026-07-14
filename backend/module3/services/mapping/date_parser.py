"""Date parsing utilities for column type casting.

Handles Excel serial numbers, multi-format date strings (DMY/MDY),
partial dates, and compact 8-digit formats.  Extracted from column_mapper
so the mapper stays focused on field matching.
"""

import re
from datetime import datetime, timedelta

import pandas as pd

_EXCEL_EPOCH = datetime(1899, 12, 30)
_CURRENT_YEAR = datetime.today().year

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

_ORDINAL_RE = re.compile(r"(\d+)(st|nd|rd|th)\b", re.IGNORECASE)
_COMPACT_8_RE = re.compile(r"^\d{8}$")
_YEAR_ONLY_RE = re.compile(r"^\d{4}$")
_TIME_RE = re.compile(
    r"\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$", re.IGNORECASE
)
_ISO_T_RE = re.compile(
    r"T\d{2}:\d{2}(:\d{2})?(\..*?)?(Z|[+-]\d{2}:\d{2})?$", re.IGNORECASE
)

_DMY_MASKS = [
    "%d-%m-%Y", "%d-%b-%Y", "%d-%B-%Y",
    "%d-%m-%y", "%d-%b-%y", "%d-%B-%y",
    "%Y-%m-%d", "%y-%m-%d",
]
_MDY_MASKS = [
    "%m-%d-%Y", "%b-%d-%Y", "%B-%d-%Y",
    "%m-%d-%y", "%b-%d-%y", "%B-%d-%y",
    "%Y-%m-%d", "%y-%m-%d",
]


def _excel_serial(serial) -> datetime | None:
    """Convert an Excel serial number to a datetime."""
    try:
        return _EXCEL_EPOCH + timedelta(days=float(serial))
    except Exception:
        return None


def _date_preprocess(raw) -> str:
    """Clean a raw date string: strip time, ordinals, unify separators."""
    s = _ISO_T_RE.sub("", str(raw).strip()).strip()
    s = _TIME_RE.sub("", s).strip()
    s = _ORDINAL_RE.sub(r"\1", s)
    return re.sub(r"[/\.\s,]+", "-", s).strip("-")


def _parse_partial_date(s: str) -> datetime | None:
    """Handle year-only or month-year partial dates."""
    if _YEAR_ONLY_RE.match(s):
        return datetime(int(s), 1, 1)
    parts = s.split("-")
    if len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()
        if a.lower() in _MONTH_MAP and b.isdigit() and len(b) == 4:
            return datetime(int(b), _MONTH_MAP[a.lower()], 1)
        if b.lower() in _MONTH_MAP and a.isdigit() and len(a) == 4:
            return datetime(int(a), _MONTH_MAP[b.lower()], 1)
        if a.isdigit() and b.lower() in _MONTH_MAP:
            return datetime(_CURRENT_YEAR, _MONTH_MAP[b.lower()], int(a))
        if b.isdigit() and a.lower() in _MONTH_MAP:
            return datetime(_CURRENT_YEAR, _MONTH_MAP[a.lower()], int(b))
    return None


def _try_date_masks(s: str, masks: list[str]) -> datetime | None:
    """Try parsing with a list of strptime masks."""
    for fmt in masks:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _profile_date_series(series: pd.Series) -> str:
    """Profile a series to determine DMY vs MDY order.

    Returns 'DMY' or 'MDY'.
    """
    score_dmy = score_mdy = 0
    months_re = re.compile(
        r"(?i)^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
    )
    for val in series.dropna():
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            continue
        s = str(val).strip()
        if _COMPACT_8_RE.match(s):
            continue
        s = re.sub(
            r"[/\.\s,]+", "-",
            _ISO_T_RE.sub("", _TIME_RE.sub("", s).strip()).strip(),
        )
        parts = s.split("-")
        if len(parts) < 2:
            continue
        p0, p1 = parts[0], parts[1]
        try:
            if int(p0) > 12:
                score_dmy += 1
        except ValueError:
            pass
        try:
            if int(p1) > 12:
                score_mdy += 1
        except ValueError:
            pass
        if months_re.match(p0):
            score_mdy += 1
        if months_re.match(p1):
            score_dmy += 1
    return "MDY" if score_mdy > score_dmy else "DMY"


def _parse_one_date(raw, masks: list[str]) -> datetime | None:
    """Parse a single raw value through a multi-gate pipeline."""
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return _excel_serial(raw)

    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return None

    # Excel serial stored as string (e.g. '45734')
    if re.match(r"^\d{5}$", s):
        serial = int(s)
        if 18000 <= serial <= 73050:
            return _excel_serial(serial)

    # Compact 8-digit (YYYYMMDD / DDMMYYYY / MMDDYYYY)
    if _COMPACT_8_RE.match(s):
        for fmt in (
            "%Y%m%d",
            "%d%m%Y" if masks is _DMY_MASKS else "%m%d%Y",
            "%m%d%Y" if masks is _DMY_MASKS else "%d%m%Y",
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue

    clean = _date_preprocess(s)

    # ISO year-first fast path
    if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", clean):
        try:
            return datetime.strptime(clean, "%Y-%m-%d")
        except ValueError:
            pass

    result = _try_date_masks(clean, masks)
    if result:
        return result

    result = _parse_partial_date(clean)
    if result:
        return result

    # Pandas mixed fallback
    try:
        ts = pd.to_datetime(
            clean, dayfirst=(masks is _DMY_MASKS),
            format="mixed", errors="coerce",
        )
        if pd.notna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass
    return None


def parse_date_column(series: pd.Series) -> pd.Series:
    """Profile a column for DMY/MDY order and parse every value to datetime.

    Args:
        series: Raw pandas Series (mixed types okay).

    Returns:
        Series of datetime objects (NaT for unparseable values).
    """
    order = _profile_date_series(series)
    masks = _DMY_MASKS if order == "DMY" else _MDY_MASKS
    parsed = series.apply(lambda v: _parse_one_date(v, masks))
    return pd.to_datetime(parsed, errors="coerce")
