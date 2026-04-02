"""
Normalization Agents Module
Production-scale data normalization agents.

Design: deterministic rules first, AI only for unresolved values.
All agents process ALL unique values (no caps), use concurrent batching,
and return cost/timing metadata.
"""

import re
import json
import difflib
import traceback
import warnings
from datetime import datetime, timedelta
import pandas as pd
from .helpers import (
    get_client, get_model, CostTracker,
    _batch_ai_mapping, _find_column,
)



# ═══════════════════════════════════════════════════════════════════════════════
#  LOOKUP TABLES
# ═══════════════════════════════════════════════════════════════════════════════

_SUPPLIER_SUFFIXES = re.compile(
    r'\b('
    r'Inc\.?|Incorporated|Ltd\.?|Limited|LLC|L\.?L\.?C\.?|'
    r'Corp\.?|Corporation|Co\.?|Company|'
    r'GmbH|AG|SA|S\.?A\.?|S\.?A\.?S\.?|S\.?R\.?L\.?|'
    r'Pty\.?|PLC|P\.?L\.?C\.?|LLP|L\.?L\.?P\.?|LP|'
    r'Holdings|Holding|International|Intl\.?|'
    r'Group|Grp\.?|Enterprises?|Solutions?|Services?|'
    r'Technologies|Technology|Tech|'
    r'KG|KGaA|BV|B\.?V\.?|NV|N\.?V\.?|OY|AB|AS|A/S|'
    r'Pvt\.?|Private'
    r')\s*,?\s*$',
    re.IGNORECASE,
)
_DOMAIN_PATTERN = re.compile(r'\.(com|net|org|io|co|biz|info)\b', re.IGNORECASE)
_MULTI_SPACE = re.compile(r'\s{2,}')

ISO_COUNTRY_MAP = {
    'US': 'United States', 'USA': 'United States', 'U.S.': 'United States', 'U.S.A.': 'United States',
    'UNITED STATES OF AMERICA': 'United States',
    'UK': 'United Kingdom', 'GB': 'United Kingdom', 'GREAT BRITAIN': 'United Kingdom',
    'DE': 'Germany', 'DEU': 'Germany', 'GERM': 'Germany', 'DEUTSCHLAND': 'Germany',
    'FR': 'France', 'FRA': 'France',
    'JP': 'Japan', 'JPN': 'Japan',
    'CN': 'China', 'CHN': 'China', 'PRC': 'China',
    'IN': 'India', 'IND': 'India',
    'BR': 'Brazil', 'BRA': 'Brazil', 'BRASIL': 'Brazil',
    'CA': 'Canada', 'CAN': 'Canada',
    'AU': 'Australia', 'AUS': 'Australia',
    'IT': 'Italy', 'ITA': 'Italy', 'ITALIA': 'Italy',
    'ES': 'Spain', 'ESP': 'Spain', 'ESPANA': 'Spain',
    'MX': 'Mexico', 'MEX': 'Mexico',
    'KR': 'South Korea', 'KOR': 'South Korea', 'KOREA': 'South Korea',
    'NL': 'Netherlands', 'NLD': 'Netherlands', 'HOLLAND': 'Netherlands',
    'CH': 'Switzerland', 'CHE': 'Switzerland', 'SUISSE': 'Switzerland',
    'SE': 'Sweden', 'SWE': 'Sweden',
    'NO': 'Norway', 'NOR': 'Norway',
    'DK': 'Denmark', 'DNK': 'Denmark',
    'FI': 'Finland', 'FIN': 'Finland',
    'AT': 'Austria', 'AUT': 'Austria',
    'BE': 'Belgium', 'BEL': 'Belgium',
    'PT': 'Portugal', 'PRT': 'Portugal',
    'IE': 'Ireland', 'IRL': 'Ireland',
    'PL': 'Poland', 'POL': 'Poland',
    'CZ': 'Czech Republic', 'CZE': 'Czech Republic', 'CZECHIA': 'Czech Republic',
    'RO': 'Romania', 'ROU': 'Romania',
    'HU': 'Hungary', 'HUN': 'Hungary',
    'GR': 'Greece', 'GRC': 'Greece',
    'TR': 'Turkey', 'TUR': 'Turkey', 'TURKIYE': 'Turkey',
    'RU': 'Russia', 'RUS': 'Russia',
    'ZA': 'South Africa', 'ZAF': 'South Africa',
    'NG': 'Nigeria', 'NGA': 'Nigeria',
    'EG': 'Egypt', 'EGY': 'Egypt',
    'KE': 'Kenya', 'KEN': 'Kenya',
    'SA': 'Saudi Arabia', 'SAU': 'Saudi Arabia', 'KSA': 'Saudi Arabia',
    'AE': 'United Arab Emirates', 'ARE': 'United Arab Emirates', 'UAE': 'United Arab Emirates',
    'IL': 'Israel', 'ISR': 'Israel',
    'SG': 'Singapore', 'SGP': 'Singapore',
    'MY': 'Malaysia', 'MYS': 'Malaysia',
    'TH': 'Thailand', 'THA': 'Thailand',
    'ID': 'Indonesia', 'IDN': 'Indonesia',
    'PH': 'Philippines', 'PHL': 'Philippines',
    'VN': 'Vietnam', 'VNM': 'Vietnam',
    'TW': 'Taiwan', 'TWN': 'Taiwan',
    'HK': 'Hong Kong', 'HKG': 'Hong Kong',
    'NZ': 'New Zealand', 'NZL': 'New Zealand',
    'AR': 'Argentina', 'ARG': 'Argentina',
    'CL': 'Chile', 'CHL': 'Chile',
    'CO': 'Colombia', 'COL': 'Colombia',
    'PE': 'Peru', 'PER': 'Peru',
    'LU': 'Luxembourg', 'LUX': 'Luxembourg',
    'SK': 'Slovakia', 'SVK': 'Slovakia',
    'SI': 'Slovenia', 'SVN': 'Slovenia',
    'HR': 'Croatia', 'HRV': 'Croatia',
    'BG': 'Bulgaria', 'BGR': 'Bulgaria',
    'RS': 'Serbia', 'SRB': 'Serbia',
    'UA': 'Ukraine', 'UKR': 'Ukraine',
    'LT': 'Lithuania', 'LTU': 'Lithuania',
    'LV': 'Latvia', 'LVA': 'Latvia',
    'EE': 'Estonia', 'EST': 'Estonia',
    'CY': 'Cyprus', 'CYP': 'Cyprus',
    'MT': 'Malta', 'MLT': 'Malta',
    'IS': 'Iceland', 'ISL': 'Iceland',
    'PK': 'Pakistan', 'PAK': 'Pakistan',
    'BD': 'Bangladesh', 'BGD': 'Bangladesh',
    'LK': 'Sri Lanka', 'LKA': 'Sri Lanka',
    'MM': 'Myanmar', 'MMR': 'Myanmar',
    'KH': 'Cambodia', 'KHM': 'Cambodia',
    'QA': 'Qatar', 'QAT': 'Qatar',
    'KW': 'Kuwait', 'KWT': 'Kuwait',
    'BH': 'Bahrain', 'BHR': 'Bahrain',
    'OM': 'Oman', 'OMN': 'Oman',
    'JO': 'Jordan', 'JOR': 'Jordan',
    'LB': 'Lebanon', 'LBN': 'Lebanon',
    'MA': 'Morocco', 'MAR': 'Morocco',
    'TN': 'Tunisia', 'TUN': 'Tunisia',
    'DZ': 'Algeria', 'DZA': 'Algeria',
    'GH': 'Ghana', 'GHA': 'Ghana',
    'TZ': 'Tanzania', 'TZA': 'Tanzania',
    'ET': 'Ethiopia', 'ETH': 'Ethiopia',
    'PR': 'Puerto Rico', 'PRI': 'Puerto Rico',
    'CR': 'Costa Rica', 'CRI': 'Costa Rica',
    'PA': 'Panama', 'PAN': 'Panama',
    'DO': 'Dominican Republic', 'DOM': 'Dominican Republic',
    'EC': 'Ecuador', 'ECU': 'Ecuador',
    'UY': 'Uruguay', 'URY': 'Uruguay',
    'VE': 'Venezuela', 'VEN': 'Venezuela',
    'BO': 'Bolivia', 'BOL': 'Bolivia',
    'PY': 'Paraguay', 'PRY': 'Paraguay',
}
_VALID_COUNTRIES = set(ISO_COUNTRY_MAP.values())
_COUNTRY_LOOKUP = {}
for k, v in ISO_COUNTRY_MAP.items():
    _COUNTRY_LOOKUP[k.upper()] = v
    _COUNTRY_LOOKUP[k.upper().replace('.', '')] = v
for v in _VALID_COUNTRIES:
    _COUNTRY_LOOKUP[v.upper()] = v

COUNTRY_TO_REGION = {
    'United States': 'NA', 'Canada': 'NA', 'Puerto Rico': 'NA',
    'Mexico': 'LATAM', 'Brazil': 'LATAM', 'Argentina': 'LATAM', 'Chile': 'LATAM',
    'Colombia': 'LATAM', 'Peru': 'LATAM', 'Ecuador': 'LATAM', 'Venezuela': 'LATAM',
    'Uruguay': 'LATAM', 'Paraguay': 'LATAM', 'Bolivia': 'LATAM',
    'Costa Rica': 'LATAM', 'Panama': 'LATAM', 'Dominican Republic': 'LATAM',
    'United Kingdom': 'EMEA', 'Germany': 'EMEA', 'France': 'EMEA', 'Italy': 'EMEA',
    'Spain': 'EMEA', 'Netherlands': 'EMEA', 'Belgium': 'EMEA', 'Switzerland': 'EMEA',
    'Austria': 'EMEA', 'Sweden': 'EMEA', 'Norway': 'EMEA', 'Denmark': 'EMEA',
    'Finland': 'EMEA', 'Ireland': 'EMEA', 'Portugal': 'EMEA', 'Poland': 'EMEA',
    'Czech Republic': 'EMEA', 'Romania': 'EMEA', 'Hungary': 'EMEA', 'Greece': 'EMEA',
    'Turkey': 'EMEA', 'Russia': 'EMEA', 'Ukraine': 'EMEA', 'Luxembourg': 'EMEA',
    'Slovakia': 'EMEA', 'Slovenia': 'EMEA', 'Croatia': 'EMEA', 'Bulgaria': 'EMEA',
    'Serbia': 'EMEA', 'Lithuania': 'EMEA', 'Latvia': 'EMEA', 'Estonia': 'EMEA',
    'Cyprus': 'EMEA', 'Malta': 'EMEA', 'Iceland': 'EMEA',
    'South Africa': 'EMEA', 'Nigeria': 'EMEA', 'Egypt': 'EMEA', 'Kenya': 'EMEA',
    'Ghana': 'EMEA', 'Tanzania': 'EMEA', 'Ethiopia': 'EMEA',
    'Morocco': 'EMEA', 'Tunisia': 'EMEA', 'Algeria': 'EMEA',
    'Saudi Arabia': 'EMEA', 'United Arab Emirates': 'EMEA', 'Israel': 'EMEA',
    'Qatar': 'EMEA', 'Kuwait': 'EMEA', 'Bahrain': 'EMEA', 'Oman': 'EMEA',
    'Jordan': 'EMEA', 'Lebanon': 'EMEA',
    'Japan': 'APAC', 'China': 'APAC', 'India': 'APAC', 'South Korea': 'APAC',
    'Australia': 'APAC', 'New Zealand': 'APAC', 'Singapore': 'APAC',
    'Malaysia': 'APAC', 'Thailand': 'APAC', 'Indonesia': 'APAC',
    'Philippines': 'APAC', 'Vietnam': 'APAC', 'Taiwan': 'APAC',
    'Hong Kong': 'APAC', 'Pakistan': 'APAC', 'Bangladesh': 'APAC',
    'Sri Lanka': 'APAC', 'Myanmar': 'APAC', 'Cambodia': 'APAC',
}


# ═══════════════════════════════════════════════════════════════════════════════
#  DETERMINISTIC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_supplier_name(name):
    """Deterministic suffix removal and cleanup for a single supplier name."""
    s = str(name).strip()
    s = _DOMAIN_PATTERN.sub('', s)
    prev = ''
    while prev != s:
        prev = s
        s = _SUPPLIER_SUFFIXES.sub('', s).strip()
    s = s.rstrip(' ,.-&/')
    s = _MULTI_SPACE.sub(' ', s).strip()
    return s


def _fuzzy_dedup(mapping, threshold=0.9):
    """Group near-duplicate cleaned names and pick the most common form."""
    values = list(set(mapping.values()))
    if len(values) <= 1:
        return mapping

    canonical = {}
    for v in sorted(values, key=lambda x: x.lower()):
        matched = False
        for canon in canonical:
            if difflib.SequenceMatcher(None, v.lower(), canon.lower()).ratio() > threshold:
                canonical[canon].append(v)
                matched = True
                break
        if not matched:
            canonical[v] = [v]

    remap = {}
    for canon, variants in canonical.items():
        for var in variants:
            remap[var] = canon

    return {orig: remap.get(cleaned, cleaned) for orig, cleaned in mapping.items()}


def _lookup_country(val):
    """Deterministic country lookup. Returns standardized name or None."""
    key = str(val).strip().upper().replace('.', '')
    return _COUNTRY_LOOKUP.get(key)


def _lookup_region(val):
    """Deterministic region lookup by country name."""
    return COUNTRY_TO_REGION.get(str(val).strip())


_PAYMENT_REGEX = [
    (re.compile(r'^(COD|C\.?O\.?D\.?)$', re.I), '0', '', ''),
    (re.compile(r'^(CIA|C\.?I\.?A\.?)$', re.I), '0', 'Cash in advance', ''),
    (re.compile(r'^(Cash|Immediate|Sofort)$', re.I), '0', '', ''),
    (re.compile(r'^Due\s*on\s*Receipt$', re.I), '0', '', ''),
    (re.compile(r'^Net[\s\-_]*(\d+)$', re.I), None, '', ''),
    (re.compile(r'^N(\d+)$', re.I), None, '', ''),
    (re.compile(r'^D(\d+)$', re.I), None, '', ''),
    (re.compile(r'^(\d+)\s*Days?$', re.I), None, '', ''),
    (re.compile(r'^(\d+)$'), None, '', ''),
    (re.compile(r'^(\d+)[%/](\d+)\s*(?:Net|N)[\s\-_]*(\d+)$', re.I), None, None, ''),
]


def _parse_payment_term(val):
    """Deterministic payment term parsing. Returns (days, discount, doubt) or None."""
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', 'null', ''):
        return ('', '', '')

    for pattern, days_tmpl, disc_tmpl, doubt_tmpl in _PAYMENT_REGEX:
        m = pattern.match(s)
        if m:
            groups = m.groups()
            if days_tmpl is None and disc_tmpl is None:
                return (groups[2], f"{groups[0]}% discount if paid within {groups[1]} days", '')
            if days_tmpl is None:
                return (groups[0] if groups else '', disc_tmpl, doubt_tmpl)
            return (days_tmpl, disc_tmpl, doubt_tmpl)
    return None


def _upsert_adjacent_column(df, source_col, new_col, values):
    """Insert or move a derived column so it stays immediately after its source column."""
    insert_at = df.columns.get_loc(source_col) + 1

    if new_col in df.columns:
        df.pop(new_col)

    df.insert(insert_at, new_col, values)


def _upsert_adjacent_columns(df, source_col, column_defs):
    """Insert multiple derived columns in order immediately after a source column."""
    insert_at = df.columns.get_loc(source_col) + 1
    for new_col, values in column_defs:
        if new_col in df.columns:
            df.pop(new_col)
        df.insert(insert_at, new_col, values)
        insert_at += 1


# ═══════════════════════════════════════════════════════════════════════════════
#  AGENTS
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_supplier_name_agent(df, api_key=None, **kwargs):
    """Normalize supplier names: regex cleanup + fuzzy dedup + AI for ambiguous."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

    target_col = _find_column(
        df,
        ['supplier_name', 'supplier name', 'vendor_name', 'vendor name', 'suppliername', 'vendorname'],
        ai_client=get_client(api_key), model=get_model(),
        ai_description='Supplier Name or Vendor Name',
    )
    if not target_col:
        return df, "[WARN] Could not identify a 'Supplier Name' column.", cost.summary()

    all_unique = df[target_col].dropna().astype(str).unique().tolist()
    print(f"  Supplier Names: {len(all_unique)} unique values from {len(df)} rows")

    det_mapping = {v: _clean_supplier_name(v) for v in all_unique}
    det_mapping = _fuzzy_dedup(det_mapping)

    unchanged = [v for v, cleaned in det_mapping.items() if cleaned == v]
    print(f"  Deterministic: {len(all_unique) - len(unchanged)} resolved, {len(unchanged)} -> AI")

    if unchanged and not custom_inst:
        sys_msg = custom_sys or "Output JSON only."
        prompt_tmpl = (
            "Standardize these Company/Supplier names.\n"
            "- Remove legal suffixes (Inc, Ltd, LLC, GmbH, Corp, etc).\n"
            "- Remove websites (.com, .net) and clean typos.\n"
            "- Keep short but recognizable.\n\n"
            "Input: {batch}\n"
            "Return JSON ONLY: {{ \"Original\": \"Standardized\" }}"
        )
        if custom_inst:
            prompt_tmpl = custom_inst

        ai_mapping, cost = _batch_ai_mapping(
            unchanged, sys_msg, prompt_tmpl, api_key,
            batch_size=80, max_workers=4, cost_tracker=cost,
        )
        det_mapping.update(ai_mapping)

    new_col = "NORMALIZED SUPPLIER_NAME_BAIN"
    normalized = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])
    _upsert_adjacent_column(df, target_col, new_col, normalized)

    return df, f"[OK] Normalized {len(det_mapping)} supplier names -> **{new_col}**.", cost.summary()


def normalize_supplier_country_agent(df, api_key=None, **kwargs):
    """Normalize supplier country: ISO 3166 lookup + AI for unknowns."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

    target_col = kwargs.get('country_col')
    if target_col and target_col in df.columns:
        pass  # use user-selected column
    else:
        target_col = _find_column(
            df,
            ['supplier_country', 'supplier country', 'vendor_country', 'vendor country', 'country'],
            ai_client=get_client(api_key), model=get_model(),
            ai_description='Supplier Country or Vendor Country',
        )
    if not target_col:
        return df, "[WARN] Could not identify a 'Supplier Country' column.", cost.summary()

    all_unique = df[target_col].dropna().astype(str).unique().tolist()
    print(f"  Supplier Country: {len(all_unique)} unique values from {len(df)} rows")

    det_mapping = {}
    unresolved = []
    for v in all_unique:
        result = _lookup_country(v)
        if result:
            det_mapping[v] = result
        else:
            unresolved.append(v)

    print(f"  ISO lookup: {len(det_mapping)} resolved, {len(unresolved)} -> AI")
    det_keys = set(det_mapping.keys())  # snapshot before AI merges in

    if unresolved:
        sys_msg = custom_sys or "Output JSON only."
        prompt_tmpl = custom_inst if custom_inst else (
            "Standardize these country names to their **Full English Name** (Title Case).\n"
            "- Expand abbreviations: 'US' -> 'United States', 'DE' -> 'Germany'.\n"
            "- Fix misspellings.\n\n"
            "Input: {batch}\nReturn JSON ONLY: {{ \"Original\": \"Standardized\" }}"
        )
        ai_mapping, cost = _batch_ai_mapping(
            unresolved, sys_msg, prompt_tmpl, api_key,
            batch_size=80, max_workers=4, cost_tracker=cost,
        )
        for orig, cleaned in ai_mapping.items():
            if cleaned in _VALID_COUNTRIES:
                det_mapping[orig] = cleaned
            else:
                nearest = _lookup_country(cleaned)
                det_mapping[orig] = nearest if nearest else cleaned

    full_mapping = {**det_mapping}
    new_col = "SUPPLIER COUNTRY NORMALIZED"
    normalized = df[target_col].astype(str).map(full_mapping).fillna(df[target_col])
    _upsert_adjacent_column(df, target_col, new_col, normalized)

    # Row-level metrics
    n_total = len(df)
    empty_mask = df[target_col].isna() | (df[target_col].astype(str).str.strip() == "")
    n_empty = int(empty_mask.sum())
    non_empty_vals = df.loc[~empty_mask, target_col].astype(str)
    n_deterministic = int(non_empty_vals.isin(det_keys).sum())
    n_in_mapping = int(non_empty_vals.isin(full_mapping.keys()).sum())
    n_ai = n_in_mapping - n_deterministic
    n_unresolved = int((~empty_mask).sum()) - n_in_mapping

    metrics = {
        "n_total": n_total,
        "n_empty": n_empty,
        "n_normalized": n_in_mapping,
        "n_deterministic": n_deterministic,
        "n_ai": n_ai,
        "n_unresolved": n_unresolved,
    }

    return df, f"[OK] Normalized {len(full_mapping)} countries -> **{new_col}**.", cost.summary(), metrics


def add_record_id_agent(df):
    """Add sequential Record ID column."""
    if any(str(c).lower() == "record id" for c in df.columns):
        return df, "Record ID already exists."
    df.insert(0, "Record ID", range(1, len(df) + 1))
    return df, "[OK] Added 'Record ID' column."


# ═══════════════════════════════════════════════════════════════════════════════
#  DATE NORMALIZATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

_DATE_TARGET_FMT = "%d-%m-%Y"
_EXCEL_EPOCH = datetime(1899, 12, 30)
_CURRENT_YEAR = datetime.today().year

_MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

_ORDINAL_RE   = re.compile(r'(\d+)(st|nd|rd|th)\b', re.IGNORECASE)
_COMPACT_8_RE = re.compile(r'^\d{8}$')
_YEAR_ONLY_RE = re.compile(r'^\d{4}$')
_TIME_RE      = re.compile(r'\s+\d{1,2}:\d{2}(:\d{2})?(\s*(AM|PM))?$', re.IGNORECASE)
_ISO_T_RE     = re.compile(r'T\d{2}:\d{2}(:\d{2})?(\..*?)?(Z|[+-]\d{2}:\d{2})?$', re.IGNORECASE)

_DMY_MASKS = ['%d-%m-%Y','%d-%b-%Y','%d-%B-%Y','%d-%m-%y','%d-%b-%y','%d-%B-%y','%Y-%m-%d','%y-%m-%d']
_MDY_MASKS = ['%m-%d-%Y','%b-%d-%Y','%B-%d-%Y','%m-%d-%y','%b-%d-%y','%B-%d-%y','%Y-%m-%d','%y-%m-%d']


def _excel_serial(serial):
    """Convert an Excel serial number to a datetime."""
    try:
        return _EXCEL_EPOCH + timedelta(days=float(serial))
    except Exception:
        return None


def _date_preprocess(raw):
    """Clean a raw date string: strip time, ordinals, unify separators."""
    s = _ISO_T_RE.sub('', str(raw).strip()).strip()
    s = _TIME_RE.sub('', s).strip()
    s = _ORDINAL_RE.sub(r'\1', s)
    return re.sub(r'[/\.\s,]+', '-', s).strip('-')


def _parse_partial_date(s):
    """Handle year-only or month-year partial dates."""
    if _YEAR_ONLY_RE.match(s):
        return datetime(int(s), 1, 1)
    parts = s.split('-')
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


def _try_date_masks(s, masks):
    """Try parsing with a list of strptime masks."""
    for fmt in masks:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _profile_date_series(series):
    """Profile a series to determine DMY vs MDY order."""
    score_dmy = score_mdy = 0
    months_re = re.compile(r'(?i)^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)')
    for val in series.dropna():
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            continue
        s = str(val).strip()
        if _COMPACT_8_RE.match(s):
            continue
        s = re.sub(r'[/\.\s,]+', '-', _ISO_T_RE.sub('', _TIME_RE.sub('', s).strip()).strip())
        parts = s.split('-')
        if len(parts) < 2:
            continue
        p0, p1 = parts[0], parts[1]
        try:
            if int(p0) > 12: score_dmy += 1
        except ValueError:
            pass
        try:
            if int(p1) > 12: score_mdy += 1
        except ValueError:
            pass
        if months_re.match(p0): score_mdy += 1
        if months_re.match(p1): score_dmy += 1
    return 'MDY' if score_mdy > score_dmy else 'DMY'


def _parse_one_date(raw, masks):
    """Parse a single raw value through a multi-gate pipeline."""
    # Gate 1 — Excel serial (numeric type)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return _excel_serial(raw)
    s = str(raw).strip()
    if not s or s.lower() in ('nan', 'none', 'nat', ''):
        return None
    # Gate 1b — Excel serial as string (e.g. '45734' from dtype=str loading)
    if re.match(r'^\d{5}$', s):
        serial = int(s)
        if 18000 <= serial <= 73050:
            return _excel_serial(serial)
    # Gate 2 — Compact 8-digit
    if _COMPACT_8_RE.match(s):
        for fmt in ('%Y%m%d',
                    '%d%m%Y' if masks is _DMY_MASKS else '%m%d%Y',
                    '%m%d%Y' if masks is _DMY_MASKS else '%d%m%Y'):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
    # Gate 3 — Pre-process
    clean = _date_preprocess(s)
    # Gate 4 — ISO year-first fast path
    if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', clean):
        try:
            return datetime.strptime(clean, '%Y-%m-%d')
        except ValueError:
            pass
    # Gate 5 — Format masks
    result = _try_date_masks(clean, masks)
    if result:
        return result
    # Gate 6 — Partial date
    result = _parse_partial_date(clean)
    if result:
        return result
    # Gate 7 — Pandas mixed fallback
    try:
        ts = pd.to_datetime(clean, dayfirst=(masks is _DMY_MASKS), format='mixed', errors='coerce')
        if pd.notna(ts):
            return ts.to_pydatetime()
    except Exception:
        pass
    return None


def _normalize_date_series(series, target_fmt, force_order=None):
    """Normalize a pandas Series of raw date values. Returns (Series, order_str)."""
    order = force_order or _profile_date_series(series)
    masks = _DMY_MASKS if order == 'DMY' else _MDY_MASKS
    results = [
        (dt.strftime(target_fmt) if (dt := _parse_one_date(v, masks)) else '')
        for v in series
    ]
    return pd.Series(results, index=series.index, name=series.name), order


_FILE_COL_RE = re.compile(
    r'(?i)(file[_\s]?name|file[_\s]?id|filename|source[_\s]?file|'
    r'data[_\s]?source|input[_\s]?file|source|origin|src|file)'
)


def _detect_file_column(df):
    """Return the name of the file/source identity column, or None."""
    for col in df.columns:
        if _FILE_COL_RE.search(str(col)):
            return col
    return None


def date_normalization_agent(df, api_key=None, user_format=None, **kwargs):
    """Normalize date columns using per-value multi-gate parsing.

    If a file/source column is detected, normalizes per group for accurate
    DMY/MDY profiling on stitched datasets.  Otherwise processes as one block.
    """
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')
    target_fmt = user_format or _DATE_TARGET_FMT
    log = []

    # Derive a human-readable label and a column-safe suffix from the target format
    _FMT_LABELS = {
        "%d-%m-%Y": ("dd-mm-yyyy", "ddmmyyyy"),
        "%m-%d-%Y": ("mm-dd-yyyy", "mmddyyyy"),
    }
    fmt_label, fmt_suffix = _FMT_LABELS.get(target_fmt, (target_fmt, target_fmt.replace('%', '').replace('-', '')))

    date_cols = [
        c for c in df.columns
        if ("date" in str(c).lower() or "dob" in str(c).lower() or "time" in str(c).lower())
        and not str(c).startswith("Norm_Date_")
    ]
    if not date_cols:
        return df, "No new date columns found.", cost.summary()

    file_col = _detect_file_column(df)
    print(f"  File/source column : {file_col!r}")
    print(f"  Date columns       : {date_cols}")

    # Build groups
    if file_col and file_col in df.columns:
        groups = list(df.groupby(file_col, sort=False))
    else:
        groups = [('(all)', df)]

    # Prepare normalized buffers
    norm_buffers = {col: pd.Series('', index=df.index, dtype=str) for col in date_cols}

    for src_val, grp in groups:
        for col in date_cols:
            try:
                norm_series, order = _normalize_date_series(grp[col], target_fmt)
                norm_buffers[col].loc[norm_series.index] = norm_series.values
                total   = grp[col].notna().sum()
                success = (norm_series != '').sum()

                # AI fallback for > 20% failure within this group
                failed = total - success
                if total > 0 and failed > total * 0.2:
                    samples = _diverse_date_samples(grp[col], max_samples=30)
                    if samples:
                        client = get_client(api_key)
                        prompt = (
                            f"These date values need parsing: {json.dumps(samples[:20])}\n"
                            f"Return a Python strptime format string that parses MOST of these.\n"
                            f"Return JSON ONLY: {{ \"format\": \"%d/%m/%Y\" }}"
                        )
                        if custom_inst:
                            prompt = custom_inst.replace(
                                '{sample_dates}', json.dumps(samples)
                            ).replace('{target_format}', target_fmt)
                        try:
                            resp = client.chat.completions.create(
                                model=get_model(),
                                messages=[
                                    {"role": "system", "content": custom_sys or "Output JSON only."},
                                    {"role": "user", "content": prompt},
                                ],
                                response_format={"type": "json_object"},
                            )
                            cost.record(resp)
                            ai_fmt = json.loads(resp.choices[0].message.content).get('format', '')
                            if ai_fmt:
                                # Re-parse only the failed values with the AI-suggested format
                                for idx_val in grp[col].index:
                                    if norm_buffers[col].loc[idx_val] == '':
                                        raw = grp[col].loc[idx_val]
                                        clean = _date_preprocess(str(raw))
                                        try:
                                            dt = datetime.strptime(clean, ai_fmt)
                                            norm_buffers[col].loc[idx_val] = dt.strftime(target_fmt)
                                        except Exception:
                                            pass
                        except Exception as e:
                            cost.record_error(f"Date AI fallback for '{col}' group '{src_val}': {e}")

            except Exception as e:
                print(f"  [ERR] '{col}' group='{src_val}': {e}")

    # Insert normalized columns adjacent to originals
    for col in date_cols:
        new_col = f"Norm_Date_{col}_{fmt_suffix}"
        _upsert_adjacent_column(df, col, new_col, norm_buffers[col])

    # Build a single user-friendly summary line
    if date_cols:
        msg = ", ".join(date_cols) + f" -> normalized to {fmt_label}"
    else:
        msg = "No dates normalized."
    return df, msg, cost.summary()


_PAYMENT_TERMS_PROMPT = """Standardize these Payment Terms into THREE separate values:
1. "days"     - The net payment days as a string number ("30", "45", "0").
                Leave BLANK "" if you cannot determine days.
2. "discount" - Human-readable discount description. Leave "" if none.
3. "doubt"    - "Yes" if the value does NOT look like a payment term at all
                (e.g. a date like "2024-12-06", random text, descriptions).
                Otherwise "".

Input: {batch}

REFERENCE EXAMPLES (learn the patterns, don't hardcode):
  "N30"            -> days:"30",  discount:"",                                     doubt:""
  "N45"            -> days:"45",  discount:"",                                     doubt:""
  "1%30N45"        -> days:"45",  discount:"1% discount if paid within 30 days",   doubt:""
  "Net 30"         -> days:"30",  discount:"",                                     doubt:""
  "2/10 Net 30"    -> days:"30",  discount:"2% discount if paid within 10 days",   doubt:""
  "COD"            -> days:"0",   discount:"",                                     doubt:""
  "Cash"           -> days:"0",   discount:"",                                     doubt:""
  "CIA"            -> days:"0",   discount:"Cash in advance",                      doubt:""
  "2024-12-06"     -> days:"",    discount:"",                                     doubt:"Yes"

IMPORTANT: Parse the PATTERN, not just exact matches.

Return JSON ONLY:
{{
  "N30": {{"days":"30","discount":"","doubt":""}},
  ...
}}"""


def payment_terms_agent(df, api_key=None, **kwargs):
    """Normalize payment terms: regex pre-pass + concurrent AI for unknowns."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

    target_col = _find_column(
        df,
        ['payment_terms', 'payment terms', 'pay term', 'payment condition', 'payterm'],
        ai_client=get_client(api_key), model=get_model(),
        ai_description='Payment Terms (raw/messy, not already normalized)',
    )
    if not target_col:
        return df, "[WARN] No payment terms column found.", cost.summary()

    all_unique = df[target_col].dropna().astype(str).unique().tolist()
    all_unique = [v for v in all_unique if v.strip() and v.lower() not in ('nan', 'none', 'null')]
    print(f"  Payment Terms: {len(all_unique)} unique values from {len(df)} rows")

    det_mapping = {}
    unresolved = []
    for v in all_unique:
        result = _parse_payment_term(v)
        if result is not None:
            det_mapping[v] = {'days': result[0], 'discount': result[1], 'doubt': result[2]}
        else:
            unresolved.append(v)

    print(f"  Regex: {len(det_mapping)} resolved, {len(unresolved)} -> AI")

    if unresolved:
        sys_msg = custom_sys or "Output JSON only. Follow the exact format specified."
        prompt_tmpl = custom_inst if custom_inst else _PAYMENT_TERMS_PROMPT

        ai_mapping, cost = _batch_ai_mapping(
            unresolved, sys_msg, prompt_tmpl, api_key,
            batch_size=80, max_workers=4, cost_tracker=cost,
        )
        for k, v in ai_mapping.items():
            if isinstance(v, dict):
                det_mapping[k] = v

    norm_col = "PAYMENT TERMS_NORMALIZED"
    discount_col = "Discount_Payment_Terms"
    doubt_col = "Payment_Terms_Doubt"
    if norm_col in df.columns:
        norm_col = f"PAYMENT TERMS_NORMALIZED_{target_col}"
    if discount_col in df.columns:
        discount_col = f"Discount_Payment_Terms_{target_col}"
    if doubt_col in df.columns:
        doubt_col = f"Payment_Terms_Doubt_{target_col}"

    def _get_field(val, field):
        r = det_mapping.get(str(val))
        return r.get(field, '') if isinstance(r, dict) else ''

    s = df[target_col].astype(str)
    norm_values = s.apply(lambda v: _get_field(v, 'days'))
    discount_values = s.apply(lambda v: _get_field(v, 'discount'))
    doubt_values = s.apply(lambda v: _get_field(v, 'doubt'))
    _upsert_adjacent_columns(df, target_col, [
        (norm_col, norm_values),
        (discount_col, discount_values),
        (doubt_col, doubt_values),
    ])

    discount_count = (df[discount_col] != '').sum()
    doubt_count = (df[doubt_col] == 'Yes').sum()

    msg = (
        f"[OK] Created '{norm_col}', '{discount_col}', '{doubt_col}' from '{target_col}' "
        f"({len(det_mapping)} terms, {discount_count} discounts, {doubt_count} doubts)"
    )
    return df, msg, cost.summary()


def normalize_region_agent(df, api_key=None, **kwargs):
    """Normalize regions: country-to-region lookup + AI for unknowns."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

    target_col = _find_column(df, ['region'])
    if not target_col:
        return df, "[WARN] No Region column found.", cost.summary()

    all_unique = df[target_col].dropna().astype(str).unique().tolist()
    print(f"  Region: {len(all_unique)} unique values from {len(df)} rows")

    country_col = _find_column(df, ['country', 'supplier_country', 'supplier country', 'vendor country'])

    det_mapping = {}
    unresolved = []
    for v in all_unique:
        r = _lookup_region(v)
        if r:
            det_mapping[v] = r
        elif v.upper() in ('NA', 'EMEA', 'APAC', 'LATAM'):
            det_mapping[v] = v.upper()
        else:
            unresolved.append(v)

    if unresolved and country_col:
        country_to_val = {}
        for idx, row in df.iterrows():
            rv = str(row[target_col]).strip()
            cv = str(row[country_col]).strip()
            if rv in unresolved and cv:
                country_to_val.setdefault(rv, set()).add(cv)
        for rv, countries in country_to_val.items():
            for c in countries:
                region = _lookup_region(c)
                if not region:
                    std = _lookup_country(c)
                    if std:
                        region = _lookup_region(std)
                if region:
                    det_mapping[rv] = region
                    break
        unresolved = [v for v in unresolved if v not in det_mapping]

    print(f"  Deterministic: {len(det_mapping)} resolved, {len(unresolved)} -> AI")

    if unresolved:
        sys_msg = custom_sys or "JSON only"
        prompt_tmpl = custom_inst if custom_inst else (
            "Map to: 'NA', 'EMEA', 'APAC', 'LATAM'.\n\n"
            "Input: {batch}\nReturn JSON: {{ \"Original\": \"Standardized\" }}"
        )
        ai_mapping, cost = _batch_ai_mapping(
            unresolved, sys_msg, prompt_tmpl, api_key,
            batch_size=80, max_workers=4, cost_tracker=cost,
        )
        det_mapping.update(ai_mapping)

    new_col = f"Norm_Region_{target_col}"
    normalized = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])
    _upsert_adjacent_column(df, target_col, new_col, normalized)

    return df, f"[OK] Normalized {len(det_mapping)} regions -> **{new_col}**.", cost.summary()


def normalize_plant_agent(df, api_key=None, **kwargs):
    """Normalize plant/site names: deterministic cleanup + concurrent AI."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

    target_col = _find_column(df, ['plant', 'site', 'location', 'facility'])
    if not target_col:
        return df, "[WARN] No Plant column found.", cost.summary()

    all_unique = df[target_col].dropna().astype(str).unique().tolist()
    print(f"  Plant: {len(all_unique)} unique values from {len(df)} rows")

    det_mapping = {}
    unresolved = []
    for v in all_unique:
        cleaned = re.sub(r'[-_]\d{3,}$', '', str(v).strip())
        cleaned = re.sub(r'\s*\(.*?\)\s*$', '', cleaned)
        cleaned = _MULTI_SPACE.sub(' ', cleaned).strip()
        if cleaned and cleaned != v:
            det_mapping[v] = cleaned
        else:
            unresolved.append(v)

    print(f"  Deterministic: {len(det_mapping)} resolved, {len(unresolved)} -> AI")

    if unresolved:
        sys_msg = custom_sys or "JSON only"
        prompt_tmpl = custom_inst if custom_inst else (
            "Clean Plant/Site/Location names:\n"
            "- Remove trailing numeric codes, noise, parenthetical codes.\n"
            "- Normalize whitespace and capitalization.\n"
            "- Keep location meaning.\n\n"
            "Input: {batch}\nReturn JSON: {{ \"Original\": \"Cleaned\" }}"
        )
        ai_mapping, cost = _batch_ai_mapping(
            unresolved, sys_msg, prompt_tmpl, api_key,
            batch_size=80, max_workers=4, cost_tracker=cost,
        )
        det_mapping.update(ai_mapping)

    new_col = f"Norm_Plant_{target_col}"
    normalized = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])
    _upsert_adjacent_column(df, target_col, new_col, normalized)

    return df, f"[OK] Normalized {len(det_mapping)} plant names -> **{new_col}**.", cost.summary()


# ═══════════════════════════════════════════════════════════════════════════════
#  INTERNAL HELPERS (kept for backward compat)
# ═══════════════════════════════════════════════════════════════════════════════

def _diverse_date_samples(series, max_samples=30):
    """Collect a format-diverse sample of date values."""
    vals = series.dropna().astype(str)
    if vals.empty:
        return []
    uniques = vals.unique()

    def _pattern(v):
        return re.sub(r'[A-Za-z]+', 'A', re.sub(r'\d+', 'D', v))

    buckets = {}
    for v in uniques:
        p = _pattern(v)
        if p not in buckets:
            buckets[p] = []
        if len(buckets[p]) < max_samples:
            buckets[p].append(v)

    per_bucket = max(2, max_samples // max(len(buckets), 1))
    result = []
    for vals_list in buckets.values():
        result.extend(vals_list[:per_bucket])

    return result[:max_samples]

# ═══════════════════════════════════════════════════════════════════════════════
#  SPEND NORMALIZATION (CURRENCY CONVERSION)
# ═══════════════════════════════════════════════════════════════════════════════

def assess_supplier_country(df, **kwargs):
    """
    Assess the dataset for supplier country normalization.
    Returns population stats for the selected country column.
    """
    country_col = kwargs.get('country_col')

    if not country_col or country_col not in df.columns:
        return {
            "population": None,
            "error": f"Column '{country_col}' not found." if country_col else "No column specified.",
        }

    total_rows = len(df)
    pop_mask = df[country_col].notna() & (df[country_col].astype(str).str.strip() != "")
    n_populated = int(pop_mask.sum())
    pct = (n_populated / total_rows * 100) if total_rows > 0 else 0

    return {
        "population": {
            "n_populated": n_populated,
            "n_total": total_rows,
            "pct_populated": round(pct, 1),
            "warn": pct < 60,
        },
    }


from .fx_rates import load_fx_table, run_conversion

def assess_currency_conversion(df, **kwargs):
    """
    Assess the dataset for currency conversion.
    Returns: dict of warnings, candidate columns, default selections, etc.
    """
    currency_col = kwargs.get('currency_col')
    
    date_cols = [
        c for c in df.columns
        if ("date" in str(c).lower() or "dob" in str(c).lower() or "time" in str(c).lower())
        and not str(c).startswith("Norm_Date_")
    ]
    
    try:
        fx_data = load_fx_table()
        FX, LATEST_RATE, SUPPORTED_CURRENCIES, LATEST_PERIOD, FX_YEARLY, FX_YEARLY_MONTHS = fx_data
    except Exception as e:
        return {
            "needs_confirmation": True,
            "warnings": [f"Could not load FX lookup table: {e}"],
            "candidate_dates": date_cols,
            "recommended_date": date_cols[0] if date_cols else None,
            "population": None,
            "unsupported_currencies": [],
        }
        
    warnings_list = []
    supported_set = set(c.upper() for c in SUPPORTED_CURRENCIES) | {"USD"}
    needs_confirmation = False
    
    if currency_col and currency_col in df.columns:
        total_rows = len(df)
        pop_mask = df[currency_col].notna() & (df[currency_col].astype(str).str.strip() != "")
        n_populated = int(pop_mask.sum())
        pct = (n_populated / total_rows * 100) if total_rows > 0 else 0
        
        if pct < 60:
            warnings_list.append(f"Currency column is only {pct:.1f}% populated (minimum threshold: 60%). Unpopulated rows will produce NaN.")
            needs_confirmation = True
            
        ccy_values = df[currency_col].dropna().astype(str).str.strip().str.upper()
        ccy_values = ccy_values[ccy_values != ""]
        unsupported_counts = ccy_values[~ccy_values.isin(supported_set)].value_counts()
        
        if not unsupported_counts.empty:
            total_affected = int(unsupported_counts.sum())
            details = ", ".join([f"{ccy}: {cnt}" for ccy, cnt in unsupported_counts.items()])
            warnings_list.append(f"Unsupported currency codes found affecting {total_affected} rows: {details}. These rows will produce NaN.")
            needs_confirmation = True
    elif currency_col:
        warnings_list.append(f"Currency column '{currency_col}' not found.")
        needs_confirmation = True

    return {
        "needs_confirmation": needs_confirmation,
        "warnings": warnings_list,
        "candidate_dates": date_cols,
        "recommended_date": date_cols[0] if date_cols else None,
        "population": {
            "n_populated": n_populated,
            "n_total": total_rows,
            "pct_populated": round(pct, 1),
            "warn": pct < 60,
        } if (currency_col and currency_col in df.columns) else None,
        "unsupported_currencies": [
            {"code": str(ccy), "row_count": int(cnt)}
            for ccy, cnt in unsupported_counts.items()
        ] if (currency_col and currency_col in df.columns and not unsupported_counts.empty) else [],
    }

def _coerce_spend_columns(raw_cols):
    if raw_cols is None: return []
    if isinstance(raw_cols, str): raw_cols = [raw_cols]
    if not isinstance(raw_cols, (list, tuple, set)): return []
    return [str(c).strip() for c in raw_cols if str(c).strip()]

def normalize_spend_agent(df, api_key=None, **kwargs):
    cost = CostTracker()
    currency_col = kwargs.get('currency_col')
    spend_cols = _coerce_spend_columns(kwargs.get('spend_cols'))
    date_col = kwargs.get('date_col')
    scope_year = kwargs.get('scope_year')
    target_currency = kwargs.get('target_currency', 'USD').upper().strip()
    fx_overrides = kwargs.get('fx_overrides') or {}

    conversion_mode = "monthly"
    if date_col == "No date col" or not date_col or date_col not in df.columns:
        if scope_year and str(scope_year).strip():
            conversion_mode = "scope_year"
        else:
            conversion_mode = "latest_fallback"
            
    if not currency_col or currency_col not in df.columns:
        return df, "[WARN] Valid currency column is required.", cost.summary()
        
    if not spend_cols:
        return df, "[WARN] No spend columns specified.", cost.summary()
        
    try:
        fx_data = load_fx_table()
    except Exception as e:
        return df, f"[WARN] Could not load FX lookup table: {e}", cost.summary()
        
    results = []
    all_metrics = []

    for spend_col in spend_cols:
        if spend_col not in df.columns: continue

        df, metrics, fx_col, out_col, status_col = run_conversion(
            df, spend_col, currency_col,
            conversion_mode=conversion_mode,
            date_col=date_col if conversion_mode == "monthly" else None,
            scope_year=int(scope_year) if scope_year and str(scope_year).isdigit() else None,
            fx_data=fx_data,
            fx_overrides=fx_overrides,
        )

        if conversion_mode == "monthly":
            mode_desc = f"Monthly date-based (column: '{date_col}')"
        elif conversion_mode == "scope_year":
            mode_desc = f"Scope year ({scope_year})"
        else:
            mode_desc = "Latest fallback"

        n_unconverted = (
            metrics['n_currency_missing'] +
            metrics['n_unsupported'] +
            metrics['n_spend_invalid'] +
            metrics['n_date_unparseable']
        )
        msg = (
            f"Conversion complete\n"
            f"• FX rate column: '{fx_col}'\n"
            f"• Output column: '{out_col}'\n"
            f"• Status column: '{status_col}'\n"
            f"• FX mode: {mode_desc}\n"
            f"• Rows converted: {metrics['n_converted']}\n"
            f"• Rows via fallback: {metrics['n_fallback']}\n"
            f"• Rows not converted: {n_unconverted}"
        )
        results.append(msg)
        all_metrics.append({"spend_col": spend_col, "status_col": status_col, **metrics})

    final_msg = "[OK] \n" + "\n\n".join(results)
    return df, final_msg, cost.summary(), all_metrics
