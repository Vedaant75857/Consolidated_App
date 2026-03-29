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
import pandas as pd
from .helpers import (
    get_client, get_model, CostTracker,
    _batch_ai_mapping, _find_column,
)
from .fx_rates import (
    detect_currency_columns, detect_spend_columns, fetch_grouped_fx_rates,
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
    print(f"  Deterministic: {len(all_unique) - len(unchanged)} resolved, {len(unchanged)} → AI")

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
    df[new_col] = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])

    return df, f"[OK] Normalized {len(det_mapping)} supplier names → **{new_col}**.", cost.summary()


def normalize_supplier_country_agent(df, api_key=None, **kwargs):
    """Normalize supplier country: ISO 3166 lookup + AI for unknowns."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')

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

    print(f"  ISO lookup: {len(det_mapping)} resolved, {len(unresolved)} → AI")

    if unresolved:
        sys_msg = custom_sys or "Output JSON only."
        prompt_tmpl = custom_inst if custom_inst else (
            "Standardize these country names to their **Full English Name** (Title Case).\n"
            "- Expand abbreviations: 'US' → 'United States', 'DE' → 'Germany'.\n"
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
    df[new_col] = df[target_col].astype(str).map(full_mapping).fillna(df[target_col])

    return df, f"[OK] Normalized {len(full_mapping)} countries → **{new_col}**.", cost.summary()


def add_record_id_agent(df):
    """Add sequential Record ID column."""
    if any(str(c).lower() == "record id" for c in df.columns):
        return df, "Record ID already exists."
    df.insert(0, "Record ID", range(1, len(df) + 1))
    return df, "[OK] Added 'Record ID' column."


def date_normalization_agent(df, api_key=None, user_format=None, **kwargs):
    """Normalize date columns via column profiling (DMY/MDY inference) + stacked mask extraction."""
    cost = CostTracker()
    custom_sys = kwargs.get('custom_system', '')
    custom_inst = kwargs.get('custom_instructions', '')
    target_fmt = user_format or "%d-%m-%Y"
    log = []

    date_cols = [
        c for c in df.columns
        if ("date" in str(c).lower() or "dob" in str(c).lower() or "time" in str(c).lower())
        and not str(c).startswith("Norm_Date_")
    ]
    if not date_cols:
        return df, "No new date columns found.", cost.summary()

    # Predefine the specific masks to sequentially cascade.
    DMY_MASKS = ['%d-%m-%Y', '%d-%b-%Y', '%d-%B-%Y', '%d-%m-%y', '%d-%b-%y', '%d-%B-%y', '%Y-%m-%d', '%y-%m-%d']
    MDY_MASKS = ['%m-%d-%Y', '%b-%d-%Y', '%B-%d-%Y', '%m-%d-%y', '%b-%d-%y', '%B-%d-%y', '%Y-%m-%d', '%y-%m-%d']

    for col in date_cols:
        try:
            series = df[col].dropna().astype(str)
            series = series[~series.isin(['', 'nan', 'None', 'NaN'])]
            if series.empty:
                continue

            total_count = series.shape[0]

            # 1. Pre-Processing: Unify separators and whitespace to hyphens.
            s_clean = series.str.replace(r'[/\.\s]+', '-', regex=True).str.strip()

            # 2. Extract parts to Profile Column
            parts = s_clean.str.split('-', expand=True)

            # Ensure we have at least 2 parts to profile
            if parts.shape[1] >= 2:
                # Count Numeric > 12
                valid_p0 = pd.to_numeric(parts[0], errors='coerce')
                count_p0_gt_12 = (valid_p0 > 12).sum()

                valid_p1 = pd.to_numeric(parts[1], errors='coerce')
                count_p1_gt_12 = (valid_p1 > 12).sum()

                # Count Month Names
                months_pattern = r'(?i)^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)'
                count_p0_month = parts[0].str.contains(months_pattern, na=False).sum()
                count_p1_month = parts[1].str.contains(months_pattern, na=False).sum()

                # Score
                score_dmy = count_p0_gt_12 + count_p1_month
                score_mdy = count_p1_gt_12 + count_p0_month

                master_pattern = 'MDY' if score_mdy > score_dmy else 'DMY'
            else:
                master_pattern = 'DMY'

            # 3. Stack Format Masks
            active_masks = DMY_MASKS if master_pattern == 'DMY' else MDY_MASKS
            parsed = pd.Series(pd.NaT, index=df.index)
            
            # Apply all primary masks concurrently
            for fmt in active_masks:
                parsed = parsed.combine_first(pd.to_datetime(s_clean, format=fmt, errors='coerce'))

            # Fallback 1: let Pandas automatically try its best on whatever is still failing
            if parsed.isna().sum() > 0:
                is_dayfirst = (master_pattern == 'DMY')
                fallback_parsed = pd.to_datetime(s_clean, dayfirst=is_dayfirst, format='mixed', errors='coerce')
                parsed = parsed.combine_first(fallback_parsed)

            # 4. AI Backing (Bottom 20% failure case)
            still_failed = parsed.isna().sum()
            if still_failed > total_count * 0.2:
                samples = _diverse_date_samples(df[col], max_samples=30)
                if samples:
                    client = get_client(api_key)
                    prompt = (
                        f"These date values need parsing: {json.dumps(samples[:20])}\n"
                        f"Return a Python strptime format string that parses MOST of these.\n"
                        f"Return JSON ONLY: {{ \"format\": \"%d/%m/%Y\" }}"
                    )
                    if custom_inst:
                        prompt = custom_inst.replace('{sample_dates}', json.dumps(samples)).replace('{target_format}', target_fmt)

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
                            ai_parsed = pd.to_datetime(s_clean, format=ai_fmt, errors='coerce')
                            parsed = parsed.combine_first(ai_parsed)
                    except Exception as e:
                        cost.record_error(f"Date AI fallback for '{col}': {e}")

            # 5. Output Extraction
            new_col = f"Norm_Date_{col}"
            df[new_col] = parsed.dt.strftime(target_fmt).fillna('')
            success = parsed.notna().sum()
            log.append(f"'{col}' [{master_pattern} anchor] → '{new_col}': {success}/{total_count} parsed")
        except Exception as e:
            log.append(f"Error '{col}': {e}")

    msg = "\n".join(log) if log else "No dates normalized."
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

    print(f"  Regex: {len(det_mapping)} resolved, {len(unresolved)} → AI")

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
    df[norm_col] = s.apply(lambda v: _get_field(v, 'days'))
    df[discount_col] = s.apply(lambda v: _get_field(v, 'discount'))
    df[doubt_col] = s.apply(lambda v: _get_field(v, 'doubt'))

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

    print(f"  Deterministic: {len(det_mapping)} resolved, {len(unresolved)} → AI")

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
    df[new_col] = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])

    return df, f"[OK] Normalized {len(det_mapping)} regions → **{new_col}**.", cost.summary()


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

    print(f"  Deterministic: {len(det_mapping)} resolved, {len(unresolved)} → AI")

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
    df[new_col] = df[target_col].astype(str).map(det_mapping).fillna(df[target_col])

    return df, f"[OK] Normalized {len(det_mapping)} plant names → **{new_col}**.", cost.summary()


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

def normalize_spend_agent(df, api_key=None, **kwargs):
    """Normalize spend amounts using grouped historical FX API rates.
    
    Reads:
        kwargs['currency_col']: column name with currency codes
        kwargs['spend_cols']: list of column names with spend amounts
        kwargs['date_col']: column name with transaction dates
        kwargs['target_currency']: target currency code (e.g. "USD")
    
    For each spend column:
    - Creates new column: SPEND AMOUNT CONVERTED_{target_currency}
    - For each row: if currency == target, copy as-is; else apply FX rate
    - Rates are fetched by grouping rows per source currency and unique dates
    
    Returns:
        (df_with_new_cols, summary_message, cost.summary())
    """
    cost = CostTracker()
    
    # Extract kwargs (all required for this agent)
    currency_col = kwargs.get('currency_col')
    spend_cols = kwargs.get('spend_cols', [])
    date_col = kwargs.get('date_col')
    target_currency = kwargs.get('target_currency', 'USD').upper().strip()
    
    if not currency_col:
        return df, "[WARN] No currency column specified.", cost.summary()
    if not spend_cols:
        return df, "[WARN] No spend columns specified.", cost.summary()
    if not date_col:
        return df, "[WARN] No date column specified.", cost.summary()
    
    # Check if all required columns exist in this DataFrame
    missing_cols = []
    if currency_col not in df.columns:
        missing_cols.append(f"currency ({currency_col})")
    if date_col not in df.columns:
        missing_cols.append(f"date ({date_col})")
    
    for col in spend_cols:
        if col not in df.columns:
            missing_cols.append(f"spend ({col})")
    
    # If columns are missing, skip this sheet gracefully instead of erroring
    if missing_cols:
        return df, f"[SKIP] Missing column(s): {', '.join(missing_cols)}. No conversion applied.", cost.summary()
    
    print(f"  Converting {len(spend_cols)} spend column(s) to {target_currency}...")
    print(f"    Currency col: {currency_col}")
    print(f"    Date col: {date_col}")
    print(f"    Spend cols: {', '.join(spend_cols)}")
    
    results = []
    new_cols = []
    
    def _parse_amount(value):
        """Parse common money formats into float."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value)

        s = str(value).strip()
        if not s:
            return None

        # Handle common accounting formats and thousand separators.
        s = s.replace(',', '').replace('$', '').replace('€', '').replace('£', '')
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]

        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    def _parse_date_iso(value):
        """Parse diverse date formats to YYYY-MM-DD."""
        if pd.isna(value):
            return None
        if hasattr(value, 'strftime'):
            try:
                return value.strftime('%Y-%m-%d')
            except Exception:
                pass

        text = str(value).strip()
        if not text:
            return None

        # First parse with default assumptions, then retry as day-first.
        dt = pd.to_datetime(text, errors='coerce')
        if pd.isna(dt):
            dt = pd.to_datetime(text, errors='coerce', dayfirst=True)
        if pd.isna(dt):
            return None

        return dt.strftime('%Y-%m-%d')

    for spend_col in spend_cols:
        new_col = f"SPEND AMOUNT CONVERTED_{target_currency}"
        # Avoid duplicate column names
        col_counter = 1
        while new_col in df.columns or new_col in new_cols:
            new_col = f"SPEND AMOUNT CONVERTED_{target_currency}_{col_counter}"
            col_counter += 1
        
        new_cols.append(new_col)
        
        # Initialize new column with nulls
        converted = [None] * len(df)
        conversion_count = 0
        no_rate_count = 0
        
        parsed_rows = []
        currency_to_dates = {}

        # First pass: parse rows and build grouped request map.
        for idx, row in df.iterrows():
            try:
                spend_val = row[spend_col]
                currency_val = str(row[currency_col]).upper().strip() if pd.notna(row[currency_col]) else None
                date_val = row[date_col]
                
                # Skip if missing critical values
                if pd.isna(spend_val) or not currency_val:
                    continue
                
                # Convert spend to numeric
                spend_numeric = _parse_amount(spend_val)
                if spend_numeric is None:
                    continue
                
                # Format date
                date_str = _parse_date_iso(date_val)
                if not date_str:
                    continue

                parsed_rows.append((idx, spend_numeric, currency_val, date_str))
                if currency_val != target_currency:
                    if currency_val not in currency_to_dates:
                        currency_to_dates[currency_val] = set()
                    currency_to_dates[currency_val].add(date_str)
                
            except Exception as e:
                print(f"    [WARN] Row {idx}: {e}")
                continue

        # One grouped API call pattern: currency -> unique dates.
        rate_map = fetch_grouped_fx_rates(currency_to_dates, target_currency)

        # Second pass: apply conversion.
        for idx, spend_numeric, currency_val, date_str in parsed_rows:
            if currency_val == target_currency:
                converted[idx] = spend_numeric
                conversion_count += 1
                continue

            rate = rate_map.get((currency_val, date_str))
            if rate is None:
                no_rate_count += 1
                continue

            converted[idx] = spend_numeric * rate
            conversion_count += 1
        
        df[new_col] = converted
        results.append(
            f"'{spend_col}' → '{new_col}': {conversion_count} converted, "
            f"{no_rate_count} no rate found"
        )
    
    msg = "[OK] " + "; ".join(results)
    return df, msg, cost.summary()
