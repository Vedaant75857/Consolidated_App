"""8-tier header matching engine for procurement field normalization.

Pipeline (fastest to slowest, stops as soon as a match is found):
  T1  Exact + alias lookup
  T2  CamelCase / snake_case split -> re-try T1
  T3  Abbreviation expansion -> re-try T1
  T4  SAP / ERP field code lookup
  T5  Multilingual dictionary
  T6  Sample-value pattern matching
  T7  Smart fuzzy scoring
  T8  Pending AI fallback (returned as AI_NEEDED)
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from difflib import SequenceMatcher

_this_dir = os.path.dirname(os.path.abspath(__file__))


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_schema_mod = _load_mod("me_schema", os.path.join(_this_dir, "schema_mapper.py"))
_aliases_mod = _load_mod("me_aliases", os.path.join(_this_dir, "aliases.py"))

STD_FIELD_NAMES = _schema_mod.STD_FIELD_NAMES
ALIAS_LOOKUP = _aliases_mod.ALIAS_LOOKUP
ERP_CODES = _aliases_mod.ERP_CODES
MULTILINGUAL = _aliases_mod.MULTILINGUAL
_norm = _aliases_mod._norm
expand_abbrevs = _aliases_mod.expand_abbrevs
split_camel = _aliases_mod.split_camel
_RAW_ALIASES = _aliases_mod._RAW_ALIASES

# ---------------------------------------------------------------------------
# Fuzzy backend: prefer rapidfuzz if available, fall back to stdlib difflib
# ---------------------------------------------------------------------------
try:
    from rapidfuzz import fuzz as _rfuzz

    def _sim(a: str, b: str) -> float:
        return _rfuzz.ratio(a, b) / 100.0

    def _partial(a: str, b: str) -> float:
        return _rfuzz.partial_ratio(a, b) / 100.0

    def _token_sort(a: str, b: str) -> float:
        return _rfuzz.token_sort_ratio(a, b) / 100.0

except ImportError:
    def _sim(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def _partial(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        short, long = (a, b) if len(a) <= len(b) else (b, a)
        return max(
            SequenceMatcher(None, short, long[i:i + len(short)]).ratio()
            for i in range(len(long) - len(short) + 1)
        )

    def _token_sort(a: str, b: str) -> float:
        ta = " ".join(sorted(a.split()))
        tb = " ".join(sorted(b.split()))
        return SequenceMatcher(None, ta, tb).ratio()


# ---------------------------------------------------------------------------
# Configurable thresholds
# ---------------------------------------------------------------------------

FUZZY_THRESHOLD = 62
AI_CONFIDENCE_THRESHOLD = 0.65
UNMAPPED_PLACEHOLDER = "UNMAPPED"

# ---------------------------------------------------------------------------
# Junk / flex-field patterns
# ---------------------------------------------------------------------------

_JUNK = re.compile(
    r"^(flex|extra|custom|spare|user|reserved|dummy)"
    r"[_\s]?(field|measure|date|string|id|fieldid|num|number|value|code)?[_\s]?\d*$",
    re.I,
)
_BLANK = re.compile(r"^$|^none$|^null$|^n\/a$|^na$|^\-+$|^\.+$", re.I)

# ---------------------------------------------------------------------------
# Pre-computed token sets for fuzzy scoring
# ---------------------------------------------------------------------------

_STD_TOKENS: dict[str, set[str]] = {
    f: set(re.findall(r"[a-z0-9]+", _norm(f))) for f in STD_FIELD_NAMES
}

_ALIAS_NORMS: list[tuple[str, str]] = [
    (an, field)
    for field, aliases in _RAW_ALIASES.items()
    for a in aliases
    for an in [_norm(a)]
    if an
]

_ALIAS_NORMS_BY_FIELD: dict[str, list[str]] = {}
for _an, _fld in _ALIAS_NORMS:
    _ALIAS_NORMS_BY_FIELD.setdefault(_fld, []).append(_an)

# ---------------------------------------------------------------------------
# Sample-value pattern detectors
# ---------------------------------------------------------------------------

_ISO_CURR = re.compile(r"^[A-Z]{3}$")
_DATE_LIKE = re.compile(r"^\d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{1,4}$")
_LARGE_NUM = re.compile(r"^[\-+]?\d[\d,]*\.?\d*$")
_PO_PREFIX = re.compile(r"^(PO|P\.O\.|4[5-9]\d{7}|45\d{6})", re.I)
_NET_TERMS = re.compile(r"^(net\s*\d+|2/10|n/30|immediate|due on receipt)", re.I)
_CC_PREFIX = re.compile(r"^(CC|COST|CTR|KS)\d+", re.I)


def _sample_value_hint(samples: list) -> str | None:
    """Infer the standard field from sample values alone."""
    if not samples:
        return None
    clean = [str(s).strip() for s in samples if s is not None and str(s).strip()]
    if not clean:
        return None
    n = len(clean)

    if sum(1 for s in clean if _ISO_CURR.match(s)) / n >= 0.5:
        return "Local Currency Code"
    if sum(1 for s in clean if _DATE_LIKE.match(s)) / n >= 0.5:
        return None  # can't disambiguate date fields from values alone
    if sum(1 for s in clean if _NET_TERMS.match(s)) / n >= 0.5:
        return "Payment Terms"
    if sum(1 for s in clean if _PO_PREFIX.match(s)) / n >= 0.4:
        return "Invoice PO Number"
    if sum(1 for s in clean if _CC_PREFIX.match(s)) / n >= 0.4:
        return "Cost Center Code"
    return None


# ---------------------------------------------------------------------------
# Smart fuzzy scorer
# ---------------------------------------------------------------------------

def _fuzzy_score(src_norm: str, field: str) -> float:
    """Score src_norm against a standard field using multiple signals (0-1)."""
    fn = _norm(field)
    src_tok = set(re.findall(r"[a-z0-9]+", src_norm))
    fn_tok = _STD_TOKENS[field]

    full_sim = _sim(src_norm, fn)
    tok_sort = _token_sort(src_norm, fn)
    partial = _partial(src_norm, fn)

    if src_tok and fn_tok:
        jacc = len(src_tok & fn_tok) / len(src_tok | fn_tok)
    else:
        jacc = 0.0

    alias_best = 0.0
    for a_norm in _ALIAS_NORMS_BY_FIELD.get(field, ()):
        s = _partial(src_norm, a_norm)
        if s > alias_best:
            alias_best = s

    score = max(
        full_sim * 0.7,
        tok_sort * 0.75,
        partial * 0.8,
        jacc * 0.7,
        alias_best * 0.85,
    )

    if len(src_norm) < 4 and len(fn) > 8:
        score *= 0.7

    return score


# ---------------------------------------------------------------------------
# Master mapping function -- 8 tiers
# ---------------------------------------------------------------------------

def map_single_header(
    raw_header: str | None,
    sample_values: list | None = None,
) -> dict:
    """Map one column header through tiers T1-T7, returning T8_PENDING if
    no deterministic match is found (for downstream AI resolution).

    Returns dict with keys: raw, tier, mapped_to, confidence, action
    """
    raw = str(raw_header) if raw_header is not None else ""
    stripped = raw.strip()

    # -- JUNK / BLANK --
    if not stripped or _BLANK.match(stripped) or _JUNK.match(stripped):
        return {"raw": raw, "tier": "JUNK", "mapped_to": None, "confidence": 0.0, "action": "DROP"}

    norm = _norm(stripped)
    if not norm:
        return {"raw": raw, "tier": "JUNK", "mapped_to": None, "confidence": 0.0, "action": "DROP"}

    def _hit(field: str, tier: str, conf: float = 1.0) -> dict:
        action = "AUTO" if conf >= 0.84 else "REVIEW"
        return {"raw": raw, "tier": tier, "mapped_to": field, "confidence": round(conf, 3), "action": action}

    # -- T1: Alias dictionary lookup --
    if norm in ALIAS_LOOKUP:
        proposed = ALIAS_LOOKUP[norm]
        is_exact_field_name = (_norm(proposed) == norm)
        if is_exact_field_name:
            return _hit(proposed, "T1_EXACT", 1.0)
        return _hit(proposed, "T1_ALIAS", 1.0)

    # -- T2: CamelCase + snake_case split -> re-try T1 --
    split = _norm(split_camel(stripped))
    if split and split != norm:
        if split in ALIAS_LOOKUP:
            return _hit(ALIAS_LOOKUP[split], "T2", 0.97)
        no_prefix = re.sub(r"^[zyx][_\s]", "", split).strip()
        if no_prefix and no_prefix in ALIAS_LOOKUP:
            return _hit(ALIAS_LOOKUP[no_prefix], "T2", 0.94)

    # -- T3: Abbreviation expansion -> re-try T1 --
    expanded = expand_abbrevs(norm)
    if expanded != norm and expanded in ALIAS_LOOKUP:
        return _hit(ALIAS_LOOKUP[expanded], "T3_ABBREV", 0.93)
    expanded_split = expand_abbrevs(split) if split else ""
    if expanded_split and expanded_split != split and expanded_split in ALIAS_LOOKUP:
        return _hit(ALIAS_LOOKUP[expanded_split], "T3_ABBREV", 0.92)

    # -- T4: SAP / ERP field code lookup --
    lower_stripped = stripped.lower().strip()
    if lower_stripped in ERP_CODES:
        return _hit(ERP_CODES[lower_stripped], "T4_ERP", 0.95)
    if norm in ERP_CODES:
        return _hit(ERP_CODES[norm], "T4_ERP", 0.95)

    # -- T5: Multilingual dictionary --
    for check in [norm, split, expanded, expanded_split]:
        if check and check in MULTILINGUAL:
            return _hit(MULTILINGUAL[check], "T5_LANG", 0.93)

    # -- T6: Sample-value pattern matching --
    if sample_values:
        hint = _sample_value_hint(sample_values)
        if hint:
            return _hit(hint, "T6_SAMPLE", 0.82)

    # -- T7: Smart fuzzy scoring (dict-based for O(1) updates) --
    score_map = {f: _fuzzy_score(norm, f) for f in STD_FIELD_NAMES}
    for alt in {split, expanded, expanded_split} - {"", norm}:
        if alt:
            for f in STD_FIELD_NAMES:
                s = _fuzzy_score(alt, f)
                if s > score_map[f]:
                    score_map[f] = s

    scores = sorted(score_map.items(), key=lambda x: x[1], reverse=True)
    best_field, best_score = scores[0]
    top_scores = [(f, round(s, 4)) for f, s in scores[:10]]

    threshold = FUZZY_THRESHOLD / 100.0
    if best_score >= threshold:
        result = _hit(best_field, "T7_FUZZY", best_score)
        result["top_scores"] = top_scores
        return result

    # -- T8: Pending AI (caller must resolve) --
    return {"raw": raw, "tier": "T8_PENDING", "mapped_to": None, "confidence": 0.0, "action": "AI_NEEDED", "top_scores": top_scores}
