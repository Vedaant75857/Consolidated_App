"""Persistent alias store -- learns from user corrections and persists to disk.

The store is a JSON file mapping canonical field names to lists of raw aliases
that were learned from user corrections (editing row 1 in output Excel files).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import threading
from datetime import datetime
from typing import Any

_this_dir = os.path.dirname(os.path.abspath(__file__))


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_schema_mod = _load_mod("as_schema", os.path.join(_this_dir, "schema_mapper.py"))
_aliases_mod = _load_mod("as_aliases", os.path.join(_this_dir, "aliases.py"))

STD_FIELD_NAMES = _schema_mod.STD_FIELD_NAMES
_norm = _aliases_mod._norm
ALIAS_LOOKUP = _aliases_mod.ALIAS_LOOKUP
_RAW_ALIASES = _aliases_mod._RAW_ALIASES

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ALIAS_CAP = 75

def _resolve_knowledge_base_dir() -> str:
    """Return a writable knowledge_base directory, aware of PyInstaller frozen mode."""
    if getattr(sys, "frozen", False):
        import shutil
        app_data = os.path.dirname(sys.executable)
        writable_kb = os.path.join(app_data, "knowledge_base")
        bundled_kb = os.path.join(  # type: ignore[attr-defined]
            sys._MEIPASS,
            "DataConsolidationAppV7", "backend", "module-1",
            "header-normalisation", "knowledge_base",
        )
        if not os.path.exists(writable_kb) and os.path.exists(bundled_kb):
            shutil.copytree(bundled_kb, writable_kb)
        return writable_kb
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "knowledge_base")


ALIASES_STORE_DIR = _resolve_knowledge_base_dir()
LEARNED_ALIASES_FILE = os.path.join(ALIASES_STORE_DIR, "learned_aliases.json")
SNAPSHOT_DIR = os.path.join(ALIASES_STORE_DIR, "snapshots")

os.makedirs(ALIASES_STORE_DIR, exist_ok=True)
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

_ALIAS_STORE_LOCK = threading.Lock()

_STD_FIELD_SET = set(STD_FIELD_NAMES)


# ---------------------------------------------------------------------------
# Load / save helpers
# ---------------------------------------------------------------------------

def _load_learned_aliases() -> dict[str, list[str]]:
    """Load user-corrected aliases from disk."""
    if os.path.exists(LEARNED_ALIASES_FILE):
        try:
            with open(LEARNED_ALIASES_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_learned_aliases(learned: dict[str, list[str]]) -> None:
    """Persist learned aliases atomically."""
    tmp = LEARNED_ALIASES_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(learned, f, indent=2, ensure_ascii=False)
    os.replace(tmp, LEARNED_ALIASES_FILE)


_LEARNED_ALIASES: dict[str, list[str]] = _load_learned_aliases()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def alias_add(canonical_field: str, raw_alias: str) -> bool:
    """Add *raw_alias* to the alias list of *canonical_field*.

    Thread-safe. Handles deduplication and the 75-alias cap.
    Returns True if a new alias was actually added.
    """
    if not raw_alias or not canonical_field:
        return False
    if canonical_field not in _STD_FIELD_SET:
        return False

    norm_alias = _norm(raw_alias)
    if not norm_alias:
        return False

    with _ALIAS_STORE_LOCK:
        existing_field = ALIAS_LOOKUP.get(norm_alias)
        if existing_field == canonical_field:
            return False

        if canonical_field not in _RAW_ALIASES:
            _RAW_ALIASES[canonical_field] = []

        current = _RAW_ALIASES[canonical_field]
        normed_current = {_norm(a) for a in current}
        if norm_alias in normed_current:
            ALIAS_LOOKUP[norm_alias] = canonical_field
            return False

        if len(current) >= ALIAS_CAP:
            removed = current.pop()
            removed_norm = _norm(removed)
            if ALIAS_LOOKUP.get(removed_norm) == canonical_field:
                del ALIAS_LOOKUP[removed_norm]

        current.append(raw_alias)
        ALIAS_LOOKUP[norm_alias] = canonical_field

        if canonical_field not in _LEARNED_ALIASES:
            _LEARNED_ALIASES[canonical_field] = []

        learned_normed = {_norm(a) for a in _LEARNED_ALIASES[canonical_field]}
        if norm_alias not in learned_normed:
            learned_list = _LEARNED_ALIASES[canonical_field]
            if len(learned_list) >= ALIAS_CAP:
                learned_list.pop()
            learned_list.append(raw_alias)

        _save_learned_aliases(_LEARNED_ALIASES)
        return True


def get_alias_store_stats() -> dict[str, Any]:
    """Return summary statistics of the learned alias store."""
    with _ALIAS_STORE_LOCK:
        total = sum(len(v) for v in _LEARNED_ALIASES.values())
        return {
            "fields_enriched": len(_LEARNED_ALIASES),
            "total_learned": total,
            "cap_per_field": ALIAS_CAP,
            "fields": {
                field: len(aliases)
                for field, aliases in sorted(_LEARNED_ALIASES.items())
            },
        }


def merge_into_lookup(target: dict) -> int:
    """Inject all learned aliases into a caller-provided ALIAS_LOOKUP dict.

    This bridges the module-boundary problem: each file that loads aliases.py
    via _load_mod gets its own ALIAS_LOOKUP instance.  Calling this with the
    engine's live dict makes learned aliases available to T1 lookups.
    """
    merged = 0
    with _ALIAS_STORE_LOCK:
        for canonical_field, learned_list in _LEARNED_ALIASES.items():
            if canonical_field not in _STD_FIELD_SET:
                continue
            for raw_a in learned_list:
                nk = _norm(raw_a)
                if nk and nk not in target:
                    target[nk] = canonical_field
                    merged += 1
    return merged


def merge_learned_aliases() -> int:
    """Merge all learned aliases into the live alias structures. Returns count."""
    merged = 0
    for canonical_field, learned_list in _LEARNED_ALIASES.items():
        if canonical_field not in _STD_FIELD_SET:
            continue
        if canonical_field not in _RAW_ALIASES:
            _RAW_ALIASES[canonical_field] = []
        current_normed = {_norm(a) for a in _RAW_ALIASES[canonical_field]}
        for raw_a in learned_list:
            nk = _norm(raw_a)
            if not nk:
                continue
            if nk not in current_normed:
                if len(_RAW_ALIASES[canonical_field]) < ALIAS_CAP:
                    _RAW_ALIASES[canonical_field].append(raw_a)
                    current_normed.add(nk)
            ALIAS_LOOKUP[nk] = canonical_field
            merged += 1
    return merged


# Run merge on import so learned aliases are immediately available
merge_learned_aliases()


# ---------------------------------------------------------------------------
# Snapshot helpers (used by watcher to detect corrections)
# ---------------------------------------------------------------------------

def _snapshot_path(output_filename: str) -> str:
    base = os.path.splitext(os.path.basename(output_filename))[0]
    return os.path.join(SNAPSHOT_DIR, base + "_snapshot.json")


def save_snapshot(output_path: str, original_headers: list, mapped_headers: list) -> None:
    """Record what the engine mapped so a watcher can diff corrections later."""
    snap: dict[str, str] = {}
    for orig, mapped in zip(original_headers, mapped_headers):
        if orig is not None:
            snap[_norm(str(orig))] = mapped
    data = {
        "output_file": output_path,
        "written_at": datetime.now().isoformat(),
        "mappings": snap,
        "originals": {_norm(str(o)): str(o) for o in original_headers if o is not None},
    }
    with open(_snapshot_path(output_path), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_snapshot(output_path: str) -> dict | None:
    p = _snapshot_path(output_path)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)
