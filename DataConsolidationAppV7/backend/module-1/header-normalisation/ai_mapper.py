"""AI-powered header mapping and validation functions.

Path A: Batch-map unmapped headers via LLM (rich prompt with profiles + hints).
Path B: Re-validate already-mapped headers via LLM.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

_this_dir = os.path.dirname(os.path.abspath(__file__))


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_schema_mod = _load_mod("aim_schema", os.path.join(_this_dir, "schema_mapper.py"))
_engine_mod = _load_mod("aim_engine", os.path.join(_this_dir, "matching_engine.py"))
_prompts_mod = _load_mod("aim_prompts", os.path.join(_this_dir, "ai", "prompts.py"))

STD_FIELD_NAMES = _schema_mod.STD_FIELD_NAMES
STD_FIELD_DESCRIPTIONS = _schema_mod.STD_FIELD_DESCRIPTIONS
AI_CONFIDENCE_THRESHOLD = _engine_mod.AI_CONFIDENCE_THRESHOLD

SYSTEM_PROMPT_PROCUREMENT_MAPPING = _prompts_mod.SYSTEM_PROMPT_PROCUREMENT_MAPPING
SYSTEM_PROMPT_HEADER_NORM_COLUMN = _prompts_mod.SYSTEM_PROMPT_HEADER_NORM_COLUMN


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AI_VALIDATION_CONF_HIGH = 0.85
AI_VALIDATION_CONF_LOW = 0.65

PORTKEY_BASE_URL = "https://portkey.bain.dev/v1"
PORTKEY_MODEL = "@personal-openai/gpt-5.4"


_VALID_STD_NAMES = set(STD_FIELD_NAMES)


# ---------------------------------------------------------------------------
# Helper: extract sample values from data rows
# ---------------------------------------------------------------------------

def _get_sample_values(col_idx: int, data_rows: list, n: int = 3) -> list:
    """Pull up to *n* clean, non-null values from column *col_idx*."""
    samples: list[str] = []
    for row in data_rows:
        if col_idx < len(row):
            val = row[col_idx]
            text = str(val).strip() if val is not None else ""
            if text and text.lower() not in ("none", "null", "nan", "n/a", ""):
                samples.append(text)
        if len(samples) >= n:
            break
    return samples


# ---------------------------------------------------------------------------
# AI call helper (uses shared.ai when available, Portkey direct otherwise)
# ---------------------------------------------------------------------------

def _call_ai_json(system_prompt: str, user_content: str, api_key: str | None) -> Any:
    """Call the LLM and return parsed JSON response.

    Tries shared.ai.call_ai_json first (uses the app's configured AI client),
    falls back to direct Portkey call.
    """
    try:
        from shared.ai import call_ai_json as _shared_call
        return _shared_call(system_prompt, user_content, api_key)
    except Exception:
        pass

    from portkey_ai import Portkey
    client = Portkey(api_key=api_key, base_url=PORTKEY_BASE_URL)
    resp = client.chat.completions.create(
        model=PORTKEY_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
    )
    raw_text = (resp.choices[0].message.content or "").strip()
    raw_text = raw_text.replace("```json", "").replace("```", "").strip()
    return json.loads(raw_text)


# ---------------------------------------------------------------------------
# Path A -- AI mapping for UNMAPPED headers
# ---------------------------------------------------------------------------

def ai_map_unmapped(
    unmapped_items: list[dict],
    data_rows: list,
    api_key: str,
    *,
    profiles: list[dict] | None = None,
    det_results_cache: list[list[dict]] | None = None,
    std_field_payload: list[dict] | None = None,
    system_prompt: str | None = None,
    batch_size: int = 10,
) -> list[dict]:
    """Send all UNMAPPED headers to the AI for field mapping.

    Batches are fired concurrently via ThreadPoolExecutor for maximum throughput.
    """
    use_rich = bool(profiles and std_field_payload and system_prompt)

    if not use_rich:
        std_list = "\n".join(
            f"  {j + 1:>2}. {f}  --  {STD_FIELD_DESCRIPTIONS.get(f, '')}"
            for j, f in enumerate(STD_FIELD_NAMES)
        )
        system_prompt = f"""You are a procurement data schema expert.
Map source column headers to the standard 73-field procurement taxonomy below.
Use the field descriptions and the provided sample values to make the most
accurate decision possible.

Standard fields with descriptions:
{std_list}

Return a JSON array -- one object per input header, preserving "idx":
[
  {{"idx": 0, "raw": "original header", "mapped_to": "exact standard field name or null", "confidence": 0.87, "reason": "one concise sentence"}}
]

Rules:
- mapped_to MUST be an exact field name from the list above, or null.
- Never invent or abbreviate field names.
- confidence: 0.0-1.0 float.
- Use sample_values as evidence.
- Return ONLY a valid JSON array. No markdown, no preamble."""

    results = {item["raw"]: item for item in unmapped_items}
    lock = threading.Lock()

    batches = [
        unmapped_items[i:i + batch_size]
        for i in range(0, len(unmapped_items), batch_size)
    ]

    def _process_batch(batch_idx: int, batch: list[dict]) -> None:
        if use_rich:
            payload = []
            for i, item in enumerate(batch):
                col_idx = item["col_idx"]
                entry: dict[str, Any] = {"idx": i}
                entry["column"] = profiles[col_idx] if col_idx < len(profiles) else {"source_name": item["raw"]}
                if det_results_cache and col_idx < len(det_results_cache):
                    entry["deterministic_hints"] = det_results_cache[col_idx]
                payload.append(entry)
            user_content = json.dumps(
                {"standard_fields": std_field_payload, "columns": payload},
                ensure_ascii=False,
            )
        else:
            payload = [
                {"idx": i, "raw": item["raw"], "sample_values": _get_sample_values(item["col_idx"], data_rows)}
                for i, item in enumerate(batch)
            ]
            user_content = f"Map these {len(batch)} unmapped headers:\n{json.dumps(payload, ensure_ascii=False)}"

        try:
            parsed = _call_ai_json(system_prompt, user_content, api_key)
            if isinstance(parsed, dict):
                parsed = next((v for v in parsed.values() if isinstance(v, list)), [])
            if not isinstance(parsed, list):
                parsed = []

            result_by_idx = {int(r.get("idx", -1)): r for r in parsed if isinstance(r, dict)}
            with lock:
                for i, item in enumerate(batch):
                    result = result_by_idx.get(i)
                    if result is None:
                        continue
                    conf = float(result.get("confidence", 0))
                    mapped = result.get("mapped_to") or result.get("suggested_std_field")
                    if mapped and mapped not in _VALID_STD_NAMES:
                        mapped = None
                    if mapped and conf >= AI_CONFIDENCE_THRESHOLD:
                        results[item["raw"]].update({
                            "tier": "T8_AI", "mapped_to": mapped,
                            "confidence": round(conf, 3),
                            "action": "AUTO" if conf >= 0.84 else "REVIEW",
                            "reason": str(result.get("reason", "")),
                        })
                    else:
                        results[item["raw"]].update({
                            "tier": "T8_AI", "mapped_to": None,
                            "confidence": round(conf, 3), "action": "UNMAPPED",
                            "reason": str(result.get("reason", "")),
                        })
        except Exception as exc:
            print(f"[ai_mapper] ai_map_unmapped batch {batch_idx}: {exc}", file=sys.stderr)
            with lock:
                for item in batch:
                    results[item["raw"]].setdefault("ai_error", str(exc))

    if len(batches) <= 1:
        for idx, b in enumerate(batches):
            _process_batch(idx, b)
    else:
        with ThreadPoolExecutor(max_workers=min(6, len(batches))) as pool:
            futures = {pool.submit(_process_batch, idx, b): idx for idx, b in enumerate(batches)}
            for fut in as_completed(futures):
                fut.result()

    return list(results.values())


# ---------------------------------------------------------------------------
# Path B -- AI re-validation for already-MAPPED headers
# ---------------------------------------------------------------------------

def ai_validate_mapped(
    decisions: list[dict],
    data_rows: list,
    api_key: str,
) -> int:
    """Validate every already-mapped header via AI.

    Batches run concurrently. Only downgrades AUTO -> REVIEW; never changes mapped_to.
    Returns number of mappings flagged.
    """
    std_list = "\n".join(
        f"  {j + 1:>2}. {f}  --  {STD_FIELD_DESCRIPTIONS.get(f, '')}"
        for j, f in enumerate(STD_FIELD_NAMES)
    )

    system = f"""You are a procurement data schema validation expert.
You will receive a structured table of column mappings that have already been
auto-mapped by a rule-based engine.  Validate whether each mapping is correct.

Use:
  - The original column header name
  - The proposed standard field it was mapped to
  - Up to 3 sample data values from that column
  - The field descriptions below to judge semantic correctness

Standard fields with descriptions:
{std_list}

Return a JSON array -- one object per row, preserving "idx":
[
  {{
    "idx": 0,
    "original_header": "...",
    "mapped_to": "...",
    "valid": true,
    "confidence": 0.95,
    "reason": "one concise sentence"
  }},
  ...
]

Rules:
- valid=true  -> mapping is correct and unambiguous given the header and samples.
- valid=false -> mapping is wrong or another standard field is clearly more appropriate.
- confidence  -> certainty in your valid/invalid judgment (0.0-1.0).
- Do NOT suggest alternative field names.
- Return ONLY a valid JSON array. No markdown, no preamble."""

    candidates = []
    for col_idx, dec in enumerate(decisions):
        if dec.get("mapped_to") and dec.get("action") in ("AUTO", "REVIEW"):
            samples = _get_sample_values(col_idx, data_rows)
            candidates.append({
                "col_idx": col_idx, "raw": dec["raw"],
                "mapped_to": dec["mapped_to"], "samples": samples,
            })

    if not candidates:
        return 0

    batch_size = 20
    batches = [candidates[i:i + batch_size] for i in range(0, len(candidates), batch_size)]
    flags = [0]
    lock = threading.Lock()

    def _validate_batch(batch: list[dict]) -> None:
        payload = [
            {
                "idx": i, "original_header": item["raw"],
                "mapped_to": item["mapped_to"],
                "sample_value_1": item["samples"][0] if item["samples"] else "",
                "sample_value_2": item["samples"][1] if len(item["samples"]) > 1 else "",
                "sample_value_3": item["samples"][2] if len(item["samples"]) > 2 else "",
            }
            for i, item in enumerate(batch)
        ]
        try:
            parsed = _call_ai_json(
                system,
                f"Validate these {len(batch)} mappings "
                f"(format: Original Header | Mapped Header | "
                f"Sample Value 1 | Sample Value 2 | Sample Value 3):\n"
                f"{json.dumps(payload, ensure_ascii=False)}",
                api_key,
            )
            if isinstance(parsed, dict):
                parsed = next((v for v in parsed.values() if isinstance(v, list)), [])
            if not isinstance(parsed, list):
                parsed = []
            result_by_idx = {int(r.get("idx", -1)): r for r in parsed if isinstance(r, dict)}
            with lock:
                for i, item in enumerate(batch):
                    result = result_by_idx.get(i)
                    if result is None:
                        continue
                    valid = bool(result.get("valid", True))
                    conf = float(result.get("confidence", 1.0))
                    reason = str(result.get("reason", ""))
                    if (not valid) and (conf >= AI_VALIDATION_CONF_LOW):
                        dec = decisions[item["col_idx"]]
                        if dec["action"] == "AUTO":
                            dec["action"] = "REVIEW"
                            flags[0] += 1
                        dec["validation_flag"] = True
                        dec["validation_confidence"] = round(conf, 3)
                        dec["validation_reason"] = reason
        except Exception as exc:
            print(f"[ai_mapper] ai_validate_mapped batch: {exc}", file=sys.stderr)

    if len(batches) <= 1:
        for b in batches:
            _validate_batch(b)
    else:
        with ThreadPoolExecutor(max_workers=min(6, len(batches))) as pool:
            futures = [pool.submit(_validate_batch, b) for b in batches]
            for fut in as_completed(futures):
                fut.result()

    return flags[0]
