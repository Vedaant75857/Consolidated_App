"""
Shared AI client — wraps Portkey / OpenAI with JSON response parsing.

Both Module 1 and Module 2 use this for all LLM calls.
"""

from __future__ import annotations

import copy
import json
import os
import time
import hashlib
import threading
from typing import Any

from portkey_ai import Portkey
from pydantic import BaseModel, ValidationError


_DEFAULT_BASE_URL = "https://portkey.bain.dev/v1"
_DEFAULT_MODEL = "@personal-openai/gpt-5.4"
_CACHE_LOCK = threading.Lock()
_AI_JSON_CACHE: dict[str, tuple[float, Any]] = {}


def _cache_key(model: str, system_prompt: str, user_obj: Any, api_key: str | None) -> str:
    payload = {
        "m": model,
        "s": system_prompt,
        "u": user_obj,
        "k": hashlib.sha256((api_key or "").encode("utf-8")).hexdigest()[:12],
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _cache_get(key: str) -> Any | None:
    now = time.time()
    with _CACHE_LOCK:
        hit = _AI_JSON_CACHE.get(key)
        if not hit:
            return None
        expiry, value = hit
        if expiry < now:
            _AI_JSON_CACHE.pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_put(key: str, value: Any, ttl_sec: int) -> None:
    if ttl_sec <= 0:
        return
    with _CACHE_LOCK:
        # Simple bounded cache to avoid unbounded memory growth.
        if len(_AI_JSON_CACHE) > 512:
            # Remove up to 64 oldest-ish entries.
            for k in list(_AI_JSON_CACHE.keys())[:64]:
                _AI_JSON_CACHE.pop(k, None)
        _AI_JSON_CACHE[key] = (time.time() + ttl_sec, copy.deepcopy(value))


def _parse_json_payload(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        first_obj = raw.find("{")
        first_arr = raw.find("[")
        candidates = [p for p in (first_obj, first_arr) if p >= 0]
        start = min(candidates) if candidates else -1
        if start >= 0:
            decoder = json.JSONDecoder()
            obj, _end = decoder.raw_decode(raw[start:])
            return obj
        raise ValueError("AI returned malformed JSON.")


def get_client(api_key: str | None = None) -> Portkey:
    key = api_key or os.getenv("PORTKEY_API_KEY", "")
    if not key:
        raise ValueError("Missing API Key. Set PORTKEY_API_KEY or pass api_key.")
    base_url = os.getenv("PORTKEY_BASE_URL", _DEFAULT_BASE_URL)
    return Portkey(api_key=key, base_url=base_url)


def get_model() -> str:
    return os.getenv("PORTKEY_MODEL", _DEFAULT_MODEL)


def call_ai_json(
    system_prompt: str,
    user_obj: Any,
    api_key: str | None = None,
    model: str | None = None,
) -> Any:
    """Call AI with a system prompt and a user payload, expecting a JSON response."""
    client = get_client(api_key)
    mdl = model or get_model()
    cache_ttl = max(0, int(os.getenv("AI_JSON_CACHE_TTL_SEC", "300")))
    ckey = _cache_key(mdl, system_prompt, user_obj, api_key)
    cached = _cache_get(ckey) if cache_ttl > 0 else None
    if cached is not None:
        return cached
    attempts = max(1, int(os.getenv("AI_JSON_RETRY_ATTEMPTS", "3")))
    backoff = max(0.0, float(os.getenv("AI_JSON_RETRY_BACKOFF_SEC", "0.35")))

    last_err: Exception | None = None
    for i in range(attempts):
        try:
            resp = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_obj) if not isinstance(user_obj, str) else user_obj},
                ],
                model=mdl,
                response_format={"type": "json_object"},
            )
            raw = resp.choices[0].message.content if resp.choices else None
            if not raw:
                raise ValueError("AI returned an empty response.")
            parsed = _parse_json_payload(raw)
            _cache_put(ckey, parsed, cache_ttl)
            return parsed
        except Exception as exc:
            last_err = exc
            if i < attempts - 1:
                time.sleep(backoff * (2**i))

    raise ValueError(f"AI JSON call failed after {attempts} attempt(s): {last_err}")


def call_ai_json_validated(
    system_prompt: str,
    user_obj: Any,
    response_model: type[BaseModel],
    api_key: str | None = None,
    model: str | None = None,
    retries: int = 1,
) -> BaseModel:
    """Call AI JSON and validate against a Pydantic model with retry."""
    last_err: Exception | None = None
    max_tries = max(1, int(retries) + 1)
    for attempt in range(max_tries):
        try:
            raw = call_ai_json(system_prompt, user_obj, api_key=api_key, model=model)
            return response_model.model_validate(raw)
        except (ValidationError, ValueError) as exc:
            last_err = exc
            if attempt < max_tries - 1:
                time.sleep(0.25 * (2**attempt))
                continue
            break
    raise ValueError(f"AI validated call failed after {max_tries} attempt(s): {last_err}")
