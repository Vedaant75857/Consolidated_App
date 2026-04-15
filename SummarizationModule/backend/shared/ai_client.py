import hashlib
import json
import logging
import os
import re
import time
from typing import Any

from portkey_ai import Portkey
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://portkey.bain.dev/v1"
_DEFAULT_MODEL = "@personal-openai/gpt-5.4"

_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = float(os.getenv("AI_CACHE_TTL_SEC", "300"))


def get_model() -> str:
    return os.getenv("PORTKEY_MODEL", _DEFAULT_MODEL)


def get_client(api_key: str | None = None) -> Portkey:
    key = (api_key or os.getenv("PORTKEY_API_KEY", "")).strip()
    if not key:
        raise ValueError("Missing API Key. Set PORTKEY_API_KEY or pass api_key.")
    base_url = os.getenv("PORTKEY_BASE_URL", _DEFAULT_BASE_URL)
    logger.info(
        "Creating Portkey client (base_url=%s, model=%s, key_len=%d, key_prefix=%s…)",
        base_url, get_model(), len(key), key[:8],
    )
    return Portkey(api_key=key, base_url=base_url, timeout=60)


def _cache_key(model: str, system: str, user: str) -> str:
    blob = f"{model}|{system}|{user}"
    return hashlib.sha256(blob.encode()).hexdigest()


def _extract_json(text: str) -> Any:
    """Try to extract JSON from text that may have markdown fences or extra prose."""
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fenced:
        text = fenced.group(1)
    text = text.strip()
    return json.loads(text)


def call_ai_json(
    system_prompt: str,
    user_obj: Any,
    api_key: str | None = None,
    model: str | None = None,
) -> Any:
    client = get_client(api_key)
    mdl = model or get_model()
    user_str = json.dumps(user_obj) if not isinstance(user_obj, str) else user_obj

    ck = _cache_key(mdl, system_prompt, user_str)
    if ck in _cache:
        ts, val = _cache[ck]
        if time.time() - ts < _CACHE_TTL:
            logger.debug("Cache hit for AI call (key=%s…)", ck[:12])
            return val

    attempts = max(1, int(os.getenv("AI_JSON_RETRY_ATTEMPTS", "3")))
    backoff = max(0.0, float(os.getenv("AI_JSON_RETRY_BACKOFF_SEC", "0.35")))

    payload_size_kb = len(user_str) / 1024
    logger.info(
        "Calling AI (model=%s, payload=%.1fKB, attempts=%d)",
        mdl, payload_size_kb, attempts,
    )

    last_err: Exception | None = None
    for i in range(attempts):
        try:
            resp = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_str},
                ],
                model=mdl,
                response_format={"type": "json_object"},
                timeout=60,
            )
            raw = resp.choices[0].message.content
            logger.debug("AI response received (%d chars)", len(raw) if raw else 0)
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = _extract_json(raw)

            _cache[ck] = (time.time(), parsed)
            return parsed
        except Exception as exc:
            last_err = exc
            logger.warning("AI call attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(backoff * (2 ** i))

    logger.error("AI JSON call failed after %d attempt(s): %s", attempts, last_err)
    raise ValueError(f"AI JSON call failed after {attempts} attempt(s): {last_err}")


def call_ai_json_validated(
    system_prompt: str,
    user_obj: Any,
    response_model: type[BaseModel],
    api_key: str | None = None,
    model: str | None = None,
    retries: int = 1,
) -> BaseModel:
    last_err: Exception | None = None
    for _ in range(max(1, retries)):
        try:
            raw = call_ai_json(system_prompt, user_obj, api_key, model)
            return response_model.model_validate(raw)
        except Exception as exc:
            last_err = exc
    raise ValueError(f"AI validated call failed: {last_err}")
