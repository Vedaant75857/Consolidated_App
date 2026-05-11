"""Pure JSON-serialisation helpers with no shared.db dependency.

Extracted from helpers.py to break the circular import between
shared.db and shared.utils.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID
from typing import Any


def _scalar_to_json_safe(value: Any) -> Any:
    """Convert a scalar non-collection value to a JSON-safe primitive.

    Returns ``None`` for NaN/Inf floats, ISO strings for date/time-like values,
    plain strings for Decimal/UUID, and str for unknown numpy-style scalars
    (matched via duck-typing on ``.item()`` so numpy is not a hard dep).
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return bytes(value).hex()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            converted = item()
            if isinstance(converted, float) and (math.isnan(converted) or math.isinf(converted)):
                return None
            return converted
        except (TypeError, ValueError):
            pass
    return value


def json_default(value: Any) -> Any:
    """Convert common non-JSON-native Python values into JSON-safe forms.

    Used as ``json.dumps(default=...)``; only needs to handle scalars the
    encoder cannot natively represent. Collections are unwrapped upstream
    via ``json_safe`` before encoding.
    """
    converted = _scalar_to_json_safe(value)
    if converted is value:
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
    return converted


def json_safe(value: Any) -> Any:
    """Recursively convert nested values into JSON-safe primitives."""
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(v) for v in value]
    return _scalar_to_json_safe(value)
