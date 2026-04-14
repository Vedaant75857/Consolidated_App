"""Pure JSON-serialisation helpers with no shared.db dependency.

Extracted from helpers.py to break the circular import between
shared.db and shared.utils.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time
from typing import Any


def json_default(value: Any) -> Any:
    """Convert common non-JSON-native Python values into JSON-safe forms."""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_safe(value: Any) -> Any:
    """Recursively convert nested values into JSON-safe primitives."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe(v) for v in value]
    if isinstance(value, set):
        return [json_safe(v) for v in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value
