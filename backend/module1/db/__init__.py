"""Module 1 DB layer — re-exports shared DB + module-specific join_ops."""

from shared.db import *  # noqa: F401,F403
from .join_ops import (
    match_keys_distinct_sql,
    compute_composite_match_rate_sql,
    left_join_sql,
    check_dim_uniqueness,
    profile_all_columns,
    classify_dim_columns,
    detect_format_pattern,
    build_adaptive_normalization,
)

__all__ = [
    "match_keys_distinct_sql",
    "compute_composite_match_rate_sql",
    "left_join_sql",
    "check_dim_uniqueness",
    "profile_all_columns",
    "classify_dim_columns",
    "detect_format_pattern",
    "build_adaptive_normalization",
]
