def format_spend(value: float) -> str:
    """Auto-scale a spend value to a human-readable string like $1.2M, $450K."""
    if value is None:
        return ""
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val / 1_000_000_000:.1f}B"
    if abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.0f}K"
    return f"{sign}${abs_val:,.0f}"


def format_pct(value: float, decimals: int = 1) -> str:
    if value is None:
        return ""
    return f"{value:.{decimals}f}%"
