from __future__ import annotations

"""Validation utilities for API data."""

import math


def is_valid_number(value) -> bool:
    """Return True if value can be converted to a finite float.

    Rejects inf, NaN, and non-numeric values.
    """
    try:
        f = float(value)
        return math.isfinite(f)
    except (ValueError, TypeError):
        return False


def validate_candle(candle: dict) -> bool:
    """Return True if all numeric fields (open, high, low, close, volume) are valid finite numbers."""
    for field in ("open", "high", "low", "close", "volume"):
        if field not in candle or not is_valid_number(candle[field]):
            return False
    return True


def validate_tick(price) -> bool:
    """Return True if price is a valid finite number."""
    return is_valid_number(price)
