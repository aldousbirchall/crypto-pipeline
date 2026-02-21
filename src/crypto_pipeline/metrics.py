from __future__ import annotations

"""Metrics engine: pure computation functions for financial metrics."""

import math
from datetime import datetime, timezone


def compute_sma(closes: list[float], period: int) -> list[float | None]:
    """Compute Simple Moving Average.

    Returns list same length as closes.
    Values are None where insufficient history (index < period - 1).
    """
    if not closes or period <= 0:
        return []
    result: list[float | None] = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            window = closes[i - period + 1 : i + 1]
            result.append(sum(window) / period)
    return result


def compute_volatility(closes: list[float], period: int = 20) -> list[float | None]:
    """Compute rolling volatility as population std dev of log returns.

    Returns list same length as closes.
    Values are None where insufficient history (index < period).
    First log return requires two prices, so first valid volatility is at index=period.
    """
    if not closes or period <= 0:
        return []

    # Compute log returns: ln(close_t / close_{t-1})
    log_returns: list[float] = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            log_returns.append(math.log(closes[i] / closes[i - 1]))
        else:
            log_returns.append(0.0)

    result: list[float | None] = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            # log_returns indices are offset by 1 from closes
            # For closes[i], the corresponding log return is log_returns[i-1]
            # We need 'period - 1' log returns ending at index i-1
            window = log_returns[i - period + 1 : i]
            mean = sum(window) / len(window)
            variance = sum((r - mean) ** 2 for r in window) / len(window)
            result.append(math.sqrt(variance))
    return result


def compute_vwap(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    periods: list[int],
) -> list[float | None]:
    """Compute VWAP with daily reset.

    typical_price = (high + low + close) / 3
    VWAP resets at each new calendar day (UTC, determined from period timestamps).
    Returns list same length as input lists.
    Returns None for any period where cumulative volume is zero.
    """
    if not highs:
        return []

    result: list[float | None] = []
    cum_tp_vol = 0.0
    cum_vol = 0.0
    prev_day = None

    for i in range(len(highs)):
        # Determine UTC calendar day from period timestamp (milliseconds)
        ts_seconds = periods[i] / 1000.0
        current_day = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).date()

        # Reset at day boundary
        if prev_day is not None and current_day != prev_day:
            cum_tp_vol = 0.0
            cum_vol = 0.0

        typical_price = (highs[i] + lows[i] + closes[i]) / 3.0
        cum_tp_vol += typical_price * volumes[i]
        cum_vol += volumes[i]

        if cum_vol == 0.0:
            result.append(None)
        else:
            result.append(cum_tp_vol / cum_vol)

        prev_day = current_day

    return result


def compute_all_metrics(candles: list[dict]) -> list[dict]:
    """Compute all metrics for a sorted (by period) list of candle dicts.

    Each input dict: {"open": float, "high": float, "low": float,
                      "close": float, "volume": float, "period": int}
    Returns list of dicts: {"period": int, "sma_20": float|None,
                            "sma_50": float|None, "volatility": float|None,
                            "vwap": float|None}
    """
    if not candles:
        return []

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    periods = [c["period"] for c in candles]

    sma_20 = compute_sma(closes, 20)
    sma_50 = compute_sma(closes, 50)
    volatility = compute_volatility(closes, 20)
    vwap = compute_vwap(highs, lows, closes, volumes, periods)

    result = []
    for i in range(len(candles)):
        result.append({
            "period": periods[i],
            "sma_20": sma_20[i],
            "sma_50": sma_50[i],
            "volatility": volatility[i],
            "vwap": vwap[i],
        })
    return result
