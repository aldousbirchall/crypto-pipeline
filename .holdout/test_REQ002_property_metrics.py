"""REQ-002: Property-based tests for derived metric computations.

Uses Hypothesis to verify mathematical properties of SMA, volatility, and VWAP
across a wide range of inputs.
"""

import math
import re
import sqlite3

import pytest
import responses
from hypothesis import given, settings, HealthCheck, assume
from hypothesis import strategies as st

from conftest import (
    COINCAP_BASE,
    make_asset_response,
    make_candle,
    make_candles_response,
)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Positive finite floats suitable for prices
positive_price = st.floats(min_value=0.01, max_value=1e8, allow_nan=False, allow_infinity=False)
positive_volume = st.floats(min_value=0.01, max_value=1e12, allow_nan=False, allow_infinity=False)


def candle_series_strategy(min_size=50, max_size=100):
    """Generate a list of candle dicts with controlled prices."""
    @st.composite
    def _strategy(draw):
        n = draw(st.integers(min_value=min_size, max_value=max_size))
        candles = []
        start_ts = 1704067200000  # 2024-01-01 00:00 UTC
        interval_ms = 3600000
        for i in range(n):
            close = draw(positive_price)
            high = close + draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
            low = close - draw(st.floats(min_value=0.0, max_value=min(close - 0.01, 100.0), allow_nan=False, allow_infinity=False))
            open_ = draw(st.floats(min_value=low, max_value=high, allow_nan=False, allow_infinity=False))
            vol = draw(positive_volume)
            candles.append(make_candle(
                period=start_ts + i * interval_ms,
                open_=open_,
                high=high,
                low=low,
                close=close,
                volume=vol,
            ))
        return candles
    return _strategy()


# ---------------------------------------------------------------------------
# Property: SMA of constant prices equals that constant
# ---------------------------------------------------------------------------

@given(
    price=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
)
@settings(
    max_examples=20,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_sma_of_constant_prices_equals_constant(run_cli, price, tmp_path):
    """SMA-N of a constant price series should equal the constant."""
    n = 60
    candles = []
    start_ts = 1704067200000
    for i in range(n):
        candles.append(make_candle(
            period=start_ts + i * 3600000,
            open_=price,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0,
        ))

    db_path = tmp_path / f"test_{hash(price)}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    metrics = conn.execute(
        "SELECT * FROM metrics WHERE asset_id = 'bitcoin' AND sma_20 IS NOT NULL ORDER BY period"
    ).fetchall()
    conn.close()

    for m in metrics:
        assert abs(m["sma_20"] - price) < 1e-4, (
            f"SMA-20 should equal {price} for constant prices, got {m['sma_20']}"
        )


# ---------------------------------------------------------------------------
# Property: Volatility of constant prices is zero
# ---------------------------------------------------------------------------

@given(
    price=st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
)
@settings(
    max_examples=20,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_volatility_of_constant_prices_is_zero(run_cli, price, tmp_path):
    """Rolling volatility of a constant price series should be zero."""
    n = 60
    candles = []
    start_ts = 1704067200000
    for i in range(n):
        candles.append(make_candle(
            period=start_ts + i * 3600000,
            open_=price,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0,
        ))

    db_path = tmp_path / f"test_vol_{hash(price)}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    metrics = conn.execute(
        "SELECT * FROM metrics WHERE asset_id = 'bitcoin' AND volatility IS NOT NULL ORDER BY period"
    ).fetchall()
    conn.close()

    for m in metrics:
        assert abs(m["volatility"]) < 1e-10, (
            f"Volatility should be 0 for constant prices, got {m['volatility']}"
        )


# ---------------------------------------------------------------------------
# Property: SMA-20 is always between min and max close in its window
# ---------------------------------------------------------------------------

@given(
    prices=st.lists(
        st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=60,
        max_size=60,
    ),
)
@settings(
    max_examples=10,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_sma20_bounded_by_window_min_max(run_cli, prices, tmp_path):
    """SMA-20 must lie between the min and max close in its 20-period window."""
    candles = []
    start_ts = 1704067200000
    for i, p in enumerate(prices):
        candles.append(make_candle(
            period=start_ts + i * 3600000,
            open_=p,
            high=p + 1.0,
            low=max(p - 1.0, 0.01),
            close=p,
            volume=1000.0,
        ))

    db_path = tmp_path / f"test_bounded_{hash(tuple(prices))}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    metrics = conn.execute(
        "SELECT * FROM metrics WHERE asset_id = 'bitcoin' AND sma_20 IS NOT NULL ORDER BY period"
    ).fetchall()
    candle_rows = conn.execute(
        "SELECT close FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    closes = [r["close"] for r in candle_rows]
    for i, m in enumerate(metrics):
        window_start = i  # metrics with non-null sma_20 start at index 19 in candles
        # The metric at position i in the non-null list corresponds to candle index i+19
        candle_idx = i + 19
        window = closes[candle_idx - 19 : candle_idx + 1]
        assert min(window) - 1e-6 <= m["sma_20"] <= max(window) + 1e-6, (
            f"SMA-20 {m['sma_20']} not in [{min(window)}, {max(window)}]"
        )


# ---------------------------------------------------------------------------
# Property: VWAP with uniform volume equals simple average of typical prices
# ---------------------------------------------------------------------------

@given(
    prices=st.lists(
        st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
        min_size=24,
        max_size=24,
    ),
)
@settings(
    max_examples=10,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_vwap_uniform_volume_equals_avg_typical_price(run_cli, prices, tmp_path):
    """With uniform volume, VWAP reduces to a running average of typical prices."""
    candles = []
    start_ts = 1704067200000  # 2024-01-01 00:00 UTC
    uniform_vol = 1000.0
    for i, p in enumerate(prices):
        candles.append(make_candle(
            period=start_ts + i * 3600000,
            open_=p,
            high=p + 5.0,
            low=max(p - 5.0, 0.01),
            close=p,
            volume=uniform_vol,
        ))

    db_path = tmp_path / f"test_vwap_{hash(tuple(prices))}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    metrics = conn.execute(
        "SELECT * FROM metrics WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    candle_rows = conn.execute(
        "SELECT * FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    # With uniform volume, VWAP = sum(tp_i) / n for each prefix
    typical_prices = [(c["high"] + c["low"] + c["close"]) / 3.0 for c in candle_rows]
    for i, m in enumerate(metrics):
        if m["vwap"] is not None:
            expected = sum(typical_prices[: i + 1]) / (i + 1)
            assert abs(m["vwap"] - expected) < 1e-2, (
                f"VWAP at {i}: expected {expected}, got {m['vwap']}"
            )
