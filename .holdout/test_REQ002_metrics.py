"""REQ-002: Compute Derived Metrics.

Tests that SMA-20, SMA-50, rolling volatility, and VWAP are correctly
computed and stored after candle data is loaded.
"""

import math
import re
import sqlite3

import pytest
import responses

from conftest import (
    COINCAP_BASE,
    make_asset_response,
    make_candle,
    make_candle_series,
    make_candles_response,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_mocks(asset_ids, candles_per_asset=60):
    """Register responses mocks for backfill."""
    for asset_id in asset_ids:
        responses.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/{asset_id}",
            json=make_asset_response(asset_id=asset_id),
            status=200,
        )
        candles = make_candle_series(n=candles_per_asset, asset=asset_id)
        responses.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )


def _get_metrics(db_path, asset="bitcoin"):
    """Retrieve all metric rows for an asset, ordered by period."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM metrics WHERE asset = ? ORDER BY period",
        (asset,),
    ).fetchall()
    conn.close()
    return rows


def _get_candles(db_path, asset="bitcoin"):
    """Retrieve all candle rows for an asset, ordered by period."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM candles WHERE asset = ? ORDER BY period",
        (asset,),
    ).fetchall()
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# AC-1: SMA-20 and SMA-50 present from 20th and 50th candles onwards
# ---------------------------------------------------------------------------

@responses.activate
def test_sma_present_from_correct_candle(run_cli):
    """AC-1: Given >=50 candles, SMA-20 present from 20th, SMA-50 from 50th."""
    _register_mocks(["bitcoin"], candles_per_asset=60)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    metrics = _get_metrics(db_path, "bitcoin")
    assert len(metrics) > 0, "No metric rows found"

    # SMA-20 should be non-NULL from index 19 (20th candle, 0-indexed) onwards
    for i, m in enumerate(metrics):
        if i >= 19:
            assert m["sma_20"] is not None, f"SMA-20 is NULL at index {i}"
        else:
            assert m["sma_20"] is None, f"SMA-20 should be NULL at index {i}"

    # SMA-50 should be non-NULL from index 49 (50th candle) onwards
    for i, m in enumerate(metrics):
        if i >= 49:
            assert m["sma_50"] is not None, f"SMA-50 is NULL at index {i}"
        else:
            assert m["sma_50"] is None, f"SMA-50 should be NULL at index {i}"


# ---------------------------------------------------------------------------
# AC-2: SMA-20 correctness
# ---------------------------------------------------------------------------

@responses.activate
def test_sma_20_value_correct(run_cli):
    """AC-2: SMA-20 equals arithmetic mean of current + preceding 19 closes."""
    _register_mocks(["bitcoin"], candles_per_asset=60)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    candles = _get_candles(db_path, "bitcoin")
    metrics = _get_metrics(db_path, "bitcoin")

    # Verify SMA-20 at the 20th candle (index 19)
    closes = [candles[i]["close"] for i in range(20)]
    expected_sma20 = sum(closes) / 20.0

    assert metrics[19]["sma_20"] is not None
    assert abs(metrics[19]["sma_20"] - expected_sma20) < 1e-6, (
        f"SMA-20 at index 19: expected {expected_sma20}, got {metrics[19]['sma_20']}"
    )

    # Verify SMA-20 at the last candle
    closes_last = [candles[i]["close"] for i in range(40, 60)]
    expected_sma20_last = sum(closes_last) / 20.0

    assert metrics[59]["sma_20"] is not None
    assert abs(metrics[59]["sma_20"] - expected_sma20_last) < 1e-6, (
        f"SMA-20 at index 59: expected {expected_sma20_last}, got {metrics[59]['sma_20']}"
    )


# ---------------------------------------------------------------------------
# AC-3: SMA-50 correctness
# ---------------------------------------------------------------------------

@responses.activate
def test_sma_50_value_correct(run_cli):
    """AC-3: SMA-50 equals arithmetic mean of current + preceding 49 closes."""
    _register_mocks(["bitcoin"], candles_per_asset=60)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    candles = _get_candles(db_path, "bitcoin")
    metrics = _get_metrics(db_path, "bitcoin")

    # Verify SMA-50 at index 49 (50th candle)
    closes = [candles[i]["close"] for i in range(50)]
    expected_sma50 = sum(closes) / 50.0

    assert metrics[49]["sma_50"] is not None
    assert abs(metrics[49]["sma_50"] - expected_sma50) < 1e-6, (
        f"SMA-50 at index 49: expected {expected_sma50}, got {metrics[49]['sma_50']}"
    )


# ---------------------------------------------------------------------------
# AC-4: Rolling volatility correctness
# ---------------------------------------------------------------------------

@responses.activate
def test_rolling_volatility_correct(run_cli):
    """AC-4: Volatility = population std dev of log returns over 20 periods."""
    _register_mocks(["bitcoin"], candles_per_asset=60)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    candles = _get_candles(db_path, "bitcoin")
    metrics = _get_metrics(db_path, "bitcoin")

    # Compute expected volatility at index 19 (20th candle)
    # Log returns from index 1 to 19 (19 returns), but the window is 20 periods
    # which gives 19 log returns within that window.
    # Actually, the requirement says "over the preceding 20 periods (including current)"
    # which means we have 20 closing prices and 19 log returns.
    closes = [candles[i]["close"] for i in range(0, 20)]
    log_returns = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    mean_lr = sum(log_returns) / len(log_returns)
    # Population std dev
    variance = sum((lr - mean_lr) ** 2 for lr in log_returns) / len(log_returns)
    expected_vol = math.sqrt(variance)

    assert metrics[19]["volatility"] is not None
    assert abs(metrics[19]["volatility"] - expected_vol) < 1e-6, (
        f"Volatility at index 19: expected {expected_vol}, got {metrics[19]['volatility']}"
    )


# ---------------------------------------------------------------------------
# AC-5: VWAP correctness (resets daily)
# ---------------------------------------------------------------------------

@responses.activate
def test_vwap_correct(run_cli):
    """AC-5: VWAP = cumsum(typical_price * volume) / cumsum(volume), daily reset."""
    _register_mocks(["bitcoin"], candles_per_asset=60)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    candles = _get_candles(db_path, "bitcoin")
    metrics = _get_metrics(db_path, "bitcoin")

    # Compute expected VWAP for the first few candles (all same day in our test data)
    # Candles start at 2024-01-01 00:00 UTC, hourly. First 24 are same day.
    cum_tp_vol = 0.0
    cum_vol = 0.0
    for i in range(min(24, len(candles))):
        c = candles[i]
        typical_price = (c["high"] + c["low"] + c["close"]) / 3.0
        cum_tp_vol += typical_price * c["volume"]
        cum_vol += c["volume"]
        expected_vwap = cum_tp_vol / cum_vol

        assert metrics[i]["vwap"] is not None, f"VWAP is NULL at index {i}"
        assert abs(metrics[i]["vwap"] - expected_vwap) < 1e-4, (
            f"VWAP at index {i}: expected {expected_vwap}, got {metrics[i]['vwap']}"
        )


# ---------------------------------------------------------------------------
# AC-6: SMA-20 and volatility NULL with insufficient history
# ---------------------------------------------------------------------------

@responses.activate
def test_sma20_and_volatility_null_insufficient_history(run_cli):
    """AC-6: With <20 candles, SMA-20 and volatility are NULL."""
    _register_mocks(["bitcoin"], candles_per_asset=15)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    metrics = _get_metrics(db_path, "bitcoin")

    for i, m in enumerate(metrics):
        assert m["sma_20"] is None, f"SMA-20 should be NULL at index {i} (only 15 candles)"
        assert m["volatility"] is None, f"Volatility should be NULL at index {i} (only 15 candles)"


# ---------------------------------------------------------------------------
# AC-7: SMA-50 NULL with insufficient history
# ---------------------------------------------------------------------------

@responses.activate
def test_sma50_null_insufficient_history(run_cli):
    """AC-7: With <50 candles, SMA-50 is NULL."""
    _register_mocks(["bitcoin"], candles_per_asset=30)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    metrics = _get_metrics(db_path, "bitcoin")

    for i, m in enumerate(metrics):
        assert m["sma_50"] is None, f"SMA-50 should be NULL at index {i} (only 30 candles)"
