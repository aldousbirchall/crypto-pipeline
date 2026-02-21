"""REQ-014: Numeric Validation.

Tests that the system rejects inf and NaN values in price and volume data,
skipping invalid records and logging warnings.
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
    make_candles_response,
)


# ---------------------------------------------------------------------------
# AC-1: NaN price value is skipped
# ---------------------------------------------------------------------------

@responses.activate
def test_nan_price_skipped(run_cli):
    """AC-1: A candle with NaN price is skipped, warning logged to stderr."""
    candles = [
        make_candle(period=1704067200000, close=100.0),
        {
            "open": "NaN",
            "high": "110",
            "low": "90",
            "close": "NaN",
            "volume": "1000",
            "period": 1704070800000,
        },
        make_candle(period=1704074400000, close=102.0),
    ]

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT * FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    # The NaN candle should be skipped
    assert len(rows) == 2, f"Expected 2 valid candles (NaN skipped), got {len(rows)}"

    # Warning should be logged to stderr
    assert len(result.stderr) > 0, "Warning about NaN should be logged to stderr"


# ---------------------------------------------------------------------------
# AC-2: inf volume value is skipped
# ---------------------------------------------------------------------------

@responses.activate
def test_inf_volume_skipped(run_cli):
    """AC-2: A candle with inf volume is skipped, warning logged to stderr."""
    candles = [
        make_candle(period=1704067200000, close=100.0),
        {
            "open": "100",
            "high": "110",
            "low": "90",
            "close": "105",
            "volume": "Infinity",
            "period": 1704070800000,
        },
        make_candle(period=1704074400000, close=102.0),
    ]

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT * FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    assert len(rows) == 2, f"Expected 2 valid candles (inf skipped), got {len(rows)}"
    assert len(result.stderr) > 0, "Warning about inf should be logged to stderr"


# ---------------------------------------------------------------------------
# AC-3: Valid numeric values stored normally
# ---------------------------------------------------------------------------

@responses.activate
def test_valid_numerics_stored(run_cli):
    """AC-3: Valid numeric values are stored without issues."""
    candles = [
        make_candle(period=1704067200000, open_=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0),
        make_candle(period=1704070800000, open_=105.0, high=115.0, low=95.0, close=110.0, volume=2000.0),
    ]

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT * FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    assert len(rows) == 2, f"Expected 2 candles stored, got {len(rows)}"


# ---------------------------------------------------------------------------
# Additional: Negative infinity also rejected
# ---------------------------------------------------------------------------

@responses.activate
def test_negative_inf_skipped(run_cli):
    """Negative infinity values are also rejected."""
    candles = [
        make_candle(period=1704067200000, close=100.0),
        {
            "open": "-Infinity",
            "high": "110",
            "low": "90",
            "close": "105",
            "volume": "1000",
            "period": 1704070800000,
        },
        make_candle(period=1704074400000, close=102.0),
    ]

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT * FROM candles WHERE asset_id = 'bitcoin' ORDER BY period"
    ).fetchall()
    conn.close()

    assert len(rows) == 2, f"Expected 2 valid candles (-inf skipped), got {len(rows)}"


# ---------------------------------------------------------------------------
# Additional: No inf or NaN values in the database after backfill
# ---------------------------------------------------------------------------

@responses.activate
def test_no_inf_nan_in_database(run_cli):
    """No inf or NaN values should exist in the database after backfill."""
    candles = [
        make_candle(period=1704067200000 + i * 3600000, close=100.0 + i)
        for i in range(10)
    ]
    # Inject some bad candles
    candles.append({
        "open": "NaN", "high": "NaN", "low": "NaN", "close": "NaN",
        "volume": "NaN", "period": 1704067200000 + 10 * 3600000,
    })
    candles.append({
        "open": "Infinity", "high": "Infinity", "low": "-Infinity",
        "close": "Infinity", "volume": "Infinity",
        "period": 1704067200000 + 11 * 3600000,
    })

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("SELECT open, high, low, close, volume FROM candles").fetchall()
    conn.close()

    for row in rows:
        for val in row:
            assert val is not None
            assert not math.isnan(val), f"NaN found in database: {row}"
            assert not math.isinf(val), f"Inf found in database: {row}"
