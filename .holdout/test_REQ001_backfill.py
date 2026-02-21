"""REQ-001: Backfill Historical Candle Data.

Tests that `crypto-pipeline backfill` pulls hourly OHLCV candle data from
the CoinCap REST API and stores it in SQLite.
"""

import json
import re
import sqlite3
import time

import pytest
import responses

from conftest import (
    COINCAP_BASE,
    make_asset_response,
    make_candle_series,
    make_candles_response,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_backfill_mocks(asset_ids, candles_per_asset=60):
    """Register responses mocks for backfill of the given assets."""
    for asset_id in asset_ids:
        # Asset metadata endpoint
        responses.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/{asset_id}",
            json=make_asset_response(asset_id=asset_id),
            status=200,
        )
        # Candles endpoint — match any query params
        candles = make_candle_series(n=candles_per_asset, asset=asset_id)
        responses.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )


# ---------------------------------------------------------------------------
# AC-1: Fresh database, single asset, ~30 days of hourly candles
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_single_asset_populates_candles(run_cli):
    """AC-1: backfill --assets bitcoin stores hourly candle records."""
    n_candles = 720  # 30 days * 24 hours
    _register_backfill_mocks(["bitcoin"], candles_per_asset=n_candles)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT COUNT(*) FROM candles WHERE asset = 'bitcoin'"
    ).fetchone()[0]
    conn.close()

    # Should have stored the candles returned by the API
    assert rows == n_candles, f"Expected {n_candles} candle rows, got {rows}"


# ---------------------------------------------------------------------------
# AC-2: Default assets (bitcoin + ethereum)
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_default_assets(run_cli):
    """AC-2: backfill with no --assets flag stores data for bitcoin and ethereum."""
    _register_backfill_mocks(["bitcoin", "ethereum"], candles_per_asset=60)

    result, db_path = run_cli("backfill")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    assets = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT asset FROM candles ORDER BY asset"
        ).fetchall()
    ]
    conn.close()

    assert "bitcoin" in assets, "bitcoin candles missing"
    assert "ethereum" in assets, "ethereum candles missing"


# ---------------------------------------------------------------------------
# AC-3: Each candle has OHLCV + period fields
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_candle_fields(run_cli):
    """AC-3: Stored candles contain open, high, low, close, volume, period."""
    _register_backfill_mocks(["bitcoin"], candles_per_asset=5)

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM candles WHERE asset = 'bitcoin' LIMIT 1").fetchone()
    conn.close()

    assert row is not None, "No candle row found"
    columns = row.keys()
    for field in ("open", "high", "low", "close", "volume", "period"):
        assert field in columns, f"Missing field: {field}"
    # Values should be numeric and non-null
    assert row["open"] is not None
    assert row["high"] is not None
    assert row["low"] is not None
    assert row["close"] is not None
    assert row["volume"] is not None
    assert row["period"] is not None


# ---------------------------------------------------------------------------
# AC-4: No duplicate candles for the same asset + period
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_no_duplicate_candles(run_cli):
    """AC-4: Running backfill twice does not create duplicate rows."""
    _register_backfill_mocks(["bitcoin"], candles_per_asset=30)

    # Run backfill twice
    result1, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result1.returncode == 0, f"First run stderr: {result1.stderr}"

    # Re-register mocks for second call (responses clears after activation)
    _register_backfill_mocks(["bitcoin"], candles_per_asset=30)
    result2, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    # Second run may succeed or handle gracefully; key assertion is no dupes

    conn = sqlite3.connect(str(db_path))
    dupes = conn.execute("""
        SELECT asset, period, COUNT(*) as cnt
        FROM candles
        GROUP BY asset, period
        HAVING cnt > 1
    """).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert len(dupes) == 0, f"Found {len(dupes)} duplicate (asset, period) pairs"
    assert total == 30, f"Expected 30 rows, got {total}"
