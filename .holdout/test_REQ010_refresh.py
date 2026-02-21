"""REQ-010: ETL Refresh (Incremental Update).

Tests that `crypto-pipeline refresh` pulls only new candle data since the
most recent stored timestamp and recomputes metrics.
"""

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
    populate_candles,
)


def _register_asset_mock(asset_id="bitcoin"):
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/{asset_id}",
        json=make_asset_response(asset_id=asset_id),
        status=200,
    )


# ---------------------------------------------------------------------------
# AC-1: Refresh fetches only candles newer than latest stored timestamp
# ---------------------------------------------------------------------------

@responses.activate
def test_refresh_incremental_fetch(run_cli, tmp_path):
    """AC-1: Refresh only fetches candles with period > most recent stored."""
    db_path = tmp_path / "refresh.db"

    # Pre-populate with 30 candles
    start_ts = 1704067200000
    interval = 3600000
    initial_candles = make_candle_series(n=30, start_ts=start_ts)
    populate_candles(db_path, "bitcoin", initial_candles)

    # Also need schema for metrics and ticks tables
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            period INTEGER NOT NULL,
            sma_20 REAL,
            sma_50 REAL,
            volatility REAL,
            vwap REAL,
            UNIQUE(asset, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price_usd REAL NOT NULL
        )
    """)
    conn.commit()
    initial_count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    latest_period = conn.execute(
        "SELECT MAX(period) FROM candles WHERE asset = 'bitcoin'"
    ).fetchone()[0]
    conn.close()

    assert initial_count == 30

    # API returns 10 new candles after the last stored one
    new_candles = make_candle_series(
        n=10,
        start_ts=latest_period + interval,
        base_price=40300.0,
    )

    _register_asset_mock("bitcoin")
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(new_candles),
        status=200,
    )

    result, _ = run_cli("refresh", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    final_count = conn.execute("SELECT COUNT(*) FROM candles WHERE asset = 'bitcoin'").fetchone()[0]
    conn.close()

    assert final_count == 40, f"Expected 30 + 10 = 40 candles, got {final_count}"


# ---------------------------------------------------------------------------
# AC-2: Metrics recomputed after refresh
# ---------------------------------------------------------------------------

@responses.activate
def test_refresh_recomputes_metrics(run_cli, tmp_path):
    """AC-2: After refresh, derived metrics are recomputed for the updated dataset."""
    db_path = tmp_path / "refresh_metrics.db"

    # First do a full backfill with 30 candles
    start_ts = 1704067200000
    initial_candles = make_candle_series(n=30, start_ts=start_ts)

    _register_asset_mock("bitcoin")
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(initial_candles),
        status=200,
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"backfill stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    metrics_before = conn.execute(
        "SELECT COUNT(*) FROM metrics WHERE asset = 'bitcoin'"
    ).fetchone()[0]
    latest_period = conn.execute(
        "SELECT MAX(period) FROM candles WHERE asset = 'bitcoin'"
    ).fetchone()[0]
    conn.close()

    # Now refresh with 10 more candles
    new_candles = make_candle_series(
        n=10,
        start_ts=latest_period + 3600000,
        base_price=40300.0,
    )

    _register_asset_mock("bitcoin")
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(new_candles),
        status=200,
    )

    result, _ = run_cli("refresh", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"refresh stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    metrics_after = conn.execute(
        "SELECT COUNT(*) FROM metrics WHERE asset = 'bitcoin'"
    ).fetchone()[0]
    conn.close()

    # There should be more metric rows after refresh
    assert metrics_after > metrics_before, (
        f"Expected more metrics after refresh. Before: {metrics_before}, after: {metrics_after}"
    )


# ---------------------------------------------------------------------------
# AC-3: No new data available
# ---------------------------------------------------------------------------

@responses.activate
def test_refresh_no_new_data(run_cli, tmp_path):
    """AC-3: When no new data is available, no new records are inserted."""
    db_path = tmp_path / "refresh_noop.db"

    # First backfill
    candles = make_candle_series(n=30)
    _register_asset_mock("bitcoin")
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"backfill stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    count_before = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    # Refresh returns empty data
    _register_asset_mock("bitcoin")
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response([]),
        status=200,
    )

    result, _ = run_cli("refresh", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"refresh stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    count_after = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count_after == count_before, (
        f"No new candles should be inserted. Before: {count_before}, after: {count_after}"
    )
