"""REQ-013: API Error Handling.

Tests that HTTP 4xx/5xx errors and connection failures are handled correctly:
logged to stderr and exit non-zero for backfill/refresh.
"""

import re

import pytest
import responses

from conftest import COINCAP_BASE, make_asset_response


# ---------------------------------------------------------------------------
# AC-1: HTTP 429 (rate limited) -> stderr + non-zero exit
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_rate_limited(run_cli):
    """AC-1: HTTP 429 from API is logged to stderr, process exits non-zero."""
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json={"error": "Rate limit exceeded"},
        status=429,
    )
    # Also mock candles endpoint in case the implementation hits it first
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json={"error": "Rate limit exceeded"},
        status=429,
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode != 0, (
        f"Expected non-zero exit for HTTP 429. stdout: {result.stdout}"
    )
    assert len(result.stderr) > 0, "Error should be logged to stderr"


# ---------------------------------------------------------------------------
# AC-2: API unreachable -> stderr + non-zero exit
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_api_unreachable(run_cli):
    """AC-2: Connection error is logged to stderr, process exits non-zero."""
    # responses with no matching URL will raise ConnectionError
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        body=ConnectionError("Connection refused"),
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        body=ConnectionError("Connection refused"),
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode != 0, (
        f"Expected non-zero exit for connection error. stdout: {result.stdout}"
    )
    assert len(result.stderr) > 0, "Connection error should be logged to stderr"


# ---------------------------------------------------------------------------
# Additional: HTTP 500 server error
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_server_error(run_cli):
    """HTTP 500 from API is handled gracefully."""
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json={"error": "Internal server error"},
        status=500,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json={"error": "Internal server error"},
        status=500,
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode != 0, (
        f"Expected non-zero exit for HTTP 500. stdout: {result.stdout}"
    )


# ---------------------------------------------------------------------------
# Additional: refresh also handles API errors
# ---------------------------------------------------------------------------

@responses.activate
def test_refresh_api_error(run_cli, tmp_path):
    """refresh command also exits non-zero on API errors."""
    # Create a minimal database so refresh has something to work with
    import sqlite3
    db_path = tmp_path / "refresh_err.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            period INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            UNIQUE(asset, period)
        )
    """)
    conn.execute("""
        CREATE TABLE ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL, price REAL NOT NULL, timestamp TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL, period INTEGER NOT NULL,
            sma_20 REAL, sma_50 REAL, volatility REAL, vwap REAL,
            UNIQUE(asset, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            id TEXT PRIMARY KEY, name TEXT, symbol TEXT, price_usd REAL
        )
    """)
    conn.execute(
        "INSERT INTO candles (asset, period, open, high, low, close, volume) "
        "VALUES ('bitcoin', 1704067200000, 100, 110, 90, 105, 1000)"
    )
    conn.commit()
    conn.close()

    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json={"error": "Rate limit exceeded"},
        status=429,
    )
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json={"error": "Rate limit exceeded"},
        status=429,
    )

    result, _ = run_cli("refresh", "--assets", "bitcoin", db=db_path)
    assert result.returncode != 0, (
        f"Expected non-zero exit for API error during refresh. stdout: {result.stdout}"
    )
