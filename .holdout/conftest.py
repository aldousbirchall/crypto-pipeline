"""Shared fixtures for crypto-pipeline holdout test suite."""

import io
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# CLI runners
# ---------------------------------------------------------------------------

class CLIResult:
    """Mimics subprocess.CompletedProcess for in-process CLI execution."""
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture
def run_cli(tmp_path):
    """Return a helper that invokes the crypto-pipeline CLI in-process.

    This runner calls main() directly so that responses mocking works.
    Returns a (CLIResult, db_path) tuple.
    """
    db_path = tmp_path / "test.db"

    def _run(*args, timeout=30, db=None, env_extra=None):
        from crypto_pipeline.cli import main

        argv = list(args) + ["--db", str(db or db_path)]

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        with patch("sys.stdout", stdout_capture), patch("sys.stderr", stderr_capture):
            try:
                returncode = main(argv)
            except SystemExit as e:
                returncode = e.code if isinstance(e.code, int) else 1
            except Exception as e:
                stderr_capture.write(f"Error: {e}\n")
                returncode = 1

        result = CLIResult(
            returncode=returncode if returncode is not None else 0,
            stdout=stdout_capture.getvalue(),
            stderr=stderr_capture.getvalue(),
        )
        return result, (db or db_path)

    return _run


@pytest.fixture
def run_cli_subprocess(tmp_path):
    """Return a helper that invokes the CLI via subprocess.

    Use this for tests that need actual process isolation (SIGINT, etc.).
    """
    db_path = tmp_path / "test.db"

    def _run(*args, timeout=30, db=None, env_extra=None):
        cmd = [
            sys.executable, "-m", "crypto_pipeline",
        ] + list(args) + [
            "--db", str(db or db_path),
        ]
        env = os.environ.copy()
        if env_extra:
            env.update(env_extra)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        return result, (db or db_path)

    return _run


@pytest.fixture
def db_path(tmp_path):
    """Return a Path to a temporary database file (does not create it)."""
    return tmp_path / "test.db"


@pytest.fixture
def db_conn(db_path):
    """Return an open SQLite connection to a temporary database.

    The connection is closed after the test.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Implementation schema (matches design.md)
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS assets (
    asset_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    price_usd   REAL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS candles (
    asset_id    TEXT NOT NULL,
    period      INTEGER NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      REAL NOT NULL,
    PRIMARY KEY (asset_id, period),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE TABLE IF NOT EXISTS metrics (
    asset_id    TEXT NOT NULL,
    period      INTEGER NOT NULL,
    sma_20      REAL,
    sma_50      REAL,
    volatility  REAL,
    vwap        REAL,
    PRIMARY KEY (asset_id, period),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id    TEXT NOT NULL,
    price       REAL NOT NULL,
    timestamp   REAL NOT NULL,
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE INDEX IF NOT EXISTS idx_candles_asset_period ON candles(asset_id, period);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_ts ON ticks(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_asset_period ON metrics(asset_id, period);
"""


def _ensure_schema(db_path):
    """Create the full schema matching the implementation."""
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_SCHEMA_SQL)
    conn.close()


# ---------------------------------------------------------------------------
# Sample candle data
# ---------------------------------------------------------------------------

def make_candle(
    asset="bitcoin",
    period=None,
    open_=100.0,
    high=110.0,
    low=90.0,
    close=105.0,
    volume=1000.0,
):
    """Build a single CoinCap-style candle dict."""
    if period is None:
        period = int(time.time() * 1000)
    return {
        "open": str(open_),
        "high": str(high),
        "low": str(low),
        "close": str(close),
        "volume": str(volume),
        "period": period,
    }


def make_candle_series(
    n=60,
    asset="bitcoin",
    start_ts=1704067200000,  # 2024-01-01 00:00 UTC
    interval_ms=3600000,     # 1 hour
    base_price=40000.0,
    price_step=10.0,
    volume=500.0,
):
    """Generate a series of n candles with predictable prices.

    Prices increment linearly so that metric calculations are deterministic.
    """
    candles = []
    for i in range(n):
        p = base_price + i * price_step
        candles.append(make_candle(
            asset=asset,
            period=start_ts + i * interval_ms,
            open_=p,
            high=p + 5.0,
            low=p - 5.0,
            close=p + 2.0,
            volume=volume + i,
        ))
    return candles


@pytest.fixture
def sample_candles():
    """Return a factory for candle series."""
    return make_candle_series


# ---------------------------------------------------------------------------
# Mock API response builders
# ---------------------------------------------------------------------------

COINCAP_BASE = "https://api.coincap.io/v2"


def coincap_candles_url(asset="bitcoin"):
    """Return the CoinCap candles (history) endpoint URL pattern for an asset."""
    return f"{COINCAP_BASE}/candles"


def coincap_asset_url(asset="bitcoin"):
    """Return the CoinCap asset detail endpoint URL."""
    return f"{COINCAP_BASE}/assets/{asset}"


def coincap_assets_url():
    """Return the CoinCap assets list endpoint URL."""
    return f"{COINCAP_BASE}/assets"


def make_asset_response(asset_id="bitcoin", name="Bitcoin", symbol="BTC", price="42000.00"):
    """Build a CoinCap asset detail response body."""
    return {
        "data": {
            "id": asset_id,
            "rank": "1",
            "symbol": symbol,
            "name": name,
            "supply": "19000000",
            "maxSupply": "21000000",
            "marketCapUsd": "800000000000",
            "volumeUsd24Hr": "20000000000",
            "priceUsd": price,
            "changePercent24Hr": "1.5",
            "vwap24Hr": "41500.00",
        },
        "timestamp": int(time.time() * 1000),
    }


def make_candles_response(candles):
    """Wrap a list of candle dicts in a CoinCap API response envelope."""
    return {"data": candles}


@pytest.fixture
def coincap_urls():
    """Expose URL builders as a fixture for convenience."""
    return {
        "candles": coincap_candles_url,
        "asset": coincap_asset_url,
        "assets": coincap_assets_url,
        "base": COINCAP_BASE,
    }


# ---------------------------------------------------------------------------
# Database pre-population helpers
# ---------------------------------------------------------------------------

def populate_candles(db_path, asset_id, candles_data):
    """Insert candle rows directly into a database for query tests.

    candles_data is a list of dicts with keys:
        period, open, high, low, close, volume
    """
    _ensure_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    for c in candles_data:
        conn.execute(
            "INSERT OR IGNORE INTO candles (asset_id, period, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (asset_id, c["period"], float(c["open"]), float(c["high"]),
             float(c["low"]), float(c["close"]), float(c["volume"])),
        )
    conn.commit()
    conn.close()


def populate_ticks(db_path, ticks_data):
    """Insert tick rows directly into a database for query tests.

    ticks_data is a list of dicts with keys:
        asset_id, price, timestamp (float Unix seconds)
    """
    _ensure_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    for t in ticks_data:
        conn.execute(
            "INSERT INTO ticks (asset_id, price, timestamp) VALUES (?, ?, ?)",
            (t["asset_id"], t["price"], t["timestamp"]),
        )
    conn.commit()
    conn.close()


def populate_metrics(db_path, asset_id, metrics_data):
    """Insert metric rows directly into a database for query tests.

    metrics_data is a list of dicts with keys:
        period, sma_20, sma_50, volatility, vwap
    Values may be None.
    """
    _ensure_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    for m in metrics_data:
        conn.execute(
            "INSERT OR IGNORE INTO metrics (asset_id, period, sma_20, sma_50, volatility, vwap) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (asset_id, m["period"], m.get("sma_20"), m.get("sma_50"),
             m.get("volatility"), m.get("vwap")),
        )
    conn.commit()
    conn.close()


def populate_assets(db_path, assets_data):
    """Insert asset metadata rows directly into a database.

    assets_data is a list of dicts with keys:
        asset_id, name, symbol, price_usd
    """
    _ensure_schema(db_path)
    conn = sqlite3.connect(str(db_path))
    for a in assets_data:
        conn.execute(
            "INSERT OR REPLACE INTO assets (asset_id, name, symbol, price_usd, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (a["asset_id"], a["name"], a["symbol"], a["price_usd"], time.time()),
        )
    conn.commit()
    conn.close()
