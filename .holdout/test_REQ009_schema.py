"""REQ-009: Database Schema Initialisation.

Tests that the system creates required tables on first connection and
does not drop/recreate existing tables.
"""

import re
import sqlite3

import pytest
import responses

from conftest import (
    COINCAP_BASE,
    make_asset_response,
    make_candle_series,
    make_candles_response,
)


def _register_mocks():
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    candles = make_candle_series(n=5)
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )


# ---------------------------------------------------------------------------
# AC-1: New database gets all required tables
# ---------------------------------------------------------------------------

@responses.activate
def test_new_database_creates_tables(run_cli, tmp_path):
    """AC-1: A new database file gets candles, ticks, and metrics tables."""
    db_path = tmp_path / "fresh.db"
    assert not db_path.exists()

    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    tables = set(
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    )
    conn.close()

    required_tables = {"candles", "ticks", "metrics"}
    missing = required_tables - tables
    assert not missing, f"Missing required tables: {missing}. Found: {tables}"


# ---------------------------------------------------------------------------
# AC-2: Existing tables are not dropped or recreated
# ---------------------------------------------------------------------------

@responses.activate
def test_existing_tables_not_dropped(run_cli, tmp_path):
    """AC-2: Connecting to a database with existing tables preserves them."""
    db_path = tmp_path / "existing.db"

    # Create the database with the implementation's schema and a sentinel row
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
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
            PRIMARY KEY (asset_id, period)
        );
        CREATE TABLE IF NOT EXISTS ticks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id    TEXT NOT NULL,
            price       REAL NOT NULL,
            timestamp   REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS metrics (
            asset_id    TEXT NOT NULL,
            period      INTEGER NOT NULL,
            sma_20      REAL,
            sma_50      REAL,
            volatility  REAL,
            vwap        REAL,
            PRIMARY KEY (asset_id, period)
        );
    """)
    # Insert a sentinel row
    conn.execute(
        "INSERT INTO candles (asset_id, period, open, high, low, close, volume) "
        "VALUES ('sentinel', 999, 1.0, 2.0, 0.5, 1.5, 100.0)"
    )
    conn.commit()
    conn.close()

    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    # Verify sentinel row still exists
    conn = sqlite3.connect(str(db_path))
    sentinel = conn.execute(
        "SELECT COUNT(*) FROM candles WHERE asset_id = 'sentinel'"
    ).fetchone()[0]
    conn.close()

    assert sentinel == 1, "Pre-existing data should not be dropped"


# ---------------------------------------------------------------------------
# Additional: Schema includes required columns
# ---------------------------------------------------------------------------

@responses.activate
def test_candles_table_has_required_columns(run_cli, tmp_path):
    """candles table has all required OHLCV + period columns."""
    db_path = tmp_path / "schema_check.db"
    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    cols_info = conn.execute("PRAGMA table_info(candles)").fetchall()
    conn.close()

    col_names = {c[1] for c in cols_info}
    required = {"asset_id", "period", "open", "high", "low", "close", "volume"}
    missing = required - col_names
    assert not missing, f"candles table missing columns: {missing}"


@responses.activate
def test_metrics_table_has_required_columns(run_cli, tmp_path):
    """metrics table has all required metric columns."""
    db_path = tmp_path / "schema_metrics.db"
    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    cols_info = conn.execute("PRAGMA table_info(metrics)").fetchall()
    conn.close()

    col_names = {c[1] for c in cols_info}
    required = {"asset_id", "period", "sma_20", "sma_50", "volatility", "vwap"}
    missing = required - col_names
    assert not missing, f"metrics table missing columns: {missing}"


@responses.activate
def test_ticks_table_has_required_columns(run_cli, tmp_path):
    """ticks table has all required tick columns."""
    db_path = tmp_path / "schema_ticks.db"
    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    cols_info = conn.execute("PRAGMA table_info(ticks)").fetchall()
    conn.close()

    col_names = {c[1] for c in cols_info}
    required = {"asset_id", "price", "timestamp"}
    missing = required - col_names
    assert not missing, f"ticks table missing columns: {missing}"
