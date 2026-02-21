"""REQ-007: Asset Selection Flag.

Tests that the --assets flag is accepted on all subcommands, takes a
comma-separated list, and defaults to bitcoin,ethereum.
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
    populate_ticks,
    populate_candles,
    populate_metrics,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_backfill_mocks(asset_ids, n=10):
    for asset_id in asset_ids:
        responses.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/{asset_id}",
            json=make_asset_response(asset_id=asset_id),
            status=200,
        )
    # Single candle endpoint mock that matches any asset
    candles = make_candle_series(n=n)
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )


# ---------------------------------------------------------------------------
# AC-1: Comma-separated multi-asset flag
# ---------------------------------------------------------------------------

@responses.activate
def test_assets_flag_comma_separated(run_cli):
    """AC-1: --assets bitcoin,ethereum,litecoin operates on all three."""
    _register_backfill_mocks(["bitcoin", "ethereum", "litecoin"])

    result, db_path = run_cli(
        "backfill", "--assets", "bitcoin,ethereum,litecoin"
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    assets = set(
        r[0] for r in conn.execute("SELECT DISTINCT asset FROM candles").fetchall()
    )
    conn.close()

    assert "bitcoin" in assets, "bitcoin missing from candles"
    assert "ethereum" in assets, "ethereum missing from candles"
    assert "litecoin" in assets, "litecoin missing from candles"


# ---------------------------------------------------------------------------
# AC-2: Default assets (bitcoin, ethereum)
# ---------------------------------------------------------------------------

@responses.activate
def test_default_assets(run_cli):
    """AC-2: No --assets flag defaults to bitcoin and ethereum."""
    _register_backfill_mocks(["bitcoin", "ethereum"])

    result, db_path = run_cli("backfill")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    assets = set(
        r[0] for r in conn.execute("SELECT DISTINCT asset FROM candles").fetchall()
    )
    conn.close()

    assert "bitcoin" in assets, "bitcoin should be a default asset"
    assert "ethereum" in assets, "ethereum should be a default asset"
    assert len(assets) == 2, f"Expected exactly 2 default assets, got {assets}"


# ---------------------------------------------------------------------------
# AC-3: Single asset
# ---------------------------------------------------------------------------

@responses.activate
def test_single_asset(run_cli):
    """AC-3: --assets with a single asset operates on just that asset."""
    _register_backfill_mocks(["bitcoin"])

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    assets = set(
        r[0] for r in conn.execute("SELECT DISTINCT asset FROM candles").fetchall()
    )
    conn.close()

    assert assets == {"bitcoin"}, f"Expected only bitcoin, got {assets}"


# ---------------------------------------------------------------------------
# Additional: --assets flag works on query subcommands
# ---------------------------------------------------------------------------

def test_assets_flag_on_query_latest(run_cli, tmp_path):
    """--assets flag is accepted by query latest subcommand."""
    db_path = tmp_path / "assets_query.db"
    populate_ticks(db_path, [
        {"asset": "bitcoin", "price": 42000.0, "timestamp": "2026-01-15T12:00:00Z"},
        {"asset": "litecoin", "price": 120.0, "timestamp": "2026-01-15T12:00:00Z"},
    ])

    result, _ = run_cli("query", "latest", "--assets", "litecoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout.lower()
    assert "litecoin" in output or "120" in output, (
        f"Output should contain litecoin data. Got: {result.stdout}"
    )
