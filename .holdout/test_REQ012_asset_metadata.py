"""REQ-012: Asset Metadata Storage.

Tests that backfill and refresh fetch and store asset metadata (name, symbol,
current price) from the CoinCap REST API.
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


def _register_mocks(asset_id="bitcoin", name="Bitcoin", symbol="BTC", price="42000.00"):
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/{asset_id}",
        json=make_asset_response(
            asset_id=asset_id,
            name=name,
            symbol=symbol,
            price=price,
        ),
        status=200,
    )
    candles = make_candle_series(n=5, asset=asset_id)
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )


# ---------------------------------------------------------------------------
# AC-1: Asset metadata stored with name, symbol, price_usd
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_stores_asset_metadata(run_cli):
    """AC-1: backfill stores asset metadata in the assets table."""
    _register_mocks("bitcoin", "Bitcoin", "BTC", "42000.00")

    result, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Check assets table exists and has the record
    tables = set(
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    )
    assert "assets" in tables, "assets table should exist"

    row = conn.execute(
        "SELECT * FROM assets WHERE asset_id ='bitcoin'"
    ).fetchone()
    conn.close()

    assert row is not None, "No asset metadata row for bitcoin"
    assert row["name"] == "Bitcoin"
    assert row["symbol"] == "BTC"
    assert float(row["price_usd"]) == pytest.approx(42000.0, rel=1e-2)


# ---------------------------------------------------------------------------
# AC-2: Metadata is upserted, not duplicated
# ---------------------------------------------------------------------------

@responses.activate
def test_metadata_upserted_not_duplicated(run_cli):
    """AC-2: Running backfill twice updates metadata, does not duplicate."""
    _register_mocks("bitcoin", "Bitcoin", "BTC", "42000.00")

    result1, db_path = run_cli("backfill", "--assets", "bitcoin")
    assert result1.returncode == 0, f"First run stderr: {result1.stderr}"

    # Second run with updated price
    _register_mocks("bitcoin", "Bitcoin", "BTC", "43000.00")
    result2, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
    assert result2.returncode == 0, f"Second run stderr: {result2.stderr}"

    conn = sqlite3.connect(str(db_path))
    count = conn.execute(
        "SELECT COUNT(*) FROM assets WHERE asset_id ='bitcoin'"
    ).fetchone()[0]
    row = conn.execute(
        "SELECT price_usd FROM assets WHERE asset_id ='bitcoin'"
    ).fetchone()
    conn.close()

    assert count == 1, f"Expected exactly 1 asset row, got {count}"
    # Price should be updated to the latest value
    assert float(row[0]) == pytest.approx(43000.0, rel=1e-2), (
        f"Price should be updated to 43000.00, got {row[0]}"
    )


# ---------------------------------------------------------------------------
# Additional: Multiple assets stored correctly
# ---------------------------------------------------------------------------

@responses.activate
def test_multiple_assets_metadata(run_cli):
    """Metadata for multiple assets is stored correctly."""
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response("bitcoin", "Bitcoin", "BTC", "42000.00"),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/ethereum",
        json=make_asset_response("ethereum", "Ethereum", "ETH", "3200.00"),
        status=200,
    )
    candles = make_candle_series(n=5)
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin,ethereum")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    assets = conn.execute("SELECT * FROM assets ORDER BY asset_id").fetchall()
    conn.close()

    asset_ids = {a["asset_id"] for a in assets}
    assert "bitcoin" in asset_ids
    assert "ethereum" in asset_ids
