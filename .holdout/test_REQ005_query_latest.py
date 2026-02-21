"""REQ-005: Query Latest Prices.

Tests that `crypto-pipeline query latest` displays the most recent price
for each specified asset from the database.
"""

import sqlite3

import pytest

from conftest import populate_ticks


# ---------------------------------------------------------------------------
# AC-1: Latest price for a single asset
# ---------------------------------------------------------------------------

def test_query_latest_single_asset(run_cli, tmp_path):
    """AC-1: query latest --assets bitcoin shows asset name, latest price, timestamp."""
    db_path = tmp_path / "query_latest.db"

    # Pre-populate ticks (timestamps as Unix seconds)
    populate_ticks(db_path, [
        {"asset_id": "bitcoin", "price": 41000.0, "timestamp": 1736935200.0},
        {"asset_id": "bitcoin", "price": 42000.0, "timestamp": 1736938800.0},
        {"asset_id": "bitcoin", "price": 43000.50, "timestamp": 1736942400.0},
    ])

    result, _ = run_cli("query", "latest", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout.lower()
    # Should contain the asset name
    assert "bitcoin" in output, "Output should contain asset name 'bitcoin'"
    # Should contain the latest price (43000.50)
    assert "43000" in output, "Output should contain the latest price"


# ---------------------------------------------------------------------------
# AC-2: Default assets (bitcoin + ethereum)
# ---------------------------------------------------------------------------

def test_query_latest_default_assets(run_cli, tmp_path):
    """AC-2: query latest with no --assets shows latest for bitcoin and ethereum."""
    db_path = tmp_path / "query_latest_default.db"

    populate_ticks(db_path, [
        {"asset_id": "bitcoin", "price": 42000.0, "timestamp": 1736942400.0},
        {"asset_id": "ethereum", "price": 3200.0, "timestamp": 1736942400.0},
    ])

    result, _ = run_cli("query", "latest", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout.lower()
    assert "bitcoin" in output, "Output should contain bitcoin"
    assert "ethereum" in output, "Output should contain ethereum"
    assert "42000" in output, "Output should contain bitcoin price"
    assert "3200" in output, "Output should contain ethereum price"


# ---------------------------------------------------------------------------
# AC-3: No data available for asset
# ---------------------------------------------------------------------------

def test_query_latest_no_data(run_cli, tmp_path):
    """AC-3: query latest for an asset with no data indicates no data available."""
    db_path = tmp_path / "query_latest_nodata.db"

    # Create an empty ticks table
    populate_ticks(db_path, [])

    result, _ = run_cli("query", "latest", "--assets", "dogecoin", db=db_path)

    output = (result.stdout + result.stderr).lower()
    # Should indicate no data is available
    assert any(phrase in output for phrase in [
        "no data", "not found", "no tick", "unavailable", "no record", "empty",
        "no price", "n/a",
    ]), f"Expected 'no data' indication, got: {result.stdout}"


# ---------------------------------------------------------------------------
# Additional: Latest price is actually the most recent (not first or random)
# ---------------------------------------------------------------------------

def test_query_latest_returns_most_recent(run_cli, tmp_path):
    """The latest price should be the most recent by timestamp, not insertion order."""
    db_path = tmp_path / "query_latest_order.db"

    # Insert out of chronological order
    populate_ticks(db_path, [
        {"asset_id": "bitcoin", "price": 43000.0, "timestamp": 1736949600.0},
        {"asset_id": "bitcoin", "price": 41000.0, "timestamp": 1736935200.0},
        {"asset_id": "bitcoin", "price": 45000.0, "timestamp": 1736956800.0},
        {"asset_id": "bitcoin", "price": 42000.0, "timestamp": 1736942400.0},
    ])

    result, _ = run_cli("query", "latest", "--assets", "bitcoin", db=db_path)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    output = result.stdout
    # The latest tick is 45000.0
    assert "45000" in output, (
        f"Expected latest price 45000, output: {output}"
    )
