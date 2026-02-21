"""REQ-008: Database Path Flag.

Tests that the --db flag specifies the SQLite database file path and
defaults to crypto_pipeline.db in the current working directory.
"""

import os
import re
import sqlite3
from pathlib import Path

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
# AC-1: --db flag uses specified path
# ---------------------------------------------------------------------------

@responses.activate
def test_db_flag_custom_path(run_cli, tmp_path):
    """AC-1: --db /tmp/test.db uses that file as the database."""
    custom_db = tmp_path / "custom" / "my_data.db"
    custom_db.parent.mkdir(parents=True, exist_ok=True)

    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=custom_db)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    assert custom_db.exists(), f"Database file not created at {custom_db}"

    conn = sqlite3.connect(str(custom_db))
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count > 0, "Custom database should contain candle data"


# ---------------------------------------------------------------------------
# AC-2: Default database is crypto_pipeline.db in current directory
# ---------------------------------------------------------------------------

@responses.activate
def test_db_default_path(tmp_path):
    """AC-2: Without --db, the default database is crypto_pipeline.db in cwd."""
    import subprocess
    import sys

    _register_mocks()

    # Run CLI without --db flag, from tmp_path as working directory
    result = subprocess.run(
        [sys.executable, "-m", "crypto_pipeline",
         "backfill", "--assets", "bitcoin"],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        timeout=30,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}"

    default_db = tmp_path / "crypto_pipeline.db"
    assert default_db.exists(), (
        f"Default database crypto_pipeline.db not found in {tmp_path}"
    )


# ---------------------------------------------------------------------------
# AC-3: --db to non-existent file creates it automatically
# ---------------------------------------------------------------------------

@responses.activate
def test_db_creates_file_automatically(run_cli, tmp_path):
    """AC-3: --db pointing to a non-existent file creates the database."""
    new_db = tmp_path / "new_dir" / "brand_new.db"
    # Parent directory exists but file does not
    new_db.parent.mkdir(parents=True, exist_ok=True)
    assert not new_db.exists()

    _register_mocks()

    result, _ = run_cli("backfill", "--assets", "bitcoin", db=new_db)
    assert result.returncode == 0, f"stderr: {result.stderr}"

    assert new_db.exists(), f"Database file should be auto-created at {new_db}"
