"""REQ-014: Property-based tests for numeric validation.

Uses Hypothesis to generate a wide range of numeric edge cases and verify
the system correctly rejects non-finite values while accepting valid ones.
"""

import math
import re
import sqlite3

import pytest
import responses
from hypothesis import given, settings, HealthCheck, assume
from hypothesis import strategies as st

from conftest import (
    COINCAP_BASE,
    make_asset_response,
    make_candle,
    make_candles_response,
)


# ---------------------------------------------------------------------------
# Property: Any finite positive float is accepted
# ---------------------------------------------------------------------------

@given(
    price=st.floats(min_value=0.01, max_value=1e8, allow_nan=False, allow_infinity=False),
    volume=st.floats(min_value=0.01, max_value=1e12, allow_nan=False, allow_infinity=False),
)
@settings(
    max_examples=15,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_finite_values_accepted(run_cli, price, volume, tmp_path):
    """Any finite positive price and volume should be stored."""
    candles = [
        make_candle(
            period=1704067200000,
            open_=price,
            high=price + 1.0,
            low=max(price - 1.0, 0.01),
            close=price,
            volume=volume,
        ),
    ]

    db_path = tmp_path / f"finite_{hash((price, volume))}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)
        assert result.returncode == 0, f"stderr: {result.stderr}"

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count == 1, f"Expected 1 candle stored for finite values, got {count}"


# ---------------------------------------------------------------------------
# Property: NaN in any field causes the record to be skipped
# ---------------------------------------------------------------------------

@given(
    field=st.sampled_from(["open", "high", "low", "close", "volume"]),
)
@settings(
    max_examples=5,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_nan_in_any_field_rejected(run_cli, field, tmp_path):
    """NaN in any numeric field should cause the candle to be skipped."""
    candle_dict = {
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "volume": "1000",
        "period": 1704067200000,
    }
    candle_dict[field] = "NaN"

    db_path = tmp_path / f"nan_{field}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response([candle_dict]),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count == 0, f"NaN in {field} should be rejected, but {count} candles stored"


# ---------------------------------------------------------------------------
# Property: Inf in any field causes the record to be skipped
# ---------------------------------------------------------------------------

@given(
    field=st.sampled_from(["open", "high", "low", "close", "volume"]),
    inf_value=st.sampled_from(["Infinity", "-Infinity", "inf", "-inf"]),
)
@settings(
    max_examples=10,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_inf_in_any_field_rejected(run_cli, field, inf_value, tmp_path):
    """Infinity in any numeric field should cause the candle to be skipped."""
    candle_dict = {
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "volume": "1000",
        "period": 1704067200000,
    }
    candle_dict[field] = inf_value

    db_path = tmp_path / f"inf_{field}_{inf_value.replace('-', 'neg')}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response([candle_dict]),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count == 0, (
        f"{inf_value} in {field} should be rejected, but {count} candles stored"
    )


# ---------------------------------------------------------------------------
# Property: Mix of valid and invalid candles stores only valid ones
# ---------------------------------------------------------------------------

@given(
    n_valid=st.integers(min_value=1, max_value=10),
    n_invalid=st.integers(min_value=1, max_value=5),
)
@settings(
    max_examples=10,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_mixed_valid_invalid_stores_only_valid(run_cli, n_valid, n_invalid, tmp_path):
    """When a batch has both valid and invalid candles, only valid ones are stored."""
    candles = []
    start_ts = 1704067200000

    # Add valid candles
    for i in range(n_valid):
        candles.append(make_candle(
            period=start_ts + i * 3600000,
            close=100.0 + i,
            volume=1000.0 + i,
        ))

    # Add invalid candles (NaN or Inf)
    for i in range(n_invalid):
        candles.append({
            "open": "NaN" if i % 2 == 0 else "Infinity",
            "high": "110",
            "low": "90",
            "close": "105",
            "volume": "1000",
            "period": start_ts + (n_valid + i) * 3600000,
        })

    db_path = tmp_path / f"mixed_{n_valid}_{n_invalid}.db"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/bitcoin",
            json=make_asset_response(),
            status=200,
        )
        rsps.add(
            responses.GET,
            re.compile(rf"{COINCAP_BASE}/candles.*"),
            json=make_candles_response(candles),
            status=200,
        )

        result, _ = run_cli("backfill", "--assets", "bitcoin", db=db_path)

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]
    conn.close()

    assert count == n_valid, (
        f"Expected {n_valid} valid candles stored, got {count}"
    )
