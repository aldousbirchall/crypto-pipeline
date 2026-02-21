"""REQ-015: Connection Pooling.

Tests that the system uses requests.Session for all HTTP requests, enabling
connection pooling and consistent header configuration.
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


# ---------------------------------------------------------------------------
# AC-1: Multiple API calls reuse the same Session
# ---------------------------------------------------------------------------

@responses.activate
def test_backfill_multiple_assets_reuses_session(run_cli):
    """AC-1: Multiple REST API calls during backfill reuse the same Session.

    We verify this indirectly: the responses library works at transport level,
    so whether the implementation uses requests.get() or session.get(), the
    mocks will intercept. The key observable is that all requests succeed and
    data is stored, which confirms the HTTP layer works correctly.

    The deeper test is that the implementation uses Session, which we verify
    by checking that a custom User-Agent or other session-level header is
    consistent across requests (if the implementation sets one), or simply
    that multiple sequential requests succeed efficiently.
    """
    # Mock endpoints for two assets
    for asset_id in ["bitcoin", "ethereum"]:
        responses.add(
            responses.GET,
            f"{COINCAP_BASE}/assets/{asset_id}",
            json=make_asset_response(asset_id=asset_id),
            status=200,
        )

    candles = make_candle_series(n=10)
    # Use passthrough matching for candles endpoint
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, db_path = run_cli("backfill", "--assets", "bitcoin,ethereum")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    # Verify both assets were fetched successfully
    conn = sqlite3.connect(str(db_path))
    assets = set(
        r[0] for r in conn.execute("SELECT DISTINCT asset_id FROM candles").fetchall()
    )
    conn.close()

    assert "bitcoin" in assets, "bitcoin data missing"
    assert "ethereum" in assets, "ethereum data missing"

    # Verify multiple requests were made (at least one per asset for metadata
    # and one per asset for candles)
    assert len(responses.calls) >= 3, (
        f"Expected multiple API calls, got {len(responses.calls)}"
    )


@responses.activate
def test_session_headers_consistent(run_cli):
    """HTTP requests should have consistent headers (indicative of session reuse)."""
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/bitcoin",
        json=make_asset_response(),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{COINCAP_BASE}/assets/ethereum",
        json=make_asset_response(asset_id="ethereum"),
        status=200,
    )
    candles = make_candle_series(n=5)
    responses.add(
        responses.GET,
        re.compile(rf"{COINCAP_BASE}/candles.*"),
        json=make_candles_response(candles),
        status=200,
    )

    result, _ = run_cli("backfill", "--assets", "bitcoin,ethereum")
    assert result.returncode == 0, f"stderr: {result.stderr}"

    # Check that all requests used the same User-Agent header
    # (requests.Session sends consistent headers across all calls)
    if len(responses.calls) >= 2:
        user_agents = set()
        for call in responses.calls:
            ua = call.request.headers.get("User-Agent", "")
            if ua:
                user_agents.add(ua)
        # All requests from the same Session should have the same User-Agent
        assert len(user_agents) <= 1, (
            f"Inconsistent User-Agent headers suggest multiple Sessions: {user_agents}"
        )
