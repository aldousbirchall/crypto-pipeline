from __future__ import annotations

"""Orchestration layer: backfill, refresh, stream, and query workflows."""

import sys
import time
from datetime import datetime, timezone

from crypto_pipeline.api_client import CoinCapClient
from crypto_pipeline.metrics import compute_all_metrics
from crypto_pipeline.storage import Database
from crypto_pipeline.validation import validate_candle, validate_tick


def backfill(db: Database, client: CoinCapClient, assets: list[str], days: int = 30) -> None:
    """Full backfill: fetch asset metadata, pull candles for last `days` days,
    compute metrics, store all.

    Raises SystemExit on API errors.
    """
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (days * 24 * 60 * 60 * 1000)

    for asset_id in assets:
        print(f"Backfilling {asset_id}...", file=sys.stderr)

        # Fetch and store asset metadata
        try:
            asset_data = client.get_asset(asset_id)
            db.upsert_asset(
                asset_id=asset_data["id"],
                name=asset_data["name"],
                symbol=asset_data["symbol"],
                price_usd=asset_data["priceUsd"],
            )
        except Exception as e:
            print(f"Error fetching asset {asset_id}: {e}", file=sys.stderr)
            raise SystemExit(1)

        # Fetch candles
        try:
            raw_candles = client.get_candles(asset_id, interval="h1", start=start_ms, end=now_ms)
        except Exception as e:
            print(f"Error fetching candles for {asset_id}: {e}", file=sys.stderr)
            raise SystemExit(1)

        # Validate and filter candles
        valid_candles = []
        for c in raw_candles:
            if validate_candle(c):
                valid_candles.append(c)
            else:
                print(f"Warning: skipping invalid candle for {asset_id}: {c}", file=sys.stderr)

        # Store candles
        inserted = db.insert_candles(asset_id, valid_candles)
        print(f"  Inserted {inserted} candles for {asset_id}", file=sys.stderr)

        # Compute and store metrics over all candles
        all_candles = db.get_candles(asset_id)
        metrics = compute_all_metrics(all_candles)
        db.insert_metrics(asset_id, metrics)
        print(f"  Computed {len(metrics)} metrics for {asset_id}", file=sys.stderr)


def refresh(db: Database, client: CoinCapClient, assets: list[str]) -> None:
    """Incremental update: fetch candles since last stored period, recompute metrics, store.

    Raises SystemExit on API errors.
    """
    now_ms = int(time.time() * 1000)

    for asset_id in assets:
        print(f"Refreshing {asset_id}...", file=sys.stderr)

        # Fetch and store asset metadata
        try:
            asset_data = client.get_asset(asset_id)
            db.upsert_asset(
                asset_id=asset_data["id"],
                name=asset_data["name"],
                symbol=asset_data["symbol"],
                price_usd=asset_data["priceUsd"],
            )
        except Exception as e:
            print(f"Error fetching asset {asset_id}: {e}", file=sys.stderr)
            raise SystemExit(1)

        # Determine start point
        latest_period = db.get_latest_candle_period(asset_id)
        if latest_period is not None:
            start_ms = latest_period + 1  # Avoid refetching the last stored candle
        else:
            # No existing data; fetch 30 days
            start_ms = now_ms - (30 * 24 * 60 * 60 * 1000)

        # Fetch new candles
        try:
            raw_candles = client.get_candles(asset_id, interval="h1", start=start_ms, end=now_ms)
        except Exception as e:
            print(f"Error fetching candles for {asset_id}: {e}", file=sys.stderr)
            raise SystemExit(1)

        # Validate and filter
        valid_candles = []
        for c in raw_candles:
            if validate_candle(c):
                valid_candles.append(c)
            else:
                print(f"Warning: skipping invalid candle for {asset_id}: {c}", file=sys.stderr)

        if valid_candles:
            inserted = db.insert_candles(asset_id, valid_candles)
            print(f"  Inserted {inserted} new candles for {asset_id}", file=sys.stderr)
        else:
            print(f"  No new candles for {asset_id}", file=sys.stderr)

        # Recompute metrics over full candle history
        all_candles = db.get_candles(asset_id)
        if all_candles:
            metrics = compute_all_metrics(all_candles)
            db.insert_metrics(asset_id, metrics)
            print(f"  Recomputed {len(metrics)} metrics for {asset_id}", file=sys.stderr)


def stream(db: Database, client: CoinCapClient, assets: list[str]) -> None:
    """Start WebSocket stream. Reconnects with exponential backoff on disconnect.

    Runs until interrupted (SIGINT/Ctrl+C). Exits cleanly with code 0.
    """
    import threading

    backoff = 1.0
    max_backoff = 60.0
    stop_event = threading.Event()

    def on_message(asset_id: str, price: float, timestamp: float) -> None:
        nonlocal backoff
        if validate_tick(price):
            db.insert_tick(asset_id, price, timestamp)
        else:
            print(f"Warning: skipping invalid tick for {asset_id}: {price}", file=sys.stderr)
        # Reset backoff on successful message
        backoff = 1.0

    def on_error(error: Exception) -> None:
        print(f"WebSocket error: {error}", file=sys.stderr)

    print(f"Starting stream for {', '.join(assets)}...", file=sys.stderr)

    while not stop_event.is_set():
        try:
            client.stream_prices(assets, on_message, on_error, stop_event)
        except KeyboardInterrupt:
            stop_event.set()
            print("\nStream stopped.", file=sys.stderr)
            return
        except Exception as e:
            if stop_event.is_set():
                return
            print(f"Connection lost: {e}. Reconnecting in {backoff}s...", file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def query_latest(db: Database, assets: list[str]) -> list[dict]:
    """Return latest tick for each asset.

    Returns list of dicts or empty entries for missing assets.
    """
    results = []
    for asset_id in assets:
        tick = db.get_latest_tick(asset_id)
        if tick:
            results.append(tick)
        else:
            results.append({"asset_id": asset_id, "price": None, "timestamp": None})
    return results


def query_history(
    db: Database, assets: list[str], start: str, end: str
) -> dict[str, list[dict]]:
    """Return candle data with metrics for each asset in the given date range.

    start/end are YYYY-MM-DD strings, converted to timestamps internally.
    Returns {asset_id: [{"period": ..., "open": ..., ..., "sma_20": ..., ...}]}
    """
    # Parse dates to timestamps (milliseconds)
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, microsecond=999000, tzinfo=timezone.utc
    )
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    results: dict[str, list[dict]] = {}
    for asset_id in assets:
        candles = db.get_candles(asset_id, start=start_ms, end=end_ms)
        metrics_list = db.get_metrics(asset_id, start=start_ms, end=end_ms)

        # Index metrics by period for joining
        metrics_by_period = {m["period"]: m for m in metrics_list}

        combined = []
        for c in candles:
            row = {
                "period": c["period"],
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c["volume"],
            }
            m = metrics_by_period.get(c["period"], {})
            row["sma_20"] = m.get("sma_20")
            row["sma_50"] = m.get("sma_50")
            row["volatility"] = m.get("volatility")
            row["vwap"] = m.get("vwap")
            combined.append(row)

        results[asset_id] = combined

    return results
