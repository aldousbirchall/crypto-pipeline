from __future__ import annotations

"""CLI entry point for crypto-pipeline."""

import argparse
import sys

from crypto_pipeline.api_client import PROVIDERS, DEFAULT_PROVIDER, get_provider
from crypto_pipeline.pipeline import (
    backfill,
    query_history,
    query_latest,
    refresh,
    stream,
)
from crypto_pipeline.storage import Database


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    # Parent parser for shared flags
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "--assets",
        type=str,
        default="bitcoin,ethereum",
        help="Comma-separated list of asset IDs (default: bitcoin,ethereum)",
    )
    parent.add_argument(
        "--db",
        type=str,
        default="crypto_pipeline.db",
        help="Path to SQLite database file (default: crypto_pipeline.db)",
    )
    parent.add_argument(
        "--provider",
        type=str,
        default=DEFAULT_PROVIDER,
        choices=sorted(PROVIDERS),
        help=f"Data provider (default: {DEFAULT_PROVIDER})",
    )

    # Main parser
    parser = argparse.ArgumentParser(
        prog="crypto-pipeline",
        description="Cryptocurrency market data pipeline",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # backfill
    subparsers.add_parser("backfill", parents=[parent], help="Backfill historical data")

    # refresh
    subparsers.add_parser("refresh", parents=[parent], help="Incremental data update")

    # stream
    subparsers.add_parser("stream", parents=[parent], help="Stream real-time prices")

    # query (with sub-subcommands)
    query_parser = subparsers.add_parser("query", help="Query stored data")
    query_sub = query_parser.add_subparsers(dest="query_command")
    query_sub.required = True

    # query latest
    query_sub.add_parser("latest", parents=[parent], help="Show latest prices")

    # query history
    history_parser = query_sub.add_parser(
        "history", parents=[parent], help="Show historical data"
    )
    history_parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    history_parser.add_argument(
        "--end", type=str, required=True, help="End date (YYYY-MM-DD)"
    )

    args = parser.parse_args(argv)

    # Parse comma-separated assets into list
    if hasattr(args, "assets"):
        args.assets = [a.strip() for a in args.assets.split(",")]

    # Set defaults for query-related fields when not present
    if not hasattr(args, "query_command"):
        args.query_command = None
    if not hasattr(args, "start"):
        args.start = None
    if not hasattr(args, "end"):
        args.end = None

    return args


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns exit code (0 success, 1 error)."""
    try:
        args = parse_args(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1

    try:
        if args.command == "backfill":
            with Database(args.db) as db:
                client = get_provider(args.provider)
                backfill(db, client, args.assets)
            return 0

        elif args.command == "refresh":
            with Database(args.db) as db:
                client = get_provider(args.provider)
                refresh(db, client, args.assets)
            return 0

        elif args.command == "stream":
            with Database(args.db) as db:
                client = get_provider(args.provider)
                stream(db, client, args.assets)
            return 0

        elif args.command == "query":
            with Database(args.db) as db:
                if args.query_command == "latest":
                    results = query_latest(db, args.assets)
                    for r in results:
                        price = r["price"] if r["price"] is not None else "N/A"
                        ts = r["timestamp"] if r["timestamp"] is not None else "N/A"
                        print(f"{r['asset_id']}\t{price}\t{ts}")
                    return 0

                elif args.query_command == "history":
                    results = query_history(db, args.assets, args.start, args.end)
                    # Check if all assets returned empty results
                    if all(len(rows) == 0 for rows in results.values()):
                        print("No data found for the requested range.")
                        return 0
                    # Print header
                    print("ASSET\tPERIOD\tOPEN\tHIGH\tLOW\tCLOSE\tVOLUME\tSMA_20\tSMA_50\tVOLATILITY\tVWAP")
                    for asset_id, rows in results.items():
                        for row in rows:
                            sma_20 = row["sma_20"] if row["sma_20"] is not None else "N/A"
                            sma_50 = row["sma_50"] if row["sma_50"] is not None else "N/A"
                            volatility = row["volatility"] if row["volatility"] is not None else "N/A"
                            vwap = row["vwap"] if row["vwap"] is not None else "N/A"
                            print(
                                f"{asset_id}\t{row['period']}\t{row['open']}\t{row['high']}\t"
                                f"{row['low']}\t{row['close']}\t{row['volume']}\t"
                                f"{sma_20}\t{sma_50}\t{volatility}\t{vwap}"
                            )
                    return 0

        print(f"Unknown command: {args.command}", file=sys.stderr)
        return 1

    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
