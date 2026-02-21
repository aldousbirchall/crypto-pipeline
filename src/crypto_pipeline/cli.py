from __future__ import annotations

"""CLI entry point for crypto-pipeline."""

import argparse
import sys


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        prog="crypto-pipeline",
        description="Cryptocurrency market data pipeline",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    # Placeholder subcommands
    subparsers.add_parser("backfill", help="Backfill historical data")
    subparsers.add_parser("refresh", help="Incremental data update")
    subparsers.add_parser("stream", help="Stream real-time prices")
    subparsers.add_parser("query", help="Query stored data")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns exit code (0 success, 1 error)."""
    try:
        args = parse_args(argv)
        print(f"Command: {args.command}", file=sys.stderr)
        return 0
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
