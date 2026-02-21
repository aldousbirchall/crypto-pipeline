# Status: crypto-pipeline

## Phase: Delivered

Build complete. All phases passed. Delivered 2026-02-21.

## Results

- **Holdout tests**: 89/89 passed
- **Verify iterations**: 2
- **Review verdict**: ACCEPT WITH ISSUES (0 critical, 2 major, 8 minor)

## What Was Built

An ETL and streaming data pipeline for cryptocurrency market data using the CoinCap.io API. Three modes of operation:

1. **Backfill**: pulls 30 days of hourly OHLCV candle data from CoinCap REST API
2. **Stream**: connects to CoinCap WebSocket for real-time price ticks with auto-reconnection
3. **Query**: CLI to query local SQLite database for latest prices and historical data with computed metrics (SMA-20, SMA-50, volatility, VWAP)

## Files

```
src/crypto_pipeline/
  __init__.py, __main__.py    # Package entry points
  cli.py                      # argparse CLI with subcommands
  api_client.py               # CoinCap REST + WebSocket client
  storage.py                  # SQLite storage layer
  metrics.py                  # SMA, volatility, VWAP computation
  pipeline.py                 # Orchestration (backfill, refresh, stream, query)
  validation.py               # Numeric validation (NaN, Inf filtering)
  setup.py                    # Package setup
  Makefile                    # Build targets
```

## Known Issues (from review)

- CP-001: Running `stream` without prior `backfill` hits FK constraint on ticks table
- C-001: Backoff reset happens on first message rather than on connection open
