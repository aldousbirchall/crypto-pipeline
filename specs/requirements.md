# Requirements: crypto-pipeline

## Overview

A CLI data pipeline that ingests cryptocurrency market data from the CoinCap.io API, computes derived financial metrics, stores everything in a local SQLite database, and exposes a query interface. Three modes of operation: ETL backfill of historical candle data, real-time WebSocket streaming of price ticks, and CLI-based querying of stored data.

## Requirements

### REQ-001: Backfill Historical Candle Data
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the user runs `crypto-pipeline backfill` THE system SHALL pull hourly OHLCV candle data for the last 30 days from the CoinCap REST API for each specified asset and store it in the local SQLite database.
- **Acceptance Criteria**:
  1. GIVEN a fresh database WHEN `crypto-pipeline backfill --assets bitcoin` is run THEN the candles table contains hourly candle records for bitcoin spanning approximately 30 days.
  2. GIVEN a fresh database WHEN `crypto-pipeline backfill` is run with default assets THEN candle data for both bitcoin and ethereum is stored.
  3. GIVEN candle data returned by the API WHEN each candle is stored THEN the record contains open, high, low, close, volume, and period (timestamp) fields.
  4. GIVEN a successful backfill WHEN the database is queried THEN no duplicate candles exist for the same asset and period.
- **Dependencies**: REQ-007, REQ-009

### REQ-002: Compute Derived Metrics
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN candle data is loaded or refreshed THE system SHALL compute SMA-20, SMA-50, rolling volatility (20-period standard deviation of log returns), and VWAP for each asset and store the results in the database.
- **Acceptance Criteria**:
  1. GIVEN at least 50 candle records for an asset WHEN metrics are computed THEN SMA-20 and SMA-50 values are present for every candle from the 20th and 50th onwards respectively.
  2. GIVEN candle data WHEN SMA-20 is computed for a candle THEN its value equals the arithmetic mean of the closing prices of that candle and the preceding 19 candles.
  3. GIVEN candle data WHEN SMA-50 is computed for a candle THEN its value equals the arithmetic mean of the closing prices of that candle and the preceding 49 candles.
  4. GIVEN candle data WHEN rolling volatility is computed for a candle THEN its value equals the population standard deviation of the log returns over the preceding 20 periods (including current).
  5. GIVEN candle data WHEN VWAP is computed for a candle THEN its value equals the cumulative sum of (typical_price * volume) divided by the cumulative sum of volume, where typical_price = (high + low + close) / 3, computed over the full day (resetting daily).
  6. GIVEN fewer than 20 candle records for an asset WHEN metrics are computed THEN SMA-20 and volatility are NULL for candles where insufficient history exists.
  7. GIVEN fewer than 50 candle records for an asset WHEN metrics are computed THEN SMA-50 is NULL for candles where insufficient history exists.
- **Dependencies**: REQ-001

### REQ-003: Stream Real-Time Price Ticks
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the user runs `crypto-pipeline stream` THE system SHALL connect to the CoinCap WebSocket endpoint and write each received price tick to the SQLite database with a timestamp.
- **Acceptance Criteria**:
  1. GIVEN a running stream WHEN a price tick is received for an asset THEN a record is inserted into the ticks table containing the asset name, price, and receipt timestamp.
  2. GIVEN a running stream WHEN the user presses Ctrl+C THEN the WebSocket connection is closed cleanly and the process exits with code 0.
  3. GIVEN the stream is running WHEN price ticks arrive for multiple assets THEN each asset's tick is stored as a separate record.
- **Dependencies**: REQ-007, REQ-009

### REQ-004: Automatic Reconnection with Exponential Backoff
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHILE the stream command is running, IF the WebSocket connection is lost THEN the system SHALL reconnect automatically using exponential backoff.
- **Acceptance Criteria**:
  1. GIVEN a lost WebSocket connection WHEN the first reconnect attempt is made THEN it occurs after approximately 1 second.
  2. GIVEN repeated reconnection failures WHEN successive attempts are made THEN the delay doubles each time up to a maximum of 60 seconds.
  3. GIVEN a successful reconnection WHEN the next tick arrives THEN it is stored in the database as normal.
  4. GIVEN a reconnection attempt WHEN it succeeds THEN the backoff delay resets to its initial value.
- **Dependencies**: REQ-003

### REQ-005: Query Latest Prices
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the user runs `crypto-pipeline query latest --assets bitcoin` THE system SHALL display the most recent price for each specified asset from the database.
- **Acceptance Criteria**:
  1. GIVEN tick data exists for bitcoin WHEN `query latest --assets bitcoin` is run THEN the output contains the asset name, latest price, and timestamp.
  2. GIVEN tick data exists for multiple assets WHEN `query latest` is run with default assets THEN the output contains the latest price for each default asset.
  3. GIVEN no data exists for an asset WHEN `query latest --assets dogecoin` is run THEN the output indicates no data is available for that asset.
- **Dependencies**: REQ-007, REQ-009

### REQ-006: Query Historical Prices and Metrics
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the user runs `crypto-pipeline query history --assets bitcoin --start YYYY-MM-DD --end YYYY-MM-DD` THE system SHALL display candle data and computed metrics for the specified asset within the given time window.
- **Acceptance Criteria**:
  1. GIVEN candle data and metrics exist for bitcoin WHEN `query history --assets bitcoin --start 2026-01-01 --end 2026-01-31` is run THEN the output contains candle records with their associated SMA-20, SMA-50, volatility, and VWAP values.
  2. GIVEN a valid time window WHEN query history is run THEN only records within the start and end dates (inclusive) are returned.
  3. GIVEN no candle data exists in the specified window WHEN query history is run THEN the output indicates no data is available.
  4. GIVEN metrics are NULL for early candles (insufficient history) WHEN query history is run THEN NULL metrics are displayed as empty or "N/A".
- **Dependencies**: REQ-002, REQ-007, REQ-009

### REQ-007: Asset Selection Flag
- **Priority**: Must
- **Type**: Functional
- **Statement**: THE system SHALL accept a `--assets` flag on all subcommands, taking a comma-separated list of asset identifiers, defaulting to `bitcoin,ethereum`.
- **Acceptance Criteria**:
  1. GIVEN `--assets bitcoin,ethereum,litecoin` WHEN any subcommand is run THEN the command operates on bitcoin, ethereum, and litecoin.
  2. GIVEN no `--assets` flag WHEN any subcommand is run THEN the command operates on bitcoin and ethereum.
  3. GIVEN `--assets` with a single asset WHEN any subcommand is run THEN the command operates on that single asset.

### REQ-008: Database Path Flag
- **Priority**: Must
- **Type**: Functional
- **Statement**: THE system SHALL accept a `--db` flag on all subcommands to specify the SQLite database file path, defaulting to `crypto_pipeline.db` in the current working directory.
- **Acceptance Criteria**:
  1. GIVEN `--db /tmp/test.db` WHEN any subcommand is run THEN the specified file is used as the database.
  2. GIVEN no `--db` flag WHEN any subcommand is run THEN `crypto_pipeline.db` in the current directory is used.
  3. GIVEN `--db` points to a non-existent file WHEN a subcommand is run THEN the database file is created automatically.

### REQ-009: Database Schema Initialisation
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the system connects to a database for the first time THE system SHALL create the required tables if they do not already exist.
- **Acceptance Criteria**:
  1. GIVEN a new database file WHEN the system connects THEN all required tables (candles, ticks, metrics) are created.
  2. GIVEN an existing database with tables already present WHEN the system connects THEN no tables are dropped or recreated.

### REQ-010: ETL Refresh (Incremental Update)
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN the user runs `crypto-pipeline refresh` THE system SHALL pull candle data from the CoinCap API starting from the most recent stored timestamp for each asset, compute metrics for the new data, and store the results.
- **Acceptance Criteria**:
  1. GIVEN existing candle data with the most recent candle at timestamp T WHEN refresh is run THEN only candles with period > T are fetched from the API.
  2. GIVEN new candles are fetched WHEN refresh completes THEN derived metrics are recomputed for the updated dataset.
  3. GIVEN no new data is available from the API WHEN refresh is run THEN no new records are inserted and the command exits normally.
- **Dependencies**: REQ-001, REQ-002, REQ-009

### REQ-011: CLI Entry Point
- **Priority**: Must
- **Type**: Functional
- **Statement**: THE system SHALL provide a CLI entry point named `crypto-pipeline` with subcommands: `backfill`, `stream`, `query`, and `refresh`.
- **Acceptance Criteria**:
  1. GIVEN the package is installed WHEN `crypto-pipeline --help` is run THEN the output lists all four subcommands with brief descriptions.
  2. GIVEN an invalid subcommand WHEN the user runs it THEN the system prints an error message and exits with a non-zero exit code.
  3. GIVEN the `query` subcommand WHEN `crypto-pipeline query --help` is run THEN the output lists `latest` and `history` as sub-subcommands.

### REQ-012: Asset Metadata Storage
- **Priority**: Must
- **Type**: Functional
- **Statement**: WHEN backfill or refresh is run THE system SHALL fetch and store asset metadata (name, symbol, current price) from the CoinCap REST API for each specified asset.
- **Acceptance Criteria**:
  1. GIVEN `crypto-pipeline backfill --assets bitcoin` is run WHEN the API returns asset metadata THEN the assets table contains a record for bitcoin with name, symbol, and price_usd fields.
  2. GIVEN asset metadata already exists WHEN backfill or refresh is run THEN the metadata is updated (upserted), not duplicated.
- **Dependencies**: REQ-007, REQ-009

## Non-Functional Requirements

### REQ-013: API Error Handling
- **Priority**: Must
- **Type**: Non-Functional
- **Statement**: IF the CoinCap REST API returns an error (HTTP 4xx or 5xx) or is unreachable THEN the system SHALL log the error to stderr and exit with a non-zero exit code (for backfill/refresh) or retry with backoff (for stream).
- **Acceptance Criteria**:
  1. GIVEN the REST API returns HTTP 429 (rate limited) WHEN backfill is running THEN the error is logged to stderr and the process exits with a non-zero code.
  2. GIVEN the REST API is unreachable WHEN backfill is running THEN a connection error is logged to stderr and the process exits with a non-zero code.

### REQ-014: Numeric Validation
- **Priority**: Must
- **Type**: Non-Functional
- **Statement**: WHEN price or volume data is received from the API THE system SHALL validate that values are finite numbers, rejecting inf and NaN.
- **Acceptance Criteria**:
  1. GIVEN an API response containing a NaN price value WHEN the data is processed THEN the record is skipped and a warning is logged to stderr.
  2. GIVEN an API response containing an inf volume value WHEN the data is processed THEN the record is skipped and a warning is logged to stderr.
  3. GIVEN valid numeric values WHEN the data is processed THEN records are stored normally.
- **Dependencies**: REQ-001, REQ-003

### REQ-015: Connection Pooling
- **Priority**: Should
- **Type**: Non-Functional
- **Statement**: THE system SHALL use `requests.Session` for all HTTP requests to enable connection pooling and consistent header configuration.
- **Acceptance Criteria**:
  1. GIVEN multiple REST API calls during a backfill WHEN requests are made THEN they reuse the same Session object.

## Glossary

| Term | Definition |
|---|---|
| OHLCV | Open, High, Low, Close, Volume. The five standard data points in a price candle. |
| Candle | A data record summarising price action over a fixed time interval, containing OHLCV data and a period timestamp. |
| SMA | Simple Moving Average. The unweighted arithmetic mean of closing prices over a specified number of periods. SMA-N uses the most recent N periods. |
| VWAP | Volume-Weighted Average Price. The ratio of cumulative (typical_price * volume) to cumulative volume, where typical_price = (high + low + close) / 3. Resets daily. |
| Volatility (rolling) | The population standard deviation of log returns over a rolling window of N periods. Log return for period t = ln(close_t / close_{t-1}). |
| Log return | The natural logarithm of the ratio of consecutive closing prices: ln(close_t / close_{t-1}). |
| Tick | A single real-time price update received via WebSocket, representing the current price of an asset at a point in time. |
| Backfill | The process of fetching historical data to populate the database from scratch. |
| Refresh | An incremental update that fetches only data newer than the most recent stored record. |
| ETL | Extract, Transform, Load. The pattern of pulling data from a source, transforming it, and loading it into a target store. |
