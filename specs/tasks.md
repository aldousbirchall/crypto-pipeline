# Implementation Tasks: crypto-pipeline

## Critical Path

TASK-001 > TASK-002 > TASK-003 > TASK-005 > TASK-006 > TASK-008 > TASK-009 > TASK-010

The longest chain runs through project setup, storage, API client, metrics, backfill orchestration, refresh, and CLI integration. The streaming path (TASK-007) and query path (TASK-009) branch off from the middle of the chain.

## Tasks

### TASK-001: Project scaffolding and package structure
- **Component**: Project root
- **Files**: `src/crypto_pipeline/__init__.py`, `src/setup.py` (or `src/pyproject.toml`), `src/crypto_pipeline/cli.py` (stub), `src/crypto_pipeline/api_client.py` (stub), `src/crypto_pipeline/metrics.py` (stub), `src/crypto_pipeline/storage.py` (stub), `src/crypto_pipeline/pipeline.py` (stub), `src/crypto_pipeline/validation.py` (stub)
- **Dependencies**: None
- **Acceptance Criteria**: Package installs via `pip install -e src/`. Running `crypto-pipeline --help` prints usage without errors. All module stubs import without errors.
- **Complexity**: S
- **Notes**: Entry point: `console_scripts = ["crypto-pipeline = crypto_pipeline.cli:main"]`. Use `setup.py` or `pyproject.toml`. Runtime dependencies: requests, websocket-client.

### TASK-002: Storage layer (SQLite database)
- **Component**: storage.py
- **Files**: `src/crypto_pipeline/storage.py`
- **Dependencies**: TASK-001
- **Acceptance Criteria**: `Database` class creates all four tables on first connection. Supports context manager protocol. `upsert_asset`, `insert_candles`, `insert_tick`, `insert_metrics` write data correctly. `get_candles`, `get_metrics`, `get_latest_tick`, `get_latest_candle_period`, `get_ticks` return correct results. Duplicate candle inserts (same asset_id + period) are silently skipped. Works with both file and `:memory:` databases.
- **Complexity**: M
- **Notes**: Use `INSERT OR IGNORE` for candle deduplication. Use `INSERT OR REPLACE` for metrics and asset upserts. Schema from design.md. All timestamps: candle periods in milliseconds, tick timestamps in seconds (float).

### TASK-003: API client (CoinCap REST and WebSocket)
- **Component**: api_client.py
- **Files**: `src/crypto_pipeline/api_client.py`
- **Dependencies**: TASK-001
- **Acceptance Criteria**: `CoinCapClient` uses `requests.Session` for all HTTP requests. `get_asset()` fetches and returns asset metadata dict. `get_candles()` fetches candle data with correct query parameters (exchange=poloniex, quoteId=united-states-dollar). `stream_prices()` connects to WebSocket and invokes callback on each message. Constructor accepts optional `session` parameter for dependency injection. HTTP errors raise `requests.HTTPError`.
- **Complexity**: M
- **Notes**: All CoinCap numeric values arrive as strings; convert to float in the client. WebSocket message format: `{"bitcoin": "45123.45", "ethereum": "2345.67"}`. Use `websocket-client`'s `WebSocketApp` for the streaming connection. The `stop_event` parameter allows clean shutdown in tests and on SIGINT.

### TASK-004: Validation utilities
- **Component**: validation.py
- **Files**: `src/crypto_pipeline/validation.py`
- **Dependencies**: TASK-001
- **Acceptance Criteria**: `is_valid_number()` returns True for finite floats and valid numeric strings; returns False for inf, NaN, None, and non-numeric strings. `validate_candle()` checks all five OHLCV fields. `validate_tick()` checks a single price value. Uses `math.isfinite()`.
- **Complexity**: S
- **Notes**: Convert string values to float before calling `math.isfinite()`. Catch `ValueError` and `TypeError` from float conversion and return False.

### TASK-005: Metrics engine (pure computation)
- **Component**: metrics.py
- **Files**: `src/crypto_pipeline/metrics.py`
- **Dependencies**: TASK-001
- **Acceptance Criteria**: `compute_sma(closes, period)` returns correct SMA values with None for insufficient history. `compute_volatility(closes, period)` returns population standard deviation of log returns with None for insufficient history. `compute_vwap(highs, lows, closes, volumes, periods)` returns VWAP with daily reset based on UTC calendar day. `compute_all_metrics(candles)` orchestrates all metric computations and returns a list of metric dicts. All functions are pure (no I/O, no side effects). Empty input returns empty output.
- **Complexity**: M
- **Notes**: Log return = ln(close_t / close_{t-1}). Volatility uses population standard deviation (divides by N, not N-1). VWAP resets at UTC midnight boundaries. Use `math.log` for natural log. typical_price = (high + low + close) / 3. First valid volatility value is at index=period (need period+1 prices to get period log returns).

### TASK-006: Backfill orchestration
- **Component**: pipeline.py
- **Files**: `src/crypto_pipeline/pipeline.py`
- **Dependencies**: TASK-002, TASK-003, TASK-004, TASK-005
- **Acceptance Criteria**: `backfill()` fetches asset metadata and stores it, pulls 30 days of hourly candles from the API, validates each candle, computes all metrics, and stores candles and metrics. Invalid candles (containing NaN/inf) are skipped with a warning to stderr. API errors cause SystemExit with a message to stderr.
- **Complexity**: M
- **Notes**: Calculate start timestamp as `now - 30 days` in milliseconds. End timestamp is `now` in milliseconds. Process each asset sequentially. Log progress to stderr.

### TASK-007: Stream orchestration with reconnection
- **Component**: pipeline.py
- **Files**: `src/crypto_pipeline/pipeline.py`
- **Dependencies**: TASK-002, TASK-003, TASK-004
- **Acceptance Criteria**: `stream()` connects to the WebSocket, validates each incoming price tick, and stores valid ticks. On connection loss, reconnects with exponential backoff (initial 1s, doubling to max 60s). On successful reconnect, backoff resets. SIGINT (Ctrl+C) causes clean shutdown with exit code 0. Invalid ticks (inf/NaN) are skipped with a warning.
- **Complexity**: M
- **Notes**: Use `signal.signal(signal.SIGINT, handler)` or catch `KeyboardInterrupt` for clean shutdown. The `on_error` callback from the API client triggers the reconnection logic. Track backoff state in a local variable within `stream()`.

### TASK-008: Refresh orchestration (incremental update)
- **Component**: pipeline.py
- **Files**: `src/crypto_pipeline/pipeline.py`
- **Dependencies**: TASK-002, TASK-003, TASK-004, TASK-005
- **Acceptance Criteria**: `refresh()` queries the database for the most recent candle period per asset, fetches only newer candles from the API, validates and stores them, then recomputes metrics for the full candle set. If no new data is available, exits normally without inserting anything. API errors cause SystemExit.
- **Complexity**: M
- **Notes**: Metrics must be recomputed over the full candle history (not just new candles) because SMA and volatility windows may span old and new data. Use `get_latest_candle_period()` to determine the start parameter for the API call. Add 1 millisecond to avoid refetching the last stored candle.

### TASK-009: Query functions
- **Component**: pipeline.py
- **Files**: `src/crypto_pipeline/pipeline.py`
- **Dependencies**: TASK-002
- **Acceptance Criteria**: `query_latest()` returns the most recent tick for each specified asset, or an indicator for missing data. `query_history()` converts YYYY-MM-DD date strings to timestamps, queries candles and metrics within the range, and returns combined results. Missing metrics are represented as None.
- **Complexity**: S
- **Notes**: Date string to timestamp conversion: parse YYYY-MM-DD to datetime, convert to Unix milliseconds. Start of start_date to end of end_date (23:59:59.999). Join candle and metric data by (asset_id, period).

### TASK-010: CLI argument parsing and dispatch
- **Component**: cli.py
- **Files**: `src/crypto_pipeline/cli.py`
- **Dependencies**: TASK-006, TASK-007, TASK-008, TASK-009
- **Acceptance Criteria**: `parse_args()` handles all four subcommands with correct flags. `--assets` parses comma-separated string into a list, defaults to `["bitcoin", "ethereum"]`. `--db` defaults to `crypto_pipeline.db`. `query` subcommand has `latest` and `history` sub-subcommands. `history` requires `--start` and `--end` flags. `main()` dispatches to the correct pipeline function, prints output for query commands, and returns appropriate exit codes (0 for success, 1 for errors). `--help` works at all levels.
- **Complexity**: M
- **Notes**: Use `argparse` with subparsers. Add parent parser for shared `--assets` and `--db` flags. Format query output as tab-separated values. Wrap pipeline calls in try/except to catch SystemExit and unexpected exceptions, printing errors to stderr.

## Dependency Graph

```
TASK-001 (project scaffolding)
  +-- TASK-002 (storage)
  |     +-- TASK-006 (backfill) [also needs TASK-003, TASK-004, TASK-005]
  |     +-- TASK-007 (stream) [also needs TASK-003, TASK-004]
  |     +-- TASK-008 (refresh) [also needs TASK-003, TASK-004, TASK-005]
  |     +-- TASK-009 (query)
  +-- TASK-003 (API client)
  |     +-- TASK-006 (backfill)
  |     +-- TASK-007 (stream)
  |     +-- TASK-008 (refresh)
  +-- TASK-004 (validation)
  |     +-- TASK-006 (backfill)
  |     +-- TASK-007 (stream)
  |     +-- TASK-008 (refresh)
  +-- TASK-005 (metrics)
        +-- TASK-006 (backfill)
        +-- TASK-008 (refresh)

TASK-010 (CLI) depends on TASK-006, TASK-007, TASK-008, TASK-009
```

**Parallelisable groups** (after TASK-001 completes):
- Group A: TASK-002, TASK-003, TASK-004, TASK-005 (all independent of each other)
- Group B: TASK-006, TASK-007, TASK-008, TASK-009 (all depend on Group A; independent of each other)
- Group C: TASK-010 (depends on Group B)
