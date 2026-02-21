# Design: crypto-pipeline

## Architecture Overview

The system is structured as four layers with strict separation to enable independent testing:

```
┌──────────────────────────────────────────────┐
│                   CLI Layer                   │
│         (argparse, subcommand routing)        │
├──────────────────────────────────────────────┤
│              Orchestration Layer              │
│   (backfill, refresh, stream, query logic)   │
├─────────────────┬────────────────────────────┤
│   API Client    │     Metrics Engine         │
│  (REST + WS)    │  (pure functions, no I/O)  │
├─────────────────┴────────────────────────────┤
│               Storage Layer                   │
│         (SQLite via sqlite3 stdlib)           │
└──────────────────────────────────────────────┘
```

**API Client**: thin wrappers around `requests.Session` (REST) and `websocket-client` (WebSocket). No business logic. Easily mockable.

**Metrics Engine**: pure functions that accept lists of candle data and return computed metrics. No I/O, no database, no network. Independently testable.

**Storage Layer**: all SQLite operations. Schema creation, inserts, queries. Testable with in-memory databases.

**Orchestration Layer**: wires the other three together. Calls the API client, feeds data through the metrics engine, writes to storage.

**CLI Layer**: parses arguments and dispatches to the orchestration layer.

## Technology Decisions

- **Language**: Python 3.9+
- **HTTP Client**: requests (via `requests.Session` for connection pooling)
- **WebSocket Client**: websocket-client
- **Storage**: sqlite3 (stdlib), no ORM
- **CLI Framework**: argparse (stdlib)
- **Package Manager**: pip
- **Test Framework**: pytest + hypothesis + responses (test mocking)
- **Rationale**: Minimal dependency footprint. All external runtime dependencies (requests, websocket-client) are well-established. sqlite3 is stdlib and requires no server. argparse is sufficient for a subcommand CLI.

## Components

### CLI (`cli.py`)
- **Responsibility**: Parse command-line arguments, configure logging, dispatch to orchestration functions.
- **Interface**:
  ```python
  def main(argv: list[str] | None = None) -> int:
      """Entry point. Returns exit code (0 success, 1 error)."""

  def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
      """Parse CLI arguments. Returns namespace with:
         - command: str ("backfill" | "stream" | "query" | "refresh")
         - query_command: str | None ("latest" | "history", only for query)
         - assets: list[str] (parsed from comma-separated string)
         - db: str (database file path)
         - start: str | None (YYYY-MM-DD, query history only)
         - end: str | None (YYYY-MM-DD, query history only)
      """
  ```
- **Dependencies**: orchestration layer

### API Client (`api_client.py`)
- **Responsibility**: All HTTP and WebSocket communication with CoinCap. No data transformation.
- **Interface**:
  ```python
  class CoinCapClient:
      BASE_URL: str = "https://api.coincap.io/v2"
      WS_URL: str = "wss://ws.coincap.io/prices"

      def __init__(self, session: requests.Session | None = None):
          """Accept optional Session for dependency injection in tests."""

      def get_asset(self, asset_id: str) -> dict:
          """GET /v2/assets/{asset_id}
          Returns: {"id": str, "name": str, "symbol": str, "priceUsd": str, ...}
          Raises: requests.HTTPError on 4xx/5xx, requests.ConnectionError on network failure.
          """

      def get_candles(
          self,
          asset_id: str,
          interval: str = "h1",
          start: int | None = None,
          end: int | None = None,
      ) -> list[dict]:
          """GET /v2/candles?exchange=poloniex&interval={interval}&baseId={asset_id}&quoteId=united-states-dollar&start={start}&end={end}
          start/end are Unix timestamps in milliseconds.
          Returns: list of {"open": str, "high": str, "low": str, "close": str, "volume": str, "period": int}
          Raises: requests.HTTPError, requests.ConnectionError.
          """

      def stream_prices(
          self,
          assets: list[str],
          on_message: Callable[[str, float, float], None],
          on_error: Callable[[Exception], None],
          stop_event: threading.Event | None = None,
      ) -> None:
          """Connect to WebSocket and call on_message(asset, price, timestamp) for each tick.
          on_error is called on connection loss.
          If stop_event is set, close the connection and return.
          Blocking call.
          """
  ```
- **Dependencies**: requests, websocket-client

### Metrics Engine (`metrics.py`)
- **Responsibility**: Pure computation of derived financial metrics from candle data. No I/O.
- **Interface**:
  ```python
  def compute_sma(closes: list[float], period: int) -> list[float | None]:
      """Compute Simple Moving Average.
      Returns list same length as closes.
      Values are None where insufficient history (index < period - 1).
      """

  def compute_volatility(closes: list[float], period: int = 20) -> list[float | None]:
      """Compute rolling volatility as population std dev of log returns.
      Returns list same length as closes.
      Values are None where insufficient history (index < period).
      First log return requires two prices, so first valid volatility is at index=period.
      """

  def compute_vwap(
      highs: list[float],
      lows: list[float],
      closes: list[float],
      volumes: list[float],
      periods: list[int],
  ) -> list[float | None]:
      """Compute VWAP with daily reset.
      typical_price = (high + low + close) / 3
      VWAP resets at each new calendar day (UTC, determined from period timestamps).
      Returns list same length as input lists.
      Returns None for any period where cumulative volume is zero.
      """

  def compute_all_metrics(candles: list[dict]) -> list[dict]:
      """Compute all metrics for a sorted (by period) list of candle dicts.
      Each input dict: {"open": float, "high": float, "low": float, "close": float, "volume": float, "period": int}
      Returns list of dicts: {"period": int, "sma_20": float|None, "sma_50": float|None, "volatility": float|None, "vwap": float|None}
      """
  ```
- **Dependencies**: math (stdlib) only

### Storage Layer (`storage.py`)
- **Responsibility**: All SQLite database operations. Schema management, inserts, queries.
- **Interface**:
  ```python
  class Database:
      def __init__(self, db_path: str):
          """Open database connection. Create tables if they don't exist."""

      def close(self) -> None:
          """Close the database connection."""

      def __enter__(self) -> "Database":
          ...
      def __exit__(self, *args) -> None:
          self.close()

      # --- Schema ---
      def init_schema(self) -> None:
          """Create tables if they do not exist. Idempotent."""

      # --- Assets ---
      def upsert_asset(self, asset_id: str, name: str, symbol: str, price_usd: float) -> None:
          """Insert or update asset metadata."""

      # --- Candles ---
      def insert_candles(self, asset_id: str, candles: list[dict]) -> int:
          """Bulk insert candle records. Skips duplicates (same asset_id + period).
          Returns number of records inserted.
          """

      def get_latest_candle_period(self, asset_id: str) -> int | None:
          """Return the most recent period timestamp for the given asset, or None."""

      def get_candles(
          self, asset_id: str, start: int | None = None, end: int | None = None
      ) -> list[dict]:
          """Return candle records for asset, optionally filtered by period range.
          Returns list of dicts with keys: open, high, low, close, volume, period.
          Sorted by period ascending.
          """

      # --- Metrics ---
      def insert_metrics(self, asset_id: str, metrics: list[dict]) -> None:
          """Bulk insert/replace metric records for an asset.
          Each dict: {"period": int, "sma_20": float|None, "sma_50": float|None, "volatility": float|None, "vwap": float|None}
          """

      def get_metrics(
          self, asset_id: str, start: int | None = None, end: int | None = None
      ) -> list[dict]:
          """Return metric records for asset, optionally filtered by period range.
          Sorted by period ascending.
          """

      # --- Ticks ---
      def insert_tick(self, asset_id: str, price: float, timestamp: float) -> None:
          """Insert a single real-time price tick."""

      def get_latest_tick(self, asset_id: str) -> dict | None:
          """Return the most recent tick for the given asset, or None.
          Returns dict: {"asset_id": str, "price": float, "timestamp": float}
          """

      def get_ticks(
          self, asset_id: str, start: float | None = None, end: float | None = None
      ) -> list[dict]:
          """Return tick records for asset, optionally filtered by timestamp range."""
  ```
- **Dependencies**: sqlite3 (stdlib)

### Orchestration (`pipeline.py`)
- **Responsibility**: Wire API client, metrics engine, and storage together. Implement backfill, refresh, stream, and query workflows.
- **Interface**:
  ```python
  def backfill(db: Database, client: CoinCapClient, assets: list[str], days: int = 30) -> None:
      """Full backfill: fetch asset metadata, pull candles for last `days` days, compute metrics, store all.
      Raises SystemExit on API errors.
      """

  def refresh(db: Database, client: CoinCapClient, assets: list[str]) -> None:
      """Incremental update: fetch candles since last stored period, recompute metrics, store.
      Raises SystemExit on API errors.
      """

  def stream(db: Database, client: CoinCapClient, assets: list[str]) -> None:
      """Start WebSocket stream. Reconnects with exponential backoff on disconnect.
      Runs until interrupted (SIGINT/Ctrl+C). Exits cleanly with code 0.
      """

  def query_latest(db: Database, assets: list[str]) -> list[dict]:
      """Return latest tick for each asset. Returns list of dicts or empty entries for missing assets."""

  def query_history(
      db: Database, assets: list[str], start: str, end: str
  ) -> dict[str, list[dict]]:
      """Return candle data with metrics for each asset in the given date range.
      start/end are YYYY-MM-DD strings, converted to timestamps internally.
      Returns {asset_id: [{"period": ..., "open": ..., ..., "sma_20": ..., ...}]}
      """
  ```
- **Dependencies**: api_client, metrics, storage

### Validation Utilities (`validation.py`)
- **Responsibility**: Numeric validation of API data.
- **Interface**:
  ```python
  def is_valid_number(value) -> bool:
      """Return True if value can be converted to a finite float. Rejects inf, NaN, and non-numeric."""

  def validate_candle(candle: dict) -> bool:
      """Return True if all numeric fields (open, high, low, close, volume) are valid finite numbers."""

  def validate_tick(price) -> bool:
      """Return True if price is a valid finite number."""
  ```
- **Dependencies**: math (stdlib)

## Data Model

### SQLite Schema

```sql
CREATE TABLE IF NOT EXISTS assets (
    asset_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    price_usd   REAL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS candles (
    asset_id    TEXT NOT NULL,
    period      INTEGER NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      REAL NOT NULL,
    PRIMARY KEY (asset_id, period),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE TABLE IF NOT EXISTS metrics (
    asset_id    TEXT NOT NULL,
    period      INTEGER NOT NULL,
    sma_20      REAL,
    sma_50      REAL,
    volatility  REAL,
    vwap        REAL,
    PRIMARY KEY (asset_id, period),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id    TEXT NOT NULL,
    price       REAL NOT NULL,
    timestamp   REAL NOT NULL,
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
);
CREATE INDEX IF NOT EXISTS idx_candles_asset_period ON candles(asset_id, period);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_ts ON ticks(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_asset_period ON metrics(asset_id, period);
```

**Column notes**:
- `assets.updated_at`: Unix timestamp of last metadata update.
- `candles.period`: Unix timestamp in milliseconds (matches CoinCap API format).
- `metrics.period`: Foreign key to candles.period for the same asset.
- `metrics.sma_20`, `sma_50`, `volatility`, `vwap`: NULL where insufficient history exists.
- `ticks.timestamp`: Unix timestamp in seconds (float, from local clock at receipt).

### Relationships

- **assets 1:N candles**: One asset has many candle records (one per hourly period).
- **assets 1:N ticks**: One asset has many tick records (one per WebSocket message).
- **candles 1:1 metrics**: Each candle period has at most one metrics record.

## CoinCap API Contract

**Assumption**: These response shapes are based on CoinCap API v2 documentation. The review agent should verify these against the live API or current documentation.

### GET /v2/assets/{id}
- **Purpose**: Fetch metadata for a single asset.
- **Example**: `GET https://api.coincap.io/v2/assets/bitcoin`
- **Response** (200):
  ```json
  {
    "data": {
      "id": "bitcoin",
      "rank": "1",
      "symbol": "BTC",
      "name": "Bitcoin",
      "supply": "19000000.0000000000000000",
      "maxSupply": "21000000.0000000000000000",
      "marketCapUsd": "850000000000.00",
      "volumeUsd24Hr": "15000000000.00",
      "priceUsd": "45000.0000000000000000",
      "changePercent24Hr": "2.50",
      "vwap24Hr": "44500.0000000000000000",
      "explorer": "https://blockchain.info/"
    },
    "timestamp": 1700000000000
  }
  ```
- **Fields used**: `id`, `name`, `symbol`, `priceUsd`

### GET /v2/candles
- **Purpose**: Fetch historical OHLCV candle data.
- **Parameters**:
  - `exchange` (required): Exchange identifier (use `poloniex`)
  - `interval` (required): Candle interval (`m1`, `m5`, `m15`, `m30`, `h1`, `h2`, `h4`, `h8`, `h12`, `d1`, `w1`)
  - `baseId` (required): Asset identifier (e.g., `bitcoin`)
  - `quoteId` (required): Quote currency (use `united-states-dollar`)
  - `start` (optional): Start time in Unix milliseconds
  - `end` (optional): End time in Unix milliseconds
- **Example**: `GET https://api.coincap.io/v2/candles?exchange=poloniex&interval=h1&baseId=bitcoin&quoteId=united-states-dollar&start=1699900000000&end=1700000000000`
- **Response** (200):
  ```json
  {
    "data": [
      {
        "open": "44800.0000000000000000",
        "high": "45100.0000000000000000",
        "low": "44700.0000000000000000",
        "close": "45000.0000000000000000",
        "volume": "120.5000000000000000",
        "period": 1699900000000
      }
    ],
    "timestamp": 1700000000000
  }
  ```
- **Note**: All numeric values are returned as strings. The client must convert to float.

### WebSocket wss://ws.coincap.io/prices?assets={asset_list}
- **Purpose**: Real-time price updates.
- **Connection**: `wss://ws.coincap.io/prices?assets=bitcoin,ethereum`
- **Message format** (JSON):
  ```json
  {
    "bitcoin": "45123.45",
    "ethereum": "2345.67"
  }
  ```
- **Note**: Each message may contain prices for one or more assets. Prices are strings, must be converted to float.

### Rate Limits
- **Without API key**: 200 requests per minute.
- **With API key**: 500 requests per minute (not implemented in this version; noted for future enhancement).

### Error Responses
- **429 Too Many Requests**: Rate limit exceeded.
- **404 Not Found**: Invalid asset ID or endpoint.
- **5xx Server Error**: Transient server issues.

## Interface Contracts

### CLI Commands

```
crypto-pipeline backfill [--assets ASSETS] [--db DB_PATH]
crypto-pipeline refresh [--assets ASSETS] [--db DB_PATH]
crypto-pipeline stream [--assets ASSETS] [--db DB_PATH]
crypto-pipeline query latest [--assets ASSETS] [--db DB_PATH]
crypto-pipeline query history --start YYYY-MM-DD --end YYYY-MM-DD [--assets ASSETS] [--db DB_PATH]
```

**Global flags**:
- `--assets`: Comma-separated list of asset IDs. Default: `bitcoin,ethereum`.
- `--db`: Path to SQLite database file. Default: `crypto_pipeline.db`.

**Exit codes**:
- `0`: Success.
- `1`: Error (API failure, invalid arguments, etc.).

**Output format**:
- `query latest`: Tab-separated lines: `ASSET\tPRICE\tTIMESTAMP`
- `query history`: Tab-separated lines with header: `ASSET\tPERIOD\tOPEN\tHIGH\tLOW\tCLOSE\tVOLUME\tSMA_20\tSMA_50\tVOLATILITY\tVWAP`
- Errors and warnings: printed to stderr.

### Package Structure

```
crypto-pipeline/
  src/
    crypto_pipeline/
      __init__.py
      cli.py
      api_client.py
      metrics.py
      storage.py
      pipeline.py
      validation.py
    setup.py          # or pyproject.toml
```

**Entry point** (in setup.py or pyproject.toml):
```
console_scripts = ["crypto-pipeline = crypto_pipeline.cli:main"]
```

## Traceability Matrix

| Requirement | Component(s) | Interface(s) |
|---|---|---|
| REQ-001 | api_client, storage, pipeline | `CoinCapClient.get_candles()`, `Database.insert_candles()`, `backfill()` |
| REQ-002 | metrics, storage, pipeline | `compute_all_metrics()`, `Database.insert_metrics()`, `backfill()`, `refresh()` |
| REQ-003 | api_client, storage, pipeline | `CoinCapClient.stream_prices()`, `Database.insert_tick()`, `stream()` |
| REQ-004 | pipeline | `stream()` (exponential backoff logic) |
| REQ-005 | storage, pipeline, cli | `Database.get_latest_tick()`, `query_latest()`, `main()` |
| REQ-006 | storage, pipeline, cli | `Database.get_candles()`, `Database.get_metrics()`, `query_history()`, `main()` |
| REQ-007 | cli | `parse_args()` |
| REQ-008 | cli | `parse_args()` |
| REQ-009 | storage | `Database.__init__()`, `Database.init_schema()` |
| REQ-010 | api_client, metrics, storage, pipeline | `CoinCapClient.get_candles()`, `Database.get_latest_candle_period()`, `compute_all_metrics()`, `refresh()` |
| REQ-011 | cli | `main()`, `parse_args()` |
| REQ-012 | api_client, storage, pipeline | `CoinCapClient.get_asset()`, `Database.upsert_asset()`, `backfill()`, `refresh()` |
| REQ-013 | api_client, pipeline | `CoinCapClient.get_candles()`, `CoinCapClient.get_asset()`, `backfill()`, `refresh()`, `stream()` |
| REQ-014 | validation, pipeline | `validate_candle()`, `validate_tick()`, `is_valid_number()` |
| REQ-015 | api_client | `CoinCapClient.__init__()` |
