from __future__ import annotations

"""Storage layer: SQLite database operations."""

import sqlite3
import time


class Database:
    """SQLite database for cryptocurrency pipeline data."""

    def __init__(self, db_path: str):
        """Open database connection. Create tables if they don't exist."""
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # --- Schema ---

    def init_schema(self) -> None:
        """Create tables if they do not exist. Idempotent."""
        self._conn.executescript("""
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
        """)

    # --- Assets ---

    def upsert_asset(self, asset_id: str, name: str, symbol: str, price_usd: float) -> None:
        """Insert or update asset metadata."""
        self._conn.execute(
            """INSERT OR REPLACE INTO assets (asset_id, name, symbol, price_usd, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (asset_id, name, symbol, price_usd, time.time()),
        )
        self._conn.commit()

    # --- Candles ---

    def insert_candles(self, asset_id: str, candles: list[dict]) -> int:
        """Bulk insert candle records. Skips duplicates (same asset_id + period).
        Returns number of records inserted.
        """
        if not candles:
            return 0
        cursor = self._conn.cursor()
        inserted = 0
        for c in candles:
            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO candles (asset_id, period, open, high, low, close, volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (asset_id, c["period"], c["open"], c["high"], c["low"], c["close"], c["volume"]),
                )
                inserted += cursor.rowcount
            except sqlite3.IntegrityError:
                pass
        self._conn.commit()
        return inserted

    def get_latest_candle_period(self, asset_id: str) -> int | None:
        """Return the most recent period timestamp for the given asset, or None."""
        row = self._conn.execute(
            "SELECT MAX(period) as max_period FROM candles WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
        if row and row["max_period"] is not None:
            return row["max_period"]
        return None

    def get_candles(
        self, asset_id: str, start: int | None = None, end: int | None = None
    ) -> list[dict]:
        """Return candle records for asset, optionally filtered by period range.
        Sorted by period ascending.
        """
        query = "SELECT open, high, low, close, volume, period FROM candles WHERE asset_id = ?"
        params: list = [asset_id]
        if start is not None:
            query += " AND period >= ?"
            params.append(start)
        if end is not None:
            query += " AND period <= ?"
            params.append(end)
        query += " ORDER BY period ASC"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    # --- Metrics ---

    def insert_metrics(self, asset_id: str, metrics: list[dict]) -> None:
        """Bulk insert/replace metric records for an asset."""
        if not metrics:
            return
        for m in metrics:
            self._conn.execute(
                """INSERT OR REPLACE INTO metrics (asset_id, period, sma_20, sma_50, volatility, vwap)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (asset_id, m["period"], m.get("sma_20"), m.get("sma_50"),
                 m.get("volatility"), m.get("vwap")),
            )
        self._conn.commit()

    def get_metrics(
        self, asset_id: str, start: int | None = None, end: int | None = None
    ) -> list[dict]:
        """Return metric records for asset, optionally filtered by period range.
        Sorted by period ascending.
        """
        query = "SELECT period, sma_20, sma_50, volatility, vwap FROM metrics WHERE asset_id = ?"
        params: list = [asset_id]
        if start is not None:
            query += " AND period >= ?"
            params.append(start)
        if end is not None:
            query += " AND period <= ?"
            params.append(end)
        query += " ORDER BY period ASC"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    # --- Ticks ---

    def insert_tick(self, asset_id: str, price: float, timestamp: float) -> None:
        """Insert a single real-time price tick."""
        self._conn.execute(
            "INSERT INTO ticks (asset_id, price, timestamp) VALUES (?, ?, ?)",
            (asset_id, price, timestamp),
        )
        self._conn.commit()

    def get_latest_tick(self, asset_id: str) -> dict | None:
        """Return the most recent tick for the given asset, or None."""
        row = self._conn.execute(
            "SELECT asset_id, price, timestamp FROM ticks WHERE asset_id = ? ORDER BY timestamp DESC LIMIT 1",
            (asset_id,),
        ).fetchone()
        if row:
            return dict(row)
        return None

    def get_ticks(
        self, asset_id: str, start: float | None = None, end: float | None = None
    ) -> list[dict]:
        """Return tick records for asset, optionally filtered by timestamp range."""
        query = "SELECT asset_id, price, timestamp FROM ticks WHERE asset_id = ?"
        params: list = [asset_id]
        if start is not None:
            query += " AND timestamp >= ?"
            params.append(start)
        if end is not None:
            query += " AND timestamp <= ?"
            params.append(end)
        query += " ORDER BY timestamp ASC"
        rows = self._conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
