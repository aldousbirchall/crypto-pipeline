from __future__ import annotations

"""Crypto data providers: abstract interface with CoinGecko, Binance, and CoinCap backends."""

import abc
import json
import signal
import threading
import time
from typing import Callable

import requests
import websocket


# ---------------------------------------------------------------------------
# Asset ID mapping: canonical IDs (CoinGecko-style) to provider-specific IDs
# ---------------------------------------------------------------------------

BINANCE_SYMBOLS = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "cardano": "ADAUSDT",
    "dogecoin": "DOGEUSDT",
    "polkadot": "DOTUSDT",
    "avalanche-2": "AVAXUSDT",
    "chainlink": "LINKUSDT",
    "litecoin": "LTCUSDT",
    "ripple": "XRPUSDT",
}

BINANCE_REVERSE = {v: k for k, v in BINANCE_SYMBOLS.items()}


def _binance_symbol(asset_id: str) -> str:
    """Convert canonical asset ID to Binance symbol."""
    sym = BINANCE_SYMBOLS.get(asset_id)
    if sym is None:
        raise ValueError(
            f"Unknown asset '{asset_id}' for Binance. "
            f"Known: {', '.join(sorted(BINANCE_SYMBOLS))}"
        )
    return sym


# ---------------------------------------------------------------------------
# Abstract provider
# ---------------------------------------------------------------------------

class CryptoProvider(abc.ABC):
    """Abstract interface for cryptocurrency data providers."""

    name: str = "abstract"

    @abc.abstractmethod
    def get_asset(self, asset_id: str) -> dict:
        """Fetch asset metadata. Returns dict with id, name, symbol, priceUsd."""

    @abc.abstractmethod
    def get_candles(
        self,
        asset_id: str,
        interval: str = "h1",
        start: int | None = None,
        end: int | None = None,
    ) -> list[dict]:
        """Fetch OHLCV candles. Returns list of dicts with open, high, low, close, volume, period."""

    @abc.abstractmethod
    def stream_prices(
        self,
        assets: list[str],
        on_message: Callable[[str, float, float], None],
        on_error: Callable[[Exception], None],
        stop_event: threading.Event | None = None,
    ) -> None:
        """Stream real-time prices. Calls on_message(asset_id, price, timestamp)."""


# ---------------------------------------------------------------------------
# CoinGecko provider (free, no key, OHLC without volume)
# ---------------------------------------------------------------------------

class CoinGeckoProvider(CryptoProvider):
    """CoinGecko API v3. Free tier, no API key required.

    OHLC candles are 4-hour granularity for 30-day requests (no volume data).
    Streaming uses polling since CoinGecko has no free WebSocket.
    """

    name = "coingecko"
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()

    def get_asset(self, asset_id: str) -> dict:
        resp = self._session.get(
            f"{self.BASE_URL}/coins/{asset_id}",
            params={"localization": "false", "tickers": "false",
                    "community_data": "false", "developer_data": "false"},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "id": data["id"],
            "name": data["name"],
            "symbol": data["symbol"].upper(),
            "priceUsd": data["market_data"]["current_price"]["usd"],
        }

    def get_candles(
        self,
        asset_id: str,
        interval: str = "h1",
        start: int | None = None,
        end: int | None = None,
    ) -> list[dict]:
        # CoinGecko OHLC endpoint: days parameter controls granularity.
        # 1-2 days = 30min, 3-30 days = 4h, 31+ = 4 days.
        days = 30
        if start is not None and end is not None:
            days = max(1, int((end - start) / (24 * 60 * 60 * 1000)))

        resp = self._session.get(
            f"{self.BASE_URL}/coins/{asset_id}/ohlc",
            params={"vs_currency": "usd", "days": min(days, 90)},
        )
        resp.raise_for_status()
        raw = resp.json()  # [[timestamp, open, high, low, close], ...]

        candles = []
        for entry in raw:
            ts, o, h, l, c = entry
            # Filter by time range if specified
            if start is not None and ts < start:
                continue
            if end is not None and ts > end:
                continue
            candles.append({
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": 0.0,  # CoinGecko OHLC has no volume data
                "period": ts,
            })
        return candles

    def stream_prices(
        self,
        assets: list[str],
        on_message: Callable[[str, float, float], None],
        on_error: Callable[[Exception], None],
        stop_event: threading.Event | None = None,
    ) -> None:
        """Poll /simple/price every 10 seconds (CoinGecko has no free WebSocket)."""
        ids_param = ",".join(assets)
        while stop_event is None or not stop_event.is_set():
            try:
                resp = self._session.get(
                    f"{self.BASE_URL}/simple/price",
                    params={"ids": ids_param, "vs_currencies": "usd"},
                )
                resp.raise_for_status()
                data = resp.json()
                ts = time.time()
                for asset_id in assets:
                    if asset_id in data and "usd" in data[asset_id]:
                        on_message(asset_id, float(data[asset_id]["usd"]), ts)
            except KeyboardInterrupt:
                return
            except Exception as e:
                on_error(e)

            # Wait 10 seconds between polls, checking stop_event
            for _ in range(100):
                if stop_event is not None and stop_event.is_set():
                    return
                time.sleep(0.1)


# ---------------------------------------------------------------------------
# Binance provider (free, no key for market data, full OHLCV, WebSocket)
# ---------------------------------------------------------------------------

class BinanceProvider(CryptoProvider):
    """Binance public API. No API key needed for market data.

    Hourly OHLCV candles with full volume. WebSocket for real-time trades.
    Uses canonical asset IDs (bitcoin, ethereum) mapped to Binance symbols (BTCUSDT, ETHUSDT).
    """

    name = "binance"
    BASE_URL = "https://api.binance.com/api/v3"
    WS_URL = "wss://stream.binance.com:9443/ws"

    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()

    def get_asset(self, asset_id: str) -> dict:
        symbol = _binance_symbol(asset_id)
        resp = self._session.get(
            f"{self.BASE_URL}/ticker/24hr",
            params={"symbol": symbol},
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "id": asset_id,
            "name": asset_id.replace("-", " ").title(),
            "symbol": symbol.replace("USDT", ""),
            "priceUsd": float(data["lastPrice"]),
        }

    def get_candles(
        self,
        asset_id: str,
        interval: str = "h1",
        start: int | None = None,
        end: int | None = None,
    ) -> list[dict]:
        symbol = _binance_symbol(asset_id)
        # Map interval names: h1 -> 1h, d1 -> 1d
        binance_interval = {"h1": "1h", "h4": "4h", "d1": "1d"}.get(interval, "1h")

        params = {"symbol": symbol, "interval": binance_interval, "limit": 720}
        if start is not None:
            params["startTime"] = start
        if end is not None:
            params["endTime"] = end

        resp = self._session.get(f"{self.BASE_URL}/klines", params=params)
        resp.raise_for_status()
        raw = resp.json()
        # Binance kline: [openTime, open, high, low, close, volume, closeTime, ...]

        candles = []
        for k in raw:
            candles.append({
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "period": k[0],
            })
        return candles

    def stream_prices(
        self,
        assets: list[str],
        on_message: Callable[[str, float, float], None],
        on_error: Callable[[Exception], None],
        stop_event: threading.Event | None = None,
    ) -> None:
        """Connect to Binance WebSocket for real-time mini-tickers."""
        streams = [f"{_binance_symbol(a).lower()}@miniTicker" for a in assets]
        url = f"{self.WS_URL}/{'/'.join(streams)}"

        ws_app = None

        def _on_message(ws, message):
            data = json.loads(message)
            symbol = data.get("s", "")
            canonical = BINANCE_REVERSE.get(symbol)
            if canonical and "c" in data:
                on_message(canonical, float(data["c"]), time.time())

        def _on_error(ws, error):
            on_error(error if isinstance(error, Exception) else Exception(str(error)))

        def _on_close(ws, code, msg):
            pass

        def _check_stop():
            if stop_event is not None:
                while not stop_event.is_set():
                    time.sleep(0.1)
                if ws_app:
                    ws_app.close()

        ws_app = websocket.WebSocketApp(
            url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

        old_handler = signal.signal(signal.SIGINT, lambda s, f: (
            stop_event.set() if stop_event else None,
            ws_app.close() if ws_app else None,
        ))

        if stop_event is not None:
            threading.Thread(target=_check_stop, daemon=True).start()

        try:
            ws_app.run_forever(ping_timeout=10)
        finally:
            signal.signal(signal.SIGINT, old_handler)


# ---------------------------------------------------------------------------
# CoinCap provider (original, currently down as of 2026-02-21)
# ---------------------------------------------------------------------------

class CoinCapProvider(CryptoProvider):
    """CoinCap API v2. Hourly OHLCV via Poloniex, WebSocket for real-time prices.

    Note: api.coincap.io has been returning ECONNREFUSED since at least 2026-02-21.
    Kept as a provider for when the service returns.
    """

    name = "coincap"
    BASE_URL = "https://api.coincap.io/v2"
    WS_URL = "wss://ws.coincap.io/prices"

    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()

    def get_asset(self, asset_id: str) -> dict:
        resp = self._session.get(f"{self.BASE_URL}/assets/{asset_id}")
        resp.raise_for_status()
        data = resp.json()["data"]
        for key in ("priceUsd", "marketCapUsd", "volumeUsd24Hr",
                     "changePercent24Hr", "vwap24Hr", "supply", "maxSupply"):
            if key in data and data[key] is not None:
                data[key] = float(data[key])
        return data

    def get_candles(
        self,
        asset_id: str,
        interval: str = "h1",
        start: int | None = None,
        end: int | None = None,
    ) -> list[dict]:
        params = {
            "exchange": "poloniex",
            "interval": interval,
            "baseId": asset_id,
            "quoteId": "united-states-dollar",
        }
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end

        resp = self._session.get(f"{self.BASE_URL}/candles", params=params)
        resp.raise_for_status()
        raw = resp.json()["data"]

        candles = []
        for c in raw:
            candles.append({
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": float(c["volume"]),
                "period": c["period"],
            })
        return candles

    def stream_prices(
        self,
        assets: list[str],
        on_message: Callable[[str, float, float], None],
        on_error: Callable[[Exception], None],
        stop_event: threading.Event | None = None,
    ) -> None:
        url = f"{self.WS_URL}?assets={','.join(assets)}"
        ws_app = None

        def _on_message(ws, message):
            ts = time.time()
            data = json.loads(message)
            for asset_id, price_str in data.items():
                on_message(asset_id, float(price_str), ts)

        def _on_error(ws, error):
            on_error(error if isinstance(error, Exception) else Exception(str(error)))

        def _on_close(ws, code, msg):
            pass

        def _check_stop():
            if stop_event is not None:
                while not stop_event.is_set():
                    time.sleep(0.1)
                if ws_app:
                    ws_app.close()

        ws_app = websocket.WebSocketApp(
            url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

        old_handler = signal.signal(signal.SIGINT, lambda s, f: (
            stop_event.set() if stop_event else None,
            ws_app.close() if ws_app else None,
        ))

        if stop_event is not None:
            threading.Thread(target=_check_stop, daemon=True).start()

        try:
            ws_app.run_forever(ping_timeout=10)
        finally:
            signal.signal(signal.SIGINT, old_handler)


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

PROVIDERS = {
    "coingecko": CoinGeckoProvider,
    "binance": BinanceProvider,
    "coincap": CoinCapProvider,
}

DEFAULT_PROVIDER = "binance"


def get_provider(name: str | None = None, session: requests.Session | None = None) -> CryptoProvider:
    """Instantiate a provider by name. Defaults to Binance."""
    name = name or DEFAULT_PROVIDER
    cls = PROVIDERS.get(name)
    if cls is None:
        raise ValueError(f"Unknown provider '{name}'. Available: {', '.join(sorted(PROVIDERS))}")
    return cls(session=session)


# Backward-compatible alias
CoinCapClient = CoinCapProvider
