from __future__ import annotations

"""CoinCap API client (REST and WebSocket)."""

import json
import threading
import time
from typing import Callable

import requests
import websocket


class CoinCapClient:
    """Client for the CoinCap API v2 (REST and WebSocket)."""

    BASE_URL: str = "https://api.coincap.io/v2"
    WS_URL: str = "wss://ws.coincap.io/prices"

    def __init__(self, session: requests.Session | None = None):
        """Accept optional Session for dependency injection in tests."""
        self._session = session or requests.Session()
        self._session.max_redirects = 10

    def get_asset(self, asset_id: str) -> dict:
        """GET /v2/assets/{asset_id}

        Returns dict with id, name, symbol, priceUsd (float), etc.
        Raises: requests.HTTPError on 4xx/5xx, requests.ConnectionError on network failure.
        """
        resp = self._session.get(f"{self.BASE_URL}/assets/{asset_id}")
        resp.raise_for_status()
        data = resp.json()["data"]
        # Convert numeric strings to float
        for key in ("priceUsd", "marketCapUsd", "volumeUsd24Hr", "changePercent24Hr",
                     "vwap24Hr", "supply", "maxSupply"):
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
        """GET /v2/candles with required exchange/quoteId parameters.

        Returns list of candle dicts with numeric values converted to float.
        Raises: requests.HTTPError, requests.ConnectionError.
        """
        params: dict = {
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
        raw_candles = resp.json()["data"]

        candles = []
        for c in raw_candles:
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
        """Connect to WebSocket and call on_message(asset, price, timestamp) for each tick.

        on_error is called on connection loss.
        If stop_event is set, close the connection and return.
        Blocking call.
        """
        asset_param = ",".join(assets)
        url = f"{self.WS_URL}?assets={asset_param}"

        ws_app = None

        def _on_message(ws, message):
            ts = time.time()
            data = json.loads(message)
            for asset_id, price_str in data.items():
                on_message(asset_id, float(price_str), ts)

        def _on_error(ws, error):
            on_error(error if isinstance(error, Exception) else Exception(str(error)))

        def _on_close(ws, close_status_code, close_msg):
            pass

        def _on_open(ws):
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
            on_open=_on_open,
        )

        if stop_event is not None:
            stop_thread = threading.Thread(target=_check_stop, daemon=True)
            stop_thread.start()

        ws_app.run_forever()
