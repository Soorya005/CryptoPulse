"""
coinbase_ws.py — Coinbase Exchange WebSocket client.

Connects to wss://ws-feed.exchange.coinbase.com, subscribes to the
``ticker`` and ``matches`` channels for a configurable list of trading
pairs, and continuously publishes validated, INR-enriched events to the
``stock-ticks`` Kafka topic.

Reconnection / resubscription
-------------------------------
* The client uses an exponential back-off (capped at MAX_BACKOFF_S)
  before attempting each reconnect so a flapping server does not hammer
  the network.
* On every successful (re)connect the subscription message is resent
  automatically.

Message handling
-----------------
* ``ticker`` messages — emitted by Coinbase roughly every second per
  pair; contain ``price``, ``best_bid``, ``best_ask``, ``volume_24h``,
  and ``time``.
* ``match`` / ``last_match`` messages — individual trade executions;
  contain ``price``, ``size``, and ``time``.
* Any other type (``subscriptions``, ``heartbeat``, ``error``, …) is
  logged and silently discarded so the loop keeps running.
* Malformed JSON and missing/invalid fields are caught and logged; the
  loop continues.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import websocket  # websocket-client package

from inr_converter import get_usd_to_inr
from producer import create_producer, publish_trade

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WS_URL = "wss://ws-feed.exchange.coinbase.com"

# Trading pairs to subscribe to.
PRODUCT_IDS: list[str] = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
]

# Channels that deliver price data we care about.
SUBSCRIBE_CHANNELS: list[str] = ["ticker", "matches"]

# Reconnection back-off limits (seconds).
INITIAL_BACKOFF_S: float = 1.0
MAX_BACKOFF_S: float = 60.0
BACKOFF_FACTOR: float = 2.0


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

def _parse_ticker(msg: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract the normalized event dict from a Coinbase ``ticker`` message.

    Coinbase ticker fields used:
        product_id  — e.g. ``"BTC-USD"``
        price       — last traded price as a string
        volume_24h  — 24-hour rolling volume as a string
        time        — ISO-8601 timestamp with 'Z' suffix

    Returns None if any required field is absent or cannot be cast.
    """
    try:
        symbol: str = msg["product_id"]
        price_usd: float = float(msg["price"])
        # Prioritize last_size (individual trade size) over volume_24h (24h aggregate market volume).
        volume: float = float(msg.get("last_size") or msg.get("volume_24h") or "0")
        if volume <= 0:
            volume = float(msg.get("volume_24h") or "0.0001")
        timestamp: str = msg["time"]
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("ticker parse error: %s — raw: %s", exc, msg)
        return None

    rate_info = get_usd_to_inr()
    price_inr = round(price_usd * rate_info["rate"], 8) if rate_info["rate"] else None

    return {
        "symbol": symbol,
        "price": price_usd,
        "volume": volume if volume > 0 else 0.0001,
        "timestamp": timestamp,
        "price_usd": price_usd,
        "price_inr": price_inr,
        "exchange_rate": rate_info["rate"],
        "rate_timestamp": rate_info["rate_timestamp"],
    }


def _parse_match(msg: dict[str, Any]) -> dict[str, Any] | None:
    """
    Extract the normalized event dict from a Coinbase ``match`` or
    ``last_match`` message.

    Coinbase match fields used:
        product_id  — e.g. ``"BTC-USD"``
        price       — execution price as a string
        size        — trade size as a string
        time        — ISO-8601 timestamp with 'Z' suffix

    Returns None if any required field is absent or cannot be cast.
    """
    try:
        symbol: str = msg["product_id"]
        price_usd: float = float(msg["price"])
        volume: float = float(msg["size"])
        timestamp: str = msg["time"]
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("match parse error: %s — raw: %s", exc, msg)
        return None

    rate_info = get_usd_to_inr()
    price_inr = round(price_usd * rate_info["rate"], 8) if rate_info["rate"] else None

    return {
        "symbol": symbol,
        "price": price_usd,
        "volume": volume,
        "timestamp": timestamp,
        "price_usd": price_usd,
        "price_inr": price_inr,
        "exchange_rate": rate_info["rate"],
        "rate_timestamp": rate_info["rate_timestamp"],
    }


def parse_coinbase_message(raw: str) -> dict[str, Any] | None:
    """
    Parse one raw WebSocket text frame from Coinbase.

    Returns a normalized event dict ready for ``publish_trade()``, or
    None if the message should be silently discarded.

    Raises nothing — all errors are caught and logged.
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Malformed JSON from Coinbase: %s — raw snippet: %.200s", exc, raw)
        return None

    if not isinstance(msg, dict):
        logger.warning("Unexpected non-dict message: %.200s", raw)
        return None

    msg_type = msg.get("type", "")

    if msg_type == "ticker":
        return _parse_ticker(msg)

    if msg_type in ("match", "last_match"):
        return _parse_match(msg)

    # Non-data messages — subscriptions ack, heartbeat, error, etc.
    if msg_type == "error":
        logger.error("Coinbase WS error message: %s", msg.get("message", raw))
    elif msg_type not in ("subscriptions", "heartbeat", "received", "open", "done", "activate"):
        logger.debug("Ignoring message type '%s'", msg_type)

    return None


# ---------------------------------------------------------------------------
# Subscription helper
# ---------------------------------------------------------------------------

def _build_subscribe_msg() -> str:
    """Return the JSON subscription request string."""
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": PRODUCT_IDS,
            "channels": SUBSCRIBE_CHANNELS,
        }
    )


# ---------------------------------------------------------------------------
# WebSocket runner
# ---------------------------------------------------------------------------

class CoinbaseWebSocketClient:
    """
    Long-running WebSocket client that feeds live Coinbase data into Kafka.

    Usage::

        client = CoinbaseWebSocketClient()
        client.run_forever()   # blocks; Ctrl-C to stop
    """

    def __init__(
        self,
        producer: Any | None = None,
        dry_run: bool = False,
        max_events: int | None = None,
    ) -> None:
        self.dry_run = dry_run
        self.max_events = max_events
        self._events_count = 0
        if dry_run:
            self._producer = None
        else:
            self._producer = producer or create_producer()
        self._ws: websocket.WebSocketApp | None = None
        self._backoff: float = INITIAL_BACKOFF_S
        self._running: bool = True

    # ------------------------------------------------------------------
    # WebSocketApp callbacks
    # ------------------------------------------------------------------

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        logger.info("WebSocket connected — subscribing to %s", PRODUCT_IDS)
        ws.send(_build_subscribe_msg())
        # Reset back-off on successful connect.
        self._backoff = INITIAL_BACKOFF_S

    def _on_message(self, ws: websocket.WebSocketApp, raw: str) -> None:
        event = parse_coinbase_message(raw)
        if event is None:
            return
        try:
            if self.dry_run:
                from validator import validate_trade
                validated = validate_trade(event)
                logger.info(
                    "[DRY-RUN] Validated trade: symbol=%s, price=%.2f, price_inr=%s, exchange_rate=%s",
                    validated["symbol"],
                    validated["price"],
                    validated["price_inr"],
                    validated["exchange_rate"],
                )
            else:
                publish_trade(self._producer, event)

            self._events_count += 1
            if self.max_events and self._events_count >= self.max_events:
                self.stop()
        except ValueError as exc:
            logger.warning("Validation failed — skipping event: %s", exc)
        except Exception as exc:
            logger.error("Failed to publish event to Kafka: %s", exc)

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        logger.error("WebSocket error: %s", error)

    def _on_close(
        self,
        ws: websocket.WebSocketApp,
        close_status_code: int | None,
        close_msg: str | None,
    ) -> None:
        logger.warning(
            "WebSocket closed (code=%s, msg=%s)", close_status_code, close_msg
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        """
        Connect to Coinbase and publish events until the process is
        interrupted (KeyboardInterrupt / SIGTERM).

        Reconnects automatically with exponential back-off after any
        connection failure or unexpected close.
        """
        logger.info("Starting Coinbase WebSocket client — URL: %s", WS_URL)
        while self._running:
            self._ws = websocket.WebSocketApp(
                WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            try:
                # run_until_permanently_closed blocks until the socket closes.
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error("Unexpected exception in run_forever: %s", exc)

            if not self._running:
                break

            logger.info(
                "Reconnecting in %.1f s (back-off capped at %.0f s)…",
                self._backoff,
                MAX_BACKOFF_S,
            )
            time.sleep(self._backoff)
            self._backoff = min(self._backoff * BACKOFF_FACTOR, MAX_BACKOFF_S)

        logger.info("WebSocket client stopped.")
        if self._producer:
            self._producer.close()

    def stop(self) -> None:
        """Signal the client to stop after the current connection closes."""
        self._running = False
        if self._ws:
            self._ws.close()
