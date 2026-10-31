"""
Real-Time Crypto Producer

Streams real-time cryptocurrency prices from Coinbase WebSocket to Kafka.
No API key required - uses public Coinbase WebSocket API.

Symbols: BTC, ETH, SOL, XRP, DOGE, LTC (vs USD)

Usage:
    python src/production/producer/crypto_producer.py
"""

import json
import math
import os
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone
import uuid

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

# =============================================================================
# CONFIGURATION
# =============================================================================

# Use env var for Docker (kafka:29092) or default to localhost for local dev
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-ticks"  # Reuse same topic for compatibility

# Crypto symbols to track (Coinbase product IDs)
PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "LTC-USD"]

# Map Coinbase product IDs to display names
SYMBOL_NAMES = {
    "BTC-USD": "BTC",
    "ETH-USD": "ETH",
    "SOL-USD": "SOL",
    "XRP-USD": "XRP",
    "DOGE-USD": "DOGE",
    "LTC-USD": "LTC",
}

# =============================================================================
# EXCHANGE RATE MANAGER (USD -> INR)
# =============================================================================

class ExchangeRateManager:
    """
    Manages USD to INR exchange rate fetching, caching, validation, and failover.

    1. Primary:     Frankfurter API (European Central Bank reference rates)
    2. Secondary:   Open Exchange Rates open endpoint
    3. Stale cache: Retains last known valid rate if all upstream calls fail
    4. Unavailable: If no valid rate exists, returns (None, None).
       Never publishes fabricated or hardcoded fallback rates.

    Cache TTL vs Provider Freshness:
    The 10-minute cache TTL (`cache_ttl_seconds`) controls HTTP polling frequency
    to prevent rate-limiting upstream free APIs. It does NOT represent or alter the
    provider's published data update schedule (e.g. ECB publishes daily). When a cached
    rate is reused, `rate_timestamp` reflects the provider's actual update timestamp
    and is NOT refreshed on cache hits.

    Timezone Semantics:
    Timestamps represent naive UTC formatted as 'YYYY-MM-DD HH:MM:SS' to match ClickHouse
    DateTime column requirements. If an upstream provider omits or provides an unparseable
    publication timestamp, `rate_timestamp` is stored as None (NULL) rather than fabricated.
    """

    def __init__(self, cache_ttl_seconds: int = 600, time_provider=time.monotonic):
        self.cache_ttl = cache_ttl_seconds
        self._time = time_provider
        self._cached_rate: float | None = None
        self._rate_timestamp: str | None = None  # Format: 'YYYY-MM-DD HH:MM:SS' or None
        self._last_fetch_monotonic: float = -float("inf")

    def _validate_rate(self, rate_val) -> float:
        """Validate that rate is numeric, finite, and strictly greater than zero."""
        rate = float(rate_val)
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError(f"Invalid exchange rate value: {rate}")
        return rate

    def _fetch_frankfurter(self) -> tuple[float, str | None]:
        req = urllib.request.Request(
            "https://api.frankfurter.app/latest?from=USD&to=INR",
            headers={"User-Agent": "CryptoProducer/1.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.getcode() != 200:
                raise ValueError(f"Frankfurter returned HTTP status {resp.getcode()}")
            data = json.loads(resp.read().decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Frankfurter response is not a valid JSON object")

            rates = data.get("rates")
            if not isinstance(rates, dict) or "INR" not in rates:
                raise ValueError("Missing 'INR' field in Frankfurter rates payload")

            rate = self._validate_rate(rates["INR"])

            # Frankfurter publishes reference date 'YYYY-MM-DD' (ECB daily fix)
            date_str = data.get("date")
            ts = None
            if date_str and isinstance(date_str, str):
                try:
                    dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                    ts = dt.strftime("%Y-%m-%d 00:00:00")
                except ValueError:
                    ts = None

            return rate, ts

    def _fetch_open_er(self) -> tuple[float, str | None]:
        req = urllib.request.Request(
            "https://open.er-api.com/v6/latest/USD",
            headers={"User-Agent": "CryptoProducer/1.0"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.getcode() != 200:
                raise ValueError(f"Open ER returned HTTP status {resp.getcode()}")
            data = json.loads(resp.read().decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Open ER response is not a valid JSON object")

            rates = data.get("rates")
            if not isinstance(rates, dict) or "INR" not in rates:
                raise ValueError("Missing 'INR' field in Open ER rates payload")

            rate = self._validate_rate(rates["INR"])

            # Open ER publishes 'time_last_update_utc' e.g. 'Sat, 17 Oct 2026 00:02:32 +0000'
            raw_ts = data.get("time_last_update_utc")
            ts = None
            if raw_ts and isinstance(raw_ts, str):
                try:
                    dt = datetime.strptime(raw_ts[:25].strip(), "%a, %d %b %Y %H:%M:%S")
                    ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts = None

            return rate, ts

    def get_rate(self) -> tuple[float | None, str | None]:
        """
        Returns (rate, rate_timestamp).
        - Cache hit: Returns cached (rate, rate_timestamp) WITHOUT refreshing the timestamp.
        - Cache expired: Fetches fresh rate from primary -> secondary with strict validation.
        - Failover: Retains stale cached rate if all upstream calls fail.
        - Unavailable: Returns (None, None) if no valid rate has ever been fetched.
        """
        now_mono = self._time()
        if self._cached_rate is not None and (now_mono - self._last_fetch_monotonic) < self.cache_ttl:
            return self._cached_rate, self._rate_timestamp

        # Try primary API (Frankfurter)
        try:
            rate, ts = self._fetch_frankfurter()
            self._cached_rate = rate
            self._rate_timestamp = ts
            self._last_fetch_monotonic = now_mono
            return self._cached_rate, self._rate_timestamp
        except Exception as e1:
            print(f"[exchange-rate] Frankfurter failed: {e1}, trying secondary...", file=sys.stderr)

        # Try secondary API (Open ER)
        try:
            rate, ts = self._fetch_open_er()
            self._cached_rate = rate
            self._rate_timestamp = ts
            self._last_fetch_monotonic = now_mono
            return self._cached_rate, self._rate_timestamp
        except Exception as e2:
            print(f"[exchange-rate] Secondary API failed: {e2}", file=sys.stderr)

        # Fallback to stale cached rate if one was ever successfully fetched
        if self._cached_rate is not None:
            print(
                f"[exchange-rate] Retaining stale cached rate {self._cached_rate} from {self._rate_timestamp}",
                file=sys.stderr,
            )
            return self._cached_rate, self._rate_timestamp

        # No valid rate available anywhere
        print("[exchange-rate] No exchange rate available from any provider. Marking INR unavailable.", file=sys.stderr)
        return None, None


rate_manager = ExchangeRateManager(cache_ttl_seconds=600)

# =============================================================================
# KAFKA PRODUCER
# =============================================================================

def process_ticker_message(message: str, rate_mgr: ExchangeRateManager) -> dict | None:
    """Parse and transform a Coinbase WebSocket ticker message into a dual-currency tick."""
    try:
        data = json.loads(message)
        if data.get("type") != "ticker":
            return None

        product_id = data.get("product_id", "")
        symbol = SYMBOL_NAMES.get(product_id, product_id.replace("-USD", ""))
        price = float(data.get("price", 0))
        # Keep exact floating-point volume to prevent VWAP distortion on fractional crypto trades
        volume = float(data.get("last_size", 0))

        if price <= 0 or volume <= 0:
            return None

        rate, rate_ts = rate_mgr.get_rate()
        price_inr = round(price * rate, 2) if rate is not None else None

        return {
            "event_id": str(uuid.uuid4().hex),
            "symbol": symbol,
            "price": price,                       # Preserved for backward compatibility (= price_usd)
            "price_usd": price,                   # Explicit USD price
            "price_inr": price_inr,               # Converted INR price (None if unavailable)
            "exchange_rate": rate,                # USD/INR conversion rate (None if unavailable)
            "rate_timestamp": rate_ts,            # Provider fetch timestamp formatted for ClickHouse (None if unavail)
            "volume": volume,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"Error processing message: {e}", file=sys.stderr)
        return None


producer = None

def get_producer():
    """Lazily initialize KafkaProducer when Kafka is available."""
    global producer
    if producer is None and KafkaProducer is not None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return producer

# =============================================================================
# WEBSOCKET HANDLER
# =============================================================================

def on_send_error(excp):
    print(f"[kafka] Failed to deliver tick: {excp}", file=sys.stderr)


def on_message(ws, message):
    """Handle incoming WebSocket message."""
    tick = process_ticker_message(message, rate_manager)
    if tick:
        p = get_producer()
        if p:
            # Key partition by symbol to guarantee per-symbol ordering across partitions
            p.send(
                KAFKA_TOPIC,
                key=tick["symbol"].encode("utf-8"),
                value=tick,
            ).add_errback(on_send_error)
        print(f"Sent: {tick['symbol']} | ${tick['price']} | Vol: {tick['volume']}")


def on_error(ws, error):
    """Handle WebSocket error."""
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket close."""
    print(f"WebSocket closed: {close_status_code} - {close_msg}")


def on_open(ws):
    """Handle WebSocket open."""
    print("WebSocket connected to Coinbase")

    # Subscribe to ticker channel for all products
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["ticker"]
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"Subscribed to: {', '.join(SYMBOL_NAMES.values())}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import websocket

    print("=" * 60)
    print("Real-Time Crypto Producer (Coinbase WebSocket)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
    print(f"Symbols: {', '.join(SYMBOL_NAMES.values())}")
    print()
    print("Press Ctrl+C to stop.")
    print("=" * 60)

    # Coinbase WebSocket URL
    ws_url = "wss://ws-feed.exchange.coinbase.com"

    # Create WebSocket connection
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\nShutting down...")
        ws.close()
        if producer:
            producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run WebSocket (blocks until closed)
    ws.run_forever()


if __name__ == "__main__":
    main()
