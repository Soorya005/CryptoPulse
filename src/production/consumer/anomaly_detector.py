"""
Anomaly Detector -- Rolling Z-score on per-symbol price returns

Reads:  Kafka topic 'stock-ticks'
Writes: ClickHouse stocks.market_alerts

IN-MEMORY STATE WARNING
-----------------------
All rolling return histories are held in process memory (Python dicts of deques).
They are NOT persisted to disk or any external store. A container restart resets
all per-symbol histories to empty, and each symbol must accumulate at least
MIN_SAMPLES new ticks before Z-score detection resumes. This is documented
intentional behaviour. Persistent detector state and warm-up from historical
offsets will be addressed in the evaluation milestone if required.

OFFSET BEHAVIOUR
----------------
auto_offset_reset="latest" means this consumer only processes messages that
arrive AFTER it starts. Messages published while the detector was offline are
intentionally skipped. Replay-based evaluation with known injected anomalies
is implemented separately in src/production/evaluation/replay_evaluator.py
(evaluation milestone).

Algorithm
---------
For each symbol, maintain a rolling deque of price *returns* (not raw prices):

    r_t = (price_t - price_{t-1}) / price_{t-1}

First tick for a symbol:
    Only last_price[symbol] is initialised. Nothing is appended to the window
    because there is no prior price from which to compute a return.

Subsequent ticks:
    1. Compute r_t from the previous price.
    2. Record n_prior = len(window)  -- the number of prior observations.
    3. Call compute_z_score(window, r_t) using ONLY the prior observations.
    4. Append r_t to window AFTER scoring (preserves "previous obs only" guarantee).
    5. Update last_price[symbol] = price.

The sample_count stored in market_alerts is n_prior -- the number of prior
observations that were actually used to compute the Z-score for that alert.

An alert fires when ALL of the following hold:
  - n_prior >= MIN_SAMPLES   (enough prior history)
  - stdev(window) > 1e-10    (avoids near-zero division)
  - |Z| >= THRESHOLD

Configuration (all via environment variables)
---------------------------------------------
ANOMALY_THRESHOLD   float  default 3.0   |Z| >= this triggers an alert
ANOMALY_MIN_SAMPLES int    default 10    minimum prior observations before scoring
ANOMALY_WINDOW_SIZE int    default 30    rolling window length per symbol (deque maxlen)
"""

import json
import os
import sys
from collections import deque
from datetime import datetime, timezone
from statistics import mean, stdev

import urllib.request

import clickhouse_connect
from kafka import KafkaConsumer

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-ticks"
KAFKA_GROUP_ID = "anomaly-detector-v1"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD", "3.0"))
MIN_SAMPLES = int(os.environ.get("ANOMALY_MIN_SAMPLES", "10"))
WINDOW_SIZE = int(os.environ.get("ANOMALY_WINDOW_SIZE", "30"))
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "").strip()

try:
    from webhook_dispatcher import AlertWebhookDispatcher
except ImportError:
    from src.production.consumer.webhook_dispatcher import AlertWebhookDispatcher

webhook_dispatcher = AlertWebhookDispatcher(webhook_url=ALERT_WEBHOOK_URL, cooldown_seconds=15)


def dispatch_alert_webhook(payload: dict) -> tuple[bool, str]:
    """Dispatch alert payload to external HTTP webhook (Discord, Telegram, Slack, custom)."""
    return webhook_dispatcher.dispatch(payload)

# =============================================================================
# PER-SYMBOL IN-MEMORY STATE
# =============================================================================

# Most recently seen price per symbol (needed to compute returns on the next tick).
# Entirely in-memory: cleared on every process restart.
last_price: dict[str, float] = {}

# Rolling return history per symbol.
# deque maxlen = WINDOW_SIZE enforces the rolling bound automatically.
# Entirely in-memory: cleared on every process restart.
return_windows: dict[str, deque] = {}


def get_window(symbol: str) -> deque:
    """Return (or create) the rolling return deque for a symbol."""
    if symbol not in return_windows:
        return_windows[symbol] = deque(maxlen=WINDOW_SIZE)
    return return_windows[symbol]


# =============================================================================
# Z-SCORE -- PREVIOUS OBSERVATIONS ONLY
# =============================================================================


def compute_z_score(
    window: deque, current_return: float
) -> tuple[float | None, float | None, float | None]:
    """
    Compute the Z-score of `current_return` against the observations already
    present in `window` at the time of the call (prior observations only).

    The caller MUST append `current_return` to `window` AFTER this call so that
    the current return is never included in its own Z-score computation.

    Returns:
        (z, mu, sigma)     when all scoring conditions are satisfied.
        (None, None, None) when:
          - fewer than MIN_SAMPLES prior observations are available, or
          - stdev of the prior observations is near zero (constant prices).
    """
    n = len(window)
    if n < MIN_SAMPLES:
        return None, None, None
    mu = mean(window)
    sigma = stdev(window)  # sample stdev (ddof=1); n >= MIN_SAMPLES >= 10 ensures n >= 2
    if sigma < 1e-10:
        return None, None, None
    return (current_return - mu) / sigma, mu, sigma


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    print("=" * 60)
    print("Anomaly Detector -- Rolling Z-score on price returns")
    print(f"Kafka:      {KAFKA_BOOTSTRAP_SERVERS} / topic={KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} / db=stocks")
    print(f"Threshold:  |Z| >= {THRESHOLD}")
    print(f"Min samples before scoring: {MIN_SAMPLES}")
    print(f"Rolling window size:        {WINDOW_SIZE}")
    print()
    print("NOTE: Rolling history is in-memory only.")
    print(f"      A restart clears all histories; each symbol needs {MIN_SAMPLES}")
    print("      new ticks before scoring can resume.")
    print()
    print("NOTE: auto_offset_reset=latest -- live events only.")
    print("      Messages published while offline are skipped.")
    print("      Replay evaluation is handled by replay_evaluator.py.")
    print("=" * 60)

    ch_client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database="stocks",
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # Only process messages arriving after this consumer starts.
        # Historical backfill and replay testing are handled separately.
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    print("Listening for trades...\n")

    for msg in consumer:
        try:
            data = msg.value
            symbol = data.get("symbol")

            # Accept both current schema ('price') and Milestone 2 schema ('price_usd').
            # 'or' short-circuits: price_usd is preferred when present and non-zero.
            price_raw = data.get("price_usd") or data.get("price")
            if price_raw is None:
                continue
            price = float(price_raw)

            timestamp_str = data.get("timestamp")
            source = data.get("source", "coinbase")

            # Skip messages that are structurally invalid
            if not symbol or price <= 0 or not timestamp_str:
                continue

            window = get_window(symbol)
            prev = last_price.get(symbol)

            if prev is None:
                # First tick for this symbol.
                # Only record the price; do NOT append anything to the window
                # because there is no prior price to compute a return from.
                last_price[symbol] = price
                continue

            if prev <= 0:
                # Previous price was invalid (should not normally happen, but guard anyway).
                last_price[symbol] = price
                continue

            # --- Compute price return from prior price ---
            ret = (price - prev) / prev

            # --- Record n_prior: observations available BEFORE appending ret ---
            # This is the count used by compute_z_score and stored in the alert.
            n_prior = len(window)

            # --- Score against PREVIOUS window (before appending current return) ---
            z, mu, sigma = compute_z_score(window, ret)

            # --- Append current return AFTER scoring (previous-obs-only guarantee) ---
            window.append(ret)

            # --- Advance last price ---
            last_price[symbol] = price

            # --- Raise alert if threshold exceeded ---
            if z is not None and abs(z) >= THRESHOLD:
                try:
                    ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except ValueError:
                    ts = datetime.now(timezone.utc)

                print(
                    f"  ALERT  symbol={symbol}  Z={z:+.3f}  "
                    f"ret={ret:+.4f}  price={price:.4f}  "
                    f"n_prior={n_prior}"
                )

                ch_client.insert(
                    "market_alerts",
                    [[
                        symbol,
                        ts,
                        price,
                        ret,
                        z,
                        THRESHOLD,
                        WINDOW_SIZE,
                        n_prior,              # prior sample count used in Z-score
                        source,
                        datetime.now(timezone.utc),
                    ]],
                    column_names=[
                        "symbol",
                        "alert_time",
                        "observed_price",
                        "price_return",
                        "z_score",
                        "threshold",
                        "window_size",
                        "sample_count",       # = n_prior (before appending ret)
                        "source",
                        "created_at",
                    ],
                )

                success, msg = dispatch_alert_webhook({
                    "event": "MARKET_VOLATILITY_ALERT",
                    "symbol": symbol,
                    "alert_time": ts.isoformat(),
                    "price_usd": price,
                    "price_return_pct": round(ret * 100, 3),
                    "z_score": round(z, 2),
                    "threshold": THRESHOLD,
                    "window_size": WINDOW_SIZE,
                    "sample_count": n_prior,
                    "source": source,
                })
                if success:
                    print(f"  [webhook] {msg}")
                elif msg and not msg.startswith("No webhook URL"):
                    print(f"  [webhook] {msg}")

        except Exception as exc:
            print(f"[anomaly-detector] error: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
