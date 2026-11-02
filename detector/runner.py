"""
runner.py — Polling orchestrator that runs registered detectors against ClickHouse.

Periodically queries ``stocks.v_ticks_1m_agg`` for the latest 1-minute windows,
feeds them (along with historical context) to each registered detector, and
writes any fired alerts to ``stocks.market_alerts``.
"""

from __future__ import annotations
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from detector.base import Alert, BaseDetector

logger = logging.getLogger("detector_runner")

# ---------------------------------------------------------------------------
# ClickHouse connection defaults (same env vars as spark/clickhouse_writer.py)
# ---------------------------------------------------------------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "stocks")

# Runner-specific
POLL_INTERVAL_S = int(os.getenv("DETECTOR_POLL_INTERVAL_S", "30"))
HISTORY_DEPTH = int(os.getenv("DETECTOR_HISTORY_DEPTH", "30"))
MAX_SEEN_WINDOWS = int(os.getenv("DETECTOR_MAX_SEEN_WINDOWS", "10000"))

# Singleton client (same pattern as clickhouse_writer.py)
_client = None


def get_clickhouse_client():
    """Return a cached clickhouse-connect client, reconnecting if necessary."""
    global _client
    if _client is not None:
        try:
            if _client.ping():
                return _client
        except Exception:
            _client = None

    import clickhouse_connect

    logger.info(
        "Connecting to ClickHouse at %s:%d (database=%s)",
        CLICKHOUSE_HOST,
        CLICKHOUSE_PORT,
        CLICKHOUSE_DATABASE,
    )
    _client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=10,
        send_receive_timeout=30,
    )
    return _client


# ---------------------------------------------------------------------------
# Data access helpers
# ---------------------------------------------------------------------------

def fetch_latest_windows(
    client,
    symbol: str,
    limit: int = HISTORY_DEPTH,
) -> list[dict[str, Any]]:
    """Fetch the most recent 1-minute aggregate windows for *symbol*.

    Reads from ``stocks.v_ticks_1m_agg`` (deduplicated view) and returns
    rows ordered **oldest-first** so that ``history[-1]`` is the most recent.
    """
    query = (
        "SELECT symbol, window_start, window_end, event_count, "
        "       avg_price, min_price, max_price, total_volume, avg_price_inr "
        "FROM stocks.v_ticks_1m_agg "
        "WHERE symbol = {symbol:String} "
        "ORDER BY window_start DESC "
        "LIMIT {limit:UInt32}"
    )
    result = client.query(query, parameters={"symbol": symbol, "limit": limit})
    columns = result.column_names

    rows = [dict(zip(columns, row)) for row in result.result_rows]
    rows.reverse()  # oldest first
    return rows


def fetch_active_symbols(client) -> list[str]:
    """Return symbols that have at least one aggregate window in the last hour."""
    query = (
        "SELECT DISTINCT symbol "
        "FROM stocks.v_ticks_1m_agg "
        "WHERE window_start >= now() - INTERVAL 1 HOUR "
        "ORDER BY symbol"
    )
    result = client.query(query)
    return [row[0] for row in result.result_rows]


def write_alerts(client, alerts: list[Alert]) -> int:
    """Bulk-insert alerts into ``stocks.market_alerts``.  Returns count written."""
    if not alerts:
        return 0

    rows = [a.as_row() for a in alerts]
    client.insert(
        table="market_alerts",
        data=rows,
        column_names=Alert.insert_columns(),
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class DetectorRunner:
    """Polls ClickHouse and runs a list of detectors in a loop.

    Parameters
    ----------
    detectors:
        One or more ``BaseDetector`` instances to evaluate each cycle.
    poll_interval_s:
        Seconds to sleep between polling cycles.
    history_depth:
        Number of historical windows to fetch per symbol.
    client_factory:
        Callable returning a ClickHouse client (defaults to module-level
        ``get_clickhouse_client``).  Pass a mock for testing.
    max_seen_windows:
        Maximum number of window keys to keep in memory before pruning oldest.
    """

    def __init__(
        self,
        detectors: list[BaseDetector],
        poll_interval_s: int = POLL_INTERVAL_S,
        history_depth: int = HISTORY_DEPTH,
        client_factory=None,
        max_seen_windows: int = MAX_SEEN_WINDOWS,
    ) -> None:
        if not detectors:
            raise ValueError("At least one detector must be provided")
        self._detectors: list[BaseDetector] = list(detectors)
        self._poll_interval_s = poll_interval_s
        self._history_depth = history_depth
        self._client_factory = client_factory or get_clickhouse_client
        self._max_seen_windows = max_seen_windows
        self._running = False

        # Track which windows we have already processed to avoid duplicate alerts
        # Key: (symbol, window_start)
        self._seen_windows: set[tuple[str, Any]] = set()

        # Telemetry / operational metrics
        self._total_cycles = 0
        self._total_alerts = 0
        self._total_written = 0
        self._last_run_time: datetime | None = None
        self._detection_delays: list[float] = []

    @property
    def detectors(self) -> list[BaseDetector]:
        """Return a copy of the list of registered detectors."""
        return list(self._detectors)

    def add_detector(self, detector: BaseDetector) -> None:
        """Register an additional detector dynamically."""
        if detector not in self._detectors:
            self._detectors.append(detector)
            logger.info("Registered detector: %s", detector.name)

    @property
    def stats(self) -> dict[str, Any]:
        """Return run statistics and health metrics."""
        avg_delay = (
            round(sum(self._detection_delays) / len(self._detection_delays), 4)
            if self._detection_delays
            else 0.0
        )
        return {
            "total_cycles": self._total_cycles,
            "total_alerts": self._total_alerts,
            "total_written": self._total_written,
            "seen_windows_count": len(self._seen_windows),
            "avg_detection_delay_s": avg_delay,
            "last_run_time": (
                self._last_run_time.isoformat() if self._last_run_time else None
            ),
        }

    def _prune_seen_windows(self) -> None:
        """Prevent unbounded memory growth by discarding older entries."""
        if len(self._seen_windows) > self._max_seen_windows:
            excess = len(self._seen_windows) - (self._max_seen_windows // 2)
            # Sets are unordered in older Python, but converting to list allows dropping half
            items = list(self._seen_windows)
            self._seen_windows = set(items[excess:])
            logger.debug(
                "Pruned seen windows cache from %d to %d entries",
                len(items),
                len(self._seen_windows),
            )

    def run_once(self) -> list[Alert]:
        """Execute a single polling cycle.  Returns all alerts fired."""
        self._total_cycles += 1
        now_utc = datetime.now(timezone.utc)
        self._last_run_time = now_utc
        self._prune_seen_windows()

        client = self._client_factory()
        symbols = fetch_active_symbols(client)
        if not symbols:
            logger.debug("No active symbols found in the last hour")
            return []

        all_alerts: list[Alert] = []

        for sym in symbols:
            windows = fetch_latest_windows(client, sym, self._history_depth)
            if not windows:
                continue

            latest = windows[-1]
            key = (sym, latest["window_start"])

            if key in self._seen_windows:
                continue  # already processed
            self._seen_windows.add(key)

            history = windows[:-1]

            for det in self._detectors:
                try:
                    fired = det.detect(sym, latest, history)
                    if fired:
                        all_alerts.extend(fired)
                        for a in fired:
                            ts = a.timestamp
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                            delay = max(0.0, (now_utc - ts).total_seconds())
                            self._detection_delays.append(delay)
                except Exception:
                    logger.exception(
                        "Detector %s crashed for %s window %s",
                        det.name,
                        sym,
                        latest.get("window_start"),
                    )

        self._total_alerts += len(all_alerts)

        # Write alerts to ClickHouse
        if all_alerts:
            try:
                count = write_alerts(client, all_alerts)
                self._total_written += count
                logger.info("Wrote %d alert(s) to stocks.market_alerts", count)
            except Exception:
                logger.exception("Failed to write %d alerts to ClickHouse", len(all_alerts))

        return all_alerts

    def run_forever(self) -> None:
        """Blocking loop: poll → detect → write alerts → sleep → repeat."""
        self._running = True
        detector_names = ", ".join(d.name for d in self._detectors)
        logger.info(
            "Starting DetectorRunner with [%s] — polling every %ds",
            detector_names,
            self._poll_interval_s,
        )

        try:
            while self._running:
                try:
                    alerts = self.run_once()
                    if alerts:
                        logger.info("Cycle complete — %d alert(s) fired", len(alerts))
                except Exception:
                    logger.exception("Error in detection cycle — will retry next interval")

                time.sleep(self._poll_interval_s)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received — shutting down gracefully...")
        finally:
            self._running = False
            logger.info(
                "DetectorRunner stopped. Total cycles: %d, alerts fired: %d",
                self._total_cycles,
                self._total_alerts,
            )

    def stop(self) -> None:
        """Signal the run loop to exit after the current cycle."""
        self._running = False
        logger.info("DetectorRunner stop requested")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run anomaly detectors (fixed-threshold, Z-score, or both) from CLI."""
    import argparse

    from detector.threshold_detector import ThresholdDetector
    from detector.zscore_detector import ZScoreDetector

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Crypto anomaly detector runner")
    parser.add_argument(
        "--detector",
        choices=["threshold", "zscore", "all"],
        default="all",
        help="Which detector(s) to run: 'threshold', 'zscore', or 'all' (default: all)",
    )
    parser.add_argument(
        "--price-threshold",
        type=float,
        default=5.0,
        help="ThresholdDetector: Price spike %% threshold (default: 5.0)",
    )
    parser.add_argument(
        "--volume-multiplier",
        type=float,
        default=2.0,
        help="ThresholdDetector: Volume surge multiplier (default: 2.0)",
    )
    parser.add_argument(
        "--price-z-threshold",
        type=float,
        default=3.0,
        help="ZScoreDetector: Price Z-Score threshold (default: 3.0)",
    )
    parser.add_argument(
        "--volume-z-threshold",
        type=float,
        default=3.0,
        help="ZScoreDetector: Volume Z-Score threshold (default: 3.0)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=20,
        help="Rolling lookback window count (default: 20)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=POLL_INTERVAL_S,
        help=f"Seconds between polls (default: {POLL_INTERVAL_S})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single detection cycle and exit",
    )
    args = parser.parse_args()

    detectors: list[BaseDetector] = []
    if args.detector in ("threshold", "all"):
        detectors.append(
            ThresholdDetector(
                price_pct_threshold=args.price_threshold,
                volume_multiplier=args.volume_multiplier,
                lookback=args.lookback,
            )
        )
    if args.detector in ("zscore", "all"):
        z_lookback = max(2, args.lookback)
        z_min_hist = min(5, z_lookback)
        detectors.append(
            ZScoreDetector(
                price_z_threshold=args.price_z_threshold,
                volume_z_threshold=args.volume_z_threshold,
                lookback=z_lookback,
                min_history=z_min_hist,
            )
        )

    runner = DetectorRunner(
        detectors=detectors,
        poll_interval_s=args.poll_interval,
    )

    if args.once:
        alerts = runner.run_once()
        print(f"\n{'=' * 60}")
        print(f"  Detection cycle complete — {len(alerts)} alert(s) fired")
        print(f"{'=' * 60}")
        for a in alerts:
            print(f"  [{a.severity.value.upper()}] {a.alert_type}: {a.message}")
    else:
        runner.run_forever()


if __name__ == "__main__":
    main()
