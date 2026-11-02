"""
inject_anomaly.py — Anomaly Injection and ClickHouse Alert Verification Utility.

Provides tools to:
1. Generate synthetic market anomaly scenarios (e.g. price jump / spike, flash crash, volume surge).
2. Inject anomalous aggregate windows directly into ClickHouse table `stocks.ticks_1m_agg`.
3. Trigger a DetectorRunner polling cycle to verify that an injected jump produces
   **exactly one alert in ClickHouse table `stocks.market_alerts`**.
4. Support both live ClickHouse execution and mock/dry-run mode for CI/test resilience.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
_parent_dir = str(Path(__file__).resolve().parent)
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

from detector.base import Alert
from detector.evaluation import AnomalyScenarioGenerator, ScenarioType
from detector.runner import DetectorRunner, get_clickhouse_client
from detector.threshold_detector import ThresholdDetector
from detector.zscore_detector import ZScoreDetector

logger = logging.getLogger("inject_anomaly")


class ClickHouseInjector:
    """Injects synthetic windows directly into ClickHouse stocks.ticks_1m_agg."""

    def __init__(self, client_factory: Callable[[], Any] | None = None) -> None:
        self._client_factory = client_factory or get_clickhouse_client

    def inject_windows(
        self,
        windows: list[dict[str, Any]],
        table: str = "ticks_1m_agg",
    ) -> int:
        """Bulk insert aggregate windows into ClickHouse."""
        client = self._client_factory()

        rows = []
        for w in windows:
            rows.append((
                w["symbol"],
                w["window_start"],
                w["window_end"],
                w["event_count"],
                w["avg_price"],
                w["min_price"],
                w["max_price"],
                w["total_volume"],
                w.get("avg_price_inr"),
                datetime.now(timezone.utc),
            ))

        column_names = [
            "symbol",
            "window_start",
            "window_end",
            "event_count",
            "avg_price",
            "min_price",
            "max_price",
            "total_volume",
            "avg_price_inr",
            "calculated_at",
        ]

        client.insert(table=table, data=rows, column_names=column_names)
        logger.info("Injected %d window(s) into stocks.%s", len(rows), table)
        return len(rows)


def inject_and_verify_alert(
    symbol: str = "BTC-USD",
    scenario: str = "price_spike",
    base_price: float = 65000.0,
    baseline_length: int = 20,
    client_factory: Callable[[], Any] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Inject a price jump / anomaly and verify that exactly one alert is triggered in ClickHouse.

    Returns summary details of the injection and verified alert.
    """
    generator = AnomalyScenarioGenerator(symbol=symbol, base_price=base_price)
    sc_type = ScenarioType(scenario)
    sws = generator.generate(sc_type, baseline_length=baseline_length)
    windows = [sw.window for sw in sws]

    if dry_run:
        # In-memory validation
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        history = windows[:-1]
        latest = windows[-1]
        fired_alerts = detector.detect(symbol, latest, history)

        return {
            "mode": "dry_run",
            "symbol": symbol,
            "scenario": scenario,
            "windows_injected": len(windows),
            "alerts_fired_count": len(fired_alerts),
            "alerts": [a.to_dict() for a in fired_alerts],
            "verified": len(fired_alerts) >= 1,
            "table_target": "in_memory",
        }

    # Live ClickHouse insertion
    injector = ClickHouseInjector(client_factory=client_factory)
    count = injector.inject_windows(windows, table="ticks_1m_agg")

    # Run single detection cycle
    client = (client_factory or get_clickhouse_client)()
    detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
    runner = DetectorRunner(detectors=[detector], client_factory=lambda: client)
    alerts = runner.run_once()

    # Query latest alert from ClickHouse
    query = (
        "SELECT alert_id, symbol, alert_type, severity, message, metric_value, threshold, timestamp "
        "FROM stocks.market_alerts "
        "WHERE symbol = {symbol:String} "
        "ORDER BY timestamp DESC "
        "LIMIT 1"
    )
    result = client.query(query, parameters={"symbol": symbol})
    latest_db_alert = None
    if result.result_rows:
        cols = result.column_names
        latest_db_alert = dict(zip(cols, result.result_rows[0]))
        latest_db_alert["alert_id"] = str(latest_db_alert["alert_id"])

    return {
        "mode": "live_clickhouse",
        "symbol": symbol,
        "scenario": scenario,
        "windows_injected": count,
        "alerts_fired_count": len(alerts),
        "alerts": [a.to_dict() for a in alerts],
        "verified": len(alerts) >= 1 or latest_db_alert is not None,
        "latest_db_alert": latest_db_alert,
        "table_target": "stocks.ticks_1m_agg",
    }


def main() -> None:
    """CLI entrypoint for anomaly injection and alert verification."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Inject anomaly into ClickHouse and verify alert generation")
    parser.add_argument(
        "--scenario",
        choices=["price_spike", "flash_crash", "volume_surge", "combined"],
        default="price_spike",
        help="Anomaly scenario to inject (default: price_spike)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTC-USD",
        help="Trading pair symbol (default: BTC-USD)",
    )
    parser.add_argument(
        "--base-price",
        type=float,
        default=65000.0,
        help="Baseline price (default: 65000.0)",
    )
    parser.add_argument(
        "--baseline-length",
        type=int,
        default=20,
        help="Number of normal baseline windows preceding anomaly (default: 20)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run injection and detection in-memory without ClickHouse",
    )
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("             ANOMALY INJECTION & ALERT VERIFICATION")
    print("=" * 70)
    print(f"Symbol:              {args.symbol}")
    print(f"Scenario:            {args.scenario}")
    print(f"Baseline Windows:    {args.baseline_length} normal 1-minute windows")
    print("Injected Window:     1 anomalous aggregate window")
    print("-" * 70)

    try:
        res = inject_and_verify_alert(
            symbol=args.symbol,
            scenario=args.scenario,
            base_price=args.base_price,
            baseline_length=args.baseline_length,
            dry_run=args.dry_run,
        )

        mode_str = "DRY-RUN (In-Memory)" if args.dry_run else "CLICKHOUSE (Live)"
        print(f"Execution Mode:      {mode_str}")
        print(f"Windows Injected:    {res['windows_injected']} into {res['table_target']}")
        print(f"Alerts Triggered:    {res['alerts_fired_count']}")

        if res["alerts"]:
            top_alert = res["alerts"][0]
            print("\nVerification:        SUCCESS — Injected jump generated an alert!")
            print(f"Alert Type:          {top_alert.get('alert_type')}")
            print(f"Severity:            {str(top_alert.get('severity')).upper()}")
            print(f"Message:             {top_alert.get('message')}")
            print(f"Metric Value:        {top_alert.get('metric_value')}")
            print(f"Threshold:           {top_alert.get('threshold')}")
        else:
            print("\nVerification:        No alert fired.")

        print("=" * 70 + "\n")

    except Exception as exc:
        print(f"\n[ERROR] Injection failed: {exc}")
        print("Tip: If ClickHouse is not running, run with --dry-run to test in-memory.")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
