"""
replay_injector.py — Replay and Anomaly Injection Testing Framework for Crypto Analytics.

Provides tools to:
1. Generate synthetic market data scenarios (baseline normal, price spikes, flash crashes,
   volume surges, combined anomalies).
2. Replay scenarios in-memory against ThresholdDetector and ZScoreDetector to verify
   detection precision, recall, and severity accuracy without external dependencies.
3. Inject synthetic anomalous aggregate windows directly into ClickHouse (stocks.ticks_1m_agg)
   to validate end-to-end alerting in DetectorRunner and verify persistence in stocks.market_alerts.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable

from detector.base import Alert, BaseDetector, Severity
from detector.metrics import (
    calculate_detection_delay_metrics,
    calculate_f1_score,
    calculate_precision,
    calculate_recall,
)
from detector.threshold_detector import ThresholdDetector
from detector.zscore_detector import ZScoreDetector

logger = logging.getLogger("replay_injector")


class ScenarioType(str, Enum):
    """Types of synthetic test scenarios."""

    NORMAL = "normal"
    PRICE_SPIKE = "price_spike"
    FLASH_CRASH = "flash_crash"
    VOLUME_SURGE = "volume_surge"
    COMBINED = "combined"
    GRADUAL_DRIFT = "gradual_drift"


@dataclass
class ScenarioWindow:
    """A single aggregate window in an injection scenario."""

    window: dict[str, Any]
    is_anomaly: bool
    expected_type: str | None = None  # "price", "volume", "both", or None


@dataclass
class ScenarioReport:
    """Evaluation metrics for a detector on a synthetic scenario."""

    detector_name: str
    scenario_name: str
    total_windows: int
    expected_anomalies: int
    alerts_fired: int
    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float
    f1_score: float
    alerts: list[Alert]
    mean_delay_seconds: float | None = 0.0
    median_delay_seconds: float | None = 0.0
    min_delay_seconds: float | None = 0.0
    max_delay_seconds: float | None = 0.0
    mean_delay_windows: float | None = 0.0
    delays: list[float] = field(default_factory=list)
    delays_windows: list[int] = field(default_factory=list)

    @property
    def detection_delay_seconds(self) -> float | None:
        """Alias for mean_delay_seconds."""
        return self.mean_delay_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert scenario evaluation report to dictionary."""
        return {
            "detector_name": self.detector_name,
            "scenario_name": self.scenario_name,
            "total_windows": self.total_windows,
            "expected_anomalies": self.expected_anomalies,
            "alerts_fired": self.alerts_fired,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "mean_delay_seconds": self.mean_delay_seconds,
            "median_delay_seconds": self.median_delay_seconds,
            "min_delay_seconds": self.min_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "mean_delay_windows": self.mean_delay_windows,
            "delays": self.delays,
            "delays_windows": self.delays_windows,
        }


# ---------------------------------------------------------------------------
# Scenario Generator
# ---------------------------------------------------------------------------

class AnomalyScenarioGenerator:
    """Generates synthetic 1-minute aggregate windows for testing and injection."""

    def __init__(
        self,
        symbol: str = "BTC-USD",
        base_price: float = 65000.0,
        base_volume: float = 10.0,
        inr_rate: float = 86.5,
    ) -> None:
        self.symbol = symbol
        self.base_price = base_price
        self.base_volume = base_volume
        self.inr_rate = inr_rate

    def _make_window(
        self,
        start_time: datetime,
        price: float,
        volume: float,
        event_count: int = 50,
    ) -> dict[str, Any]:
        """Construct a single aggregate window dictionary."""
        end_time = start_time + timedelta(minutes=1)
        return {
            "symbol": self.symbol,
            "window_start": start_time,
            "window_end": end_time,
            "event_count": event_count,
            "avg_price": round(price, 2),
            "min_price": round(price * 0.995, 2),
            "max_price": round(price * 1.005, 2),
            "total_volume": round(volume, 6),
            "avg_price_inr": round(price * self.inr_rate, 2),
        }

    def generate(
        self,
        scenario: ScenarioType | str,
        baseline_length: int = 20,
        start_time: datetime | None = None,
    ) -> list[ScenarioWindow]:
        """Generate a complete scenario containing baseline history and target event(s).

        Parameters
        ----------
        scenario:
            ScenarioType enum or string ("normal", "price_spike", "flash_crash",
            "volume_surge", "combined").
        baseline_length:
            Number of normal preceding windows (defaults to 20 to satisfy cold-start
            and rolling lookbacks).
        start_time:
            Timestamp for the first window. Defaults to UTC now minus total minutes.
        """
        if isinstance(scenario, str):
            scenario = ScenarioType(scenario)

        total_windows = baseline_length + 1
        if start_time is None:
            now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            start_time = now - timedelta(minutes=total_windows)

        result: list[ScenarioWindow] = []

        # 1. Baseline windows: deterministic slight variations (mean ~ base_price, base_volume)
        # Small sine-wave like offsets to ensure non-zero sample standard deviation
        price_offsets = [-0.3, 0.4, -0.2, 0.5, -0.4, 0.2, -0.1, 0.3, -0.5, 0.1]
        volume_offsets = [-0.5, 0.6, -0.3, 0.4, -0.2, 0.5, -0.4, 0.2, -0.1, 0.3]

        for i in range(baseline_length):
            t = start_time + timedelta(minutes=i)
            p_offset = price_offsets[i % len(price_offsets)] * (self.base_price * 0.002)
            v_offset = volume_offsets[i % len(volume_offsets)] * (self.base_volume * 0.05)

            p = self.base_price + p_offset
            v = max(0.1, self.base_volume + v_offset)

            w = self._make_window(t, p, v)
            result.append(ScenarioWindow(window=w, is_anomaly=False))

        # 2. Injected Anomaly window at index [baseline_length]
        anomaly_time = start_time + timedelta(minutes=baseline_length)

        if scenario == ScenarioType.NORMAL:
            # Baseline continuation
            p = self.base_price + (price_offsets[0] * (self.base_price * 0.002))
            v = self.base_volume
            w = self._make_window(anomaly_time, p, v)
            result.append(ScenarioWindow(window=w, is_anomaly=False))

        elif scenario == ScenarioType.PRICE_SPIKE:
            # +12% price jump (exceeds 5% fixed threshold and > 5 standard deviations)
            spike_price = self.base_price * 1.12
            w = self._make_window(anomaly_time, spike_price, self.base_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        elif scenario == ScenarioType.FLASH_CRASH:
            # -15% price drop (exceeds -5% fixed threshold and > -5 standard deviations)
            drop_price = self.base_price * 0.85
            w = self._make_window(anomaly_time, drop_price, self.base_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        elif scenario == ScenarioType.VOLUME_SURGE:
            # 5x volume surge (exceeds 2.0x threshold and > 5 standard deviations)
            surge_volume = self.base_volume * 5.0
            w = self._make_window(anomaly_time, self.base_price, surge_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="volume"))

        elif scenario == ScenarioType.COMBINED:
            # +15% price jump and 6x volume surge
            comb_price = self.base_price * 1.15
            comb_volume = self.base_volume * 6.0
            w = self._make_window(anomaly_time, comb_price, comb_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="both"))

        elif scenario == ScenarioType.GRADUAL_DRIFT:
            # Multi-window gradual price drift starting at anomaly_time:
            # Window 0: +1.8% (sub-threshold, anomalous regime onset begins)
            # Window 1: +3.6% (sub-threshold)
            # Window 2: +6.5% (crosses fixed 5% threshold -> first alert fired here, delay = 2 windows / 120s)
            # Window 3: +10.0% (continuation of anomaly)
            drift_multipliers = [1.018, 1.036, 1.065, 1.100]
            for j, mult in enumerate(drift_multipliers):
                step_time = anomaly_time + timedelta(minutes=j)
                step_price = self.base_price * mult
                w = self._make_window(step_time, step_price, self.base_volume)
                result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        return result


# ---------------------------------------------------------------------------
# Replay Evaluator (In-Memory Testing)
# ---------------------------------------------------------------------------

class ReplayEvaluator:
    """Evaluates detectors against synthetic scenarios in-memory without ClickHouse."""

    def __init__(self, detectors: list[BaseDetector]) -> None:
        self.detectors = detectors

    def evaluate_scenario(
        self,
        detector: BaseDetector,
        scenario_windows: list[ScenarioWindow],
        scenario_name: str,
    ) -> ScenarioReport:
        """Feed a sequence of windows to a detector and compute performance metrics."""
        history: list[dict[str, Any]] = []
        all_alerts: list[Alert] = []

        tp = 0
        fp = 0
        fn = 0
        expected_anomalies = sum(1 for sw in scenario_windows if sw.is_anomaly)

        delays_seconds: list[float] = []
        delays_windows: list[int] = []
        current_anomaly_start_time: datetime | None = None
        current_anomaly_start_idx: int | None = None
        episode_detected = False

        for i, sw in enumerate(scenario_windows):
            w = sw.window
            sym = w["symbol"]

            fired = detector.detect(sym, w, history)
            history.append(w)

            # Track onset of an anomaly episode
            if sw.is_anomaly and current_anomaly_start_time is None:
                current_anomaly_start_time = w["window_start"]
                current_anomaly_start_idx = i
                episode_detected = False

            if fired:
                all_alerts.extend(fired)
                if sw.is_anomaly:
                    tp += 1
                    # Measure detection delay from onset of this anomaly episode to first alert
                    if not episode_detected and current_anomaly_start_time is not None:
                        delay_s = max(0.0, (w["window_start"] - current_anomaly_start_time).total_seconds())
                        delay_w = max(0, i - (current_anomaly_start_idx or 0))
                        delays_seconds.append(delay_s)
                        delays_windows.append(delay_w)
                        episode_detected = True
                else:
                    fp += 1
            else:
                if sw.is_anomaly:
                    fn += 1

            if not sw.is_anomaly:
                current_anomaly_start_time = None
                current_anomaly_start_idx = None
                episode_detected = False

        precision = calculate_precision(tp, fp, expected_anomalies)
        recall = calculate_recall(tp, fn, expected_anomalies)
        f1 = calculate_f1_score(precision, recall, expected_anomalies, len(all_alerts))
        delay_metrics = calculate_detection_delay_metrics(
            delays_seconds, delays_windows, expected_anomalies
        )

        return ScenarioReport(
            detector_name=detector.name,
            scenario_name=scenario_name,
            total_windows=len(scenario_windows),
            expected_anomalies=expected_anomalies,
            alerts_fired=len(all_alerts),
            true_positives=tp,
            false_positives=fp,
            false_negatives=fn,
            precision=precision,
            recall=recall,
            f1_score=f1,
            alerts=all_alerts,
            mean_delay_seconds=delay_metrics.mean_delay_seconds,
            median_delay_seconds=delay_metrics.median_delay_seconds,
            min_delay_seconds=delay_metrics.min_delay_seconds,
            max_delay_seconds=delay_metrics.max_delay_seconds,
            mean_delay_windows=delay_metrics.mean_delay_windows,
            delays=delay_metrics.delays_seconds,
            delays_windows=delay_metrics.delays_windows,
        )

    def run_all_scenarios(
        self,
        baseline_length: int = 20,
        scenarios: list[ScenarioType | str] | None = None,
        include_drift: bool = False,
    ) -> list[ScenarioReport]:
        """Run standard scenarios across all configured detectors."""
        generator = AnomalyScenarioGenerator()
        reports: list[ScenarioReport] = []

        if scenarios is not None:
            target_scenarios = [
                s if isinstance(s, ScenarioType) else ScenarioType(s)
                for s in scenarios
            ]
        else:
            target_scenarios = [
                ScenarioType.NORMAL,
                ScenarioType.PRICE_SPIKE,
                ScenarioType.FLASH_CRASH,
                ScenarioType.VOLUME_SURGE,
                ScenarioType.COMBINED,
            ]
            if include_drift:
                target_scenarios.append(ScenarioType.GRADUAL_DRIFT)

        for sc in target_scenarios:
            sws = generator.generate(sc, baseline_length=baseline_length)
            for det in self.detectors:
                report = self.evaluate_scenario(det, sws, sc.value)
                reports.append(report)

        return reports


# ---------------------------------------------------------------------------
# ClickHouse Live Injector
# ---------------------------------------------------------------------------

class ClickHouseInjector:
    """Injects synthetic windows directly into ClickHouse stocks.ticks_1m_agg."""

    def __init__(self, client_factory: Callable[[], Any] | None = None) -> None:
        if client_factory is None:
            from detector.runner import get_clickhouse_client
            client_factory = get_clickhouse_client
        self._client_factory = client_factory

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
                datetime.now(timezone.utc),  # calculated_at
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


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def print_evaluation_summary(reports: list[ScenarioReport]) -> None:
    """Print formatted terminal report of scenario evaluations."""
    print("\n" + "=" * 114)
    print("                    ANOMALY DETECTOR REPLAY EVALUATION REPORT")
    print("=" * 114)
    header = (
        f"{'Detector':<15} | {'Scenario':<14} | {'Expected':<8} | "
        f"{'Fired':<6} | {'TP':<4} | {'FP':<4} | {'FN':<4} | "
        f"{'Precision':<9} | {'Recall':<6} | {'F1':<5} | {'Delay (s)':<9} | {'Delay (win)':<11}"
    )
    print(header)
    print("-" * 114)

    for r in reports:
        delay_s_str = f"{r.mean_delay_seconds:.2f}" if r.mean_delay_seconds is not None else "N/A"
        delay_win_str = f"{r.mean_delay_windows:.1f}" if r.mean_delay_windows is not None else "N/A"
        line = (
            f"{r.detector_name:<15} | {r.scenario_name:<14} | {r.expected_anomalies:<8} | "
            f"{r.alerts_fired:<6} | {r.true_positives:<4} | {r.false_positives:<4} | {r.false_negatives:<4} | "
            f"{r.precision:<9.2f} | {r.recall:<6.2f} | {r.f1_score:<5.2f} | "
            f"{delay_s_str:<9} | {delay_win_str:<11}"
        )
        print(line)

    print("=" * 114 + "\n")


def main() -> None:
    """CLI for replay evaluation and ClickHouse injection."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Replay & Anomaly Injection Testing Utility")
    parser.add_argument(
        "--mode",
        choices=["evaluate", "inject"],
        default="evaluate",
        help="Mode: 'evaluate' (in-memory test) or 'inject' (insert into ClickHouse)",
    )
    parser.add_argument(
        "--scenario",
        choices=["normal", "price_spike", "flash_crash", "volume_surge", "combined", "gradual_drift", "all"],
        default="all",
        help="Scenario to run (default: all)",
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
        help="Number of normal baseline windows (default: 20)",
    )
    args = parser.parse_args()

    detectors = [
        ThresholdDetector(price_pct_threshold=5.0, volume_multiplier=2.0, lookback=20, min_history=5),
        ZScoreDetector(price_z_threshold=3.0, volume_z_threshold=3.0, lookback=20, min_history=5),
    ]

    generator = AnomalyScenarioGenerator(symbol=args.symbol, base_price=args.base_price)

    if args.mode == "evaluate":
        evaluator = ReplayEvaluator(detectors)

        if args.scenario == "all":
            reports = evaluator.run_all_scenarios(baseline_length=args.baseline_length)
        else:
            sws = generator.generate(ScenarioType(args.scenario), baseline_length=args.baseline_length)
            reports = [
                evaluator.evaluate_scenario(det, sws, args.scenario)
                for det in detectors
            ]

        print_evaluation_summary(reports)

        for r in reports:
            if r.alerts:
                print(f"[{r.detector_name} - {r.scenario_name}] Fired Alerts:")
                for a in r.alerts:
                    print(f"  -> [{a.severity.value.upper()}] {a.alert_type}: {a.message}")
                print()

    elif args.mode == "inject":
        target_scenario = "price_spike" if args.scenario == "all" else args.scenario
        print(f"Generating and injecting scenario '{target_scenario}' for {args.symbol} into ClickHouse...")

        sws = generator.generate(ScenarioType(target_scenario), baseline_length=args.baseline_length)
        windows = [sw.window for sw in sws]

        injector = ClickHouseInjector()
        count = injector.inject_windows(windows)
        print(f"\nSuccessfully injected {count} windows for {args.symbol} into stocks.ticks_1m_agg.")
        print("Run the DetectorRunner to observe anomaly alerts:")
        print("  python -m detector.runner --detector all --once")


if __name__ == "__main__":
    main()
