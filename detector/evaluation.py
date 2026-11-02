"""
evaluation.py — Precision, Recall, and Detection Delay Evaluation Framework.

Provides benchmark tools to:
1. Calculate formal classification accuracy (TP, FP, FN) and metrics:
   - Precision = TP / (TP + FP)
   - Recall = TP / (TP + FN)
   - F1-Score = 2 * (P * R) / (P + R)
2. Measure Detection Delay (Time-to-Detect / TTD latency):
   - Mean, median, min, max delay in seconds and window steps from anomaly onset.
3. Compare Fixed-Threshold and Statistical Z-Score detectors across synthetic scenarios.
4. Render formatted Detector Comparison Tables in CLI or as structured dictionary.
"""

from __future__ import annotations

import argparse
import logging
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Sequence

_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
_parent_dir = str(Path(__file__).resolve().parent)
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

from detector.base import Alert, BaseDetector, Severity
from detector.metrics import (
    DetectionDelayMetrics,
    DetectionMetrics,
    calculate_detection_delay_metrics,
    calculate_f1_score,
    calculate_precision,
    calculate_recall,
)
from detector.threshold_detector import ThresholdDetector
from detector.zscore_detector import ZScoreDetector

logger = logging.getLogger("detector.evaluation")


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
    expected_type: str | None = None


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
        """Generate a complete scenario containing baseline history and target anomaly."""
        if isinstance(scenario, str):
            scenario = ScenarioType(scenario)

        total_windows = baseline_length + 1
        if start_time is None:
            now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            start_time = now - timedelta(minutes=total_windows)

        result: list[ScenarioWindow] = []

        # 1. Baseline windows with slight realistic variations
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

        # 2. Anomaly window(s) starting at index [baseline_length]
        anomaly_time = start_time + timedelta(minutes=baseline_length)

        if scenario == ScenarioType.NORMAL:
            p = self.base_price + (price_offsets[0] * (self.base_price * 0.002))
            v = self.base_volume
            w = self._make_window(anomaly_time, p, v)
            result.append(ScenarioWindow(window=w, is_anomaly=False))

        elif scenario == ScenarioType.PRICE_SPIKE:
            spike_price = self.base_price * 1.12
            w = self._make_window(anomaly_time, spike_price, self.base_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        elif scenario == ScenarioType.FLASH_CRASH:
            drop_price = self.base_price * 0.85
            w = self._make_window(anomaly_time, drop_price, self.base_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        elif scenario == ScenarioType.VOLUME_SURGE:
            surge_volume = self.base_volume * 5.0
            w = self._make_window(anomaly_time, self.base_price, surge_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="volume"))

        elif scenario == ScenarioType.COMBINED:
            comb_price = self.base_price * 1.15
            comb_volume = self.base_volume * 6.0
            w = self._make_window(anomaly_time, comb_price, comb_volume)
            result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="both"))

        elif scenario == ScenarioType.GRADUAL_DRIFT:
            drift_multipliers = [1.018, 1.036, 1.065, 1.100]
            for j, mult in enumerate(drift_multipliers):
                step_time = anomaly_time + timedelta(minutes=j)
                step_price = self.base_price * mult
                w = self._make_window(step_time, step_price, self.base_volume)
                result.append(ScenarioWindow(window=w, is_anomaly=True, expected_type="price"))

        return result


class ReplayEvaluator:
    """Evaluates anomaly detectors against synthetic market scenarios."""

    def __init__(self, detectors: list[BaseDetector]) -> None:
        self.detectors = detectors

    def evaluate_scenario(
        self,
        detector: BaseDetector,
        scenario_windows: list[ScenarioWindow],
        scenario_name: str,
    ) -> ScenarioReport:
        """Feed a sequence of windows to a detector and compute Precision, Recall, and Delay."""
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
        include_drift: bool = True,
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


def print_evaluation_summary(reports: list[ScenarioReport]) -> None:
    """Print formatted terminal Detector Comparison Table."""
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
    """CLI to run detector evaluation and output the comparison table."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="Anomaly Detector Evaluation & Comparison Tool")
    parser.add_argument(
        "--scenario",
        choices=["normal", "price_spike", "flash_crash", "volume_surge", "combined", "gradual_drift", "all"],
        default="all",
        help="Target scenario to evaluate (default: all)",
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

    evaluator = ReplayEvaluator(detectors)
    generator = AnomalyScenarioGenerator()

    if args.scenario == "all":
        reports = evaluator.run_all_scenarios(baseline_length=args.baseline_length, include_drift=True)
    else:
        sws = generator.generate(ScenarioType(args.scenario), baseline_length=args.baseline_length)
        reports = [evaluator.evaluate_scenario(det, sws, args.scenario) for det in detectors]

    print_evaluation_summary(reports)


if __name__ == "__main__":
    main()
