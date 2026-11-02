"""
metrics.py — Precision, Recall, and Detection Delay Metrics for Crypto Anomaly Detection.

Provides formal calculations and dataclasses for:
1. Classification accuracy: True Positives (TP), False Positives (FP),
   False Negatives (FN), True Negatives (TN).
2. Performance metrics: Precision, Recall, F1-Score, and Specificity.
3. Detection Delay (latency / Time-to-Detect):
   - Time elapsed from anomaly onset to the first alert triggered by a detector.
   - Mean, median, min, max delay in seconds and window steps.
4. Operational pipeline delay:
   - Latency between event occurrence (window timestamp) and alert ingestion (created_at).
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence


def calculate_precision(
    tp: int,
    fp: int,
    expected_anomalies: int | None = None,
) -> float:
    """Calculate Precision = TP / (TP + FP).

    Edge case handling:
    If no alerts were fired (TP + FP == 0):
      - If no anomalies were expected (clean data with 0 false alarms): returns 1.0 (100% precision).
      - If anomalies were expected but none were detected: returns 0.0.
    """
    total_fired = tp + fp
    if total_fired == 0:
        if expected_anomalies is not None and expected_anomalies == 0:
            return 1.0
        return 0.0
    return round(tp / total_fired, 4)


def calculate_recall(
    tp: int,
    fn: int,
    expected_anomalies: int | None = None,
) -> float:
    """Calculate Recall (Sensitivity) = TP / (TP + FN).

    Edge case handling:
    If no anomalies were present (TP + FN == 0 or expected_anomalies == 0):
      - Returns 1.0 (no missed anomalies).
    """
    total_actual = tp + fn
    if total_actual == 0 or (expected_anomalies is not None and expected_anomalies == 0):
        return 1.0
    return round(tp / total_actual, 4)


def calculate_f1_score(
    precision: float,
    recall: float,
    expected_anomalies: int = 0,
    alerts_fired: int = 0,
) -> float:
    """Calculate F1-Score = 2 * (Precision * Recall) / (Precision + Recall).

    Edge case handling:
    If precision + recall == 0:
      - If expected_anomalies == 0 and alerts_fired == 0: returns 1.0.
      - Otherwise: returns 0.0.
    """
    denom = precision + recall
    if denom <= 0:
        if expected_anomalies == 0 and alerts_fired == 0:
            return 1.0
        return 0.0
    return round((2 * precision * recall) / denom, 4)


@dataclass
class DetectionDelayMetrics:
    """Detection delay (latency) measurements for detected anomalies."""

    mean_delay_seconds: float | None = 0.0
    median_delay_seconds: float | None = 0.0
    min_delay_seconds: float | None = 0.0
    max_delay_seconds: float | None = 0.0
    mean_delay_windows: float | None = 0.0
    delays_seconds: list[float] = field(default_factory=list)
    delays_windows: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert delay metrics to dictionary."""
        return {
            "mean_delay_seconds": self.mean_delay_seconds,
            "median_delay_seconds": self.median_delay_seconds,
            "min_delay_seconds": self.min_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "mean_delay_windows": self.mean_delay_windows,
            "delays_seconds": self.delays_seconds,
            "delays_windows": self.delays_windows,
        }


def calculate_detection_delay_metrics(
    delays_seconds: Sequence[float],
    delays_windows: Sequence[int] | None = None,
    expected_anomalies: int = 0,
) -> DetectionDelayMetrics:
    """Compute summary statistics for anomaly detection delays.

    Parameters
    ----------
    delays_seconds:
        List of delay times in seconds from anomaly onset to alert.
    delays_windows:
        Optional list of delay times in 1-minute window steps.
    expected_anomalies:
        Number of expected anomalies in the scenario.
    """
    sec_list = list(delays_seconds)
    win_list = list(delays_windows) if delays_windows is not None else []

    if not sec_list:
        # If no anomalies were expected, delay is 0.0 (instant/no lag).
        # If anomalies were expected but none detected (FN), delay is None (missed).
        default_val = 0.0 if expected_anomalies == 0 else None
        return DetectionDelayMetrics(
            mean_delay_seconds=default_val,
            median_delay_seconds=default_val,
            min_delay_seconds=default_val,
            max_delay_seconds=default_val,
            mean_delay_windows=default_val,
            delays_seconds=[],
            delays_windows=[],
        )

    mean_s = round(statistics.mean(sec_list), 4)
    med_s = round(statistics.median(sec_list), 4)
    min_s = round(min(sec_list), 4)
    max_s = round(max(sec_list), 4)

    mean_w = round(statistics.mean(win_list), 2) if win_list else 0.0

    return DetectionDelayMetrics(
        mean_delay_seconds=mean_s,
        median_delay_seconds=med_s,
        min_delay_seconds=min_s,
        max_delay_seconds=max_s,
        mean_delay_windows=mean_w,
        delays_seconds=sec_list,
        delays_windows=win_list,
    )


@dataclass
class DetectionMetrics:
    """Comprehensive performance metrics container."""

    total_samples: int
    expected_anomalies: int
    alerts_fired: int
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    precision: float
    recall: float
    f1_score: float
    delay_metrics: DetectionDelayMetrics

    @property
    def mean_delay_seconds(self) -> float | None:
        return self.delay_metrics.mean_delay_seconds

    @property
    def mean_delay_windows(self) -> float | None:
        return self.delay_metrics.mean_delay_windows

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "total_samples": self.total_samples,
            "expected_anomalies": self.expected_anomalies,
            "alerts_fired": self.alerts_fired,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
            "true_negatives": self.true_negatives,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "mean_delay_seconds": self.delay_metrics.mean_delay_seconds,
            "median_delay_seconds": self.delay_metrics.median_delay_seconds,
            "min_delay_seconds": self.delay_metrics.min_delay_seconds,
            "max_delay_seconds": self.delay_metrics.max_delay_seconds,
            "mean_delay_windows": self.delay_metrics.mean_delay_windows,
            "delays_seconds": self.delay_metrics.delays_seconds,
            "delays_windows": self.delay_metrics.delays_windows,
        }


@dataclass
class OperationalDelayStats:
    """Operational detection delay for live ClickHouse alerts."""

    total_alerts: int
    avg_delay_seconds: float
    median_delay_seconds: float
    min_delay_seconds: float
    max_delay_seconds: float
    p95_delay_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_alerts": self.total_alerts,
            "avg_delay_seconds": self.avg_delay_seconds,
            "median_delay_seconds": self.median_delay_seconds,
            "min_delay_seconds": self.min_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "p95_delay_seconds": self.p95_delay_seconds,
        }
