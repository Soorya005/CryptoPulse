"""
zscore_detector.py — Statistical Z-Score anomaly detector for crypto price/volume data.

Computes the rolling mean (μ) and sample standard deviation (σ) over a configurable
lookback window of preceding 1-minute aggregates, then evaluates the Z-score:

    Z = (X - μ) / σ

Fires alerts when:
* **Price Z-Score anomaly**: Absolute Z-score of ``avg_price`` exceeds ``price_z_threshold``
  (e.g., |Z| >= 3.0 standard deviations from the rolling mean).
* **Volume Z-Score surge**: Z-score of ``total_volume`` exceeds ``volume_z_threshold``
  (e.g., Z >= 3.0 standard deviations from the rolling mean).

All thresholds, lookback depths, and minimum history lengths are fully configurable.
"""

from __future__ import annotations

import logging
import math
import statistics
import sys
from pathlib import Path
from typing import Any

_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from detector.base import Alert, BaseDetector, Severity

logger = logging.getLogger("zscore_detector")


class ZScoreDetector(BaseDetector):
    """Statistical Z-score anomaly detector for price spikes/drops and volume surges.

    Parameters
    ----------
    price_z_threshold:
        Minimum **absolute** Z-score (|Z|) for ``avg_price`` relative to the rolling
        mean and sample standard deviation that triggers a price anomaly alert.
        Default ``3.0`` (approx. 99.7% confidence interval under normal distribution).
    volume_z_threshold:
        Minimum Z-score for ``total_volume`` relative to the rolling mean and
        sample standard deviation that triggers a volume surge alert.
        Default ``3.0``.
    lookback:
        Number of preceding 1-minute windows used to compute rolling mean and
        sample standard deviation. Must be >= 2. Default ``20``.
    min_history:
        Minimum number of historical windows required before the detector starts
        evaluating. Prevents noisy alerts during cold-start. Must be >= 2 and <= lookback.
        Default ``5``.
    price_alert_type:
        String identifier written to ``stocks.market_alerts.alert_type`` for price alerts.
        Default ``"price_zscore"``.
    volume_alert_type:
        String identifier written to ``stocks.market_alerts.alert_type`` for volume alerts.
        Default ``"volume_zscore"``.
    """

    def __init__(
        self,
        price_z_threshold: float = 3.0,
        volume_z_threshold: float = 3.0,
        lookback: int = 20,
        min_history: int = 5,
        price_alert_type: str = "price_zscore",
        volume_alert_type: str = "volume_zscore",
    ) -> None:
        if lookback < 2:
            raise ValueError("lookback must be >= 2 for standard deviation calculation")
        if min_history < 2:
            raise ValueError("min_history must be >= 2 for sample standard deviation")
        if min_history > lookback:
            raise ValueError("min_history cannot exceed lookback")
        if price_z_threshold <= 0:
            raise ValueError("price_z_threshold must be > 0")
        if volume_z_threshold <= 0:
            raise ValueError("volume_z_threshold must be > 0")

        self._price_z_threshold = price_z_threshold
        self._volume_z_threshold = volume_z_threshold
        self._lookback = lookback
        self._min_history = min_history
        self._price_alert_type = price_alert_type
        self._volume_alert_type = volume_alert_type

    # ------------------------------------------------------------------
    # BaseDetector interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "ZScore"

    def detect(
        self,
        symbol: str,
        latest_window: dict[str, Any],
        history: list[dict[str, Any]],
    ) -> list[Alert]:
        """Run statistical Z-score checks and return any fired alerts."""
        alerts: list[Alert] = []

        # Trim history to the configured lookback depth
        recent = history[-self._lookback :] if len(history) > self._lookback else history

        if len(recent) < self._min_history:
            logger.debug(
                "[%s] %s: Skipping — only %d/%d history windows available (cold start)",
                self.name,
                symbol,
                len(recent),
                self._min_history,
            )
            return alerts

        # --- Price Z-Score detection ---
        price_alert = self._check_price_zscore(symbol, latest_window, recent)
        if price_alert is not None:
            alerts.append(price_alert)

        # --- Volume Z-Score detection ---
        volume_alert = self._check_volume_zscore(symbol, latest_window, recent)
        if volume_alert is not None:
            alerts.append(volume_alert)

        return alerts

    # ------------------------------------------------------------------
    # Internal detection helpers
    # ------------------------------------------------------------------

    def _check_price_zscore(
        self,
        symbol: str,
        latest: dict[str, Any],
        recent: list[dict[str, Any]],
    ) -> Alert | None:
        """Calculate price Z-score against historical rolling mean and std dev."""
        current_price = float(latest["avg_price"])
        historical_prices = [float(w["avg_price"]) for w in recent]

        mean_price = statistics.mean(historical_prices)
        std_price = statistics.stdev(historical_prices)

        if std_price == 0.0 or math.isclose(std_price, 0.0, abs_tol=1e-12):
            logger.debug(
                "[%s] %s: Price standard deviation is 0 — skipping Z-score check",
                self.name,
                symbol,
            )
            return None

        z_score = (current_price - mean_price) / std_price
        abs_z = abs(z_score)

        if abs_z < self._price_z_threshold:
            return None

        severity = self._classify_severity(abs_z, self._price_z_threshold)
        direction = "spiked above" if z_score > 0 else "dropped below"

        message = (
            f"{symbol} avg price {direction} rolling mean by {abs_z:.2f} standard deviations "
            f"(${current_price:,.2f} vs mean ${mean_price:,.2f}, std=${std_price:,.2f}, "
            f"Z={z_score:+.2f} over {len(recent)} windows)"
        )

        logger.info("[%s] ALERT: %s", self.name, message)

        return Alert(
            symbol=symbol,
            alert_type=self._price_alert_type,
            severity=severity,
            message=message,
            metric_value=round(z_score, 4),
            threshold=self._price_z_threshold,
            timestamp=latest["window_start"],
        )

    def _check_volume_zscore(
        self,
        symbol: str,
        latest: dict[str, Any],
        recent: list[dict[str, Any]],
    ) -> Alert | None:
        """Calculate volume Z-score against historical rolling mean and std dev."""
        current_volume = float(latest["total_volume"])
        historical_volumes = [float(w["total_volume"]) for w in recent]

        mean_volume = statistics.mean(historical_volumes)
        std_volume = statistics.stdev(historical_volumes)

        if std_volume == 0.0 or math.isclose(std_volume, 0.0, abs_tol=1e-12):
            logger.debug(
                "[%s] %s: Volume standard deviation is 0 — skipping Z-score check",
                self.name,
                symbol,
            )
            return None

        z_score = (current_volume - mean_volume) / std_volume

        # For volume surges, we trigger when volume is abnormally higher than mean
        if z_score < self._volume_z_threshold:
            return None

        severity = self._classify_severity(z_score, self._volume_z_threshold)

        message = (
            f"{symbol} volume surged by {z_score:.2f} standard deviations above rolling mean "
            f"({current_volume:,.6f} vs mean {mean_volume:,.6f}, std={std_volume:,.6f}, "
            f"Z={z_score:+.2f} over {len(recent)} windows)"
        )

        logger.info("[%s] ALERT: %s", self.name, message)

        return Alert(
            symbol=symbol,
            alert_type=self._volume_alert_type,
            severity=severity,
            message=message,
            metric_value=round(z_score, 4),
            threshold=self._volume_z_threshold,
            timestamp=latest["window_start"],
        )

    # ------------------------------------------------------------------
    # Severity classification
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_severity(abs_z: float, threshold: float) -> Severity:
        """Map how far the Z-score exceeds the threshold to a severity level.

        * **LOW**:    1.0× <= ratio < 1.5× the threshold (e.g. 3.0 <= |Z| < 4.5)
        * **MEDIUM**: 1.5× <= ratio < 2.0× the threshold (e.g. 4.5 <= |Z| < 6.0)
        * **HIGH**:   >= 2.0× the threshold (e.g. |Z| >= 6.0)
        """
        ratio = abs_z / threshold if threshold else 1.0
        if ratio >= 2.0:
            return Severity.HIGH
        if ratio >= 1.5:
            return Severity.MEDIUM
        return Severity.LOW
