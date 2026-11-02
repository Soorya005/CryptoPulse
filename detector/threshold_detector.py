"""
threshold_detector.py — Fixed-threshold anomaly detector for crypto price/volume data.

Compares the latest 1-minute aggregate window against a rolling lookback of
previous windows.  Fires alerts when:

* **Price spike**: Percentage change in ``avg_price`` relative to the rolling
  mean exceeds ``price_pct_threshold``.
* **Volume surge**: ``total_volume`` exceeds the rolling mean by more than
  ``volume_multiplier`` times.

All thresholds are constructor arguments (no hardcoded magic numbers).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from detector.base import Alert, BaseDetector, Severity

logger = logging.getLogger("threshold_detector")


class ThresholdDetector(BaseDetector):
    """Fixed-threshold anomaly detector for price spikes and volume surges.

    Parameters
    ----------
    price_pct_threshold:
        Minimum **absolute** percentage change in ``avg_price`` (relative to
        the rolling mean of the lookback windows) that triggers a price-spike
        alert.  Default ``5.0`` means ±5 %.
    volume_multiplier:
        Factor by which ``total_volume`` must exceed the rolling-mean volume
        to trigger a volume-surge alert.  Default ``2.0`` means 2×.
    lookback:
        Number of preceding 1-min windows used to compute the rolling mean.
        Must be ≥ 1.  Default ``5``.
    min_history:
        Minimum number of historical windows required before the detector
        starts firing.  Prevents noisy alerts during cold-start.
        Default ``3``.
    """

    def __init__(
        self,
        price_pct_threshold: float = 5.0,
        volume_multiplier: float = 2.0,
        lookback: int = 5,
        min_history: int = 3,
    ) -> None:
        if lookback < 1:
            raise ValueError("lookback must be >= 1")
        if min_history < 1:
            raise ValueError("min_history must be >= 1")
        if min_history > lookback:
            raise ValueError("min_history cannot exceed lookback")
        if price_pct_threshold <= 0:
            raise ValueError("price_pct_threshold must be > 0")
        if volume_multiplier <= 0:
            raise ValueError("volume_multiplier must be > 0")

        self._price_pct_threshold = price_pct_threshold
        self._volume_multiplier = volume_multiplier
        self._lookback = lookback
        self._min_history = min_history

    # ------------------------------------------------------------------
    # BaseDetector interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "FixedThreshold"

    def detect(
        self,
        symbol: str,
        latest_window: dict[str, Any],
        history: list[dict[str, Any]],
    ) -> list[Alert]:
        """Run fixed-threshold checks and return any fired alerts."""

        alerts: list[Alert] = []

        # Trim history to the configured lookback depth
        recent = history[-self._lookback :] if len(history) > self._lookback else history

        if len(recent) < self._min_history:
            logger.debug(
                "[%s] %s: Skipping — only %d/%d history windows available",
                self.name,
                symbol,
                len(recent),
                self._min_history,
            )
            return alerts

        # --- Price-spike detection ---
        price_alert = self._check_price_spike(symbol, latest_window, recent)
        if price_alert is not None:
            alerts.append(price_alert)

        # --- Volume-surge detection ---
        volume_alert = self._check_volume_surge(symbol, latest_window, recent)
        if volume_alert is not None:
            alerts.append(volume_alert)

        return alerts

    # ------------------------------------------------------------------
    # Internal detection helpers
    # ------------------------------------------------------------------

    def _check_price_spike(
        self,
        symbol: str,
        latest: dict[str, Any],
        recent: list[dict[str, Any]],
    ) -> Alert | None:
        """Compare current avg_price against the rolling mean of recent windows."""

        current_price = float(latest["avg_price"])
        historical_prices = [float(w["avg_price"]) for w in recent]
        rolling_mean = sum(historical_prices) / len(historical_prices)

        if rolling_mean == 0:
            logger.warning("[%s] %s: Rolling mean price is 0 — skipping price check", self.name, symbol)
            return None

        pct_change = ((current_price - rolling_mean) / rolling_mean) * 100.0
        abs_pct = abs(pct_change)

        if abs_pct < self._price_pct_threshold:
            return None

        severity = self._classify_severity(abs_pct, self._price_pct_threshold)
        direction = "surged" if pct_change > 0 else "dropped"

        message = (
            f"{symbol} avg price {direction} {abs_pct:.2f}% "
            f"(${current_price:,.2f} vs rolling mean ${rolling_mean:,.2f} "
            f"over {len(recent)} windows)"
        )

        logger.info("[%s] ALERT: %s", self.name, message)

        return Alert(
            symbol=symbol,
            alert_type="price_spike",
            severity=severity,
            message=message,
            metric_value=round(pct_change, 4),
            threshold=self._price_pct_threshold,
            timestamp=latest["window_start"],
        )

    def _check_volume_surge(
        self,
        symbol: str,
        latest: dict[str, Any],
        recent: list[dict[str, Any]],
    ) -> Alert | None:
        """Compare current total_volume against the rolling mean of recent windows."""

        current_volume = float(latest["total_volume"])
        historical_volumes = [float(w["total_volume"]) for w in recent]
        rolling_mean = sum(historical_volumes) / len(historical_volumes)

        if rolling_mean == 0:
            logger.warning("[%s] %s: Rolling mean volume is 0 — skipping volume check", self.name, symbol)
            return None

        ratio = current_volume / rolling_mean

        if ratio < self._volume_multiplier:
            return None

        severity = self._classify_severity(ratio, self._volume_multiplier)

        message = (
            f"{symbol} volume surged {ratio:.2f}x vs rolling mean "
            f"({current_volume:,.6f} vs mean {rolling_mean:,.6f} "
            f"over {len(recent)} windows)"
        )

        logger.info("[%s] ALERT: %s", self.name, message)

        return Alert(
            symbol=symbol,
            alert_type="volume_surge",
            severity=severity,
            message=message,
            metric_value=round(ratio, 4),
            threshold=self._volume_multiplier,
            timestamp=latest["window_start"],
        )

    # ------------------------------------------------------------------
    # Severity classification
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_severity(actual: float, threshold: float) -> Severity:
        """Map how far a metric exceeds its threshold to a severity level.

        * **LOW**:    1×–2× the threshold exceeded
        * **MEDIUM**: 2×–3× the threshold exceeded
        * **HIGH**:   ≥ 3× the threshold exceeded
        """
        ratio = actual / threshold if threshold else 1.0
        if ratio >= 3.0:
            return Severity.HIGH
        if ratio >= 2.0:
            return Severity.MEDIUM
        return Severity.LOW
