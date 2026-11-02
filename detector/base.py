"""
base.py — Abstract base class and data types for anomaly detectors.

Every detector in the pipeline inherits from BaseDetector and implements
the `detect()` method. The runner calls each detector with the latest
1-minute window and a configurable amount of historical context.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """Alert severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass(frozen=True)
class Alert:
    """Immutable value object representing a single anomaly alert.

    Fields map 1-to-1 to the ``stocks.market_alerts`` ClickHouse table.
    ``alert_id`` and ``created_at`` are generated server-side (DEFAULT
    clauses in the DDL), so they are not included here.
    """

    symbol: str
    alert_type: str
    severity: Severity
    message: str
    metric_value: float
    threshold: float
    timestamp: datetime  # event time of the window that triggered the alert

    def as_row(self) -> tuple:
        """Return a tuple suitable for ``clickhouse-connect`` bulk insert.

        Column order matches:
            (symbol, alert_type, severity, message,
             metric_value, threshold, timestamp)
        """
        return (
            self.symbol,
            self.alert_type,
            self.severity.value,
            self.message,
            self.metric_value,
            self.threshold,
            self.timestamp,
        )

    @staticmethod
    def insert_columns() -> list[str]:
        """Column names for the ``client.insert()`` call."""
        return [
            "symbol",
            "alert_type",
            "severity",
            "message",
            "metric_value",
            "threshold",
            "timestamp",
        ]

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to a dictionary for JSON serialization or logging."""
        return {
            "symbol": self.symbol,
            "alert_type": self.alert_type,
            "severity": self.severity.value,
            "message": self.message,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "timestamp": (
                self.timestamp.isoformat()
                if isinstance(self.timestamp, datetime)
                else str(self.timestamp)
            ),
        }


class BaseDetector(ABC):
    """Interface every anomaly detector must implement."""

    REQUIRED_WINDOW_FIELDS = {
        "symbol",
        "window_start",
        "window_end",
        "avg_price",
        "min_price",
        "max_price",
        "total_volume",
    }

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable detector name (used in logs and evaluation tables)."""

    @abstractmethod
    def detect(
        self,
        symbol: str,
        latest_window: dict[str, Any],
        history: list[dict[str, Any]],
    ) -> list[Alert]:
        """Run detection logic against the latest 1-min window.

        Parameters
        ----------
        symbol:
            Trading pair, e.g. ``"BTC-USD"``.
        latest_window:
            Dict with keys matching ``stocks.v_ticks_1m_agg`` columns:
            ``window_start``, ``window_end``, ``avg_price``, ``min_price``,
            ``max_price``, ``total_volume``, ``event_count``, ``avg_price_inr``.
        history:
            List of preceding window dicts (oldest first), used for rolling
            comparisons. May be empty during cold-start.

        Returns
        -------
        list[Alert]
            Zero or more alerts fired by this detector.
        """

    @classmethod
    def validate_window(cls, window: dict[str, Any]) -> bool:
        """Validate that a 1-minute aggregate window contains all necessary fields."""
        if not isinstance(window, dict):
            return False
        return cls.REQUIRED_WINDOW_FIELDS.issubset(window.keys())
