"""
schemas.py — Pydantic request/response schemas for the Alert API.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class SeverityEnum(str, Enum):
    """Supported severity tiers for anomaly alerts."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AlertBase(BaseModel):
    """Base fields representing an anomaly alert."""

    symbol: str = Field(..., description="Trading pair symbol (e.g., BTC-USD)")
    alert_type: str = Field(..., description="Anomaly detector category (e.g., price_spike, volume_zscore)")
    severity: SeverityEnum = Field(..., description="Severity classification (low, medium, high)")
    message: str = Field(..., description="Human-readable alert message")
    metric_value: float = Field(..., description="Observed metric value (pct change or Z-score)")
    threshold: float = Field(..., description="Trigger threshold value")
    timestamp: datetime = Field(..., description="Event time of the window that triggered the alert")


class AlertCreate(BaseModel):
    """Payload to manually publish or simulate an alert."""

    symbol: str
    alert_type: str
    severity: SeverityEnum
    message: str
    metric_value: float
    threshold: float
    timestamp: Optional[datetime] = None


class AlertItem(AlertBase):
    """Representation of an alert stored in stocks.market_alerts."""

    alert_id: str = Field(..., description="Unique UUID identifier for the alert")
    created_at: Optional[datetime] = Field(None, description="Time the alert was persisted to ClickHouse")


class AlertListResponse(BaseModel):
    """Paginated collection of anomaly alerts."""

    total: int = Field(..., description="Total count matching query filters")
    limit: int = Field(..., description="Page limit")
    offset: int = Field(..., description="Page offset")
    alerts: list[AlertItem] = Field(..., description="List of alert records")


class DetectionDelayStats(BaseModel):
    """Operational detection delay (latency) metrics from ClickHouse market alerts."""

    total_alerts: int = Field(..., description="Total count of alerts analyzed")
    avg_delay_seconds: float = Field(..., description="Average detection latency from window event time to alert creation")
    median_delay_seconds: float = Field(..., description="Median detection latency in seconds")
    min_delay_seconds: float = Field(..., description="Minimum detection latency in seconds")
    max_delay_seconds: float = Field(..., description="Maximum detection latency in seconds")
    p95_delay_seconds: float = Field(..., description="95th percentile detection latency in seconds")


class AlertStatsResponse(BaseModel):
    """Aggregated statistics across anomaly alerts."""

    total_alerts: int
    by_severity: dict[str, int]
    by_alert_type: dict[str, int]
    by_symbol: dict[str, int]
    recent_alerts_1h: int
    detection_delay: Optional[DetectionDelayStats] = None


class ScenarioEvaluationItem(BaseModel):
    """Evaluation report for a detector evaluated on a synthetic scenario."""

    detector_name: str = Field(..., description="Name of the detector (e.g. FixedThreshold, ZScore)")
    scenario_name: str = Field(..., description="Name of the scenario (e.g. price_spike, gradual_drift)")
    total_windows: int = Field(..., description="Total aggregate windows evaluated")
    expected_anomalies: int = Field(..., description="Ground-truth anomalous windows")
    alerts_fired: int = Field(..., description="Total alerts triggered by detector")
    true_positives: int = Field(..., description="Correctly identified anomalous windows")
    false_positives: int = Field(..., description="False alarms on normal windows")
    false_negatives: int = Field(..., description="Missed anomalous windows")
    precision: float = Field(..., description="Precision: TP / (TP + FP)")
    recall: float = Field(..., description="Recall: TP / (TP + FN)")
    f1_score: float = Field(..., description="Harmonic mean of precision and recall")
    mean_delay_seconds: Optional[float] = Field(None, description="Average detection delay from anomaly onset to first alert (seconds)")
    median_delay_seconds: Optional[float] = Field(None, description="Median detection delay in seconds")
    min_delay_seconds: Optional[float] = Field(None, description="Minimum detection delay in seconds")
    max_delay_seconds: Optional[float] = Field(None, description="Maximum detection delay in seconds")
    mean_delay_windows: Optional[float] = Field(None, description="Average detection delay in number of 1-minute windows")


class EvaluationMetricsResponse(BaseModel):
    """Summary of detector evaluation metrics across benchmark scenarios."""

    overall_precision: float = Field(..., description="Macro-averaged precision across evaluated scenarios")
    overall_recall: float = Field(..., description="Macro-averaged recall across evaluated scenarios")
    overall_f1_score: float = Field(..., description="Macro-averaged F1 score")
    avg_detection_delay_seconds: Optional[float] = Field(None, description="Average detection delay across all anomaly scenarios")
    avg_detection_delay_windows: Optional[float] = Field(None, description="Average detection delay in windows")
    evaluations: list[ScenarioEvaluationItem] = Field(..., description="Per-scenario evaluation breakdown")
    timestamp: datetime = Field(..., description="Evaluation timestamp")


class DetectorProfile(BaseModel):
    """Architectural profile and aggregate benchmark performance for a single detector."""

    name: str = Field(..., description="Detector identifier (e.g. FixedThreshold, ZScore)")
    display_name: str = Field(..., description="Human-readable title")
    category: str = Field(..., description="Algorithm classification (Rule-based or Statistical)")
    description: str = Field(..., description="Summary of mathematical approach")
    strengths: list[str] = Field(..., description="Key algorithmic advantages")
    weaknesses: list[str] = Field(..., description="Trade-offs and vulnerabilities")
    parameters: dict[str, Any] = Field(..., description="Active configuration parameters")
    overall_precision: float = Field(..., description="Macro precision")
    overall_recall: float = Field(..., description="Macro recall")
    overall_f1: float = Field(..., description="Macro F1 score")
    avg_delay_seconds: float = Field(..., description="Average detection delay in seconds")
    avg_delay_windows: float = Field(..., description="Average detection delay in windows")


class ScenarioComparisonRow(BaseModel):
    """Side-by-side detector comparison on a specific test scenario."""

    scenario: str = Field(..., description="Scenario identifier (e.g. gradual_drift)")
    scenario_display: str = Field(..., description="Formatted scenario name")
    description: str = Field(..., description="Scenario event characteristics")
    expected_anomalies: int = Field(..., description="Total ground-truth anomalous windows")

    # Threshold Detector stats
    threshold_fired: int
    threshold_tp: int
    threshold_fp: int
    threshold_fn: int
    threshold_precision: float
    threshold_recall: float
    threshold_f1: float
    threshold_delay_s: Optional[float]
    threshold_delay_win: Optional[float]

    # Z-Score Detector stats
    zscore_fired: int
    zscore_tp: int
    zscore_fp: int
    zscore_fn: int
    zscore_precision: float
    zscore_recall: float
    zscore_f1: float
    zscore_delay_s: Optional[float]
    zscore_delay_win: Optional[float]

    winner: str = Field(..., description="Winner detector for this scenario ('Threshold', 'Z-Score', or 'Tie')")
    notes: str = Field(..., description="Analysis rationale")


class DetectorComparisonResponse(BaseModel):
    """Complete dataset for the Detector Comparison Dashboard."""

    detectors: list[DetectorProfile]
    scenario_matrix: list[ScenarioComparisonRow]
    macro_comparison: dict[str, Any]
    recommendations: dict[str, str]
    timestamp: datetime


class AggregateWindowItem(BaseModel):
    """Deduplicated 1-minute window aggregate from stocks.v_ticks_1m_agg."""

    symbol: str
    window_start: datetime
    window_end: datetime
    event_count: int
    avg_price: float
    min_price: float
    max_price: float
    total_volume: float
    avg_price_inr: Optional[float] = None


class HealthResponse(BaseModel):
    """Service health and database connectivity status."""

    status: str
    clickhouse_connected: bool
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    timestamp: datetime
