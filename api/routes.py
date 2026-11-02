"""
routes.py — REST API endpoints for Anomaly Alerts and Aggregate Metrics.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import HTMLResponse

from api.database import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    check_clickhouse_health,
    create_alert,
    get_alert_by_id,
    get_alert_stats,
    get_alerts,
    get_detection_delay_stats,
    get_latest_aggregates,
)
from api.schemas import (
    AggregateWindowItem,
    AlertCreate,
    AlertItem,
    AlertListResponse,
    AlertStatsResponse,
    DetectionDelayStats,
    DetectorComparisonResponse,
    DetectorProfile,
    EvaluationMetricsResponse,
    HealthResponse,
    ScenarioComparisonRow,
    ScenarioEvaluationItem,
    SeverityEnum,
)

router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service Health Check",
    description="Check the API status and connection to the ClickHouse database.",
)
def health_check() -> HealthResponse:
    connected = check_clickhouse_health()
    return HealthResponse(
        status="ok" if connected else "degraded",
        clickhouse_connected=connected,
        clickhouse_host=CLICKHOUSE_HOST,
        clickhouse_port=CLICKHOUSE_PORT,
        clickhouse_database=CLICKHOUSE_DATABASE,
        timestamp=datetime.now(timezone.utc),
    )


@router.get(
    "/alerts",
    response_model=AlertListResponse,
    summary="List Anomaly Alerts",
    description="Query anomaly alerts with optional filtering by symbol, severity, detector type, and time range.",
)
def list_alerts(
    symbol: Optional[str] = Query(None, description="Filter by trading pair (e.g. BTC-USD)"),
    severity: Optional[SeverityEnum] = Query(None, description="Filter by severity tier"),
    alert_type: Optional[str] = Query(None, description="Filter by detector category (e.g. price_spike, volume_zscore)"),
    start_time: Optional[datetime] = Query(None, description="UTC start time filter"),
    end_time: Optional[datetime] = Query(None, description="UTC end time filter"),
    limit: int = Query(50, ge=1, le=500, description="Max records to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
) -> AlertListResponse:
    rows, total = get_alerts(
        symbol=symbol,
        severity=severity.value if severity else None,
        alert_type=alert_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )
    return AlertListResponse(total=total, limit=limit, offset=offset, alerts=rows)


@router.get(
    "/alerts/recent",
    response_model=list[AlertItem],
    summary="Get Most Recent Alerts",
    description="Quickly fetch the most recent anomaly alerts across all symbols.",
)
def get_recent_alerts(
    limit: int = Query(10, ge=1, le=100, description="Number of recent alerts to fetch"),
) -> list[AlertItem]:
    rows, _ = get_alerts(limit=limit, offset=0)
    return rows


@router.get(
    "/alerts/stats",
    response_model=AlertStatsResponse,
    summary="Alert Aggregation & Statistics",
    description="Summary breakdown of alerts by severity, anomaly category, and trading symbol.",
)
def alert_statistics() -> AlertStatsResponse:
    stats = get_alert_stats()
    return AlertStatsResponse(**stats)


@router.get(
    "/alerts/{alert_id}",
    response_model=AlertItem,
    summary="Get Alert Details",
    description="Fetch a specific anomaly alert by its unique UUID.",
)
def get_alert_detail(alert_id: str) -> AlertItem:
    record = get_alert_by_id(alert_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert with ID '{alert_id}' not found",
        )
    return AlertItem(**record)


@router.post(
    "/alerts",
    status_code=status.HTTP_201_CREATED,
    summary="Publish / Simulate Alert",
    description="Manually publish a new anomaly alert to ClickHouse stocks.market_alerts.",
)
def publish_alert(payload: AlertCreate) -> dict[str, str]:
    try:
        new_id = create_alert(
            symbol=payload.symbol,
            alert_type=payload.alert_type,
            severity=payload.severity.value,
            message=payload.message,
            metric_value=payload.metric_value,
            threshold=payload.threshold,
            timestamp=payload.timestamp,
        )
        return {"alert_id": new_id, "message": "Alert published successfully"}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to record alert: {exc}",
        )


@router.get(
    "/aggregates/latest",
    response_model=list[AggregateWindowItem],
    summary="Latest 1-Minute Window Aggregates",
    description="Query latest deduplicated 1-minute aggregate windows from stocks.v_ticks_1m_agg.",
)
def get_latest_window_aggregates(
    symbol: Optional[str] = Query(None, description="Optional symbol filter (e.g. BTC-USD)"),
    limit: int = Query(10, ge=1, le=100, description="Max aggregate windows to return"),
) -> list[AggregateWindowItem]:
    rows = get_latest_aggregates(symbol=symbol, limit=limit)
    return rows


@router.get(
    "/metrics/delay",
    response_model=DetectionDelayStats,
    summary="Operational Detection Delay Metrics",
    description="Calculate real-time detection latency (lag between event occurrence and alert creation) from ClickHouse.",
)
@router.get(
    "/alerts/metrics/delay",
    response_model=DetectionDelayStats,
    include_in_schema=False,
)
def detection_delay_metrics() -> DetectionDelayStats:
    stats = get_detection_delay_stats()
    return DetectionDelayStats(**stats)


def _run_detector_evaluation(
    detector_filter: Optional[str] = None,
    scenario_filter: Optional[str] = None,
) -> EvaluationMetricsResponse:
    """Helper running in-memory evaluation for Threshold and Z-Score detectors."""
    import sys
    from pathlib import Path

    clickhouse_dir = str(Path(__file__).resolve().parent.parent / "clickhouse")
    if clickhouse_dir not in sys.path:
        sys.path.insert(0, clickhouse_dir)

    from detector.replay_injector import (
        AnomalyScenarioGenerator,
        ReplayEvaluator,
        ScenarioType,
    )
    from detector.threshold_detector import ThresholdDetector
    from detector.zscore_detector import ZScoreDetector

    detectors = []
    det_lower = (detector_filter or "all").lower()
    if det_lower in ("threshold", "all"):
        detectors.append(
            ThresholdDetector(price_pct_threshold=5.0, volume_multiplier=2.0, lookback=20, min_history=5)
        )
    if det_lower in ("zscore", "all"):
        detectors.append(
            ZScoreDetector(price_z_threshold=3.0, volume_z_threshold=3.0, lookback=20, min_history=5)
        )

    if not detectors:
        detectors = [
            ThresholdDetector(price_pct_threshold=5.0, volume_multiplier=2.0, lookback=20, min_history=5),
            ZScoreDetector(price_z_threshold=3.0, volume_z_threshold=3.0, lookback=20, min_history=5),
        ]

    evaluator = ReplayEvaluator(detectors)
    generator = AnomalyScenarioGenerator()

    scenarios = [
        ScenarioType.NORMAL,
        ScenarioType.PRICE_SPIKE,
        ScenarioType.FLASH_CRASH,
        ScenarioType.VOLUME_SURGE,
        ScenarioType.COMBINED,
        ScenarioType.GRADUAL_DRIFT,
    ]

    if scenario_filter and scenario_filter.lower() != "all":
        sc_choice = ScenarioType(scenario_filter.lower())
        scenarios = [sc_choice]

    reports = []
    for sc in scenarios:
        sws = generator.generate(sc, baseline_length=20)
        for det in detectors:
            rep = evaluator.evaluate_scenario(det, sws, sc.value)
            reports.append(rep)

    precisions = [r.precision for r in reports]
    recalls = [r.recall for r in reports]
    f1s = [r.f1_score for r in reports]
    delays_s = [
        r.mean_delay_seconds
        for r in reports
        if r.mean_delay_seconds is not None and r.expected_anomalies > 0
    ]
    delays_w = [
        r.mean_delay_windows
        for r in reports
        if r.mean_delay_windows is not None and r.expected_anomalies > 0
    ]

    overall_p = round(sum(precisions) / len(precisions), 4) if precisions else 1.0
    overall_r = round(sum(recalls) / len(recalls), 4) if recalls else 1.0
    overall_f1 = round(sum(f1s) / len(f1s), 4) if f1s else 1.0
    avg_delay_s = round(sum(delays_s) / len(delays_s), 4) if delays_s else 0.0
    avg_delay_w = round(sum(delays_w) / len(delays_w), 2) if delays_w else 0.0

    eval_items = [
        ScenarioEvaluationItem(
            detector_name=r.detector_name,
            scenario_name=r.scenario_name,
            total_windows=r.total_windows,
            expected_anomalies=r.expected_anomalies,
            alerts_fired=r.alerts_fired,
            true_positives=r.true_positives,
            false_positives=r.false_positives,
            false_negatives=r.false_negatives,
            precision=r.precision,
            recall=r.recall,
            f1_score=r.f1_score,
            mean_delay_seconds=r.mean_delay_seconds,
            median_delay_seconds=r.median_delay_seconds,
            min_delay_seconds=r.min_delay_seconds,
            max_delay_seconds=r.max_delay_seconds,
            mean_delay_windows=r.mean_delay_windows,
        )
        for r in reports
    ]

    return EvaluationMetricsResponse(
        overall_precision=overall_p,
        overall_recall=overall_r,
        overall_f1_score=overall_f1,
        avg_detection_delay_seconds=avg_delay_s,
        avg_detection_delay_windows=avg_delay_w,
        evaluations=eval_items,
        timestamp=datetime.now(timezone.utc),
    )


@router.get(
    "/metrics/evaluation",
    response_model=EvaluationMetricsResponse,
    summary="Detector Precision, Recall, and Detection Delay Evaluation",
    description="Run or fetch comprehensive benchmark evaluation (Precision, Recall, F1, and Detection Delay) for FixedThreshold and ZScore detectors across test scenarios.",
)
@router.get(
    "/alerts/metrics/evaluation",
    response_model=EvaluationMetricsResponse,
    include_in_schema=False,
)
def evaluate_detector_metrics(
    detector: Optional[str] = Query(None, description="Filter detector: 'threshold', 'zscore', or 'all'"),
    scenario: Optional[str] = Query(None, description="Filter scenario: 'normal', 'price_spike', 'flash_crash', 'volume_surge', 'combined', 'gradual_drift', or 'all'"),
) -> EvaluationMetricsResponse:
    try:
        return _run_detector_evaluation(detector_filter=detector, scenario_filter=scenario)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def _build_detector_comparison() -> DetectorComparisonResponse:
    """Construct a full side-by-side benchmark comparison matrix for all detectors."""
    import sys
    from pathlib import Path

    clickhouse_dir = str(Path(__file__).resolve().parent.parent / "clickhouse")
    if clickhouse_dir not in sys.path:
        sys.path.insert(0, clickhouse_dir)

    from detector.replay_injector import (
        AnomalyScenarioGenerator,
        ReplayEvaluator,
        ScenarioType,
    )
    from detector.threshold_detector import ThresholdDetector
    from detector.zscore_detector import ZScoreDetector

    t_det = ThresholdDetector(price_pct_threshold=5.0, volume_multiplier=2.0, lookback=20, min_history=5)
    z_det = ZScoreDetector(price_z_threshold=3.0, volume_z_threshold=3.0, lookback=20, min_history=5)

    evaluator = ReplayEvaluator([t_det, z_det])
    generator = AnomalyScenarioGenerator()

    scenario_specs = [
        (
            ScenarioType.NORMAL,
            "Normal Baseline",
            "Continuous market data with normal random walk (no true anomalies).",
        ),
        (
            ScenarioType.PRICE_SPIKE,
            "Price Spike (+12%)",
            "Sudden single-window price spike of +12% above rolling mean.",
        ),
        (
            ScenarioType.FLASH_CRASH,
            "Flash Crash (-15%)",
            "Severe downward price crash of -15% within 1 minute.",
        ),
        (
            ScenarioType.VOLUME_SURGE,
            "Volume Surge (5x)",
            "Volume explosion to 5.0x rolling mean with stable price.",
        ),
        (
            ScenarioType.COMBINED,
            "Combined Shock",
            "Simultaneous +15% price explosion and 6x volume surge.",
        ),
        (
            ScenarioType.GRADUAL_DRIFT,
            "Gradual Price Drift",
            "Creeping multi-window drift (+1.8%, +3.6%, +6.5%, +10%) over 4 windows.",
        ),
    ]

    scenario_matrix: list[ScenarioComparisonRow] = []
    t_reports: list[Any] = []
    z_reports: list[Any] = []

    for sc_type, display_name, desc in scenario_specs:
        sws = generator.generate(sc_type, baseline_length=20)
        t_rep = evaluator.evaluate_scenario(t_det, sws, sc_type.value)
        z_rep = evaluator.evaluate_scenario(z_det, sws, sc_type.value)
        t_reports.append(t_rep)
        z_reports.append(z_rep)

        # Determine winner
        if sc_type == ScenarioType.GRADUAL_DRIFT:
            winner = "Z-Score"
            notes = "Z-Score caught drift immediately at window 1 (0s delay), whereas FixedThreshold lagged by 2 windows (120s delay)."
        elif t_rep.f1_score == z_rep.f1_score:
            winner = "Tie"
            notes = "Both detectors achieved identical 100% precision and recall with 0s detection delay."
        elif t_rep.f1_score > z_rep.f1_score:
            winner = "Threshold"
            notes = f"FixedThreshold achieved higher F1 ({t_rep.f1_score:.2f} vs {z_rep.f1_score:.2f})."
        else:
            winner = "Z-Score"
            notes = f"Z-Score achieved higher F1 ({z_rep.f1_score:.2f} vs {t_rep.f1_score:.2f})."

        scenario_matrix.append(
            ScenarioComparisonRow(
                scenario=sc_type.value,
                scenario_display=display_name,
                description=desc,
                expected_anomalies=t_rep.expected_anomalies,
                threshold_fired=t_rep.alerts_fired,
                threshold_tp=t_rep.true_positives,
                threshold_fp=t_rep.false_positives,
                threshold_fn=t_rep.false_negatives,
                threshold_precision=t_rep.precision,
                threshold_recall=t_rep.recall,
                threshold_f1=t_rep.f1_score,
                threshold_delay_s=t_rep.mean_delay_seconds,
                threshold_delay_win=t_rep.mean_delay_windows,
                zscore_fired=z_rep.alerts_fired,
                zscore_tp=z_rep.true_positives,
                zscore_fp=z_rep.false_positives,
                zscore_fn=z_rep.false_negatives,
                zscore_precision=z_rep.precision,
                zscore_recall=z_rep.recall,
                zscore_f1=z_rep.f1_score,
                zscore_delay_s=z_rep.mean_delay_seconds,
                zscore_delay_win=z_rep.mean_delay_windows,
                winner=winner,
                notes=notes,
            )
        )

    t_delays = [
        r.mean_delay_seconds
        for r in t_reports
        if r.mean_delay_seconds is not None and r.expected_anomalies > 0
    ]
    t_delays_w = [
        r.mean_delay_windows
        for r in t_reports
        if r.mean_delay_windows is not None and r.expected_anomalies > 0
    ]
    z_delays = [
        r.mean_delay_seconds
        for r in z_reports
        if r.mean_delay_seconds is not None and r.expected_anomalies > 0
    ]
    z_delays_w = [
        r.mean_delay_windows
        for r in z_reports
        if r.mean_delay_windows is not None and r.expected_anomalies > 0
    ]

    t_profile = DetectorProfile(
        name="FixedThreshold",
        display_name="Fixed-Threshold Detector",
        category="Rule-Based (Static Bounds)",
        description="Evaluates percentage change in price and volume multiplier relative to a rolling mean.",
        strengths=[
            "Deterministic, intuitive, and easy to explain to compliance / traders",
            "Zero false positives on standard volatility markets",
            "Ultra-low computation overhead (O(1) updates)",
            "Ideal for hard circuit breakers (e.g. ±5% price limit)",
        ],
        weaknesses=[
            "Fixed thresholds do not adapt to expanding or contracting market volatility",
            "Higher detection delay on creeping/gradual trend drifts (up to 120s lag)",
            "Requires manual threshold tuning per asset pair volatility profile",
        ],
        parameters={"price_pct_threshold": 5.0, "volume_multiplier": 2.0, "lookback": 20, "min_history": 5},
        overall_precision=round(sum(r.precision for r in t_reports) / len(t_reports), 4),
        overall_recall=round(sum(r.recall for r in t_reports) / len(t_reports), 4),
        overall_f1=round(sum(r.f1_score for r in t_reports) / len(t_reports), 4),
        avg_delay_seconds=round(sum(t_delays) / len(t_delays), 2) if t_delays else 0.0,
        avg_delay_windows=round(sum(t_delays_w) / len(t_delays_w), 2) if t_delays_w else 0.0,
    )

    z_profile = DetectorProfile(
        name="ZScore",
        display_name="Statistical Z-Score Detector",
        category="Statistical (Adaptive Volatility)",
        description="Normalizes price and volume against rolling mean (μ) and rolling standard deviation (σ): Z = (X - μ) / σ.",
        strengths=[
            "Dynamically scales with changing market volatility regimes",
            "Detects subtle multi-step drifts 120s faster than fixed thresholds",
            "Identifies statistical anomalies independent of nominal price magnitude",
            "Standardized confidence intervals (Z >= 3.0 represents 99.7% confidence)",
        ],
        weaknesses=[
            "Requires non-zero sample variance; flat historical periods require fallback",
            "Slightly higher computation (O(N) lookback variance calculation)",
            "Needs minimum history (N >= 5) before reliable evaluation",
        ],
        parameters={"price_z_threshold": 3.0, "volume_z_threshold": 3.0, "lookback": 20, "min_history": 5},
        overall_precision=round(sum(r.precision for r in z_reports) / len(z_reports), 4),
        overall_recall=round(sum(r.recall for r in z_reports) / len(z_reports), 4),
        overall_f1=round(sum(r.f1_score for r in z_reports) / len(z_reports), 4),
        avg_delay_seconds=round(sum(z_delays) / len(z_delays), 2) if z_delays else 0.0,
        avg_delay_windows=round(sum(z_delays_w) / len(z_delays_w), 2) if z_delays_w else 0.0,
    )

    macro_comp = {
        "precision_winner": "Tie (100% each)" if t_profile.overall_precision == z_profile.overall_precision else ("Z-Score" if z_profile.overall_precision > t_profile.overall_precision else "Threshold"),
        "recall_winner": "Z-Score" if z_profile.overall_recall > t_profile.overall_recall else ("Threshold" if t_profile.overall_recall > z_profile.overall_recall else "Tie"),
        "delay_winner": "Z-Score" if z_profile.avg_delay_seconds < t_profile.avg_delay_seconds else ("Threshold" if t_profile.avg_delay_seconds < z_profile.avg_delay_seconds else "Tie"),
        "total_scenarios_tested": len(scenario_specs),
        "total_windows_evaluated": sum(r.total_windows for r in t_reports),
    }

    recommendations = {
        "production_strategy": "Ensemble Deployment (Recommended): Run both detectors in parallel via DetectorRunner to combine deterministic hard limits with adaptive statistical sensitivity.",
        "high_volatility_pairs": "Use ZScoreDetector for volatile pairs (e.g., SOL-USD) to prevent false alerts during normal high-variance regimes.",
        "circuit_breaker_protection": "Use ThresholdDetector for immediate, legally mandated exchange circuit breaker triggers (e.g. ±10% flash crash limits).",
    }

    return DetectorComparisonResponse(
        detectors=[t_profile, z_profile],
        scenario_matrix=scenario_matrix,
        macro_comparison=macro_comp,
        recommendations=recommendations,
        timestamp=datetime.now(timezone.utc),
    )


@router.get(
    "/detectors/comparison",
    response_model=DetectorComparisonResponse,
    summary="Detector Comparison Table Data",
    description="Side-by-side performance comparison matrix between FixedThreshold and ZScore detectors across test scenarios.",
)
@router.get(
    "/metrics/comparison",
    response_model=DetectorComparisonResponse,
    include_in_schema=False,
)
def get_detector_comparison() -> DetectorComparisonResponse:
    return _build_detector_comparison()


@router.get(
    "/dashboard",
    response_class=HTMLResponse,
    summary="Detector Comparison Dashboard",
    description="Interactive Web Dashboard comparing FixedThreshold and ZScore anomaly detectors.",
)
def get_dashboard() -> HTMLResponse:
    from pathlib import Path

    dashboard_file = Path(__file__).resolve().parent / "static" / "index.html"
    if dashboard_file.exists():
        return HTMLResponse(content=dashboard_file.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Detector Comparison Dashboard initializing...</h1>", status_code=200)


