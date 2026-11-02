"""
test_api.py — Unit tests for the FastAPI Alert API endpoints.

All database queries are mocked so the test suite runs self-contained without
requiring a live ClickHouse instance.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import patch
import pytest

from fastapi import status
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def _sample_alert_dict(
    symbol: str = "BTC-USD",
    alert_type: str = "price_spike",
    severity: str = "high",
    alert_id: str | None = None,
) -> dict:
    """Helper creating a sample alert dict as returned by database queries."""
    return {
        "alert_id": alert_id or str(uuid.uuid4()),
        "symbol": symbol,
        "alert_type": alert_type,
        "severity": severity,
        "message": f"{symbol} price spiked abnormally",
        "metric_value": 12.5,
        "threshold": 5.0,
        "timestamp": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "created_at": datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc),
    }


def _sample_aggregate_dict(symbol: str = "BTC-USD") -> dict:
    """Helper creating a sample 1m aggregate window dict."""
    return {
        "symbol": symbol,
        "window_start": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "window_end": datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
        "event_count": 64,
        "avg_price": 65200.0,
        "min_price": 65150.0,
        "max_price": 65280.0,
        "total_volume": 12.45,
        "avg_price_inr": 5639800.0,
    }


class TestAlertAPI:
    """Unit tests for FastAPI endpoints."""

    def test_root_endpoint(self):
        response = client.get("/")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "docs_url" in data
        assert data["version"] == "1.0.0"

    @patch("api.routes.check_clickhouse_health")
    def test_health_endpoint_healthy(self, mock_health):
        mock_health.return_value = True
        response = client.get("/health")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "ok"
        assert data["clickhouse_connected"] is True

    @patch("api.routes.check_clickhouse_health")
    def test_health_endpoint_degraded(self, mock_health):
        mock_health.return_value = False
        response = client.get("/health")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "degraded"
        assert data["clickhouse_connected"] is False

    @patch("api.routes.get_alerts")
    def test_list_alerts_empty(self, mock_get_alerts):
        mock_get_alerts.return_value = ([], 0)
        response = client.get("/alerts")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 0
        assert data["alerts"] == []

    @patch("api.routes.get_alerts")
    def test_list_alerts_with_filters(self, mock_get_alerts):
        sample = _sample_alert_dict(symbol="ETH-USD", severity="medium")
        mock_get_alerts.return_value = ([sample], 1)

        response = client.get("/alerts?symbol=ETH-USD&severity=medium&limit=20")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total"] == 1
        assert len(data["alerts"]) == 1
        assert data["alerts"][0]["symbol"] == "ETH-USD"
        assert data["alerts"][0]["severity"] == "medium"

    @patch("api.routes.get_alerts")
    def test_get_recent_alerts(self, mock_get_alerts):
        samples = [
            _sample_alert_dict("BTC-USD"),
            _sample_alert_dict("SOL-USD"),
        ]
        mock_get_alerts.return_value = (samples, 2)

        response = client.get("/alerts/recent?limit=5")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 2
        assert data[0]["symbol"] == "BTC-USD"
        assert data[1]["symbol"] == "SOL-USD"

    @patch("api.routes.get_alert_stats")
    def test_alert_statistics(self, mock_stats):
        mock_stats.return_value = {
            "total_alerts": 42,
            "by_severity": {"high": 12, "medium": 20, "low": 10},
            "by_alert_type": {"price_spike": 25, "volume_zscore": 17},
            "by_symbol": {"BTC-USD": 30, "ETH-USD": 12},
            "recent_alerts_1h": 8,
        }
        response = client.get("/alerts/stats")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total_alerts"] == 42
        assert data["by_severity"]["high"] == 12
        assert data["recent_alerts_1h"] == 8

    @patch("api.routes.get_alert_by_id")
    def test_get_alert_by_id_found(self, mock_get_by_id):
        aid = str(uuid.uuid4())
        mock_get_by_id.return_value = _sample_alert_dict(alert_id=aid)

        response = client.get(f"/alerts/{aid}")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["alert_id"] == aid

    @patch("api.routes.get_alert_by_id")
    def test_get_alert_by_id_not_found(self, mock_get_by_id):
        mock_get_by_id.return_value = None
        fake_id = str(uuid.uuid4())

        response = client.get(f"/alerts/{fake_id}")
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert f"'{fake_id}' not found" in response.json()["detail"]

    @patch("api.routes.create_alert")
    def test_publish_alert_success(self, mock_create):
        new_uuid = str(uuid.uuid4())
        mock_create.return_value = new_uuid

        payload = {
            "symbol": "BTC-USD",
            "alert_type": "price_spike",
            "severity": "high",
            "message": "Manual test alert",
            "metric_value": 8.5,
            "threshold": 5.0,
        }
        response = client.post("/alerts", json=payload)
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert data["alert_id"] == new_uuid
        assert data["message"] == "Alert published successfully"

    @patch("api.routes.get_latest_aggregates")
    def test_get_latest_window_aggregates(self, mock_aggs):
        mock_aggs.return_value = [_sample_aggregate_dict("BTC-USD")]

        response = client.get("/aggregates/latest?symbol=BTC-USD&limit=5")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data) == 1
        assert data[0]["symbol"] == "BTC-USD"
        assert data[0]["avg_price"] == 65200.0

    @patch("api.routes.get_detection_delay_stats")
    def test_operational_detection_delay_endpoint(self, mock_delay_stats):
        mock_delay_stats.return_value = {
            "total_alerts": 50,
            "avg_delay_seconds": 1.42,
            "median_delay_seconds": 1.20,
            "min_delay_seconds": 0.35,
            "max_delay_seconds": 4.15,
            "p95_delay_seconds": 2.90,
        }
        response = client.get("/metrics/delay")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["total_alerts"] == 50
        assert data["avg_delay_seconds"] == 1.42
        assert data["median_delay_seconds"] == 1.20
        assert data["min_delay_seconds"] == 0.35
        assert data["max_delay_seconds"] == 4.15
        assert data["p95_delay_seconds"] == 2.90

    def test_evaluation_metrics_endpoint(self):
        response = client.get("/metrics/evaluation")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "overall_precision" in data
        assert "overall_recall" in data
        assert "overall_f1_score" in data
        assert "avg_detection_delay_seconds" in data
        assert "avg_detection_delay_windows" in data
        assert "evaluations" in data
        assert len(data["evaluations"]) > 0

        # Verify precision, recall, and detection delay metrics in evaluation items
        for item in data["evaluations"]:
            assert item["precision"] >= 0.0
            assert item["recall"] >= 0.0
            assert item["f1_score"] >= 0.0
            assert "mean_delay_seconds" in item
            assert "mean_delay_windows" in item

    def test_evaluation_metrics_filtered(self):
        response = client.get("/metrics/evaluation?detector=threshold&scenario=price_spike")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert len(data["evaluations"]) == 1
        item = data["evaluations"][0]
        assert item["detector_name"] == "FixedThreshold"
        assert item["scenario_name"] == "price_spike"
        assert item["precision"] == 1.0
        assert item["recall"] == 1.0
        assert item["mean_delay_seconds"] == 0.0

    @patch("api.routes.get_alert_stats")
    def test_alert_statistics_with_delay(self, mock_stats):
        mock_stats.return_value = {
            "total_alerts": 10,
            "by_severity": {"high": 5, "medium": 5},
            "by_alert_type": {"price_spike": 10},
            "by_symbol": {"BTC-USD": 10},
            "recent_alerts_1h": 2,
            "detection_delay": {
                "total_alerts": 10,
                "avg_delay_seconds": 0.85,
                "median_delay_seconds": 0.80,
                "min_delay_seconds": 0.50,
                "max_delay_seconds": 1.50,
                "p95_delay_seconds": 1.40,
            },
        }
        response = client.get("/alerts/stats")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["detection_delay"] is not None
        assert data["detection_delay"]["avg_delay_seconds"] == 0.85

    def test_detector_comparison_endpoint(self):
        response = client.get("/detectors/comparison")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "detectors" in data
        assert len(data["detectors"]) == 2
        detector_names = [d["name"] for d in data["detectors"]]
        assert "FixedThreshold" in detector_names
        assert "ZScore" in detector_names

        assert "scenario_matrix" in data
        assert len(data["scenario_matrix"]) >= 5
        for row in data["scenario_matrix"]:
            assert "threshold_precision" in row
            assert "zscore_precision" in row
            assert "winner" in row

        assert "macro_comparison" in data
        assert "recommendations" in data

    def test_dashboard_endpoint(self):
        response = client.get("/dashboard")
        assert response.status_code == status.HTTP_200_OK
        assert "text/html" in response.headers.get("content-type", "")
        content = response.text
        assert "Detector Comparison Matrix" in content
        assert "comparisonTable" in content


