"""
test_detector.py — Unit tests for the Anomaly Detector modules (Threshold & Z-Score & Replay Injector).

Covers:
  • ThresholdDetector: price spike, volume surge, severity classification,
    cold-start skipping, edge cases (zero rolling mean, boundary values).
  • ZScoreDetector: statistical price Z-score anomalies, volume Z-score surges,
    sigma threshold validation, cold-start skipping, flat history handling.
  • DetectorRunner: orchestration with mocked ClickHouse helpers,
    multi-detector concurrent execution, deduplication of already-seen windows,
    error resilience.
  • Replay & Injection: synthetic anomaly scenario generation, in-memory replay
    evaluation (precision/recall/F1 metrics), and ClickHouse window injection.
  • Alert data object: as_row() serialisation, to_dict(), and insert_columns().

All tests are self-contained (no ClickHouse connection required).
"""

from __future__ import annotations

import types
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from detector.base import Alert, BaseDetector, Severity
from detector.metrics import (
    DetectionDelayMetrics,
    DetectionMetrics,
    OperationalDelayStats,
    calculate_detection_delay_metrics,
    calculate_f1_score,
    calculate_precision,
    calculate_recall,
)
from detector.replay_injector import (
    AnomalyScenarioGenerator,
    ClickHouseInjector,
    ReplayEvaluator,
    ScenarioReport,
    ScenarioType,
    print_evaluation_summary,
)
from detector.runner import DetectorRunner
from detector.threshold_detector import ThresholdDetector
from detector.zscore_detector import ZScoreDetector


# ======================================================================
# Helper factories
# ======================================================================

def _window(
    avg_price: float = 100.0,
    total_volume: float = 1.0,
    minutes_offset: int = 0,
    symbol: str = "BTC-USD",
) -> dict:
    """Create a minimal 1-minute aggregate window dict."""
    base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    from datetime import timedelta
    ws = base + timedelta(minutes=minutes_offset)
    we = ws + timedelta(minutes=1)
    return {
        "symbol": symbol,
        "window_start": ws,
        "window_end": we,
        "avg_price": avg_price,
        "min_price": avg_price * 0.99,
        "max_price": avg_price * 1.01,
        "total_volume": total_volume,
        "event_count": 50,
        "avg_price_inr": avg_price * 83.0,
    }


def _history(
    n: int,
    avg_price: float = 100.0,
    total_volume: float = 1.0,
    symbol: str = "BTC-USD",
) -> list[dict]:
    """Build *n* history windows with constant price/volume."""
    return [
        _window(avg_price=avg_price, total_volume=total_volume,
                minutes_offset=i, symbol=symbol)
        for i in range(n)
    ]


# ======================================================================
# 1.  ThresholdDetector – constructor validation
# ======================================================================

class TestThresholdDetectorInit:
    """Ensure invalid constructor args are rejected."""

    def test_lookback_zero_raises(self):
        with pytest.raises(ValueError, match="lookback"):
            ThresholdDetector(lookback=0)

    def test_min_history_zero_raises(self):
        with pytest.raises(ValueError, match="min_history"):
            ThresholdDetector(min_history=0)

    def test_min_history_gt_lookback_raises(self):
        with pytest.raises(ValueError, match="min_history cannot exceed"):
            ThresholdDetector(lookback=3, min_history=5)

    def test_negative_price_threshold_raises(self):
        with pytest.raises(ValueError, match="price_pct_threshold"):
            ThresholdDetector(price_pct_threshold=-1)

    def test_zero_volume_multiplier_raises(self):
        with pytest.raises(ValueError, match="volume_multiplier"):
            ThresholdDetector(volume_multiplier=0)

    def test_valid_construction(self):
        d = ThresholdDetector(
            price_pct_threshold=3.0,
            volume_multiplier=1.5,
            lookback=10,
            min_history=2,
        )
        assert d.name == "FixedThreshold"


# ======================================================================
# 2.  ThresholdDetector – cold-start / insufficient history
# ======================================================================

class TestColdStart:
    """Detector should not fire when there is insufficient history."""

    def test_empty_history_returns_no_alerts(self):
        det = ThresholdDetector(min_history=3)
        alerts = det.detect("BTC-USD", _window(avg_price=200), history=[])
        assert alerts == []

    def test_below_min_history_returns_no_alerts(self):
        det = ThresholdDetector(min_history=3)
        hist = _history(2)  # only 2, need 3
        alerts = det.detect("BTC-USD", _window(avg_price=200), hist)
        assert alerts == []

    def test_at_min_history_fires(self):
        det = ThresholdDetector(min_history=3, price_pct_threshold=1.0)
        hist = _history(3, avg_price=100)
        # 50 % spike → should fire
        alerts = det.detect("BTC-USD", _window(avg_price=150), hist)
        assert len(alerts) >= 1


# ======================================================================
# 3.  ThresholdDetector – price spike detection
# ======================================================================

class TestPriceSpike:
    """Test price-spike alerting logic."""

    def test_no_alert_below_threshold(self):
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(5, avg_price=100)
        # 3 % increase → below 5 % threshold
        alerts = det.detect("BTC-USD", _window(avg_price=103), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert price_alerts == []

    def test_alert_above_threshold_upward(self):
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(5, avg_price=100)
        # 10 % increase
        alerts = det.detect("BTC-USD", _window(avg_price=110), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert len(price_alerts) == 1
        assert price_alerts[0].severity in (Severity.LOW, Severity.MEDIUM, Severity.HIGH)
        assert price_alerts[0].metric_value > 0  # positive = upward

    def test_alert_above_threshold_downward(self):
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(5, avg_price=100)
        # 10 % drop
        alerts = det.detect("BTC-USD", _window(avg_price=90), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert len(price_alerts) == 1
        assert price_alerts[0].metric_value < 0  # negative = downward
        assert "dropped" in price_alerts[0].message

    def test_exact_threshold_fires(self):
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(5, avg_price=100)
        # exactly 5 % increase → abs(5.0) is NOT < 5.0, so should fire
        alerts = det.detect("BTC-USD", _window(avg_price=105), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert len(price_alerts) == 1

    def test_zero_rolling_mean_price_no_crash(self):
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(3, avg_price=0)
        # Should handle gracefully (skip, no crash)
        alerts = det.detect("BTC-USD", _window(avg_price=100), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert price_alerts == []  # rolling mean 0 → skip


# ======================================================================
# 4.  ThresholdDetector – volume surge detection
# ======================================================================

class TestVolumeSurge:
    """Test volume-surge alerting logic."""

    def test_no_alert_below_multiplier(self):
        det = ThresholdDetector(volume_multiplier=2.0, min_history=1)
        hist = _history(5, total_volume=10)
        # 1.5× → below 2.0 threshold
        alerts = det.detect("BTC-USD", _window(total_volume=15), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_surge"]
        assert vol_alerts == []

    def test_alert_above_multiplier(self):
        det = ThresholdDetector(volume_multiplier=2.0, min_history=1)
        hist = _history(5, total_volume=10)
        # 3× → above 2.0
        alerts = det.detect("BTC-USD", _window(total_volume=30), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_surge"]
        assert len(vol_alerts) == 1
        assert vol_alerts[0].metric_value == pytest.approx(3.0, abs=0.01)

    def test_exact_multiplier_fires(self):
        det = ThresholdDetector(volume_multiplier=2.0, min_history=1)
        hist = _history(5, total_volume=10)
        # exactly 2× → ratio (2.0) is NOT < 2.0 → should fire
        alerts = det.detect("BTC-USD", _window(total_volume=20), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_surge"]
        assert len(vol_alerts) == 1

    def test_zero_rolling_mean_volume_no_crash(self):
        det = ThresholdDetector(volume_multiplier=2.0, min_history=1)
        hist = _history(3, total_volume=0)
        alerts = det.detect("BTC-USD", _window(total_volume=100), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_surge"]
        assert vol_alerts == []  # rolling mean 0 → skip


# ======================================================================
# 5.  ThresholdDetector – severity classification
# ======================================================================

class TestSeverity:
    """Verify the _classify_severity static method."""

    @pytest.mark.parametrize("actual, threshold, expected", [
        (5.0, 5.0, Severity.LOW),       # ratio 1.0
        (9.9, 5.0, Severity.LOW),       # ratio 1.98
        (10.0, 5.0, Severity.MEDIUM),   # ratio 2.0
        (14.9, 5.0, Severity.MEDIUM),   # ratio 2.98
        (15.0, 5.0, Severity.HIGH),     # ratio 3.0
        (100.0, 5.0, Severity.HIGH),    # ratio 20.0
    ])
    def test_classify_severity(self, actual, threshold, expected):
        assert ThresholdDetector._classify_severity(actual, threshold) == expected

    def test_severity_propagates_to_alert(self):
        # 15% spike on 5% threshold → ratio 3.0 → HIGH
        det = ThresholdDetector(price_pct_threshold=5.0, min_history=1)
        hist = _history(5, avg_price=100)
        alerts = det.detect("BTC-USD", _window(avg_price=115), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert len(price_alerts) == 1
        assert price_alerts[0].severity == Severity.HIGH


# ======================================================================
# 6.  ThresholdDetector – lookback trimming
# ======================================================================

class TestLookbackTrimming:
    """Ensure only the last `lookback` windows are used."""

    def test_long_history_trimmed(self):
        det = ThresholdDetector(
            price_pct_threshold=5.0,
            lookback=3,
            min_history=1,
        )
        # 20 old windows at price=200, then 3 recent at price=100
        old = _history(20, avg_price=200)
        recent = _history(3, avg_price=100)
        # Concatenate: the detector should only look at the last 3 (price=100)
        full_hist = old + recent
        # Latest at 103 → only 3% above the recent mean of 100 → no alert
        alerts = det.detect("BTC-USD", _window(avg_price=103), full_hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_spike"]
        assert price_alerts == []


# ======================================================================
# 7.  ThresholdDetector – combined alerts
# ======================================================================

class TestCombinedAlerts:
    """Both price and volume can fire simultaneously."""

    def test_both_price_and_volume_fire(self):
        det = ThresholdDetector(
            price_pct_threshold=5.0,
            volume_multiplier=2.0,
            min_history=1,
        )
        hist = _history(5, avg_price=100, total_volume=10)
        # 20% price spike + 5× volume
        latest = _window(avg_price=120, total_volume=50)
        alerts = det.detect("BTC-USD", latest, hist)
        types = {a.alert_type for a in alerts}
        assert "price_spike" in types
        assert "volume_surge" in types
        assert len(alerts) == 2


# ======================================================================
# 8.  Alert data object
# ======================================================================

class TestAlert:
    """Test the Alert dataclass helpers."""

    def test_as_row_order(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = Alert(
            symbol="ETH-USD",
            alert_type="price_spike",
            severity=Severity.MEDIUM,
            message="test msg",
            metric_value=7.5,
            threshold=5.0,
            timestamp=ts,
        )
        row = a.as_row()
        assert row == ("ETH-USD", "price_spike", "medium", "test msg", 7.5, 5.0, ts)

    def test_insert_columns_matches_as_row(self):
        cols = Alert.insert_columns()
        assert len(cols) == 7
        assert cols[0] == "symbol"
        assert cols[-1] == "timestamp"

    def test_alert_is_immutable(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        a = Alert("X", "t", Severity.LOW, "m", 1.0, 1.0, ts)
        with pytest.raises(AttributeError):
            a.symbol = "Y"  # type: ignore[misc]

    def test_to_dict_serialization(self):
        ts = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        a = Alert("BTC-USD", "price_spike", Severity.HIGH, "test alert", 10.0, 5.0, ts)
        d = a.to_dict()
        assert d["symbol"] == "BTC-USD"
        assert d["alert_type"] == "price_spike"
        assert d["severity"] == "high"
        assert d["metric_value"] == 10.0
        assert d["threshold"] == 5.0
        assert "2026-01-01" in d["timestamp"]


# ======================================================================
# 9.  BaseDetector – interface & validation
# ======================================================================

class TestBaseDetectorInterface:
    """Verify BaseDetector abstract behavior and validation utilities."""

    def test_cannot_instantiate_base_detector_directly(self):
        with pytest.raises(TypeError):
            BaseDetector()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_name_and_detect(self):
        class IncompleteDetector(BaseDetector):
            pass

        with pytest.raises(TypeError):
            IncompleteDetector()  # type: ignore[abstract]

        class DummyDetector(BaseDetector):
            @property
            def name(self) -> str:
                return "Dummy"

            def detect(self, symbol, latest_window, history):
                return []

        d = DummyDetector()
        assert d.name == "Dummy"
        assert d.detect("BTC-USD", {}, []) == []

    def test_validate_window_valid(self):
        w = _window()
        assert BaseDetector.validate_window(w) is True

    def test_validate_window_missing_fields(self):
        assert BaseDetector.validate_window({"symbol": "BTC-USD"}) is False
        assert BaseDetector.validate_window(None) is False  # type: ignore[arg-type]


# ======================================================================
# 10. DetectorRunner – orchestration with mocks
# ======================================================================

class TestDetectorRunner:
    """Test the runner orchestrator without a real ClickHouse."""

    def _make_runner(self, detectors=None, windows=None, symbols=None):
        """Build a DetectorRunner with patched data-access helpers."""
        if detectors is None:
            detectors = [ThresholdDetector(min_history=1)]
        if symbols is None:
            symbols = ["BTC-USD"]
        if windows is None:
            windows = _history(6, avg_price=100, total_volume=10) + [
                _window(avg_price=120, total_volume=50, minutes_offset=6)
            ]

        mock_client = MagicMock()
        mock_client.ping.return_value = True

        runner = DetectorRunner(
            detectors=detectors,
            poll_interval_s=0,
            client_factory=lambda: mock_client,
        )

        return runner, mock_client, windows, symbols

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_run_once_fires_alerts(self, mock_symbols, mock_windows, mock_write):
        runner, client, windows, symbols = self._make_runner()
        mock_symbols.return_value = symbols
        mock_windows.return_value = windows
        mock_write.return_value = 2

        alerts = runner.run_once()
        assert len(alerts) >= 1  # price spike and/or volume surge

        # write_alerts should have been called once
        mock_write.assert_called_once()

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_deduplication(self, mock_symbols, mock_windows, mock_write):
        """Same window_start should not be processed twice."""
        runner, client, windows, symbols = self._make_runner()
        mock_symbols.return_value = symbols
        mock_windows.return_value = windows
        mock_write.return_value = 2

        first = runner.run_once()
        second = runner.run_once()

        assert len(first) >= 1
        assert second == []  # deduplicated

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_no_active_symbols(self, mock_symbols, mock_windows, mock_write):
        runner, *_ = self._make_runner()
        mock_symbols.return_value = []

        alerts = runner.run_once()
        assert alerts == []
        mock_windows.assert_not_called()

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_detector_crash_does_not_halt_runner(self, mock_symbols, mock_windows, mock_write):
        """A crashing detector should be caught; runner continues."""
        bad_detector = MagicMock(spec=BaseDetector)
        bad_detector.name = "CrashyDetector"
        bad_detector.detect.side_effect = RuntimeError("boom")

        runner, client, windows, symbols = self._make_runner(detectors=[bad_detector])
        mock_symbols.return_value = symbols
        mock_windows.return_value = windows

        # Should not raise
        alerts = runner.run_once()
        assert alerts == []

    def test_constructor_rejects_empty_detectors(self):
        with pytest.raises(ValueError, match="At least one detector"):
            DetectorRunner(detectors=[])

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_runner_stats_and_metrics(self, mock_symbols, mock_windows, mock_write):
        runner, client, windows, symbols = self._make_runner()
        mock_symbols.return_value = symbols
        mock_windows.return_value = windows
        mock_write.return_value = 2

        assert runner.stats["total_cycles"] == 0
        assert runner.stats["total_alerts"] == 0
        assert runner.stats["total_written"] == 0

        alerts = runner.run_once()

        stats = runner.stats
        assert stats["total_cycles"] == 1
        assert stats["total_alerts"] == len(alerts)
        assert stats["total_written"] == 2
        assert stats["seen_windows_count"] == 1
        assert stats["last_run_time"] is not None

    def test_add_detector_dynamically(self):
        det1 = ThresholdDetector(min_history=1)
        det2 = ZScoreDetector(min_history=2)
        runner = DetectorRunner(detectors=[det1])
        assert len(runner.detectors) == 1

        runner.add_detector(det2)
        assert len(runner.detectors) == 2
        assert runner.detectors[1] == det2

    def test_seen_windows_pruning(self):
        runner, _, _, _ = self._make_runner()
        runner._max_seen_windows = 10
        for i in range(15):
            runner._seen_windows.add(("BTC-USD", f"2026-01-01T00:{i:02d}:00Z"))

        assert len(runner._seen_windows) == 15
        runner._prune_seen_windows()
        assert len(runner._seen_windows) <= 10


# ======================================================================
# 10.  DetectorRunner – run_forever / stop
# ======================================================================

class TestRunForeverStop:
    """Verify the blocking loop can be stopped."""

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_stop_breaks_loop(self, mock_symbols, mock_windows, mock_write):
        import threading

        runner, _, windows, symbols = TestDetectorRunner._make_runner(
            TestDetectorRunner(), detectors=[ThresholdDetector(min_history=1)]
        )
        mock_symbols.return_value = symbols
        mock_windows.return_value = windows
        mock_write.return_value = 0

        # run_forever in a thread, then stop it
        t = threading.Thread(target=runner.run_forever, daemon=True)
        t.start()
        runner.stop()
        t.join(timeout=3)
        assert not t.is_alive()


# ======================================================================
# 11. ZScoreDetector – constructor validation
# ======================================================================

class TestZScoreDetectorInit:
    """Ensure invalid constructor args are rejected."""

    def test_lookback_less_than_two_raises(self):
        with pytest.raises(ValueError, match="lookback"):
            ZScoreDetector(lookback=1)

    def test_min_history_less_than_two_raises(self):
        with pytest.raises(ValueError, match="min_history"):
            ZScoreDetector(min_history=1)

    def test_min_history_gt_lookback_raises(self):
        with pytest.raises(ValueError, match="min_history cannot exceed"):
            ZScoreDetector(lookback=5, min_history=6)

    def test_negative_price_z_threshold_raises(self):
        with pytest.raises(ValueError, match="price_z_threshold"):
            ZScoreDetector(price_z_threshold=-1.0)

    def test_zero_volume_z_threshold_raises(self):
        with pytest.raises(ValueError, match="volume_z_threshold"):
            ZScoreDetector(volume_z_threshold=0.0)

    def test_valid_construction(self):
        d = ZScoreDetector(
            price_z_threshold=2.5,
            volume_z_threshold=3.5,
            lookback=25,
            min_history=5,
        )
        assert d.name == "ZScore"


# ======================================================================
# 12. ZScoreDetector – cold-start / insufficient history
# ======================================================================

class TestZScoreColdStart:
    """ZScoreDetector should not fire when there is insufficient history."""

    def test_empty_history_returns_no_alerts(self):
        det = ZScoreDetector(min_history=5)
        alerts = det.detect("BTC-USD", _window(avg_price=200), history=[])
        assert alerts == []

    def test_below_min_history_returns_no_alerts(self):
        det = ZScoreDetector(min_history=5)
        hist = _history(4, avg_price=100)  # only 4, need 5
        alerts = det.detect("BTC-USD", _window(avg_price=200), hist)
        assert alerts == []

    def test_at_min_history_fires(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3, lookback=10)
        # 3 history points with mean=100, stdev=1.0
        hist = [
            _window(avg_price=99.0, minutes_offset=0),
            _window(avg_price=100.0, minutes_offset=1),
            _window(avg_price=101.0, minutes_offset=2),
        ]
        # Current price = 104.0 -> Z = +4.0 >= 3.0 -> fires
        alerts = det.detect("BTC-USD", _window(avg_price=104.0, minutes_offset=3), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert len(price_alerts) == 1


# ======================================================================
# 13. ZScoreDetector – price Z-score detection
# ======================================================================

class TestZScorePriceDetection:
    """Test price anomaly detection using standard deviations."""

    def _history_std1(self) -> list[dict]:
        """Return 3 windows with mean=100.0, stdev=1.0."""
        return [
            _window(avg_price=99.0, minutes_offset=0),
            _window(avg_price=100.0, minutes_offset=1),
            _window(avg_price=101.0, minutes_offset=2),
        ]

    def test_no_alert_within_normal_range(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3)
        hist = self._history_std1()
        # Price 101.5 -> Z = 1.5 < 3.0 -> no alert
        alerts = det.detect("BTC-USD", _window(avg_price=101.5), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert price_alerts == []

    def test_upward_spike_fires(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3)
        hist = self._history_std1()
        # Price 104.0 -> Z = +4.0 >= 3.0
        alerts = det.detect("BTC-USD", _window(avg_price=104.0), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert len(price_alerts) == 1
        assert price_alerts[0].metric_value == pytest.approx(4.0, abs=0.01)
        assert price_alerts[0].threshold == 3.0
        assert "spiked above" in price_alerts[0].message

    def test_downward_drop_fires(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3)
        hist = self._history_std1()
        # Price 96.0 -> Z = -4.0 <= -3.0
        alerts = det.detect("BTC-USD", _window(avg_price=96.0), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert len(price_alerts) == 1
        assert price_alerts[0].metric_value == pytest.approx(-4.0, abs=0.01)
        assert "dropped below" in price_alerts[0].message

    def test_exact_threshold_fires(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3)
        hist = self._history_std1()
        # Exactly 3.0 standard deviations
        alerts = det.detect("BTC-USD", _window(avg_price=103.0), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert len(price_alerts) == 1

    def test_zero_stdev_flat_history_no_crash(self):
        det = ZScoreDetector(price_z_threshold=3.0, min_history=3)
        hist = _history(5, avg_price=100.0)  # all prices identical -> stdev is 0
        alerts = det.detect("BTC-USD", _window(avg_price=150.0), hist)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert price_alerts == []


# ======================================================================
# 14. ZScoreDetector – volume Z-score detection
# ======================================================================

class TestZScoreVolumeDetection:
    """Test volume surge detection using standard deviations."""

    def _history_vol_std1(self) -> list[dict]:
        """Return 3 windows with mean=10.0, volume stdev=1.0."""
        return [
            _window(total_volume=9.0, minutes_offset=0),
            _window(total_volume=10.0, minutes_offset=1),
            _window(total_volume=11.0, minutes_offset=2),
        ]

    def test_normal_volume_no_alert(self):
        det = ZScoreDetector(volume_z_threshold=3.0, min_history=3)
        hist = self._history_vol_std1()
        # Volume 11.5 -> Z = 1.5 < 3.0 -> no alert
        alerts = det.detect("BTC-USD", _window(total_volume=11.5), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_zscore"]
        assert vol_alerts == []

    def test_volume_surge_fires(self):
        det = ZScoreDetector(volume_z_threshold=3.0, min_history=3)
        hist = self._history_vol_std1()
        # Volume 15.0 -> Z = 5.0 >= 3.0 -> surge alert
        alerts = det.detect("BTC-USD", _window(total_volume=15.0), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_zscore"]
        assert len(vol_alerts) == 1
        assert vol_alerts[0].metric_value == pytest.approx(5.0, abs=0.01)
        assert "volume surged" in vol_alerts[0].message

    def test_volume_drop_does_not_fire_surge(self):
        det = ZScoreDetector(volume_z_threshold=3.0, min_history=3)
        hist = self._history_vol_std1()
        # Abnormally low volume (e.g. 5.0 -> Z = -5.0) is not a volume surge
        alerts = det.detect("BTC-USD", _window(total_volume=5.0), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_zscore"]
        assert vol_alerts == []

    def test_zero_stdev_volume_no_crash(self):
        det = ZScoreDetector(volume_z_threshold=3.0, min_history=3)
        hist = _history(5, total_volume=10.0)  # stdev is 0
        alerts = det.detect("BTC-USD", _window(total_volume=100.0), hist)
        vol_alerts = [a for a in alerts if a.alert_type == "volume_zscore"]
        assert vol_alerts == []


# ======================================================================
# 15. ZScoreDetector – severity classification
# ======================================================================

class TestZScoreSeverity:
    """Verify Z-score severity thresholds."""

    @pytest.mark.parametrize("z_val, threshold, expected", [
        (3.0, 3.0, Severity.LOW),       # ratio 1.0
        (4.4, 3.0, Severity.LOW),       # ratio 1.46
        (4.5, 3.0, Severity.MEDIUM),    # ratio 1.5
        (5.9, 3.0, Severity.MEDIUM),    # ratio 1.96
        (6.0, 3.0, Severity.HIGH),      # ratio 2.0
        (-7.5, 3.0, Severity.HIGH),     # ratio 2.5
    ])
    def test_classify_severity(self, z_val, threshold, expected):
        assert ZScoreDetector._classify_severity(abs(z_val), threshold) == expected


# ======================================================================
# 16. ZScoreDetector – lookback trimming & combined alerts
# ======================================================================

class TestZScoreLookbackAndCombined:
    """Verify history window trimming and combined price/volume alerts."""

    def test_lookback_trims_older_windows(self):
        det = ZScoreDetector(price_z_threshold=3.0, lookback=3, min_history=3)
        # 20 old windows at price 500, then 3 recent at [99, 100, 101]
        old = _history(20, avg_price=500.0)
        recent = [
            _window(avg_price=99.0, minutes_offset=20),
            _window(avg_price=100.0, minutes_offset=21),
            _window(avg_price=101.0, minutes_offset=22),
        ]
        # With lookback=3, only recent 3 are used (mean=100, std=1.0)
        # Price 101.5 is only 1.5 sigma -> no alert
        alerts = det.detect("BTC-USD", _window(avg_price=101.5, minutes_offset=23), old + recent)
        price_alerts = [a for a in alerts if a.alert_type == "price_zscore"]
        assert price_alerts == []

    def test_combined_price_and_volume_zscore_alerts(self):
        det = ZScoreDetector(
            price_z_threshold=3.0,
            volume_z_threshold=3.0,
            min_history=3,
        )
        hist = [
            _window(avg_price=99.0, total_volume=9.0, minutes_offset=0),
            _window(avg_price=100.0, total_volume=10.0, minutes_offset=1),
            _window(avg_price=101.0, total_volume=11.0, minutes_offset=2),
        ]
        # Price 105 (+5 sigma) and Volume 16 (+6 sigma)
        latest = _window(avg_price=105.0, total_volume=16.0, minutes_offset=3)
        alerts = det.detect("BTC-USD", latest, hist)
        types = {a.alert_type for a in alerts}
        assert "price_zscore" in types
        assert "volume_zscore" in types
        assert len(alerts) == 2


# ======================================================================
# 17. Multi-detector integration in DetectorRunner
# ======================================================================

class TestMultiDetectorRunner:
    """Verify DetectorRunner handles both ThresholdDetector and ZScoreDetector simultaneously."""

    @patch("detector.runner.write_alerts")
    @patch("detector.runner.fetch_latest_windows")
    @patch("detector.runner.fetch_active_symbols")
    def test_runner_executes_both_detectors(self, mock_symbols, mock_windows, mock_write):
        t_det = ThresholdDetector(price_pct_threshold=5.0, min_history=3)
        z_det = ZScoreDetector(price_z_threshold=3.0, min_history=3)

        mock_symbols.return_value = ["BTC-USD"]
        # History where mean=100, std=1.0
        windows = [
            _window(avg_price=99.0, minutes_offset=0),
            _window(avg_price=100.0, minutes_offset=1),
            _window(avg_price=101.0, minutes_offset=2),
            _window(avg_price=110.0, minutes_offset=3),  # 10% spike and +10 sigma
        ]
        mock_windows.return_value = windows
        mock_write.return_value = 2

        mock_client = MagicMock()
        mock_client.ping.return_value = True

        runner = DetectorRunner(
            detectors=[t_det, z_det],
            poll_interval_s=0,
            client_factory=lambda: mock_client,
        )

        alerts = runner.run_once()
        # Should collect alerts from both detectors
        alert_types = {a.alert_type for a in alerts}
        assert "price_spike" in alert_types
        assert "price_zscore" in alert_types
        assert len(alerts) == 2


# ======================================================================
# 18. AnomalyScenarioGenerator – synthetic data generation
# ======================================================================

class TestAnomalyScenarioGenerator:
    """Verify synthetic scenario generator creates valid baseline and anomaly windows."""

    def test_generate_baseline_normal(self):
        gen = AnomalyScenarioGenerator(symbol="BTC-USD", base_price=50000.0, base_volume=10.0)
        sws = gen.generate(ScenarioType.NORMAL, baseline_length=10)
        assert len(sws) == 11  # 10 baseline + 1 continuation
        assert all(not sw.is_anomaly for sw in sws)
        assert all(sw.window["symbol"] == "BTC-USD" for sw in sws)

    def test_generate_price_spike(self):
        gen = AnomalyScenarioGenerator(base_price=100.0)
        sws = gen.generate(ScenarioType.PRICE_SPIKE, baseline_length=5)
        assert len(sws) == 6
        anomaly_sw = sws[-1]
        assert anomaly_sw.is_anomaly is True
        assert anomaly_sw.expected_type == "price"
        assert anomaly_sw.window["avg_price"] == pytest.approx(112.0, abs=0.1)

    def test_generate_flash_crash(self):
        gen = AnomalyScenarioGenerator(base_price=100.0)
        sws = gen.generate(ScenarioType.FLASH_CRASH, baseline_length=5)
        anomaly_sw = sws[-1]
        assert anomaly_sw.is_anomaly is True
        assert anomaly_sw.expected_type == "price"
        assert anomaly_sw.window["avg_price"] == pytest.approx(85.0, abs=0.1)

    def test_generate_volume_surge(self):
        gen = AnomalyScenarioGenerator(base_volume=10.0)
        sws = gen.generate(ScenarioType.VOLUME_SURGE, baseline_length=5)
        anomaly_sw = sws[-1]
        assert anomaly_sw.is_anomaly is True
        assert anomaly_sw.expected_type == "volume"
        assert anomaly_sw.window["total_volume"] == pytest.approx(50.0, abs=0.1)

    def test_generate_combined(self):
        gen = AnomalyScenarioGenerator(base_price=100.0, base_volume=10.0)
        sws = gen.generate(ScenarioType.COMBINED, baseline_length=5)
        anomaly_sw = sws[-1]
        assert anomaly_sw.is_anomaly is True
        assert anomaly_sw.expected_type == "both"
        assert anomaly_sw.window["avg_price"] > 110.0
        assert anomaly_sw.window["total_volume"] > 50.0


# ======================================================================
# 19. ReplayEvaluator – in-memory testing & metrics
# ======================================================================

class TestReplayEvaluator:
    """Verify in-memory replay evaluation calculates precision, recall, and F1."""

    def test_evaluate_normal_scenario_zero_false_positives(self):
        detector = ThresholdDetector(min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.NORMAL, baseline_length=15)

        report = evaluator.evaluate_scenario(detector, sws, "normal")
        assert report.expected_anomalies == 0
        assert report.alerts_fired == 0
        assert report.false_positives == 0
        assert report.precision == 1.0
        assert report.recall == 1.0
        assert report.f1_score == 1.0

    def test_evaluate_price_spike_scenario(self):
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.PRICE_SPIKE, baseline_length=15)

        report = evaluator.evaluate_scenario(detector, sws, "price_spike")
        assert report.expected_anomalies == 1
        assert report.true_positives == 1
        assert report.false_positives == 0
        assert report.false_negatives == 0
        assert report.precision == 1.0
        assert report.recall == 1.0
        assert report.f1_score == 1.0
        assert len(report.alerts) >= 1

    def test_run_all_scenarios(self):
        t_det = ThresholdDetector(min_history=5)
        z_det = ZScoreDetector(min_history=5)
        evaluator = ReplayEvaluator(detectors=[t_det, z_det])

        reports = evaluator.run_all_scenarios(baseline_length=10)
        # 5 scenarios * 2 detectors = 10 reports
        assert len(reports) == 10
        for r in reports:
            assert r.total_windows == 11
            assert r.precision >= 0.0


# ======================================================================
# 20. ClickHouseInjector – table insertion
# ======================================================================

class TestClickHouseInjector:
    """Verify ClickHouseInjector formats and passes rows to client.insert."""

    def test_inject_windows_calls_client_insert(self):
        mock_client = MagicMock()
        injector = ClickHouseInjector(client_factory=lambda: mock_client)

        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.NORMAL, baseline_length=3)
        windows = [sw.window for sw in sws]

        count = injector.inject_windows(windows, table="ticks_1m_agg")
        assert count == 4
        mock_client.insert.assert_called_once()
        kwargs = mock_client.insert.call_args.kwargs
        assert kwargs["table"] == "ticks_1m_agg"
        assert len(kwargs["data"]) == 4
        assert kwargs["column_names"][0] == "symbol"
        assert kwargs["column_names"][-1] == "calculated_at"


# ======================================================================
# 21. Precision, Recall, and Detection Delay Metrics
# ======================================================================

class TestMetricsModule:
    """Unit tests for the metrics calculation functions and dataclasses."""

    def test_calculate_precision(self):
        # Normal cases
        assert calculate_precision(tp=10, fp=0) == 1.0
        assert calculate_precision(tp=8, fp=2) == 0.8
        assert calculate_precision(tp=0, fp=5) == 0.0

        # Edge case: No alerts fired
        assert calculate_precision(tp=0, fp=0, expected_anomalies=0) == 1.0
        assert calculate_precision(tp=0, fp=0, expected_anomalies=3) == 0.0

    def test_calculate_recall(self):
        # Normal cases
        assert calculate_recall(tp=10, fn=0) == 1.0
        assert calculate_recall(tp=5, fn=5) == 0.5
        assert calculate_recall(tp=0, fn=5) == 0.0

        # Edge case: No anomalies present
        assert calculate_recall(tp=0, fn=0, expected_anomalies=0) == 1.0

    def test_calculate_f1_score(self):
        assert calculate_f1_score(precision=1.0, recall=1.0) == 1.0
        assert calculate_f1_score(precision=0.8, recall=0.5) == 0.6154
        assert calculate_f1_score(precision=0.0, recall=0.0, expected_anomalies=0, alerts_fired=0) == 1.0
        assert calculate_f1_score(precision=0.0, recall=0.0, expected_anomalies=1, alerts_fired=0) == 0.0

    def test_calculate_detection_delay_metrics(self):
        # Empty delays
        empty_res = calculate_detection_delay_metrics([], [], expected_anomalies=0)
        assert empty_res.mean_delay_seconds == 0.0
        assert empty_res.mean_delay_windows == 0.0

        # Empty delays with missed anomalies
        missed_res = calculate_detection_delay_metrics([], [], expected_anomalies=2)
        assert missed_res.mean_delay_seconds is None
        assert missed_res.mean_delay_windows is None

        # Multiple delays
        delays_s = [0.0, 60.0, 120.0]
        delays_w = [0, 1, 2]
        res = calculate_detection_delay_metrics(delays_s, delays_w, expected_anomalies=3)
        assert res.mean_delay_seconds == 60.0
        assert res.median_delay_seconds == 60.0
        assert res.min_delay_seconds == 0.0
        assert res.max_delay_seconds == 120.0
        assert res.mean_delay_windows == 1.0
        d = res.to_dict()
        assert d["mean_delay_seconds"] == 60.0

    def test_detection_metrics_and_operational_stats_dataclasses(self):
        delay_m = DetectionDelayMetrics(
            mean_delay_seconds=30.0,
            median_delay_seconds=30.0,
            min_delay_seconds=0.0,
            max_delay_seconds=60.0,
            mean_delay_windows=0.5,
            delays_seconds=[0.0, 60.0],
            delays_windows=[0, 1],
        )
        dm = DetectionMetrics(
            total_samples=25,
            expected_anomalies=2,
            alerts_fired=2,
            true_positives=2,
            false_positives=0,
            false_negatives=0,
            true_negatives=23,
            precision=1.0,
            recall=1.0,
            f1_score=1.0,
            delay_metrics=delay_m,
        )
        assert dm.mean_delay_seconds == 30.0
        assert dm.mean_delay_windows == 0.5
        d = dm.to_dict()
        assert d["precision"] == 1.0
        assert d["mean_delay_seconds"] == 30.0

        ops = OperationalDelayStats(
            total_alerts=10,
            avg_delay_seconds=1.25,
            median_delay_seconds=1.1,
            min_delay_seconds=0.5,
            max_delay_seconds=3.2,
            p95_delay_seconds=2.8,
        )
        op_d = ops.to_dict()
        assert op_d["total_alerts"] == 10
        assert op_d["avg_delay_seconds"] == 1.25


# ======================================================================
# 22. Detection Delay and Replay Evaluation Scenarios
# ======================================================================

class TestDetectionDelayEvaluation:
    """Verify detection delay calculations during scenario replay."""

    def test_immediate_detection_delay_zero(self):
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.PRICE_SPIKE, baseline_length=15)

        report = evaluator.evaluate_scenario(detector, sws, "price_spike")
        assert report.expected_anomalies == 1
        assert report.true_positives == 1
        assert report.precision == 1.0
        assert report.recall == 1.0
        assert report.mean_delay_seconds == 0.0
        assert report.mean_delay_windows == 0.0
        assert report.detection_delay_seconds == 0.0
        assert report.delays == [0.0]

    def test_gradual_drift_multi_window_delay(self):
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.GRADUAL_DRIFT, baseline_length=15)

        report = evaluator.evaluate_scenario(detector, sws, "gradual_drift")
        # Gradual drift has 4 anomaly windows (multipliers: 1.018, 1.036, 1.065, 1.100)
        # ThresholdDetector fires on the 3rd window (+6.5%)
        # Detection delay = 2 windows = 120.0s
        assert report.expected_anomalies == 4
        assert report.true_positives == 2
        assert report.false_negatives == 2
        assert report.false_positives == 0
        assert report.precision == 1.0
        assert report.recall == 0.5
        assert report.f1_score == 0.6667
        assert report.mean_delay_seconds == 120.0
        assert report.mean_delay_windows == 2.0

    def test_missed_anomaly_delay_is_none(self):
        # High threshold that will never trigger
        detector = ThresholdDetector(price_pct_threshold=99.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.PRICE_SPIKE, baseline_length=15)

        report = evaluator.evaluate_scenario(detector, sws, "price_spike")
        assert report.expected_anomalies == 1
        assert report.true_positives == 0
        assert report.false_negatives == 1
        assert report.recall == 0.0
        assert report.precision == 0.0
        assert report.mean_delay_seconds is None
        assert report.mean_delay_windows is None

    def test_scenario_report_to_dict(self):
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        gen = AnomalyScenarioGenerator()
        sws = gen.generate(ScenarioType.PRICE_SPIKE, baseline_length=10)

        report = evaluator.evaluate_scenario(detector, sws, "price_spike")
        d = report.to_dict()
        assert d["detector_name"] == "FixedThreshold"
        assert d["scenario_name"] == "price_spike"
        assert "mean_delay_seconds" in d
        assert "mean_delay_windows" in d
        assert "delays" in d
        assert "delays_windows" in d

    def test_print_evaluation_summary_output(self, capsys):
        detector = ThresholdDetector(price_pct_threshold=5.0, min_history=5)
        evaluator = ReplayEvaluator(detectors=[detector])
        reports = evaluator.run_all_scenarios(baseline_length=5, include_drift=True)

        print_evaluation_summary(reports)
        captured = capsys.readouterr()
        assert "ANOMALY DETECTOR REPLAY EVALUATION REPORT" in captured.out
        assert "Delay (s)" in captured.out
        assert "Delay (win)" in captured.out
        assert "FixedThreshold" in captured.out

