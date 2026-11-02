"""
Decoupled Test Suite for Milestone 4: Dual-Currency API & Web Dashboard

Separates pure database-independent unit tests from live ClickHouse integration tests.
"""

import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# =============================================================================
# SUITE 1: PURE DATABASE-INDEPENDENT UNIT TESTS
# =============================================================================

class MockQueryClient:
    """Simulates ClickHouse queries for unit testing pagination and draining."""

    def __init__(self, dataset: list[list]):
        self.dataset = dataset

    def query(self, sql: str):
        res = MagicMock()
        filtered = self.dataset

        import re
        # Match keyset tuple comparison: (timestamp, effective_event_id) > (toDateTime('...'), '...')
        match = re.search(r"\(toDateTime\('([^']+)'\),\s*'([^']*)'\)", sql)
        if match:
            start_ts = datetime.fromisoformat(match.group(1))
            start_id = match.group(2)
            filtered = [
                r for r in self.dataset
                if (r[0], r[8]) > (start_ts, start_id)
            ]

        # Match LIMIT
        limit_match = re.search(r"LIMIT\s+(\d+)", sql)
        limit = int(limit_match.group(1)) if limit_match else len(filtered)
        res.result_rows = filtered[:limit]
        return res


class TestPureUnitHelpers(unittest.TestCase):
    """Pure unit tests with zero database dependency."""

    def test_validate_symbols(self):
        """Test symbol validator accepts allowed cryptos and rejects invalid/injected symbols."""
        from src.production.dashboard.web_app import validate_symbols
        from fastapi import HTTPException

        self.assertEqual(validate_symbols("btc,eth"), ["BTC", "ETH"])
        self.assertEqual(validate_symbols("  SOL , LTC  "), ["SOL", "LTC"])
        self.assertEqual(validate_symbols(""), [])

        with self.assertRaises(HTTPException):
            validate_symbols("BTC; DROP TABLE ticks;")

        with self.assertRaises(HTTPException):
            validate_symbols("INVALID_COIN")

    def test_more_than_100_same_second_records(self):
        """Test keyset pagination drains 150 rows sharing the exact same timestamp."""
        from src.production.dashboard.web_app import SSECursorTracker

        base_time = datetime(2026, 10, 17, 12, 0, 0)
        # 150 rows with same timestamp but distinct event_id
        dataset = [
            [base_time, "BTC", 80000.0 + i, 1, 80000.0 + i, 7600000.0, 95.0, "2026-10-17 00:00:00", f"id_{i:04d}"]
            for i in range(150)
        ]
        mock_client = MockQueryClient(dataset)
        tracker = SSECursorTracker(lookback_seconds=5, batch_size=100, max_pages_per_poll=5)
        tracker.is_primed = True
        tracker.high_watermark_ts = base_time

        events = tracker.fetch_next_events(mock_client)
        # Must drain all 150 records across 2 pages without starvation
        self.assertEqual(len(events), 150)
        self.assertIsNone(tracker.drain_cursor)
        self.assertEqual(events[0]["event_id"], "id_0000")
        self.assertEqual(events[-1]["event_id"], "id_0149")

    def test_drain_across_polls_with_page_limit(self):
        """Test that when max_pages_per_poll is reached, state is preserved across polls."""
        from src.production.dashboard.web_app import SSECursorTracker

        base_time = datetime(2026, 10, 17, 12, 0, 0)
        dataset = [
            [base_time + timedelta(seconds=i // 50), "BTC", 80000.0, 1, 80000.0, 7600000.0, 95.0, None, f"id_{i:04d}"]
            for i in range(250)
        ]
        mock_client = MockQueryClient(dataset)
        # Constrain to 1 page per poll
        tracker = SSECursorTracker(lookback_seconds=5, batch_size=100, max_pages_per_poll=1)
        tracker.is_primed = True
        tracker.high_watermark_ts = base_time + timedelta(seconds=5)

        # Poll 1 fetches page 1 (100 rows)
        events_poll1 = tracker.fetch_next_events(mock_client)
        self.assertEqual(len(events_poll1), 100)
        self.assertIsNotNone(tracker.drain_cursor)

        # Poll 2 continues from drain_cursor, fetches page 2 (100 rows)
        events_poll2 = tracker.fetch_next_events(mock_client)
        self.assertEqual(len(events_poll2), 100)
        self.assertIsNotNone(tracker.drain_cursor)

        # Poll 3 fetches page 3 (remaining 50 rows) and finishes draining
        events_poll3 = tracker.fetch_next_events(mock_client)
        self.assertEqual(len(events_poll3), 50)
        self.assertIsNone(tracker.drain_cursor)

    def test_late_arrival_discovery_within_lookback(self):
        """Test that an event arriving late (within 5s lookback) is discovered and emitted."""
        from src.production.dashboard.web_app import SSECursorTracker

        t_cur = datetime(2026, 10, 17, 12, 0, 4)
        t_late = datetime(2026, 10, 17, 12, 0, 2)  # 2s before current cursor

        dataset = [
            [t_cur, "BTC", 81000.0, 1, 81000.0, 7695000.0, 95.0, None, "id_current"],
            [t_late, "ETH", 2500.0, 1, 2500.0, 237500.0, 95.0, None, "id_late"],
        ]
        # Sort in chronological order as ClickHouse returns
        dataset.sort(key=lambda r: (r[0], r[8]))

        mock_client = MockQueryClient(dataset)
        tracker = SSECursorTracker(lookback_seconds=5, batch_size=100)
        tracker.is_primed = True
        tracker.high_watermark_ts = t_cur
        tracker.seen_signatures["id_current"] = t_cur

        events = tracker.fetch_next_events(mock_client)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], "id_late")

    def test_late_arrival_beyond_lookback_stated_limit(self):
        """Demonstrates best-effort limitation: event older than 5s is not in query window."""
        from src.production.dashboard.web_app import SSECursorTracker

        t_cur = datetime(2026, 10, 17, 12, 0, 10)
        t_too_late = datetime(2026, 10, 17, 12, 0, 2)  # 8s before cursor (> 5s lookback)

        dataset = [
            [t_too_late, "ETH", 2500.0, 1, 2500.0, 237500.0, 95.0, None, "id_ancient"],
        ]
        mock_client = MockQueryClient(dataset)
        tracker = SSECursorTracker(lookback_seconds=5, batch_size=100)
        tracker.is_primed = True
        tracker.high_watermark_ts = t_cur

        events = tracker.fetch_next_events(mock_client)
        # Because lookback starts at 12:00:05, row at 12:00:02 is filtered by SQL
        self.assertEqual(len(events), 0)

    def test_deduplication_cleanup_preserves_lookback_window(self):
        """Verify pruning removes only signatures older than high_watermark - lookback."""
        from src.production.dashboard.web_app import SSECursorTracker

        tracker = SSECursorTracker(lookback_seconds=5)
        tracker.is_primed = True
        tracker.high_watermark_ts = datetime(2026, 10, 17, 12, 0, 10)

        tracker.seen_signatures = {
            "sig_old": datetime(2026, 10, 17, 12, 0, 4),  # 6s old -> should be pruned
            "sig_lookback": datetime(2026, 10, 17, 12, 0, 6),  # 4s old -> inside lookback window -> MUST be retained
            "sig_current": datetime(2026, 10, 17, 12, 0, 10),  # current -> MUST be retained
        }
        mock_client = MockQueryClient([])
        tracker.fetch_next_events(mock_client)

        self.assertNotIn("sig_old", tracker.seen_signatures)
        self.assertIn("sig_lookback", tracker.seen_signatures)
        self.assertIn("sig_current", tracker.seen_signatures)

    def test_identical_legacy_rows_deduplication(self):
        """Test tracker deduplication on two identical legacy rows computing same ID."""
        from src.production.dashboard.web_app import SSECursorTracker

        t0 = datetime(2026, 10, 17, 12, 0, 0)
        # Two identical rows sharing the same computed effective_event_id
        dataset = [
            [t0, "BTC", 81000.0, 1, 81000.0, 7695000.0, 95.0, None, "hash_identical"],
            [t0, "BTC", 81000.0, 1, 81000.0, 7695000.0, 95.0, None, "hash_identical"],
        ]
        mock_client = MockQueryClient(dataset)
        tracker = SSECursorTracker(lookback_seconds=5, batch_size=100)
        tracker.is_primed = True
        tracker.high_watermark_ts = t0

        events = tracker.fetch_next_events(mock_client)
        # First row is emitted, second row is deduplicated
        self.assertEqual(len(events), 1)

    def test_reconnect_fresh_tracker(self):
        """Verify reconnect creates fresh unprimed tracker."""
        from src.production.dashboard.web_app import SSECursorTracker

        tracker = SSECursorTracker()
        self.assertFalse(tracker.is_primed)
        self.assertIsNone(tracker.high_watermark_ts)

    def test_landing_and_terminal_routes(self):
        """Verify / serves landing page and /terminal serves trading terminal."""
        from fastapi.testclient import TestClient
        from src.production.dashboard.web_app import app

        client = TestClient(app)

        res_root = client.get("/")
        self.assertEqual(res_root.status_code, 200)
        self.assertIn("See what", res_root.text)

        res_term = client.get("/terminal")
        self.assertEqual(res_term.status_code, 200)
        self.assertIn("appDashboard", res_term.text)

        res_landing = client.get("/landing")
        self.assertEqual(res_landing.status_code, 200)
        self.assertIn("See what", res_landing.text)


# =============================================================================
# SUITE 2: REAL CLICKHOUSE SQL INTEGRATION TESTS
# =============================================================================

class TestClickHouseIntegration(unittest.TestCase):
    """Executes real ClickHouse queries to validate SQL semantics."""

    @classmethod
    def setUpClass(cls):
        from src.production.dashboard.web_app import get_client
        cls.client = get_client()

    def test_clickhouse_connection_and_table_schema(self):
        """Verify ClickHouse connection and table schema has all 9 columns."""
        res = self.client.query("DESCRIBE TABLE stocks.ticks").result_rows
        col_names = [r[0] for r in res]
        expected_cols = [
            "symbol", "price", "volume", "timestamp",
            "price_usd", "price_inr", "exchange_rate", "rate_timestamp", "event_id"
        ]
        for col in expected_cols:
            self.assertIn(col, col_names)

    def test_effective_event_id_fallback_and_identical_rows(self):
        """Test SQL CTE fallback and CityHash64 behavior for identical legacy rows."""
        query = """
        WITH test_data AS (
            SELECT
                toDateTime('2026-10-17 12:00:00') as timestamp,
                'BTC' as symbol,
                toFloat64(81000.0) as price,
                toFloat64(1.0) as volume,
                '' as event_id
            UNION ALL
            SELECT
                toDateTime('2026-10-17 12:00:00') as timestamp,
                'BTC' as symbol,
                toFloat64(81000.0) as price,
                toFloat64(1.0) as volume,
                '' as event_id
        )
        SELECT
            timestamp,
            symbol,
            COALESCE(
                NULLIF(event_id, ''),
                lower(hex(cityHash64(symbol, toString(price), toString(volume), toString(timestamp))))
            ) as effective_event_id
        FROM test_data
        """
        res = self.client.query(query).result_rows
        self.assertEqual(len(res), 2)
        # Both identical rows compute the exact same CityHash64 fallback
        self.assertEqual(res[0][2], res[1][2])
        self.assertEqual(len(res[0][2]), 16)

    def test_clickhouse_keyset_tuple_comparison_150_rows(self):
        """Test ClickHouse keyset tuple comparison across 150 rows sharing one second."""
        query = """
        WITH generated AS (
            SELECT
                toDateTime('2026-10-17 14:00:00') as timestamp,
                'BTC' as symbol,
                number as price,
                concat('event_', leftPad(toString(number), 4, '0')) as effective_event_id
            FROM numbers(150)
        )
        SELECT timestamp, effective_event_id
        FROM generated
        WHERE (timestamp, effective_event_id) > (toDateTime('2026-10-17 14:00:00'), 'event_0099')
        ORDER BY timestamp ASC, effective_event_id ASC
        LIMIT 100
        """
        res = self.client.query(query).result_rows
        # Page 2 returns the remaining 50 rows starting at event_0100
        self.assertEqual(len(res), 50)
        self.assertEqual(res[0][1], "event_0100")
        self.assertEqual(res[-1][1], "event_0149")

    def test_api_endpoints_live_queries(self):
        """Test all API SQL queries directly against live ClickHouse data."""
        # /api/latest tuple argMax
        latest_res = self.client.query("""
            SELECT
                symbol,
                t.1 as price,
                t.2 as price_usd,
                t.3 as price_inr,
                t.4 as exchange_rate,
                t.5 as rate_timestamp,
                t.6 as volume,
                t.7 as last_update
            FROM (
                SELECT
                    symbol,
                    argMax((price, price_usd, price_inr, exchange_rate, rate_timestamp, volume, timestamp), timestamp) as t
                FROM stocks.ticks
                GROUP BY symbol
            )
            ORDER BY symbol
        """).result_rows
        self.assertGreater(len(latest_res), 0)
        for row in latest_res:
            self.assertIsNotNone(row[0])  # symbol
            self.assertIsNotNone(row[1])  # price

        # /api/stats
        stats_res = self.client.query("""
            SELECT
                symbol,
                count() as tick_count,
                round(min(price), 2) as min_price,
                round(max(price), 2) as max_price,
                round(avg(price), 2) as avg_price,
                sum(volume) as total_volume,
                round(min(price_inr), 2) as min_price_inr,
                round(max(price_inr), 2) as max_price_inr,
                round(avg(price_inr), 2) as avg_price_inr
            FROM stocks.ticks
            GROUP BY symbol
            ORDER BY symbol
        """).result_rows
        self.assertGreater(len(stats_res), 0)
        for r in stats_res:
            self.assertGreater(r[1], 0)  # tick_count > 0

        # /api/aggregates
        agg_res = self.client.query("""
            SELECT
                symbol, window_start, window_end, vwap, avg_price,
                min_price, max_price, total_volume, tick_count,
                vwap_inr, avg_price_inr, min_price_inr, max_price_inr
            FROM stocks.ticks_1m_agg FINAL
            ORDER BY window_start DESC
            LIMIT 10
        """).result_rows
        self.assertGreater(len(agg_res), 0)

        # /api/candles (OHLC 1-minute aggregations for USD & INR)
        candles_res = self.client.query("""
            SELECT
                toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
                symbol,
                argMin(price, timestamp) as open_usd,
                max(price) as high_usd,
                min(price) as low_usd,
                argMax(price, timestamp) as close_usd,
                argMin(price_inr, timestamp) as open_inr,
                max(price_inr) as high_inr,
                min(price_inr) as low_inr,
                argMax(price_inr, timestamp) as close_inr,
                sum(volume) as volume
            FROM stocks.ticks
            WHERE symbol IN ('BTC', 'ETH')
              AND timestamp >= now() - INTERVAL 60 MINUTE
            GROUP BY candle_time, symbol
            ORDER BY candle_time ASC
        """).result_rows
        self.assertGreater(len(candles_res), 0)
        for candle in candles_res:
            self.assertIsNotNone(candle[0])  # candle_time
            self.assertIn(candle[1], ('BTC', 'ETH'))  # symbol
            self.assertGreaterEqual(candle[3], candle[4])  # high >= low


    def test_telemetry_live_query(self):
        """Test telemetry queries for latency, ingestion rate, and ClickHouse compression ratio."""
        lat_res = self.client.query("""
            SELECT
                dateDiff('second', max(timestamp), now()) as latency_sec,
                max(timestamp) as latest_tick_time
            FROM stocks.ticks
        """).result_rows[0]
        self.assertIsNotNone(lat_res[1])

        st_res = self.client.query("""
            SELECT
                formatReadableSize(sum(data_compressed_bytes)) AS comp_size,
                formatReadableSize(sum(data_uncompressed_bytes)) AS uncomp_size,
                round(sum(data_uncompressed_bytes) / nullif(sum(data_compressed_bytes), 0), 2) AS ratio,
                count() AS parts
            FROM system.parts
            WHERE database = 'stocks' AND active = 1
        """).result_rows[0]
        self.assertIsNotNone(st_res[0])
        self.assertGreater(st_res[2], 1.0)  # Compression ratio must be > 1.0

    def test_order_flow_imbalance_calculation(self):
        """Test Lee-Ready (1991) tick rule order flow calculation on real ClickHouse ticks."""
        rows = self.client.query("""
            SELECT
                symbol, price, volume, timestamp
            FROM (
                SELECT
                    symbol, price, volume, timestamp,
                    row_number() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                FROM stocks.ticks
                WHERE symbol = 'BTC'
            )
            WHERE rn <= 50
            ORDER BY timestamp ASC
        """).result_rows
        self.assertGreater(len(rows), 0)

        buy_vol, sell_vol, last_dir = 0.0, 0.0, 1
        for i in range(len(rows)):
            p, v, ts = rows[i][1], rows[i][2], rows[i][3]
            if i > 0:
                prev_p = rows[i - 1][1]
                if p > prev_p:
                    last_dir = 1
                elif p < prev_p:
                    last_dir = -1
            if last_dir == 1:
                buy_vol += v
            else:
                sell_vol += v

        tot = buy_vol + sell_vol
        self.assertGreater(tot, 0)
        imbalance = ((buy_vol - sell_vol) / tot) * 100
        self.assertTrue(-100.0 <= imbalance <= 100.0)

    def test_technical_indicators_calculation(self):
        """Test Bollinger Bands (SMA20, Upper, Lower) and EMA-9 calculation."""
        import math
        rows = self.client.query("""
            SELECT price FROM stocks.ticks WHERE symbol = 'BTC' ORDER BY timestamp DESC LIMIT 30
        """).result_rows
        self.assertGreaterEqual(len(rows), 20)
        prices = [r[0] for r in reversed(rows)]

        # SMA 20 and StdDev
        window = prices[-20:]
        sma = sum(window) / 20
        var = sum((x - sma) ** 2 for x in window) / 20
        std = math.sqrt(var)
        upper = sma + (2 * std)
        lower = sma - (2 * std)

        self.assertGreater(upper, sma)
        self.assertLess(lower, sma)

        # EMA-9
        k = 2 / (9 + 1)
        ema = prices[0]
        for p in prices:
            ema = p * k + ema * (1 - k)
        self.assertGreater(ema, 0)

    def test_webhook_dispatcher_platforms_and_throttling(self):
        """Test AlertWebhookDispatcher platform detection, embed formatting, and per-symbol throttling."""
        from src.production.consumer.webhook_dispatcher import AlertWebhookDispatcher

        # Platform detection
        d_disp = AlertWebhookDispatcher("https://discord.com/api/webhooks/123/token")
        self.assertEqual(d_disp.get_platform(), "discord")

        t_disp = AlertWebhookDispatcher("https://api.telegram.org/bot123:ABC/sendMessage?chat_id=456")
        self.assertEqual(t_disp.get_platform(), "telegram")

        s_disp = AlertWebhookDispatcher("https://hooks.slack.com/services/T/B/X")
        self.assertEqual(s_disp.get_platform(), "slack")

        g_disp = AlertWebhookDispatcher("https://webhook.site/abc-123")
        self.assertEqual(g_disp.get_platform(), "generic")

        # Discord payload structure
        alert_data = {
            "symbol": "BTC",
            "price_usd": 81500.0,
            "price_return_pct": 0.45,
            "z_score": 3.85,
            "threshold": 3.0,
        }
        payload, headers, url = d_disp._build_discord_payload(alert_data)
        self.assertIn("embeds", payload)
        self.assertEqual(payload["embeds"][0]["color"], 0x00FF88)  # Green for positive spike
        self.assertGreater(len(payload["embeds"][0]["fields"]), 5)

        # Telegram payload structure
        t_payload, t_headers, t_url = t_disp._build_telegram_payload(alert_data)
        self.assertEqual(t_payload["chat_id"], "456")
        self.assertEqual(t_payload["parse_mode"], "HTML")
        self.assertIn("BTC", t_payload["text"])

        # Throttling
        test_disp = AlertWebhookDispatcher("", cooldown_seconds=10)
        self.assertTrue(test_disp.can_dispatch("BTC"))
        test_disp.record_dispatch("BTC")
        self.assertFalse(test_disp.can_dispatch("BTC"))
        self.assertTrue(test_disp.can_dispatch("ETH"))  # Different symbol not throttled

    def test_price_cache_and_connection_manager(self):
        """Test in-memory price caching and WebSocket ConnectionManager."""
        import asyncio
        from src.production.dashboard.web_app import (
            ConnectionManager,
            latest_prices_cache,
            get_latest_prices,
        )

        # 1. Test in-memory cache fast path
        latest_prices_cache["BTC"] = {
            "symbol": "BTC",
            "price": 75000.0,
            "price_usd": 75000.0,
            "price_inr": 7125000.0,
            "exchange_rate": 95.0,
            "rate_timestamp": None,
            "volume": 1.0,
            "last_update": "2026-10-31 12:00:00",
        }
        res = asyncio.run(get_latest_prices("BTC"))
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["symbol"], "BTC")
        self.assertEqual(res[0]["price"], 75000.0)

        # 2. Test ConnectionManager
        from unittest.mock import AsyncMock
        mgr = ConnectionManager()
        mock_ws = MagicMock()
        mock_ws.accept = AsyncMock()
        mock_ws.send_json = AsyncMock()

        asyncio.run(mgr.connect(mock_ws))
        self.assertIn(mock_ws, mgr.active_connections)

        asyncio.run(mgr.broadcast({"type": "test", "data": 123}))
        mock_ws.send_json.assert_called_once_with({"type": "test", "data": 123})

        mgr.disconnect(mock_ws)
        self.assertNotIn(mock_ws, mgr.active_connections)


if __name__ == "__main__":
    unittest.main()


