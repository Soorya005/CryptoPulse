"""
test_ws_parser.py — Unit tests for WebSocket message parsing and INR conversion.

Tests cover:
    1.  parse_coinbase_message() — ticker messages
    2.  parse_coinbase_message() — match / last_match messages
    3.  parse_coinbase_message() — ignored / error message types
    4.  Malformed JSON handling
    5.  INR conversion: successful fetch
    6.  INR conversion: cached-rate reuse (no second HTTP call)
    7.  INR conversion: API failure → stale cache preserved
    8.  INR conversion: API failure with empty cache → None returned
    9.  Reconnection / resubscription helpers (_build_subscribe_msg)
   10.  Full publish path with null INR fields

All tests are pure-Python unit tests; no real network connections or
Kafka brokers are opened.  The ``requests`` library and WebSocket
connections are fully mocked.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ticker_msg(
    product_id="BTC-USD",
    price="81562.81",
    volume_24h="1234.5",
    time_str="2026-10-16T10:30:15.000000Z",
) -> str:
    return json.dumps(
        {
            "type": "ticker",
            "product_id": product_id,
            "price": price,
            "volume_24h": volume_24h,
            "time": time_str,
        }
    )


def _match_msg(
    product_id="ETH-USD",
    price="2500.00",
    size="1.5",
    time_str="2026-10-16T10:30:20.000000Z",
) -> str:
    return json.dumps(
        {
            "type": "match",
            "product_id": product_id,
            "price": price,
            "size": size,
            "time": time_str,
        }
    )


def _subscriptions_ack() -> str:
    return json.dumps(
        {
            "type": "subscriptions",
            "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}],
        }
    )


def _error_msg() -> str:
    return json.dumps({"type": "error", "message": "Authentication Failed"})


# ---------------------------------------------------------------------------
# Fixture: mock a fresh INR rate so parse tests don't hit the network
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_inr_converter():
    """Replace get_usd_to_inr with a canned response for all WS parser tests."""
    with patch(
        "coinbase_ws.get_usd_to_inr",
        return_value={"rate": 84.0, "rate_timestamp": "2026-10-16T00:00:00Z"},
    ):
        yield


# ---------------------------------------------------------------------------
# 1. Ticker message parsing
# ---------------------------------------------------------------------------

class TestParseTickerMessage:
    def test_ticker_returns_dict(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg())
        assert isinstance(result, dict)

    def test_ticker_symbol(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg(product_id="BTC-USD"))
        assert result["symbol"] == "BTC-USD"

    def test_ticker_price_usd_is_float(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg(price="81562.81"))
        assert isinstance(result["price_usd"], float)
        assert result["price_usd"] == pytest.approx(81562.81)

    def test_ticker_price_equals_price_usd(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg())
        assert result["price"] == result["price_usd"]

    def test_ticker_inr_calculated(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg(price="100.0"))
        # rate is 84.0 from fixture
        assert result["price_inr"] == pytest.approx(100.0 * 84.0)

    def test_ticker_exchange_rate_set(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg())
        assert result["exchange_rate"] == pytest.approx(84.0)

    def test_ticker_timestamp_present(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg())
        assert "timestamp" in result

    def test_ticker_volume_positive(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_ticker_msg(volume_24h="500.0"))
        assert result["volume"] > 0


# ---------------------------------------------------------------------------
# 2. Match message parsing
# ---------------------------------------------------------------------------

class TestParseMatchMessage:
    def test_match_returns_dict(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_match_msg())
        assert isinstance(result, dict)

    def test_match_symbol(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_match_msg(product_id="ETH-USD"))
        assert result["symbol"] == "ETH-USD"

    def test_match_price_usd_is_float(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_match_msg(price="2500.00"))
        assert isinstance(result["price_usd"], float)
        assert result["price_usd"] == pytest.approx(2500.00)

    def test_match_volume_is_float(self):
        from coinbase_ws import parse_coinbase_message

        result = parse_coinbase_message(_match_msg(size="1.5"))
        assert isinstance(result["volume"], float)
        assert result["volume"] == pytest.approx(1.5)

    def test_last_match_type_accepted(self):
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps(
            {
                "type": "last_match",
                "product_id": "SOL-USD",
                "price": "150.00",
                "size": "10.0",
                "time": "2026-10-16T10:30:20.000000Z",
            }
        )
        result = parse_coinbase_message(msg)
        assert result is not None
        assert result["symbol"] == "SOL-USD"


# ---------------------------------------------------------------------------
# 3. Non-data message types → None returned
# ---------------------------------------------------------------------------

class TestIgnoredMessageTypes:
    def test_subscriptions_ack_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        assert parse_coinbase_message(_subscriptions_ack()) is None

    def test_error_message_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        assert parse_coinbase_message(_error_msg()) is None

    def test_heartbeat_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps({"type": "heartbeat", "sequence": 1})
        assert parse_coinbase_message(msg) is None


# ---------------------------------------------------------------------------
# 4. Malformed / unexpected input
# ---------------------------------------------------------------------------

class TestMalformedMessages:
    def test_invalid_json_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        assert parse_coinbase_message("{not valid json}") is None

    def test_non_dict_json_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        assert parse_coinbase_message("[1, 2, 3]") is None

    def test_ticker_missing_price_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps(
            {"type": "ticker", "product_id": "BTC-USD", "time": "2026-10-16T10:30:15Z"}
        )
        assert parse_coinbase_message(msg) is None

    def test_ticker_invalid_price_type_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps(
            {
                "type": "ticker",
                "product_id": "BTC-USD",
                "price": "not-a-number",
                "volume_24h": "100",
                "time": "2026-10-16T10:30:15Z",
            }
        )
        assert parse_coinbase_message(msg) is None

    def test_match_missing_size_returns_none(self):
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps(
            {
                "type": "match",
                "product_id": "ETH-USD",
                "price": "2500.00",
                "time": "2026-10-16T10:30:15Z",
            }
        )
        assert parse_coinbase_message(msg) is None


# ---------------------------------------------------------------------------
# 5. INR conversion — successful fetch
# ---------------------------------------------------------------------------

class TestInrConverterSuccess:
    def test_get_usd_to_inr_returns_rate(self):
        """A successful API response should populate the cache and return a rate."""
        import inr_converter

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"rates": {"INR": 84.5}}

        with patch("inr_converter.requests.get", return_value=mock_resp):
            # Reset the cache so it triggers a real refresh
            inr_converter._cache["rate"] = None
            inr_converter._cache["fetched_at"] = 0.0

            result = inr_converter.get_usd_to_inr(force_refresh=True)

        assert result["rate"] == pytest.approx(84.5)
        assert result["rate_timestamp"] is not None

    def test_get_usd_to_inr_rate_timestamp_format(self):
        """rate_timestamp must follow YYYY-MM-DDTHH:MM:SSZ format."""
        import re
        import inr_converter

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"rates": {"INR": 84.5}}

        with patch("inr_converter.requests.get", return_value=mock_resp):
            inr_converter._cache["rate"] = None
            inr_converter._cache["fetched_at"] = 0.0
            result = inr_converter.get_usd_to_inr(force_refresh=True)

        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", result["rate_timestamp"])


# ---------------------------------------------------------------------------
# 6. INR conversion — cached-rate reuse
# ---------------------------------------------------------------------------

class TestInrConverterCaching:
    def test_cached_rate_reused_within_refresh_interval(self):
        """A second call within the refresh window must not make an HTTP request."""
        import inr_converter

        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"rates": {"INR": 84.0}}

        with patch("inr_converter.requests.get", return_value=mock_resp) as mock_get:
            # Force a fresh fetch to populate the cache.
            inr_converter._cache["rate"] = None
            inr_converter._cache["fetched_at"] = 0.0
            inr_converter.get_usd_to_inr(force_refresh=True)

            # Simulate very recent fetch time.
            inr_converter._cache["fetched_at"] = time.monotonic()

            # Second call should hit cache, not HTTP.
            inr_converter.get_usd_to_inr()

            assert mock_get.call_count == 1  # only the first call hit the network


# ---------------------------------------------------------------------------
# 7. INR conversion — API failure → stale cache preserved
# ---------------------------------------------------------------------------

class TestInrConverterApiFailure:
    def test_stale_cache_reused_on_request_error(self):
        """Network error must not wipe the cached rate."""
        import requests as req
        import inr_converter

        # Pre-populate the cache with a valid rate.
        inr_converter._cache["rate"] = 83.0
        inr_converter._cache["rate_timestamp"] = "2026-10-16T00:00:00Z"
        inr_converter._cache["fetched_at"] = 0.0  # expired

        with patch(
            "inr_converter.requests.get",
            side_effect=req.exceptions.ConnectionError("timeout"),
        ):
            result = inr_converter.get_usd_to_inr(force_refresh=True)

        # Stale cache should still be returned.
        assert result["rate"] == pytest.approx(83.0)

    def test_stale_cache_reused_on_non_200(self):
        """Non-200 HTTP response must not wipe the cached rate."""
        import requests as req
        import inr_converter

        inr_converter._cache["rate"] = 83.5
        inr_converter._cache["rate_timestamp"] = "2026-10-16T00:00:00Z"
        inr_converter._cache["fetched_at"] = 0.0

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError("503")

        with patch("inr_converter.requests.get", return_value=mock_resp):
            result = inr_converter.get_usd_to_inr(force_refresh=True)

        assert result["rate"] == pytest.approx(83.5)


# ---------------------------------------------------------------------------
# 8. INR conversion — empty cache + API failure → None
# ---------------------------------------------------------------------------

class TestInrConverterEmptyCache:
    def test_returns_none_when_cache_empty_and_api_fails(self):
        """If no rate has ever been fetched and the API fails, return None."""
        import requests as req
        import inr_converter

        inr_converter._cache["rate"] = None
        inr_converter._cache["rate_timestamp"] = None
        inr_converter._cache["fetched_at"] = 0.0

        with patch(
            "inr_converter.requests.get",
            side_effect=req.exceptions.ConnectionError("unreachable"),
        ):
            result = inr_converter.get_usd_to_inr(force_refresh=True)

        assert result["rate"] is None
        assert result["rate_timestamp"] is None


# ---------------------------------------------------------------------------
# 9. Subscribe message structure
# ---------------------------------------------------------------------------

class TestSubscribeMessage:
    def test_subscribe_msg_is_valid_json(self):
        from coinbase_ws import _build_subscribe_msg

        msg_str = _build_subscribe_msg()
        msg = json.loads(msg_str)
        assert isinstance(msg, dict)

    def test_subscribe_msg_type(self):
        from coinbase_ws import _build_subscribe_msg

        msg = json.loads(_build_subscribe_msg())
        assert msg["type"] == "subscribe"

    def test_subscribe_msg_has_product_ids(self):
        from coinbase_ws import _build_subscribe_msg, PRODUCT_IDS

        msg = json.loads(_build_subscribe_msg())
        assert msg["product_ids"] == PRODUCT_IDS

    def test_subscribe_msg_has_channels(self):
        from coinbase_ws import _build_subscribe_msg, SUBSCRIBE_CHANNELS

        msg = json.loads(_build_subscribe_msg())
        assert msg["channels"] == SUBSCRIBE_CHANNELS


# ---------------------------------------------------------------------------
# 10. Full publish path with null INR fields
# ---------------------------------------------------------------------------

class TestPublishWithNullInr:
    def test_ticker_with_null_inr_publishes(self):
        """When the INR rate is None the event should still be published with null INR fields."""
        from coinbase_ws import parse_coinbase_message

        with patch(
            "coinbase_ws.get_usd_to_inr",
            return_value={"rate": None, "rate_timestamp": None},
        ):
            result = parse_coinbase_message(_ticker_msg(price="50000.0"))

        assert result is not None
        assert result["price_inr"] is None
        assert result["exchange_rate"] is None
        assert result["rate_timestamp"] is None
        assert result["price_usd"] == pytest.approx(50000.0)


# ---------------------------------------------------------------------------
# 11. Ticker volume prioritization and Fallback API tests
# ---------------------------------------------------------------------------

class TestEnhancements:
    def test_ticker_prioritizes_last_size(self):
        """Ticker parsing should prioritize last_size over 24h market volume."""
        from coinbase_ws import parse_coinbase_message

        msg = json.dumps({
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "81000.00",
            "volume_24h": "5000.0",
            "last_size": "0.025",
            "time": "2026-10-16T10:30:15Z",
        })
        result = parse_coinbase_message(msg)
        assert result is not None
        assert result["volume"] == pytest.approx(0.025)

    def test_fallback_api_used_when_primary_fails(self):
        """When primary open.er-api fails, fallback Frankfurter API should be queried."""
        import inr_converter
        import requests as req

        def mock_get(url, *args, **kwargs):
            if "open.er-api.com" in url:
                raise req.exceptions.ConnectionError("Primary API down")
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {"rates": {"INR": 94.75}}
            return mock_resp

        with patch("inr_converter.requests.get", side_effect=mock_get):
            inr_converter._cache["rate"] = None
            inr_converter._cache["fetched_at"] = 0.0
            result = inr_converter.get_usd_to_inr(force_refresh=True)

        assert result["rate"] == pytest.approx(94.75)

    def test_dry_run_client_init_without_broker(self):
        """CoinbaseWebSocketClient with dry_run=True should not connect to Kafka."""
        from coinbase_ws import CoinbaseWebSocketClient

        with patch("producer.KafkaProducer") as mock_producer:
            client = CoinbaseWebSocketClient(dry_run=True, max_events=5)
            assert client.dry_run is True
            assert client.max_events == 5
            mock_producer.assert_not_called()
