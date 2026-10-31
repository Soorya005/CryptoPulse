"""
Unit tests for USD/INR currency conversion, caching, failover, and real producer path.
"""
import json
import pytest

from src.production.producer.crypto_producer import (
    ExchangeRateManager,
    process_ticker_message,
)


def test_primary_api_success(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (94.50, "2026-10-16 00:00:00"))

    rate, ts = mgr.get_rate()
    assert rate == 94.50
    assert ts == "2026-10-16 00:00:00"


def test_api_failover_on_invalid_rate(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (_ for _ in ()).throw(ValueError("Rate <= 0")))
    monkeypatch.setattr(mgr, "_fetch_open_er", lambda: (95.10, "2026-10-17 00:02:32"))

    rate, ts = mgr.get_rate()
    assert rate == 95.10
    assert ts == "2026-10-17 00:02:32"


def test_cache_reuse_preserves_timestamp(monkeypatch):
    fake_time = 1000.0
    mgr = ExchangeRateManager(cache_ttl_seconds=600, time_provider=lambda: fake_time)
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (94.50, "2026-10-16 00:00:00"))

    rate1, ts1 = mgr.get_rate()

    # Advance clock by 300s (within 600s TTL)
    fake_time += 300.0
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (99.99, "2026-10-17 12:00:00"))
    rate2, ts2 = mgr.get_rate()

    assert rate2 == 94.50
    assert ts2 == "2026-10-16 00:00:00"
    assert ts1 == ts2  # Timestamp is not refreshed on cache hit


def test_cache_expiry_and_stale_fallback(monkeypatch):
    fake_time = 1000.0
    mgr = ExchangeRateManager(cache_ttl_seconds=600, time_provider=lambda: fake_time)
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (94.50, "2026-10-16 00:00:00"))
    rate1, ts1 = mgr.get_rate()
    assert rate1 == 94.50

    # Advance clock past TTL (601s later)
    fake_time += 601.0

    def fail():
        raise ConnectionError("Network down")

    monkeypatch.setattr(mgr, "_fetch_frankfurter", fail)
    monkeypatch.setattr(mgr, "_fetch_open_er", fail)

    rate2, ts2 = mgr.get_rate()
    assert rate2 == 94.50  # Retains stale cached rate
    assert ts2 == "2026-10-16 00:00:00"  # Original fetch timestamp preserved


def test_unavailable_rate_behaviour(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    def fail():
        raise ConnectionError("Network unreachable")

    monkeypatch.setattr(mgr, "_fetch_frankfurter", fail)
    monkeypatch.setattr(mgr, "_fetch_open_er", fail)

    rate, ts = mgr.get_rate()
    assert rate is None
    assert ts is None  # No fabricated value!


def test_missing_provider_timestamp_explicit_none(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    # Upstream provides valid rate but no valid reference timestamp
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (94.50, None))

    rate, ts = mgr.get_rate()
    assert rate == 94.50
    assert ts is None  # Not fabricated to current time


def test_real_producer_path_with_rate(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    monkeypatch.setattr(mgr, "_fetch_frankfurter", lambda: (94.50, "2026-10-16 00:00:00"))

    raw_ws_message = json.dumps({
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "68500.50",
        "last_size": "0.015",
        "time": "2026-10-17T08:00:00.123456Z",
    })

    tick = process_ticker_message(raw_ws_message, mgr)
    assert tick is not None
    assert tick["symbol"] == "BTC"
    assert tick["price"] == 68500.50
    assert tick["price_usd"] == 68500.50
    assert tick["price"] == tick["price_usd"]  # Backward compatibility guaranteed
    assert tick["price_inr"] == 6473297.25     # round(68500.50 * 94.50, 2)
    assert tick["exchange_rate"] == 94.50
    assert tick["rate_timestamp"] == "2026-10-16 00:00:00"
    assert tick["volume"] == 0.015


def test_real_producer_path_unavailable_rate(monkeypatch):
    mgr = ExchangeRateManager(cache_ttl_seconds=600)
    def fail():
        raise ConnectionError("Network down")
    monkeypatch.setattr(mgr, "_fetch_frankfurter", fail)
    monkeypatch.setattr(mgr, "_fetch_open_er", fail)

    raw_ws_message = json.dumps({
        "type": "ticker",
        "product_id": "ETH-USD",
        "price": "3500.00",
        "last_size": "1.5",
        "time": "2026-10-17T08:00:00.123456Z",
    })

    tick = process_ticker_message(raw_ws_message, mgr)
    assert tick is not None
    assert tick["symbol"] == "ETH"
    assert tick["price"] == 3500.00
    assert tick["price_usd"] == 3500.00
    assert tick["price_inr"] is None           # Nullable explicitly
    assert tick["exchange_rate"] is None       # Nullable explicitly
    assert tick["rate_timestamp"] is None      # Nullable explicitly


def test_real_producer_path_ignores_non_ticker():
    mgr = ExchangeRateManager()
    subscribe_response = json.dumps({"type": "subscriptions", "channels": []})
    assert process_ticker_message(subscribe_response, mgr) is None
