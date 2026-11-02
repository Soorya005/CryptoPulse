"""
test_clickhouse_schema.py — Unit tests for ClickHouse schema definition and management script.
"""

import pathlib
from unittest.mock import MagicMock, patch
import pytest

from clickhouse.manage_db import apply_schema, execute_sql, ping_clickhouse, verify_tables


def test_init_db_sql_exists():
    sql_path = pathlib.Path(__file__).parent / "clickhouse" / "init_db.sql"
    assert sql_path.exists(), "clickhouse/init_db.sql must exist"


def test_init_db_sql_content():
    sql_path = pathlib.Path(__file__).parent / "clickhouse" / "init_db.sql"
    content = sql_path.read_text(encoding="utf-8")

    # Database
    assert "CREATE DATABASE IF NOT EXISTS stocks" in content

    # Raw ticks table
    assert "CREATE TABLE IF NOT EXISTS stocks.ticks" in content
    assert "event_id UUID DEFAULT generateUUIDv4()" in content
    assert "symbol LowCardinality(String)" in content
    assert "price Float64" in content
    assert "volume Float64" in content
    assert "timestamp DateTime64(3, 'UTC')" in content
    assert "price_usd Float64" in content
    assert "price_inr Nullable(Float64)" in content
    assert "exchange_rate Nullable(Float64)" in content
    assert "rate_timestamp Nullable(DateTime64(3, 'UTC'))" in content
    assert "ENGINE = MergeTree()" in content
    assert "ORDER BY (symbol, timestamp, price, volume)" in content

    # 1-minute aggregate table
    assert "CREATE TABLE IF NOT EXISTS stocks.ticks_1m_agg" in content
    assert "window_start DateTime64(0, 'UTC')" in content
    assert "window_end DateTime64(0, 'UTC')" in content
    assert "event_count UInt64" in content
    assert "avg_price Float64" in content
    assert "min_price Float64" in content
    assert "max_price Float64" in content
    assert "total_volume Float64" in content
    assert "avg_price_inr Nullable(Float64)" in content
    assert "ENGINE = ReplacingMergeTree(calculated_at)" in content

    # Market alerts table
    assert "CREATE TABLE IF NOT EXISTS stocks.market_alerts" in content
    assert "alert_id UUID DEFAULT generateUUIDv4()" in content
    assert "alert_type LowCardinality(String)" in content
    assert "severity LowCardinality(String)" in content

    # Deduplicated views for Member 4 & FastAPI
    assert "CREATE VIEW IF NOT EXISTS stocks.v_ticks_1m_agg" in content
    assert "FROM stocks.ticks_1m_agg FINAL" in content
    assert "CREATE VIEW IF NOT EXISTS stocks.v_ticks_dedup" in content
    assert "GROUP BY" in content


@patch("clickhouse.manage_db.requests.get")
def test_ping_clickhouse_success(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "Ok."
    mock_get.return_value = mock_resp

    assert ping_clickhouse() is True


@patch("clickhouse.manage_db.requests.get")
def test_ping_clickhouse_failure(mock_get):
    mock_get.side_effect = Exception("Connection refused")
    assert ping_clickhouse() is False


@patch("clickhouse.manage_db.requests.post")
def test_execute_sql(mock_post):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "stocks.ticks\nstocks.ticks_1m_agg\n"
    mock_post.return_value = mock_resp

    res = execute_sql("SHOW TABLES", database="stocks")
    assert "stocks.ticks" in res
    mock_post.assert_called_once()


@patch("clickhouse.manage_db.execute_sql")
@patch("clickhouse.manage_db.ping_clickhouse", return_value=True)
def test_apply_schema(mock_ping, mock_exec):
    apply_schema()
    assert mock_exec.call_count >= 4  # CREATE DATABASE + 3 CREATE TABLEs
