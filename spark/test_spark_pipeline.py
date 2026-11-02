"""
test_spark_pipeline.py — Unit tests for Spark pipeline schemas, transformations, and ClickHouse writer.
"""

from unittest.mock import MagicMock, patch
import pytest

from spark.clickhouse_writer import (
    _execute_with_retry,
    get_clickhouse_client,
    write_agg_1m_batch,
    write_raw_ticks_batch,
)
from spark.schema import (
    PYSPARK_AVAILABLE,
    get_agg_1m_schema,
    get_raw_ticks_schema,
    get_trade_json_schema,
)


# ---------------------------------------------------------------------------
# 1. Schema Tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark not yet installed locally")
def test_trade_json_schema_fields():
    schema = get_trade_json_schema()
    field_names = [f.name for f in schema.fields]
    assert "symbol" in field_names
    assert "price" in field_names
    assert "volume" in field_names
    assert "timestamp" in field_names
    assert "price_usd" in field_names
    assert "price_inr" in field_names
    assert "exchange_rate" in field_names
    assert "rate_timestamp" in field_names

    # Check nullability
    fields_dict = {f.name: f for f in schema.fields}
    assert fields_dict["symbol"].nullable is False
    assert fields_dict["price"].nullable is False
    assert fields_dict["price_usd"].nullable is False
    assert fields_dict["price_inr"].nullable is True
    assert fields_dict["exchange_rate"].nullable is True
    assert fields_dict["rate_timestamp"].nullable is True


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark not yet installed locally")
def test_agg_1m_schema_fields():
    schema = get_agg_1m_schema()
    field_names = [f.name for f in schema.fields]
    expected = [
        "symbol",
        "window_start",
        "window_end",
        "event_count",
        "avg_price",
        "min_price",
        "max_price",
        "total_volume",
        "avg_price_inr",
    ]
    for exp in expected:
        assert exp in field_names


# ---------------------------------------------------------------------------
# 2. ClickHouse Writer Micro-batch Tests
# ---------------------------------------------------------------------------

def test_writer_empty_batch_skipped():
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True

    with patch("spark.clickhouse_writer.get_clickhouse_client") as mock_client:
        write_raw_ticks_batch(mock_df, batch_id=1)
        mock_client.assert_not_called()

        write_agg_1m_batch(mock_df, batch_id=1)
        mock_client.assert_not_called()


def test_write_raw_ticks_batch_inserts_data():
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False

    sample_row = {
        "symbol": "BTC-USD",
        "price": 81500.0,
        "volume": 0.05,
        "timestamp": "2026-10-16T10:30:15Z",
        "price_usd": 81500.0,
        "price_inr": 7742500.0,
        "exchange_rate": 95.0,
        "rate_timestamp": "2026-10-16T00:00:00Z",
    }
    mock_df.select.return_value.collect.return_value = [sample_row]

    mock_client = MagicMock()
    with patch("spark.clickhouse_writer.get_clickhouse_client", return_value=mock_client):
        write_raw_ticks_batch(mock_df, batch_id=2)

        mock_client.insert.assert_called_once()
        args, kwargs = mock_client.insert.call_args
        assert kwargs["table"] == "ticks"
        assert len(kwargs["data"]) == 1
        inserted_row = kwargs["data"][0]
        assert inserted_row[0] == "BTC-USD"
        assert inserted_row[1] == 81500.0
        assert inserted_row[5] == 7742500.0  # price_inr


def test_write_agg_1m_batch_inserts_data():
    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False

    sample_agg = {
        "symbol": "ETH-USD",
        "window_start": "2026-10-16T10:30:00Z",
        "window_end": "2026-10-16T10:31:00Z",
        "event_count": 42,
        "avg_price": 2400.5,
        "min_price": 2395.0,
        "max_price": 2405.0,
        "total_volume": 12.5,
        "avg_price_inr": 228047.5,
    }
    mock_df.select.return_value.collect.return_value = [sample_agg]

    mock_client = MagicMock()
    with patch("spark.clickhouse_writer.get_clickhouse_client", return_value=mock_client):
        write_agg_1m_batch(mock_df, batch_id=3)

        mock_client.insert.assert_called_once()
        args, kwargs = mock_client.insert.call_args
        assert kwargs["table"] == "ticks_1m_agg"
        assert len(kwargs["data"]) == 1
        inserted_agg = kwargs["data"][0]
        assert inserted_agg[0] == "ETH-USD"
        assert inserted_agg[3] == 42
        assert inserted_agg[4] == 2400.5


def test_execute_with_retry_succeeds_after_transient_failure():
    mock_op = MagicMock()
    # First call raises an exception, second call succeeds
    mock_op.side_effect = [Exception("Temporary socket timeout"), "SUCCESS"]

    with patch("time.sleep") as mock_sleep:
        res = _execute_with_retry(mock_op)
        assert res == "SUCCESS"
        assert mock_op.call_count == 2
        mock_sleep.assert_called_once()


def test_execute_with_retry_exhaustion_raises():
    mock_op = MagicMock()
    mock_op.side_effect = Exception("Persistent connection failure")

    with patch("time.sleep"):
        with pytest.raises(Exception, match="Persistent connection failure"):
            _execute_with_retry(mock_op)
