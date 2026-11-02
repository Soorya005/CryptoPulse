"""
schema.py — PySpark schemas for Coinbase trade events and ClickHouse integration.

Matches the validated schema emitted by Member 2's Python producer:
  - symbol: string (required)
  - price: double (required)
  - volume: double (required)
  - timestamp: string (ISO-8601 UTC string, converted to TimestampType in pipeline)
  - price_usd: double (required)
  - price_inr: double (nullable)
  - exchange_rate: double (nullable)
  - rate_timestamp: string (nullable ISO-8601 UTC string)
"""

from __future__ import annotations

try:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    # Fallback placeholders for environments where pyspark is being installed
    StructType = None  # type: ignore
    StructField = None  # type: ignore
    StringType = None  # type: ignore
    DoubleType = None  # type: ignore
    TimestampType = None  # type: ignore
    LongType = None  # type: ignore


def get_trade_json_schema() -> StructType:
    """
    Return the explicit PySpark StructType schema for parsing raw JSON values from Kafka.
    All required fields are non-nullable; INR fields are explicitly nullable.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is not installed in the current environment.")

    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("price_usd", DoubleType(), nullable=False),
        StructField("price_inr", DoubleType(), nullable=True),
        StructField("exchange_rate", DoubleType(), nullable=True),
        StructField("rate_timestamp", StringType(), nullable=True),
    ])


def get_raw_ticks_schema() -> StructType:
    """
    Schema of the transformed DataFrame ready to be inserted into stocks.ticks.
    Timestamps are converted to native Spark TimestampType.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is not installed in the current environment.")

    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("price_usd", DoubleType(), nullable=False),
        StructField("price_inr", DoubleType(), nullable=True),
        StructField("exchange_rate", DoubleType(), nullable=True),
        StructField("rate_timestamp", TimestampType(), nullable=True),
    ])


def get_agg_1m_schema() -> StructType:
    """
    Schema for the 1-minute windowed aggregate DataFrame written to stocks.ticks_1m_agg.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is not installed in the current environment.")

    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("event_count", LongType(), nullable=False),
        StructField("avg_price", DoubleType(), nullable=False),
        StructField("min_price", DoubleType(), nullable=False),
        StructField("max_price", DoubleType(), nullable=False),
        StructField("total_volume", DoubleType(), nullable=False),
        StructField("avg_price_inr", DoubleType(), nullable=True),
    ])
