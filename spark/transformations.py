"""
transformations.py — Spark Structured Streaming transformations for crypto tick events.

Responsibilities:
1. Parse raw Kafka binary payload into JSON according to TRADE_EVENT_SCHEMA.
2. Convert ISO-8601 string timestamps to native Spark TimestampType.
3. Filter out corrupt, malformed, or out-of-spec records.
4. Apply watermarking to handle late-arriving event data.
5. Compute 1-minute windowed aggregates for each trading pair.
"""

from __future__ import annotations

import logging
from typing import Any

from spark.schema import get_trade_json_schema

logger = logging.getLogger("spark_transformations")

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    DataFrame = Any  # type: ignore
    F = Any  # type: ignore
    PYSPARK_AVAILABLE = False


def parse_kafka_stream(raw_kafka_df: DataFrame) -> DataFrame:
    """
    Parse Kafka message value (binary) as UTF-8 string, unpack the JSON payload
    using explicit schema, and extract fields.

    Parameters
    ----------
    raw_kafka_df : DataFrame
        Streaming DataFrame with Kafka columns: 'key', 'value', 'topic', 'partition', 'offset', 'timestamp'.

    Returns
    -------
    DataFrame
        Parsed DataFrame with all trade event columns.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is required for parse_kafka_stream")

    schema = get_trade_json_schema()

    # 1. Cast Kafka binary 'value' to string
    json_str_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_value")

    # 2. Parse JSON according to explicit schema
    parsed_df = json_str_df.withColumn("data", F.from_json(F.col("json_value"), schema))

    # 3. Flatten the fields
    flattened_df = parsed_df.select("data.*")

    return flattened_df


def prepare_raw_ticks(parsed_df: DataFrame) -> DataFrame:
    """
    Filter malformed/invalid records and convert timestamp strings into native TimestampType.

    Rules:
    - symbol must be non-null and non-empty.
    - price, volume, and price_usd must be positive and non-null.
    - timestamp must parse into a valid TimestampType.
    - rate_timestamp is converted to TimestampType if not null.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is required for prepare_raw_ticks")

    # Convert ISO-8601 timestamp string (e.g., '2026-10-16T10:30:15Z') to native Spark timestamp
    with_timestamps = parsed_df.withColumn(
        "parsed_timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    ).withColumn(
        "parsed_rate_timestamp",
        F.when(
            F.col("rate_timestamp").isNotNull(),
            F.to_timestamp(F.col("rate_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ).otherwise(F.lit(None).cast(TimestampType())),
    )

    # Filter out malformed/missing records without crashing stream
    valid_ticks = with_timestamps.filter(
        F.col("symbol").isNotNull()
        & (F.length(F.trim(F.col("symbol"))) > 0)
        & F.col("price").isNotNull()
        & (F.col("price") > 0.0)
        & F.col("volume").isNotNull()
        & (F.col("volume") > 0.0)
        & F.col("price_usd").isNotNull()
        & (F.col("price_usd") > 0.0)
        & F.col("parsed_timestamp").isNotNull()
    )

    # Select final standardized columns
    cleaned_df = valid_ticks.select(
        F.trim(F.upper(F.col("symbol"))).alias("symbol"),
        F.col("price").cast("double").alias("price"),
        F.col("volume").cast("double").alias("volume"),
        F.col("parsed_timestamp").alias("timestamp"),
        F.col("price_usd").cast("double").alias("price_usd"),
        F.col("price_inr").cast("double").alias("price_inr"),
        F.col("exchange_rate").cast("double").alias("exchange_rate"),
        F.col("parsed_rate_timestamp").alias("rate_timestamp"),
    )

    return cleaned_df


def compute_1m_aggregates(
    raw_ticks_df: DataFrame,
    watermark_duration: str = "1 minute",
) -> DataFrame:
    """
    Apply event-time watermark and compute 1-minute windowed aggregations for each symbol.

    Aggregations:
    - event_count: count(*)
    - avg_price: avg(price)
    - min_price: min(price)
    - max_price: max(price)
    - total_volume: sum(volume)
    - avg_price_inr: avg(price_inr) (handles nulls correctly)

    Parameters
    ----------
    raw_ticks_df : DataFrame
        Prepared DataFrame with 'symbol', 'price', 'volume', 'timestamp', and 'price_inr'.
    watermark_duration : str
        Watermark duration threshold for late-arriving events (default: '1 minute').

    Returns
    -------
    DataFrame
        Aggregated streaming DataFrame with window_start, window_end, and computed statistics.
    """
    if not PYSPARK_AVAILABLE:
        raise ImportError("pyspark is required for compute_1m_aggregates")

    # Set watermark on event timestamp
    watermarked_df = raw_ticks_df.withWatermark("timestamp", watermark_duration)

    # Group by symbol and 1-minute tumbling window
    aggregated_df = (
        watermarked_df.groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "1 minute"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.avg("price"), 4).alias("avg_price"),
            F.round(F.min("price"), 4).alias("min_price"),
            F.round(F.max("price"), 4).alias("max_price"),
            F.round(F.sum("volume"), 6).alias("total_volume"),
            F.round(F.avg("price_inr"), 4).alias("avg_price_inr"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("event_count"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("total_volume"),
            F.col("avg_price_inr"),
        )
    )

    return aggregated_df
