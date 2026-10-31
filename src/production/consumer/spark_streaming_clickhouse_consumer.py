"""
Spark Structured Streaming Consumer -> ClickHouse (with Windowed Aggregations)

This consumer demonstrates core Spark Structured Streaming concepts:
  1. Event-time processing with watermarks
  2. Stateful windowed aggregations (tumbling windows)
  3. Fault tolerance via checkpointing

Architecture:
    Kafka (stock-ticks)
        |
        v
    df_parsed -> withWatermark("timestamp", "30 seconds")
        |
        +-> Stream 1: Raw ticks -> stocks.ticks (append, 3s trigger, checkpoint)
        |
        +-> Stream 2: 1-min windowed aggs -> stocks.ticks_1m_agg (update, 10s trigger, checkpoint)

Usage:
    python src/production/consumer/spark_streaming_clickhouse_consumer.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, min as spark_min,
    max as spark_max, sum as spark_sum, count as spark_count, when, lit, coalesce,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-ticks"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DATABASE = "stocks"
CLICKHOUSE_RAW_TABLE = "ticks"
CLICKHOUSE_AGG_TABLE = "ticks_1m_agg"

# Streaming settings (optimized for sub-second / fast reflection)
RAW_BATCH_INTERVAL = "1 second"
AGG_BATCH_INTERVAL = "5 seconds"

# Watermark: how long to wait for late-arriving data before finalizing windows.
# Events arriving more than 30 seconds late (by event-time) will be dropped.
WATERMARK_DELAY = "30 seconds"

# Window duration for tumbling windows
WINDOW_DURATION = "1 minute"

# Checkpoint directories (one per stream for independent fault tolerance)
CHECKPOINT_DIR_RAW = os.environ.get("CHECKPOINT_DIR_RAW", "checkpoints/raw_ticks")
CHECKPOINT_DIR_AGG = os.environ.get("CHECKPOINT_DIR_AGG", "checkpoints/windowed_agg")
RESET_CHECKPOINTS = os.environ.get("RESET_CHECKPOINTS", "false").lower() in ("true", "1", "yes")

if RESET_CHECKPOINTS:
    import shutil
    print("[Spark] RESET_CHECKPOINTS is active. Purging checkpoint directories...")
    shutil.rmtree(CHECKPOINT_DIR_RAW, ignore_errors=True)
    shutil.rmtree(CHECKPOINT_DIR_AGG, ignore_errors=True)


# =============================================================================
# CREATE SPARK SESSION
# =============================================================================

spark = (
    SparkSession.builder.appName("StockTickConsumer-Streaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.6.0,"
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .config("spark.streaming.ui.retainedBatches", "5")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Structured Streaming Consumer (Windowed Aggregations)")
print(f"Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
print(f"Raw ticks -> ClickHouse: {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE}")
print(f"Aggregations -> ClickHouse: {CLICKHOUSE_DATABASE}.{CLICKHOUSE_AGG_TABLE}")
print(f"Watermark delay: {WATERMARK_DELAY}")
print(f"Window duration: {WINDOW_DURATION}")
print("=" * 60)

# =============================================================================
# DEFINE SCHEMA FOR JSON MESSAGES
# =============================================================================

# Producer sends:
# {
#   "symbol": "BTC",
#   "price": 81611.2,
#   "price_usd": 81611.2,
#   "price_inr": 7749799.55,
#   "exchange_rate": 94.96,
#   "rate_timestamp": "2026-10-16 00:00:00",
#   "volume": 1,
#   "timestamp": "2026-10-17T08:29:15.445677+00:00"
# }
stock_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("price_inr", DoubleType(), True),
    StructField("exchange_rate", DoubleType(), True),
    StructField("rate_timestamp", StringType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])

# =============================================================================
# READ FROM KAFKA
# =============================================================================

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# =============================================================================
# PARSE JSON INTO COLUMNS
# =============================================================================

df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), stock_schema).alias("data"))
    .select(
        coalesce(col("data.event_id"), lit("")).alias("event_id"),
        col("data.symbol").alias("symbol"),
        col("data.price").alias("price"),
        col("data.price_usd").alias("price_usd"),
        col("data.price_inr").alias("price_inr"),
        col("data.exchange_rate").alias("exchange_rate"),
        to_timestamp(col("data.rate_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("rate_timestamp"),
        col("data.volume").alias("volume"),
        to_timestamp(col("data.timestamp")).alias("timestamp"),
    )
    .filter(col("timestamp").isNotNull())
)

# =============================================================================
# EVENT-TIME PROCESSING: WATERMARK
# =============================================================================

df_with_watermark = df_parsed.withWatermark("timestamp", WATERMARK_DELAY)

# =============================================================================
# CLICKHOUSE JDBC SETUP
# =============================================================================

CLICKHOUSE_JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"

CLICKHOUSE_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "",
    "isolationLevel": "NONE",
    "batchsize": "10000",
    "createTableOptions": "ENGINE = MergeTree() ORDER BY (symbol, timestamp)",
}

AGG_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "",
    "isolationLevel": "NONE",
    "batchsize": "10000",
    "createTableOptions": "ENGINE = ReplacingMergeTree() ORDER BY (symbol, window_start)",
}

# =============================================================================
# STREAM 1: RAW TICKS -> stocks.ticks
# =============================================================================

def write_raw_to_clickhouse(batch_df, batch_id):
    """Write raw tick data to ClickHouse directly without redundant count() passes."""
    try:
        if batch_df.isEmpty():
            return
        (
            batch_df.select(
                col("symbol").cast(StringType()),
                col("price").cast(DoubleType()),
                coalesce(col("volume").cast(DoubleType()), lit(0.0)).alias("volume"),
                col("timestamp").cast(TimestampType()),
                col("price_usd").cast(DoubleType()),
                col("price_inr").cast(DoubleType()),
                col("exchange_rate").cast(DoubleType()),
                col("rate_timestamp").cast(TimestampType()),
                col("event_id").cast(StringType()),
            )
            .write
            .jdbc(
                url=CLICKHOUSE_JDBC_URL,
                table=CLICKHOUSE_RAW_TABLE,
                mode="append",
                properties=CLICKHOUSE_PROPERTIES,
            )
        )
    except Exception as e:
        print(f"[Raw Batch {batch_id}] Warning/Error writing to ClickHouse: {e}")


raw_query = (
    df_with_watermark.writeStream
    .foreachBatch(write_raw_to_clickhouse)
    .outputMode("append")
    .trigger(processingTime=RAW_BATCH_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_DIR_RAW)
    .queryName("raw_ticks_to_clickhouse")
    .start()
)

# =============================================================================
# STREAM 2: WINDOWED AGGREGATIONS -> stocks.ticks_1m_agg
# =============================================================================

# Safe INR aggregation helpers
inr_vol_sum = spark_sum(when(col("price_inr").isNotNull(), col("volume")))
inr_weighted_sum = spark_sum(when(col("price_inr").isNotNull(), col("price_inr") * col("volume")))

df_windowed = (
    df_with_watermark
    .groupBy(
        col("symbol"),
        window(col("timestamp"), WINDOW_DURATION),
    )
    .agg(
        # USD VWAP & standard metrics (canonical, backwards compatible)
        (spark_sum(col("price") * col("volume")) / spark_sum(col("volume"))).alias("vwap"),
        avg(col("price")).alias("avg_price"),
        spark_min(col("price")).alias("min_price"),
        spark_max(col("price")).alias("max_price"),
        spark_sum(col("volume")).alias("total_volume"),
        spark_count("*").alias("tick_count"),
        # Safe INR VWAP: Exclude null price_inr from numerator and denominator.
        # Returns explicit NULL if valid INR volume sum is 0 or null.
        (
            when(
                inr_vol_sum.isNotNull() & (inr_vol_sum > 0),
                inr_weighted_sum / inr_vol_sum
            ).otherwise(lit(None).cast(DoubleType()))
        ).alias("vwap_inr"),
        # Standard SQL averages/min/max ignore nulls and return null if all values in window are null
        avg(col("price_inr")).alias("avg_price_inr"),
        spark_min(col("price_inr")).alias("min_price_inr"),
        spark_max(col("price_inr")).alias("max_price_inr"),
    )
    .select(
        col("symbol").cast(StringType()),
        col("window.start").cast(TimestampType()).alias("window_start"),
        col("window.end").cast(TimestampType()).alias("window_end"),
        col("vwap").cast(DoubleType()),
        col("avg_price").cast(DoubleType()),
        col("min_price").cast(DoubleType()),
        col("max_price").cast(DoubleType()),
        coalesce(col("total_volume").cast(DoubleType()), lit(0.0)).alias("total_volume"),
        col("tick_count").cast(IntegerType()),
        col("vwap_inr").cast(DoubleType()),
        col("avg_price_inr").cast(DoubleType()),
        col("min_price_inr").cast(DoubleType()),
        col("max_price_inr").cast(DoubleType()),
    )
)


def write_agg_to_clickhouse(batch_df, batch_id):
    """
    Write windowed aggregations to ClickHouse directly without redundant count() passes.
    ClickHouse ReplacingMergeTree deduplicates on (symbol, window_start).
    """
    try:
        if batch_df.isEmpty():
            return
        (
            batch_df.select(
                col("symbol").cast(StringType()),
                col("window_start").cast(TimestampType()),
                col("window_end").cast(TimestampType()),
                col("vwap").cast(DoubleType()),
                col("avg_price").cast(DoubleType()),
                col("min_price").cast(DoubleType()),
                col("max_price").cast(DoubleType()),
                coalesce(col("total_volume").cast(DoubleType()), lit(0.0)).alias("total_volume"),
                col("tick_count").cast(IntegerType()),
                col("vwap_inr").cast(DoubleType()),
                col("avg_price_inr").cast(DoubleType()),
                col("min_price_inr").cast(DoubleType()),
                col("max_price_inr").cast(DoubleType()),
            )
            .write
            .jdbc(
                url=CLICKHOUSE_JDBC_URL,
                table=CLICKHOUSE_AGG_TABLE,
                mode="append",
                properties=AGG_PROPERTIES,
            )
        )
    except Exception as e:
        print(f"[Agg Batch {batch_id}] Warning/Error writing aggregations: {e}")


agg_query = (
    df_windowed.writeStream
    .foreachBatch(write_agg_to_clickhouse)
    .outputMode("update")
    .trigger(processingTime=AGG_BATCH_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_DIR_AGG)
    .queryName("windowed_agg_to_clickhouse")
    .start()
)

# =============================================================================
# KEEP RUNNING (BOTH STREAMS)
# =============================================================================

print("\nStreaming consumer is running with 2 streams:")
print(f"  1. Raw ticks   -> {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE} (every {RAW_BATCH_INTERVAL})")
print(f"  2. 1-min aggs  -> {CLICKHOUSE_DATABASE}.{CLICKHOUSE_AGG_TABLE} (every {AGG_BATCH_INTERVAL})")
print("\nPress Ctrl+C to stop.\n")
print("To query aggregations in ClickHouse:")
print("  SELECT * FROM stocks.ticks_1m_agg FINAL ORDER BY window_start DESC LIMIT 10;")
print()

# awaitAnyTermination() blocks until any of the streaming queries terminates.
# If one fails, this will check for checkpoint corruption, clean if necessary, and exit cleanly.
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    err_msg = str(e)
    if ("FileNotFoundException" in err_msg or "AssertionError" in err_msg or "partitions" in err_msg) and ("checkpoint" in err_msg or ".delta" in err_msg or "state" in err_msg or "partition" in err_msg):
        import shutil
        print(f"\n[Spark Alert] Detected corrupt/incompatible checkpoint state: {err_msg}")
        print("[Spark Alert] Auto-purging corrupted checkpoint directories for recovery on next run...")
        shutil.rmtree(CHECKPOINT_DIR_RAW, ignore_errors=True)
        shutil.rmtree(CHECKPOINT_DIR_AGG, ignore_errors=True)
        print("[Spark Alert] Checkpoints purged. Please restart the consumer.")
    raise

