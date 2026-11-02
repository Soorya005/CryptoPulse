"""
spark_consumer.py — Main Spark Structured Streaming application.

Ingests crypto tick events from Kafka, performs schema validation and watermarked
1-minute windowed aggregations, and writes both streams into ClickHouse via foreachBatch
with dedicated persistent checkpoint directories.

Run locally:
    python spark/spark_consumer.py

Run with custom configurations:
    python spark/spark_consumer.py --kafka-bootstrap localhost:9092 --clickhouse-host localhost
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from spark.clickhouse_writer import write_agg_1m_batch, write_raw_ticks_batch
from spark.transformations import compute_1m_aggregates, parse_kafka_stream, prepare_raw_ticks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark_consumer")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Crypto Consumer")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092 or $KAFKA_BOOTSTRAP_SERVERS)",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.getenv("KAFKA_TOPIC", "stock-ticks"),
        help="Kafka topic to consume from (default: stock-ticks)",
    )
    parser.add_argument(
        "--clickhouse-host",
        default=os.getenv("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host (default: localhost or $CLICKHOUSE_HOST)",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=os.getenv("CHECKPOINT_BASE_DIR", "./checkpoints"),
        help="Base directory for query checkpoints (default: ./checkpoints)",
    )
    parser.add_argument(
        "--watermark",
        default=os.getenv("WATERMARK_DURATION", "1 minute"),
        help="Watermark delay for late-arriving events (default: '1 minute')",
    )
    parser.add_argument(
        "--trigger-interval",
        default=os.getenv("TRIGGER_INTERVAL", "5 seconds"),
        help="Micro-batch trigger interval (default: '5 seconds')",
    )
    parser.add_argument(
        "--starting-offsets",
        default=os.getenv("STARTING_OFFSETS", "latest"),
        choices=["latest", "earliest"],
        help="Kafka starting offsets (default: latest)",
    )
    return parser.parse_args()


def create_spark_session(app_name: str = "CryptoAnalyticsStreaming"):
    """
    Build a local SparkSession configured with the spark-sql-kafka connector
    and optimized for lightweight local execution on 16 GB laptop.
    """
    from pyspark.sql import SparkSession

    logger.info("Initializing SparkSession '%s'...", app_name)
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")  # 2 local cores, conserving RAM
        .config("spark.sql.shuffle.partitions", "2")  # Low shuffle partitions for low latency
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized successfully.")
    return spark


def run_pipeline(args: argparse.Namespace) -> None:
    """Run the dual-stream Spark Structured Streaming pipeline."""
    # Ensure ClickHouse host environment variable is set for writer
    os.environ["CLICKHOUSE_HOST"] = args.clickhouse_host

    spark = create_spark_session()

    logger.info("Subscribing to Kafka broker '%s', topic '%s'...", args.kafka_bootstrap, args.kafka_topic)

    # 1. Read continuous stream from Kafka
    raw_kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.kafka_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON and validate raw ticks
    parsed_stream = parse_kafka_stream(raw_kafka_stream)
    raw_ticks_stream = prepare_raw_ticks(parsed_stream)

    # 3. Compute 1-minute windowed aggregates
    agg_1m_stream = compute_1m_aggregates(raw_ticks_stream, watermark_duration=args.watermark)

    # Configure dedicated checkpoint paths
    raw_ticks_checkpoint = os.path.join(args.checkpoint_base, "raw_ticks")
    agg_1m_checkpoint = os.path.join(args.checkpoint_base, "agg_1m")
    os.makedirs(raw_ticks_checkpoint, exist_ok=True)
    os.makedirs(agg_1m_checkpoint, exist_ok=True)

    logger.info("Checkpoint paths: Raw Ticks -> %s | 1m Aggregates -> %s", raw_ticks_checkpoint, agg_1m_checkpoint)

    # 4. Query 1: Write Raw Ticks into stocks.ticks
    logger.info("Starting Query 1: Raw Ticks -> ClickHouse stocks.ticks...")
    query_raw_ticks = (
        raw_ticks_stream.writeStream
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", raw_ticks_checkpoint)
        .foreachBatch(write_raw_ticks_batch)
        .start()
    )

    # 5. Query 2: Write 1-Minute Aggregates into stocks.ticks_1m_agg
    logger.info("Starting Query 2: 1-Minute Aggregates -> ClickHouse stocks.ticks_1m_agg...")
    query_agg_1m = (
        agg_1m_stream.writeStream
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", agg_1m_checkpoint)
        .foreachBatch(write_agg_1m_batch)
        .outputMode("update")  # Emit updated window metrics upon arrival
        .start()
    )

    logger.info("Both Spark streaming queries are active and running. Press Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user. Stopping streaming queries gracefully...")
        query_raw_ticks.stop()
        query_agg_1m.stop()
        spark.stop()
        logger.info("Spark streaming consumer stopped.")


def main():
    args = _parse_args()
    try:
        run_pipeline(args)
    except Exception as exc:
        logger.critical("Fatal error in Spark streaming consumer: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
