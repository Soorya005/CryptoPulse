"""
clickhouse_writer.py — Resilient micro-batch writer for ClickHouse integration.

Writes micro-batches from Spark Structured Streaming (via foreachBatch) into
ClickHouse tables:
  1. stocks.ticks (Raw tick events)
  2. stocks.ticks_1m_agg (1-minute windowed aggregates)

Delivery Semantics:
-------------------
Spark Structured Streaming with foreachBatch guarantees AT-LEAST-ONCE delivery
semantics when coupled with persistent checkpointing.
If a worker or the driver restarts midway after writing to ClickHouse but before
Spark commits the batch offset to the checkpoint directory, the same micro-batch
will be re-evaluated and rewritten.
- For `stocks.ticks_1m_agg`, the ClickHouse table uses `ReplacingMergeTree(calculated_at)`
  keyed on `(symbol, window_start)`, meaning duplicate writes for the same window
  are safely resolved and overwritten.
- For `stocks.ticks`, `MergeTree` preserves all high-frequency tick records; duplicate
  writes during crash-restart recovery can be filtered downstream using event_id,
  timestamp, or partition offsets.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

logger = logging.getLogger("clickhouse_writer")

# Environment configuration with safe defaults
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "stocks")
MAX_RETRIES = int(os.getenv("CLICKHOUSE_MAX_RETRIES", "3"))
RETRY_BACKOFF_S = float(os.getenv("CLICKHOUSE_RETRY_BACKOFF_S", "1.5"))

# Cache client instance across batches to prevent per-batch connection setup overhead
_client = None


def get_clickhouse_client():
    """
    Return a singleton clickhouse_connect client connected to ClickHouse HTTP interface.
    Re-creates the client if connection is closed or not yet initialized.
    """
    global _client
    if _client is not None:
        try:
            # Quick ping to verify active connection
            if _client.ping():
                return _client
        except Exception:
            _client = None

    import clickhouse_connect

    logger.info(
        "Connecting to ClickHouse at %s:%d, database: %s",
        CLICKHOUSE_HOST,
        CLICKHOUSE_PORT,
        CLICKHOUSE_DATABASE,
    )
    _client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        connect_timeout=10,
        send_receive_timeout=30,
    )
    return _client


def _execute_with_retry(operation, *args, **kwargs) -> Any:
    """Execute a database write operation with exponential back-off on transient failures."""
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return operation(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "ClickHouse write attempt %d/%d failed: %s. Retrying in %.2fs...",
                attempt,
                MAX_RETRIES,
                exc,
                RETRY_BACKOFF_S * attempt,
            )
            time.sleep(RETRY_BACKOFF_S * attempt)
            # Reset client connection on error
            global _client
            _client = None

    logger.error("All %d ClickHouse write attempts failed: %s", MAX_RETRIES, last_exc)
    raise last_exc


def write_raw_ticks_batch(batch_df, batch_id: int) -> None:
    """
    Spark foreachBatch sink for raw trade events.
    Inserts micro-batch rows into stocks.ticks.
    """
    if batch_df.isEmpty():
        logger.debug("[Batch %d] Raw ticks batch is empty — skipping write.", batch_id)
        return

    # Extract required column data
    columns = [
        "symbol",
        "price",
        "volume",
        "timestamp",
        "price_usd",
        "price_inr",
        "exchange_rate",
        "rate_timestamp",
    ]

    # Convert to Python list of tuples/rows for bulk insert
    rows = [
        (
            row["symbol"],
            float(row["price"]),
            float(row["volume"]),
            row["timestamp"],
            float(row["price_usd"]),
            float(row["price_inr"]) if row["price_inr"] is not None else None,
            float(row["exchange_rate"]) if row["exchange_rate"] is not None else None,
            row["rate_timestamp"] if row["rate_timestamp"] is not None else None,
        )
        for row in batch_df.select(*columns).collect()
    ]

    record_count = len(rows)
    logger.info("[Batch %d] Writing %d raw ticks to stocks.ticks...", batch_id, record_count)

    def _insert():
        client = get_clickhouse_client()
        client.insert(
            table="ticks",
            data=rows,
            column_names=columns,
        )

    _execute_with_retry(_insert)
    logger.info("[Batch %d] Successfully committed %d raw ticks to stocks.ticks.", batch_id, record_count)


def write_agg_1m_batch(batch_df, batch_id: int) -> None:
    """
    Spark foreachBatch sink for 1-minute windowed aggregates.
    Inserts micro-batch rows into stocks.ticks_1m_agg.
    """
    if batch_df.isEmpty():
        logger.debug("[Batch %d] 1m Aggregate batch is empty — skipping write.", batch_id)
        return

    columns = [
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

    rows = [
        (
            row["symbol"],
            row["window_start"],
            row["window_end"],
            int(row["event_count"]),
            float(row["avg_price"]),
            float(row["min_price"]),
            float(row["max_price"]),
            float(row["total_volume"]),
            float(row["avg_price_inr"]) if row["avg_price_inr"] is not None else None,
        )
        for row in batch_df.select(*columns).collect()
    ]

    record_count = len(rows)
    logger.info("[Batch %d] Writing %d aggregated windows to stocks.ticks_1m_agg...", batch_id, record_count)

    def _insert():
        client = get_clickhouse_client()
        client.insert(
            table="ticks_1m_agg",
            data=rows,
            column_names=columns,
        )

    _execute_with_retry(_insert)
    logger.info("[Batch %d] Successfully committed %d aggregates to stocks.ticks_1m_agg.", batch_id, record_count)
