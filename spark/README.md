# Spark Structured Streaming & ClickHouse Consumer (Member 3)

This module handles **real-time ingestion, schema normalization, watermarked windowed aggregation, and ClickHouse persistence** for the Crypto Analytics pipeline.

---

## Architecture

```text
Coinbase WebSocket
       │
       ▼
Python Producer (run_producer.py)
       │
       ▼
Kafka (topic: stock-ticks)
       │
  ┌────┴─────────────────────────────┐
  │   Spark Structured Streaming     │
  │   (spark/spark_consumer.py)      │
  └────┬────────────────────────┬────┘
       │                        │
       ▼                        ▼
Query 1: Raw Ticks        Query 2: 1-Minute Aggregations (Watermark: 1 min)
(ForeachBatch Sink)       (ForeachBatch Sink)
       │                        │
       ▼                        ▼
stocks.ticks              stocks.ticks_1m_agg
(ClickHouse MergeTree)    (ClickHouse ReplacingMergeTree)
                                ▲
                                │ (Member 4 Alerts)
                          stocks.market_alerts
```

---

## Component Files

| File | Purpose |
| :--- | :--- |
| [`schema.py`](file:///C:/Users/SOORYA/.gemini/antigravity-ide/scratch/Crypto-Analytics-/spark/schema.py) | Explicit PySpark `StructType` schemas for Kafka JSON decoding and ClickHouse tables. |
| [`transformations.py`](file:///C:/Users/SOORYA/.gemini/antigravity-ide/scratch/Crypto-Analytics-/spark/transformations.py) | Data cleaning, ISO-8601 timestamp parsing, watermarking, and 1-minute windowed aggregation. |
| [`clickhouse_writer.py`](file:///C:/Users/SOORYA/.gemini/antigravity-ide/scratch/Crypto-Analytics-/spark/clickhouse_writer.py) | Resilient `foreachBatch` micro-batch writer with exponential backoff and retry logic. |
| [`spark_consumer.py`](file:///C:/Users/SOORYA/.gemini/antigravity-ide/scratch/Crypto-Analytics-/spark/spark_consumer.py) | Main streaming driver managing dual streaming queries and persistent checkpoints. |
| [`Dockerfile.spark`](file:///C:/Users/SOORYA/.gemini/antigravity-ide/scratch/Crypto-Analytics-/spark/Dockerfile.spark) | Container definition bundling Java 17, PySpark 3.5.1, and `clickhouse-connect`. |

---

## Checkpointing and Recovery Semantics

### Dedicated Checkpoints
Each query maintains an isolated checkpoint directory:
* Raw ticks: `checkpoints/raw_ticks/`
* 1-minute aggregates: `checkpoints/agg_1m/`

### Delivery Semantics & Restart Behavior
Spark Structured Streaming with `foreachBatch` provides **At-Least-Once** delivery semantics:
* **Kafka Offsets**: Spark tracks offset progress in the query's `offset/` checkpoint directory.
* **Failure Recovery**: If a worker or driver restarts before the offset commit completes, Spark re-reads the uncommitted offsets and re-executes `foreachBatch`.
* **ClickHouse Idempotence & Deduplicated Views**:
  * In `stocks.ticks_1m_agg`, the table is configured with `ENGINE = ReplacingMergeTree(calculated_at)`.
  * **Important for Downstream Consumers (Member 4 & FastAPI)**: Background merges in ClickHouse are asynchronous. To guarantee that queries never read unmerged duplicate window states, downstream services should query the deduplicated view:
    ```sql
    SELECT * FROM stocks.v_ticks_1m_agg WHERE symbol = 'BTC-USD';
    ```
  * In `stocks.ticks`, `MergeTree` preserves all valid ticks. To query raw ticks filtered of any Kafka replay duplicates, query:
    ```sql
    SELECT * FROM stocks.v_ticks_dedup WHERE symbol = 'BTC-USD';
    ```

---

## Configuration Reference

| Environment Variable | CLI Argument | Default | Description |
| :--- | :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `--kafka-bootstrap` | `localhost:9092` | Kafka address (`kafka:29092` in Docker) |
| `KAFKA_TOPIC` | `--kafka-topic` | `stock-ticks` | Kafka topic to consume |
| `CLICKHOUSE_HOST` | `--clickhouse-host` | `localhost` | ClickHouse address (`clickhouse` in Docker) |
| `CLICKHOUSE_PORT` | `--clickhouse-port` | `8123` | ClickHouse HTTP port |
| `CHECKPOINT_BASE_DIR` | `--checkpoint-base` | `./checkpoints` | Root directory for query checkpoints |
| `WATERMARK_DURATION` | `--watermark` | `1 minute` | Event-time watermark delay |
| `TRIGGER_INTERVAL` | `--trigger-interval` | `5 seconds` | Micro-batch trigger interval |

---

## How to Run

### Option A: Using Docker Compose (Recommended)

Start Kafka, ClickHouse, and Spark Consumer together:
```powershell
docker compose up -d
```

### Option B: Running Spark on Host Machine

1. Ensure Kafka and ClickHouse are running:
   ```powershell
   docker compose up -d kafka clickhouse
   ```
2. Initialize ClickHouse tables (if not already done):
   ```powershell
   python clickhouse/manage_db.py
   ```
3. Run the Spark consumer:
   ```powershell
   python spark/spark_consumer.py
   ```
4. In a separate terminal, start the Python producer:
   ```powershell
   python run_producer.py
   ```

---

## Verification & Queries

Verify stored raw ticks in ClickHouse:
```powershell
curl -s "http://localhost:8123/?query=SELECT+symbol,price,volume,timestamp+FROM+stocks.ticks+ORDER+BY+timestamp+DESC+LIMIT+10+FORMAT+Pretty"
```

Verify 1-minute windowed aggregates:
```powershell
curl -s "http://localhost:8123/?query=SELECT+symbol,window_start,window_end,event_count,avg_price,total_volume+FROM+stocks.ticks_1m_agg+ORDER+BY+window_start+DESC+LIMIT+5+FORMAT+Pretty"
```
