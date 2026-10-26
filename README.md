# CryptoPulse: Real-Time Financial Tick Streaming & Distributed OLAP Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Kafka-3.7_(KRaft)-black?style=for-the-badge&logo=apachekafka" alt="Kafka" />
  <img src="https://img.shields.io/badge/Apache_Spark-3.5_Streaming-E25A1C?style=for-the-badge&logo=apachespark" alt="Spark" />
  <img src="https://img.shields.io/badge/ClickHouse-Columnar_OLAP-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black" alt="ClickHouse" />
  <img src="https://img.shields.io/badge/FastAPI-Serving_Layer-009688?style=for-the-badge&logo=fastapi" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker" alt="Docker" />
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python" alt="Python" />
</p>

> **CryptoPulse** is an enterprise-grade, distributed stream processing and real-time analytical pipeline designed to ingest, enrich, compute windowed microstructure statistics, and detect statistical market anomalies across continuous, high-frequency cryptocurrency market ticks.

---

## 🏛 Architecture Overview

```text
+-------------------+      +-------------------+      +-------------------------+      +-------------------+      +-------------------------+
|  Coinbase Pro WS  | ---> |  crypto_producer  | ---> |   Apache Kafka (KRaft)  | ---> | PySpark Streaming | ---> |  ClickHouse Columnar    |
| (6 Crypto Assets) |      | (In-Memory TTL FX)|      |   (Topic: stock-ticks)  |      | (30s Watermark)   |      |  (ReplacingMergeTree)   |
+-------------------+      +-------------------+      +-------------------------+      +-------------------+      +-------------------------+
                                     |                             |                                                           |
                                     v                             v                                                           v
                            [ECB / Frankfurter]            [Anomaly Detector]                                            [FastAPI Gateway]
                            (USD/INR Failover)             (|Z| >= 3.0σ CEP)                                             (Sub-15ms Latency)
                                                                   |                                                           |
                                                                   v                                                           v
                                                          [Webhook Dispatcher]                                           [6-View SPA UI]
                                                          (Slack / Discord)                                              (USD / INR Toggle)
```

```mermaid
flowchart TD
    subgraph INGESTION["1. Ingestion & Inline Enrichment"]
        CB["Coinbase Pro WebSocket<br/>(BTC, ETH, SOL, XRP, DOGE, LTC)"] --> CP["crypto_producer.py"]
        ECB["European Central Bank / Frankfurter"] -.->|Primary FX| ERM["ExchangeRateManager<br/>(Monotonic 10m TTL)"]
        OER["Open Exchange Rates"] -.->|Failover FX| ERM
        ERM -->|USD & INR Valuations| CP
    end

    subgraph BROKER["2. Event Backbone"]
        CP -->|Kafka Producer| KAFKA[("Apache Kafka Broker<br/>Topic: stock-ticks (KRaft Mode)")]
    end

    subgraph STREAMING["3. Distributed Stream Processing & CEP"]
        KAFKA --> SPARK["PySpark Structured Streaming Engine"]
        SPARK --> WM["Event-Time Watermark (30 Seconds)"]
        WM -->|Stream 1: Raw Append (3s Trigger)| CH_RAW[("ClickHouse: stocks.ticks")]
        WM -->|Stream 2: 1m Tumbling Window (10s Trigger)| AGG["1-Minute In-Memory Aggregations<br/>VWAP, OHLC, Min, Max, Volume"]
        AGG --> CH_AGG[("ClickHouse: stocks.ticks_1m_agg")]

        KAFKA --> AD["Anomaly Detection Engine<br/>anomaly_detector.py"]
        AD -->|Rolling Z-Score >= 3.0σ| WH["Webhook Dispatcher<br/>(Discord / Slack)"]
        AD --> CH_ALERT[("ClickHouse: stocks.market_alerts")]
    end

    subgraph SERVING["4. Analytical Serving & Modern SPA"]
        CH_RAW -.-> API["FastAPI Asynchronous Backend (Port 8502)"]
        CH_AGG -.-> API
        CH_ALERT -.-> API
        API --> UI["Institutional Single-Page Application (SPA)<br/>• Market Overview (Dashboard)<br/>• Live Market Stream<br/>• Price Analytics<br/>• Recent Trades Blotter<br/>• Anomaly Detection Engine<br/>• Pipeline Observability Cockpit"]
    end
```

---

## ⚡ Key Highlights & Engineering Features

### 1. High-Frequency Real-Time Ingestion
* Ingests real-time ticks from **Coinbase Pro WebSocket** (`wss://ws-feed.exchange.coinbase.com`) across 6 major instruments: `BTC-USD`, `ETH-USD`, `SOL-USD`, `XRP-USD`, `DOGE-USD`, and `LTC-USD`.
* Sustained ingestion velocity: **30 to 500+ messages/second** (up to ~4.3 Million records/day).

### 2. Live Macroeconomic FX Enrichment & In-Memory TTL Caching
* Real-time cross-currency joining with foreign exchange rates without rate-limiting upstream providers.
* Built-in `ExchangeRateManager` with:
  * **In-Memory Monotonic TTL Cache (600s)**: Reuses validated rates across ~30,000 ticks in RAM, slashing external HTTP overhead by **99.9%**.
  * **Strict Positive Validation**: Asserts finite, positive numbers to prevent corrupted records from poisoning downstream tables.
  * **Multi-Tiered Failover**: Primary (Frankfurter / ECB) $\rightarrow$ Secondary (Open Exchange Rates) $\rightarrow$ Stale Cache $\rightarrow$ Graceful degradation (`NULL`).

### 3. Stateful Stream Processing (PySpark Structured Streaming)
* **30-Second Event-Time Watermarking**: Discards late-arriving packets beyond the tolerance threshold, bounding state memory.
* **Dual-Branch Streaming**:
  * **Stream 1 (Raw Ingestion)**: Append-only fast micro-batch write (3s trigger) into `stocks.ticks`.
  * **Stream 2 (1-Minute Aggregations)**: Stateful tumbling window (10s trigger) computing Volume-Weighted Average Price (**VWAP**), OHLC prices, and spread into `stocks.ticks_1m_agg`.
* **Fault-Tolerant Checkpointing**: State stores and Kafka offsets preserved across worker reboots.

### 4. Complex Event Processing (CEP) & Rolling Z-Score Anomaly Engine
* Statistical detection engine evaluating rolling standard deviations over sliding 60-tick micro-windows:
  $$Z = \frac{P_t - \mu_{window}}{\sigma_{window}}$$
* Detects flash liquidations, volatility surges, and order imbalances when $|Z| \ge 3.0\sigma$ (99.73% confidence interval).
* Real-time alert dispatching via automated webhooks (Discord / Slack / Telegram) in $<200\text{ms}$.

### 5. High-Performance Columnar OLAP (ClickHouse)
* Stored using the `ReplacingMergeTree` engine for automatic background deduplication of idempotent retries.
* Columnar compression (**DoubleDelta** for timestamps, **Gorilla/ZSTD** for prices) yields a **15x reduction** in storage space (~150 MB/day for 4.3M rows).
* Vectorized SIMD query execution computes aggregations over millions of rows in **$< 15\text{ms}$**.

### 6. Institutional Single-Page Application (SPA) Dashboard
* High-performance Vanilla ES6+ and modern CSS custom properties with zero external UI framework bloat.
* **6 Dedicated Workspaces**:
  1. **Market Overview**: Executive cards, interactive Chart.js price history, 1-minute microstructure metrics, and preview cards.
  2. **Live Market Stream**: High-frequency streaming canvas with asset switcher and 24h stats.
  3. **Price Analytics**: Multi-asset comparison matrix benchmarking BTC, ETH, SOL, XRP, DOGE, and LTC.
  4. **Recent Trades Blotter**: Full-page execution blotter with symbol filtering and dual-currency notional values.
  5. **Anomaly Detection Engine**: Real-time statistical audit trail with interactive inspection triggers.
  6. **Pipeline Health & Telemetry**: Observability cockpit monitoring Kafka throughput, Spark latency, ClickHouse write rates, and API response times.
* Persistent **USD / INR currency mode** and **Light / Dark theme** toggling.

---

## 🚀 Quick Start (Docker - Recommended)

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) with WSL2 (Windows) or native Docker Engine (Linux/macOS).

### Launch the Production Pipeline
```bash
# 1. Clone repository
git clone https://github.com/Soorya005/CryptoPulse.git
cd CryptoPulse

# 2. Run Spark Streaming with 1-Minute Windowed Aggregations (Default)
./run.sh spark_streaming

# OR run Spark Micro-Batch (3-second batches)
./run.sh spark_microbatch

# OR run Apache Flink (True streaming record-by-record)
./run.sh flink
```

Open your browser to:
👉 **`http://localhost:8502`**

Press `Ctrl+C` in your terminal to cleanly stop all containers.

---

## 🛠 Local Development Setup (Outside Docker)

If you wish to run individual Python scripts locally against Kafka and ClickHouse containers:

```bash
# 1. Start core infrastructure services in background
docker compose up -d kafka clickhouse

# 2. Set up virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Terminal 1: Run Coinbase Ingestion Producer
python src/production/producer/crypto_producer.py

# 4. Terminal 2: Run Spark Structured Streaming Consumer
python src/production/consumer/spark_streaming_clickhouse_consumer.py

# 5. Terminal 3: Run Real-Time Anomaly Detector
python src/production/consumer/anomaly_detector.py

# 6. Terminal 4: Launch Web Dashboard
python src/production/dashboard/web_app.py --crypto
```

---

## 📂 Repository Structure

```text
CryptoPulse/
├── docker-compose.yml              # Multi-container production orchestration
├── run.sh                          # Automated one-click execution script
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Project metadata & linter configuration
├── benchmarks/
│   ├── load_test_pipeline.py       # High-throughput mock generator
│   └── BENCHMARK_RESULTS.md        # Latency & throughput test findings
├── docs/
│   ├── PROJECT_TECHNICAL_DOCUMENTATION.md # Master technical spec & interview defense
│   ├── ARCHITECTURE_AND_VIVA_PREP.md      # Viva & oral presentation prep
│   ├── MAJOR_PROJECT_REPORT.md            # Academic project report
│   ├── KAFKA_README.md                    # Kafka KRaft architecture guide
│   ├── PYSPARK_README.md                  # Spark Structured Streaming guide
│   └── CLICKHOUSE_README.md               # Columnar OLAP engine deep dive
├── src/
│   └── production/
│       ├── producer/
│       │   ├── Dockerfile.crypto-producer  # Container recipe for producer
│       │   └── crypto_producer.py          # WebSocket ingestion & FX enrichment
│       ├── consumer/
│       │   ├── Dockerfile.spark-consumer   # Spark streaming container recipe
│       │   ├── Dockerfile.anomaly-detector # Anomaly engine container recipe
│       │   ├── Dockerfile.flink-consumer   # Flink streaming container recipe
│       │   ├── spark_streaming_clickhouse_consumer.py  # Windowed aggregations
│       │   ├── spark_microbatch_clickhouse_consumer.py # Micro-batch append
│       │   ├── anomaly_detector.py         # Rolling Z-score CEP engine
│       │   ├── webhook_dispatcher.py       # Webhook alerting service
│       │   └── flink_clickhouse_consumer.py# Flink true streaming
│       └── dashboard/
│           ├── Dockerfile.web-dashboard    # FastAPI container recipe
│           ├── web_app.py                  # Asynchronous REST & WebSocket backend
│           └── static/
│               ├── crypto_index.html       # Institutional 6-view SPA
│               ├── css/dashboard.css       # Complete modern CSS design system
│               └── js/dashboard.js         # Chart.js, SPA view router, live polling
└── tests/
    ├── test_currency_conversion.py         # Unit tests for FX caching & failover
    └── test_dashboard_api.py               # API & ClickHouse integration tests
```

---

## 📖 Deep-Dive Documentation

| Resource | Scope |
| :--- | :--- |
| **[Master Technical Documentation](docs/PROJECT_TECHNICAL_DOCUMENTATION.md)** | End-to-end architecture, mathematical formulas, resume bullet points, and Top 7 Viva/Interview Q&As. |
| **[Viva Voce & Architecture Prep](docs/ARCHITECTURE_AND_VIVA_PREP.md)** | Comprehensive oral exam answers and engineering defense. |
| **[Major Project Report](docs/MAJOR_PROJECT_REPORT.md)** | Full academic report with literature survey, problem definition, and design specs. |
| **[Kafka Guide](docs/KAFKA_README.md)** | Partitions, consumer offset management, and KRaft consensus. |
| **[Spark Streaming Guide](docs/SPARK_STREAMING_README.md)** | Watermarking, tumbling windows, checkpointing, and VWAP computation. |
| **[ClickHouse Guide](docs/CLICKHOUSE_README.md)** | `ReplacingMergeTree`, DoubleDelta/Gorilla compression, and SIMD SQL execution. |

---

## 🧪 Running Unit & Integration Tests

```bash
# Run currency conversion & failover validation tests
pytest tests/test_currency_conversion.py -v

# Run FastAPI dashboard endpoint & mock tests
pytest tests/test_dashboard_api.py -v
```

---

## 🛡 License
This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
