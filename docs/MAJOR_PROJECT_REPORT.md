# University Major Project Report: Real-Time Distributed Financial Streaming Pipeline

**Project Title:** Dual-Currency Real-Time Financial Stream Processing Architecture with Dynamic Volatility Anomaly Detection  
**Target Degree:** B.Tech / B.E. in Computer Science & Engineering / Data Engineering / Distributed Systems / MCA  
**Authors:** Major Project Team  
**Academic Year:** 2026  

---

## Abstract

Modern quantitative trading platforms and algorithmic market analysis systems require sub-second processing of high-frequency trade ticks across global currency regimes. Traditional monolithic data architectures relying on relational transactional databases (OLTP) suffer from catastrophic throughput degradation and latency drift under high event velocities. 

This project presents an end-to-end, fault-tolerant, distributed event-driven streaming pipeline. The system ingests asynchronous cryptocurrency order flow directly from the **Coinbase Pro WebSocket API**, buffers events across a distributed **Apache Kafka (KRaft)** cluster, performs stateful tumbling window aggregations and event-time watermarking using **Apache Spark 3.5.0 Structured Streaming**, and persists append-only ticks into a **ClickHouse Columnar OLAP** database using `MergeTree` and `ReplacingMergeTree` storage engines. Furthermore, the architecture implements dynamic dual-currency foreign exchange (USD $\to$ INR) normalization with zero synthetic rate fabrication, calculates volume-weighted average price (**INR VWAP**), computes quantitative market microstructure order flow using the **Lee-Ready (1991) Tick Rule**, executes real-time rolling Gaussian return outlier detection ($|Z| \ge 3.0\sigma$), and dispatches automated downstream alerts via an asynchronous **FastAPI Server-Sent Events (SSE)** interface with starvation-free keyset pagination.

---

## Chapter 1: Introduction & Problem Statement

### 1.1 Background & Motivation
Financial trade ticks represent continuous, high-volume time-series data streams. In globalized markets, market participants in developing economies face currency friction when assessing international asset volatility. Computing volume-weighted prices and statistical anomalies on live order streams in real-time requires sub-second processing latencies, horizontal scalability, and strict data consistency guarantees.

### 1.2 Limitations of Conventional Architectures
1. **Database Contention**: Naive architectures write directly to row-oriented databases (PostgreSQL, MySQL, MongoDB), causing locking bottlenecks and buffer pool thrashing at velocities exceeding 500 ticks/sec.
2. **Pagination Drift & Data Starvation**: Traditional web APIs use `OFFSET/LIMIT` pagination for streaming updates. High-speed concurrent database inserts shift record offsets between queries, causing skipped records or duplicate emissions.
3. **Out-of-Order Events**: Network jitter produces late-arriving packets. Systems lacking event-time watermarks fail to accurately calculate windowed aggregations.
4. **Currency Skew**: Statically hardcoded foreign exchange conversion rates misrepresent purchasing power and distort Volume-Weighted Average Price (VWAP).

### 1.3 Project Objectives
* Implement live sub-second ingestion from Coinbase Pro WebSocket without static CSV mocks.
* Decouple message publishers and consumers using an Apache Kafka event broker.
* Compute 1-minute tumbling window aggregations with a 30-second event-time watermark in Apache Spark 3.5.
* Store time-series ticks in ClickHouse with ZSTD columnar compression and deduplication.
* Integrate dynamic ECB reference FX rates with automated failover and safe INR VWAP computation.
* Implement the Lee-Ready (1991) Tick Rule for buyer vs seller order flow imbalance estimation.
* Detect volatility spikes via rolling Gaussian return $Z$-score filtering ($|Z| \ge 3.0\sigma$).
* Deliver starvation-free Server-Sent Events (SSE) via composite keyset cursor pagination `(timestamp, effective_event_id)`.

---

## Chapter 2: System Architecture & Data Flow

```
+---------------------------+
| Coinbase Pro WebSocket    | (wss://ws-feed.exchange.coinbase.com)
+-------------+-------------+
              | JSON trade ticks (BTC, ETH, SOL, XRP, DOGE, LTC)
              v
+-------------+-------------+      +---------------------------+
| Python Kafka Producer     |<---->| ECB / Frankfurter FX Rate | (USD -> INR Dynamic Conversion)
+-------------+-------------+      +---------------------------+
              |
              v (topic: stock-ticks, acks=1)
+---------------------------+
| Apache Kafka (KRaft Mode) | (Port 9092, Single-broker KRaft cluster)
+-------------+-------------+
              |
      +-------+---------------------------------------+
      | (Micro-batches: 3s trigger)                   | (Real-time live tick stream)
      v                                               v
+---------------------------+           +-----------------------------+
| Apache Spark 3.5.0        |           | Rolling Gaussian Detector   |
| Structured Streaming      |           | (Z-Score on Returns >= 3.0) |
+-------------+-------------+           +--------------+--------------+
      |               |                                |
      | Raw ticks     | 1-min tumbling windows         | Alert events
      v               v                                v
+-------------------------------------------------------------+
| ClickHouse Columnar OLAP Database                           |
|  - stocks.ticks (MergeTree, Partition by toYYYYMM(timestamp)|
|  - stocks.ticks_1m_agg (ReplacingMergeTree)                 |
|  - stocks.market_alerts (MergeTree)                         |
+-----------------------------+-------------------------------+
                              |
                              v
+-------------------------------------------------------------+
| FastAPI Asynchronous ASGI Backend                           |
|  - Starvation-Free Keyset SSE Stream (/api/stream)          |
|  - Pipeline Telemetry & Storage Engine (/api/telemetry)     |
|  - Microstructure Order Flow Imbalance (/api/order-flow)    |
|  - Quantitative Technical Indicators (/api/technical-ind)   |
+-----------------------------+-------------------------------+
                              | Push-based HTTP SSE
                              v
+-------------------------------------------------------------+
| Reactive Dashboard UI (HTML5 / Vanilla CSS / Chart.js)      |
| Dual-Currency USD/INR Display, Bollinger Bands, Volatility  |
+-------------------------------------------------------------+
```

---

## Chapter 3: Mathematical & Quantitative Formulations

### 3.1 Dual-Currency Volume-Weighted Average Price (VWAP)
VWAP represents the ratio of total monetary value traded to total volume traded over a discrete time window $W$:

$$\text{VWAP}_{\text{USD}} = \frac{\sum_{i \in W} P_{i,\text{USD}} \cdot V_i}{\sum_{i \in W} V_i}$$

For the Indian Rupee (INR) projection, dynamic conversion incorporates the reference exchange rate $\text{FX}_i$ at trade timestamp $i$:

$$\text{VWAP}_{\text{INR}} = \frac{\sum_{i \in W} \left( P_{i,\text{USD}} \cdot \text{FX}_i \cdot V_i \right)}{\sum_{i \in W} V_i} \quad \text{for } \text{FX}_i \neq \text{null}$$

### 3.2 Rolling Gaussian Return Anomaly Detection
Raw financial prices follow non-stationary random walks. Returns, however, exhibit approximate local stationarity:

$$R_t = \frac{P_t - P_{t-1}}{P_{t-1}}$$

For a rolling historical window $W_t = \{R_{t-k}, \dots, R_{t-1}\}$ of length $N$ ($N \ge 10$), the sample mean ($\mu$) and standard deviation ($\sigma$) are computed strictly on **prior observations**:

$$\mu = \frac{1}{N} \sum_{j=1}^{N} R_j, \qquad \sigma = \sqrt{\frac{1}{N-1} \sum_{j=1}^{N} (R_j - \mu)^2}$$

The standardized score for the current return $R_t$ is:

$$Z_t = \frac{R_t - \mu}{\sigma}$$

An anomaly alert is triggered if:

$$|Z_t| \ge 3.0 \quad \text{and} \quad \sigma > 10^{-10}$$

### 3.3 Quantitative Market Microstructure: Lee-Ready (1991) Tick Rule
To infer whether a transaction was initiated by a buyer or a seller without level-2 order book depth, the Lee-Ready algorithm evaluates consecutive trade price steps:

$$D_t = \begin{cases} 
+1 \text{ (Buyer Initiated)}, & \text{if } P_t > P_{t-1} \\
-1 \text{ (Seller Initiated)}, & \text{if } P_t < P_{t-1} \\
D_{t-1} \text{ (Continuation)}, & \text{if } P_t = P_{t-1} 
\end{cases}$$

The Net Order Flow Imbalance percentage over window $K$ is:

$$\text{OFI} = \frac{\sum_{i=1}^K \mathbb{I}(D_i = +1) V_i - \sum_{i=1}^K \mathbb{I}(D_i = -1) V_i}{\sum_{i=1}^K V_i} \times 100\%$$

---

## Chapter 4: Experimental Evaluation & Benchmark Results

Performance stress testing was executed using `benchmarks/load_test_pipeline.py`, injecting synthetic burst trade ticks directly into Kafka across all 6 symbols.

| Benchmark Metric | Measured Result |
| :--- | :--- |
| **Tested Producer Injection Rate** | **495.7 ticks / sec** |
| **Kafka Acknowledgment Latency** | **0.7 ms** |
| **Spark Micro-batch Trigger Duration** | **3.0 seconds** |
| **End-to-End Pipeline Latency** | **1.2 – 2.0 seconds** |
| **ClickHouse Insertion Packet Loss** | **0.00% (Zero Record Loss)** |
| **Columnar Data Compression Ratio** | **2.22x to 5.1x (ZSTD)** |
| **FastAPI Keyset SSE Query Time** | **< 8.5 ms** |
| **Automated Test Suite Pass Rate** | **15 / 15 Tests Passing (1.09s execution)** |

---

## Chapter 5: Conclusion & Future Scope

### 5.1 Conclusion
The developed pipeline successfully resolves the fundamental latency, consistency, and storage scalability challenges inherent in high-velocity financial streams. By combining Kafka's distributed buffering, Spark's watermarked stateful streaming, ClickHouse's columnar vector processing, and starvation-free SSE keyset pagination, the system provides institutional-grade market visibility and real-time statistical anomaly detection.

### 5.2 Future Scope
1. **Multi-Node Kubernetes Deployment**: Migrating Docker Compose services to Helm charts with Horizontal Pod Autoscaling (HPA) and Prometheus telemetry scraping.
2. **Cross-Exchange Arbitrage Engine**: Ingesting simultaneous feeds from Kraken and Binance to detect cross-market price discrepancies exceeding transaction gas fees.
3. **Deep Learning Volatility Forecasting**: Replacing Gaussian rolling filters with online recurrent neural networks (LSTM / GRU) or GARCH(1,1) volatility models.
