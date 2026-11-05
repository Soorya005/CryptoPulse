# CryptoPulse — Master Codebase Implementation & File Directory
### Official Team Reference Guide for Project Report, Presentation & Viva
> **Share this document with all teammates:** Soorya AP, Sreerag S Menon, Vedha VK, Vyshnav Pradeep  
> **Repository Root:** `C:\Users\SOORYA\Documents\local_streaming_pipeline`  
> **Last Verified:** November 2026 (Production Main Branch)

---

## 1. System Architecture Map

```
C:\Users\SOORYA\Documents\local_streaming_pipeline\
├── src/production/producer/crypto_producer.py          ◄── Ingestion & 4-Tier INR Conversion
├── src/production/consumer/
│   ├── spark_streaming_clickhouse_consumer.py         ◄── Spark Tumbling Windows, 30s Watermark & VWAP
│   ├── flink_clickhouse_consumer.py                   ◄── Alternative Flink Engine (<10ms Latency)
│   └── webhook_dispatcher.py                          ◄── Discord / Telegram / Slack Webhooks
├── src/production/dashboard/
│   ├── web_app.py                                     ◄── FastAPI REST, WebSocket (/ws) & 24h Stats Merge
│   ├── multi_exchange_service.py                      ◄── Kraken/CoinDCX, 0.35% Fee Hurdle & India Premium
│   ├── cursor_tracker.py                              ◄── Keyset SSE Pagination (Composite Cursor)
│   └── static/
│       ├── crypto_index.html                          ◄── 6 Institutional SPA Views & Webhook Modal
│       ├── js/dashboard.js                            ◄── Chart.js, EMA(9), Bollinger(20, 2σ), Lee-Ready OFI
│       └── css/dashboard.css                          ◄── Bloomberg-style Dark/Light Design Tokens
├── detector/                                          ◄── Modular Anomaly Detection Package
│   ├── base.py                                        ◄── BaseDetector Abstract Class & Alert Dataclass
│   ├── zscore_detector.py                             ◄── Z-Score (|Z| ≥ 3.0σ) on Return Stationarity
│   ├── threshold_detector.py                          ◄── Fixed Jumps (±5%) & Volume Surges (≥2×)
│   ├── runner.py                                      ◄── Polling Orchestrator (every 30s)
│   ├── evaluation.py                                  ◄── Precision, Recall, F1 & Detection Delay (TTD)
│   ├── inject_anomaly.py                              ◄── Synthetic Injection Testing Tool
│   └── replay_injector.py                             ◄── Historical Replay Testing Engine
├── clickhouse/init_db.sql                             ◄── MergeTree, ReplacingMergeTree & FINAL Views
├── benchmarks/
│   ├── load_test_pipeline.py                          ◄── 10,000 ticks/sec Stress Testing Script
│   └── BENCHMARK_RESULTS.md                           ◄── Measured Results (993 ticks/s, 0.2ms flush)
└── docker-compose.yml                                 ◄── Kafka KRaft, Spark, ClickHouse, Dashboard
```

---

## 2. Ingestion & Real-Time INR Localization

### File: `src/production/producer/crypto_producer.py`
* **`ExchangeRateManager` (Lines 35–110)**:  
  * **USD &rarr; INR Real-Time Conversion Engine**. Implements the **4-tier failover protocol**:
    * *Tier 1*: European Central Bank reference rate via Frankfurter API.
    * *Tier 2*: Open Exchange Rates API fallback.
    * *Tier 3*: 10-minute in-memory monotonic TTL cache (`time.monotonic()`, immune to NTP clock jumps).
    * *Tier 4*: `NULL` (never fabricates fictitious rates).
* **`CryptoProducer.on_message` (Lines 130–190)**:  
  * Connects to Coinbase WebSocket (`wss://ws-feed.exchange.coinbase.com`) on the `ticker` channel.
  * Streams 6 assets: `BTC`, `ETH`, `SOL`, `XRP`, `DOGE`, and `LTC`.
  * Computes `price_inr = price_usd * exchange_rate` in-flight with zero latency.
  * Emits structured JSON events to Kafka topic `stock-ticks` partitioned strictly by `symbol`.

---

## 3. Stream Processing, Watermarks & Dual-Currency VWAP

### File: `src/production/consumer/spark_streaming_clickhouse_consumer.py`
* **30-Second Event-Time Watermark (Line 45)**:  
  ```python
  WATERMARK_DELAY = "30 seconds"
  df.withWatermark("timestamp", WATERMARK_DELAY)
  ```
  * Tolerates network jitter and out-of-order WebSocket arrivals up to 30 seconds late.
  * Once the 30-second window expires, the 1-minute window state is finalized and purged from RAM to prevent Out-Of-Memory crashes.
* **Dual-Stream Execution (Lines 110–145)**:  
  * **Stream 1 (Raw Append)**: 3-second micro-batches appending raw trades to `stocks.ticks`.
  * **Stream 2 (1-Minute Aggregations)**: 10-second micro-batches executing `groupBy(window(col("timestamp"), "1 minute"), col("symbol"))`.
  * Calculates OHLC, total trade counts, cumulative volume, and **dual-currency VWAP**:
    $$\text{VWAP}_{\text{USD}} = \frac{\sum (\text{Price} \times \text{Volume})}{\sum \text{Volume}}, \qquad \text{VWAP}_{\text{INR}} = \text{VWAP}_{\text{USD}} \times \text{Exchange Rate}$$
* **Checkpoint Fault Tolerance (Lines 160–190)**:  
  * Persists state to `/checkpoints/raw_ticks` and `/checkpoints/windowed_agg`.
  * Guarantees at-least-once delivery; resumes seamlessly from the last committed Kafka offset upon container restarts.

### File: `src/production/consumer/flink_clickhouse_consumer.py`
* **Apache Flink Low-Latency Alternative**:  
  Record-by-record continuous stream processor operating at **< 10 ms latency** for ultra-fast execution environments.

---

## 4. ClickHouse OLAP Database & Views

### File: `clickhouse/init_db.sql`
* **`stocks.ticks` (Lines 10–35)**:  
  * Raw immutable trade audit log using `MergeTree()` ordered by `(symbol, timestamp, event_id)`.
* **`stocks.ticks_1m_agg` (Lines 40–70)**:  
  * 1-minute aggregations using **`ReplacingMergeTree(calculated_at)`**.
  * Automatically deduplicates re-computed windows on `(symbol, window_start)` during background compactions.
* **`stocks.market_alerts` (Lines 75–95)**:  
  * Stores all fired anomaly alerts with timestamps, severity levels, and metric readings.
* **Deduplicating Analytical Views (Lines 100–120)**:  
  * `stocks.v_ticks_1m_agg`: Queries `ticks_1m_agg FINAL` to guarantee zero duplicate 1-minute bars during dashboard reads.
  * `stocks.v_ticks_dedup`: Deduplicates raw ticks caused by Kafka offset replays.
* **Storage Compression**:  
  * Columnar layout with ZSTD codec achieves **2.13× to 3.5× storage compression**.

---

## 5. Anomaly Detection Engine (Dual-Detector Framework)

### File: `detector/zscore_detector.py`
* **Lines 40–95 (`ZScoreDetector.detect`)**:  
  * **Return Stationarity**: Operates on percentage price returns ($R_t = \frac{P_t - P_{t-1}}{P_{t-1}} \times 100$) across the last $N = 20$ windows rather than non-stationary raw prices.
  * Calculates rolling mean ($\mu$) and sample standard deviation ($\sigma$):
    $$\mu = \frac{1}{N}\sum R_i, \qquad \sigma = \sqrt{\frac{1}{N-1}\sum (R_i - \mu)^2}$$
  * Computes $Z = \frac{R_t - \mu}{\sigma}$.
  * **Trigger Boundary**: Fires when **$|Z| \ge 3.0\sigma$** (price volatility spike/crash) or **$Z_{\text{vol}} \ge 3.0\sigma$** (volume explosion).
  * **Cold-Start Guard**: Requires $\ge 5$ windows of history before activating to prevent false alarms.

### File: `detector/threshold_detector.py`
* **Lines 30–75 (`ThresholdDetector.detect`)**:  
  * Hard safety net: fires if price changes by **$\ge 5.0\%$** vs. rolling mean or volume surges by **$\ge 2.0\times$** average volume. Protects the pipeline immediately from tick zero without warm-up delays.

### File: `detector/runner.py`
* **Lines 45–110 (`DetectorRunner.run_cycle`)**:  
  * Polls `stocks.v_ticks_1m_agg` every 30 seconds.
  * Evaluates both detectors concurrently and deduplicates alerts using a `seen_windows` cache before writing to `stocks.market_alerts`.

### File: `detector/evaluation.py`
* **Lines 50–140**:  
  * Evaluates synthetic market scenarios (`PRICE_SPIKE`, `FLASH_CRASH`, `VOLUME_SURGE`).
  * Measures mathematical **Precision**, **Recall**, **F1-Score**, and **Detection Delay (TTD)**.

---

## 6. Multi-Exchange Arbitrage & India Regulatory Premium

### File: `src/production/dashboard/multi_exchange_service.py`
* **Venue Connections (Lines 100–160)**:  
  * Connects simultaneously to **Kraken WebSocket v2** (`wss://ws.kraken.com/v2`) and **CoinDCX REST API** (`api.coindcx.com/exchange/ticker`).
* **Cross-Exchange Arbitrage & Fee Hurdle (Lines 220–270)**:  
  * **Clock Skew Bounding**: Calculates `clock_skew_ms = |t_cb - t_kr| * 1000`. Suppresses signals if $> 5,000\text{ ms}$ (stale data).
  * **Fee Hurdle**: Enforces a **0.35% round-trip trading fee** (0.15% maker + 0.20% taker):
    $$\text{Gross Spread \%} = \frac{|P_{\text{Coinbase}} - P_{\text{Kraken}}|}{\min(P_{\text{Coinbase}}, P_{\text{Kraken}})} \times 100$$
    $$\text{Net Profit \%} = \text{Gross Spread \%} - 0.35\%$$
  * Flags opportunities only when Net Profit $> 0\%$ with directional routing (*"Buy Kraken &rarr; Sell Coinbase"*).
* **India Regulatory Premium (Lines 290–350)**:  
  * Computes Global Implied Fair Value: $\text{Global\_INR} = \text{Coinbase Price (USD)} \times \text{ECB FX Rate}$.
  * Computes India Premium: $\text{Premium \%} = \frac{\text{CoinDCX} - \text{Global\_INR}}{\text{Global\_INR}} \times 100$.
  * Classifies regulatory regimes (Global Parity, Moderate Premium, High Surcharge) driven by **1% TDS (Section 194S)** and **RBI LRS capital controls**.

---

## 7. Webhook Alert Dispatcher (Discord, Telegram, Slack)

### File: `src/production/consumer/webhook_dispatcher.py` & `src/production/dashboard/webhook_dispatcher.py`
* **Discord Rich Embeds (Lines 55–120)**:  
  * Formats colored embeds (green for pumps/spikes, red for crashes/liquidations) with Z-scores, USD/INR prices, and return percentages.
* **Telegram & Slack Formatters (Lines 125–170)**:  
  * Formats structured HTML messages for Telegram bots and block attachments for Slack.
* **Cooldown Rate Limiting (Lines 180–220)**:  
  * Enforces a **15-second per-symbol rate-limit cooldown** to prevent channel flooding during market crashes.

---

## 8. FastAPI Backend & Dual Transport Layer

### File: `src/production/dashboard/web_app.py`
* **Global 24h Stats Merge (Lines 235–260 & Lines 860–930)**:  
  * Seamlessly merges official Coinbase 24-hour exchange statistics (`high`, `low`, `volume`, `open`) with ClickHouse ticks to eliminate local session gaps after container restarts.
* **FastAPI Endpoints**:  
  * `GET /api/latest` (Lines 560–640): Atomic latest price + in-memory cache (<0.1ms).
  * `GET /api/stats` (Lines 800–920): Historical price statistics by timeframe (default 24h).
  * `GET /api/candles` (Lines 980–1060): 1-minute OHLC candlestick matrix.
  * `GET /api/arbitrage` (Lines 1590–1620): Live cross-venue spread and fee hurdle status.
  * `GET /api/india-premium` (Lines 1625–1650): CoinDCX vs. Global INR premium and regime tags.
  * `GET /api/order-flow` (Lines 1400–1450): Rolling Lee-Ready order flow imbalance.
  * `GET /api/telemetry` (Lines 1110–1180): Kafka throughput, Spark latency, and watermarks.
  * `GET /api/alerts/webhook/*` (Lines 1265–1365): Webhook status, configuration, and test triggers.
* **Dual Real-Time Transport (Lines 1690–1740)**:  
  * **WebSocket (`/ws`)**: High-frequency sub-300ms canvas tick broadcasting.
  * **Server-Sent Events (`/api/stream`)**: Keyset cursor-tracked streaming powered by `cursor_tracker.py`.

### File: `src/production/dashboard/cursor_tracker.py`
* **Keyset Pagination (`SSECursorTracker`)**:  
  * Implements composite cursor `(timestamp, event_id)` with CityHash64 fingerprinting to guarantee zero dropped ticks and zero duplicates during high-velocity bursts.

---

## 9. Frontend Terminal, Indicators & Order Flow Imbalance

### File: `src/production/dashboard/static/js/dashboard.js`
* **Exponential Moving Average - EMA 9 (Lines 420–445)**:  
  $$\alpha = \frac{2}{9 + 1} = 0.2, \qquad \text{EMA}_t = (P_t \times 0.2) + (\text{EMA}_{t-1} \times 0.8)$$
* **Bollinger Bands Envelope (Lines 450–495)**:  
  * Computes 20-period SMA middle band with dynamic $\pm 2.0\sigma$ upper and lower volatility channels.
* **Lee-Ready (1991) Order Flow Imbalance (Lines 685–740)**:  
  * Signs trades as buyer-initiated or seller-initiated:
    $$\text{OFI \%} = \frac{\text{BuyVolume} - \text{SellVolume}}{\text{BuyVolume} + \text{SellVolume}} \times 100$$
* **Discord Webhook Modal Controls (Lines 2680–2760)**:  
  * Functions: `openWebhookModal()`, `closeWebhookModal()`, `saveWebhookConfiguration()`, and `sendTestWebhookAlert()`.
* **High-Frequency WebSocket Ingestion (Lines 2460–2570)**:  
  * Connects to `/ws` and streams sub-second ticks directly onto the trading canvas.

### File: `src/production/dashboard/static/crypto_index.html`
* **6 Modular SPA Views**: Overview, Live Stream, Price Analytics, Recent Trades, Anomaly Alerts, Pipeline Health.
* **Lines 930–945**: Anomaly Alerts toolbar containing **`Configure Webhook`** and **`Simulate Volatility Alert`**.
* **Lines 1310–1360**: The **Discord / Webhook Alert Dispatcher Modal (`#webhookModal`)**.

### File: `src/production/dashboard/static/css/dashboard.css`
* **Lines 1320–1385**: Action button styling for `.btn-action-primary` and `.btn-action-secondary` across both light and dark themes.

---

## 10. Performance Benchmarks & Empirical Evidence

### File: `benchmarks/load_test_pipeline.py`
* Ingestion stress test pushing up to **10,000 synthetic ticks/sec** into Kafka.

### File: `benchmarks/BENCHMARK_RESULTS.md`
* **Verified Empirical Results**:
  * Sustained Producer Ingestion: **993.0 ticks/sec** (at 1,000 msgs/s target load)
  * Kafka Broker Acknowledgment: **0.2 ms** p99 flush latency
  * End-to-End WebSocket Canvas Latency: **150 ms – 300 ms**
  * Spark Window Micro-batch Delay: **1.2 s – 2.0 s**
  * Flink True Streaming Latency: **< 10 ms**
  * ClickHouse OLAP Aggregation Query Speed: **< 5 ms**
  * Columnar Storage Compression: **2.13× to 3.5×** ZSTD compression
  * Packet Loss: **0% (100% Ingestion / Zero Loss)** across service crashes and container restarts
  * Anomaly Detection Delay (TTD): **486 ms** mean time to detection

---

## 11. Team Module Ownership Quick Guide

| Team Member | Module | Primary Code Files |
| :--- | :--- | :--- |
| **Soorya AP** | **Frontend & UI** | `dashboard.js`, `crypto_index.html`, `dashboard.css`, Chart.js canvas, USD/INR toggle, Webhook UI |
| **Sreerag S Menon** | **Ingestion & Quality** | `crypto_producer.py`, `ExchangeRateManager` 4-tier failover, Kafka schema validation, CoinDCX feed |
| **Vedha VK** | **Streaming & OLAP** | `spark_streaming_clickhouse_consumer.py`, `init_db.sql`, ReplacingMergeTree, 30s watermark, checkpoints |
| **Vyshnav Pradeep** | **Detection & Eval** | `detector/zscore_detector.py`, `threshold_detector.py`, `runner.py`, `evaluation.py`, precision/recall |

---

*Generated for CryptoPulse Production Pipeline | All paths and line numbers verified against local repository commit `892f382`.*
