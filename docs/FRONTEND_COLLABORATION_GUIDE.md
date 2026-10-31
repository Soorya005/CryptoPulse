# CryptoPulse: Frontend Collaboration & UI Development Guide

Welcome! If you are collaborating on the **CryptoPulse** frontend, this guide contains everything you need to know to build a high-converting **Landing Page** and elevate the **Institutional Trading Terminal** without getting bogged down by backend infrastructure.

---

## 🚀 1. Quickstart (Zero Docker Required)

You **do not need to install or run Docker, Kafka, Spark, or ClickHouse** to design and develop the frontend. 

The FastAPI backend runs standalone and includes offline graceful degradation, while the client-side terminal (`dashboard.js`) features an automatic simulation engine that generates realistic crypto price action when the analytics database is offline.

### Option A: Running the FastAPI Backend Locally
```bash
# 1. Install lightweight Python dependencies
pip install fastapi uvicorn clickhouse-connect pydantic

# 2. Start the local server on port 8502 (auto-reloads on file save)
python -m uvicorn src.production.dashboard.web_app:app --port 8502 --reload
```
Now open:
* **Dashboard:** [http://localhost:8502/](http://localhost:8502/) or [http://localhost:8502/terminal](http://localhost:8502/terminal)
* **Landing Page:** [http://localhost:8502/landing](http://localhost:8502/landing) *(served automatically if `static/landing.html` exists)*
* **Interactive API Docs (Swagger):** [http://localhost:8502/docs](http://localhost:8502/docs)

### Option B: Developing with a Modern Frontend Framework (React, Next.js, Vite, or Live Server)
**CORS is fully enabled (`*`) on the FastAPI backend.** 

If you are developing a separate frontend app (e.g. Vite on `http://localhost:5173` or Next.js on `http://localhost:3000`), you can make `fetch()` and `EventSource` calls directly to `http://localhost:8502/api/...` without any cross-origin issues.

---

## 📁 2. Frontend Directory Structure

All static web assets are located in `src/production/dashboard/static/`:

```
src/production/dashboard/static/
├── css/
│   └── dashboard.css          # Core CSS variables, typography, dark/light themes
├── js/
│   └── dashboard.js           # Chart.js rendering, SSE stream listener, OFI, indicators
├── crypto_index.html          # Main institutional trading terminal
├── stock_index.html           # Legacy stock terminal variant
└── landing.html               # 🌟 TARGET: New landing page (create here!)
```

---

## 🎨 3. Design System & Style Tokens

The UI is built with modern CSS custom properties with zero heavy framework bloat. You can reuse the following tokens in your landing page and dashboard components:

### Typography
* **Primary Sans-Serif:** `'Inter', -apple-system, BlinkMacSystemFont, sans-serif`
* **Tabular Numbers & Code:** `'JetBrains Mono', monospace`

### Color Palette
| Token / Variable | Hex Value | Purpose |
| :--- | :--- | :--- |
| `--accent-blue` | `#3b82f6` | Brand highlights, active tabs, buttons |
| `--accent-purple` | `#8b5cf6` | Secondary accents, gradient fills |
| `--bullish-green` | `#10b981` | Price increases, buy-side OFI volume |
| `--bearish-red` | `#ef4444` | Price drops, sell-side OFI volume |
| `--bg-dark` | `#0b0f19` | Deep institutional navy/black background |
| `--surface-dark` | `#111827` | Card backgrounds, sidebar surfaces |
| `--border-dark` | `rgba(255, 255, 255, 0.08)` | Subtle card outlines |
| `--text-primary` | `#f9fafb` | Headers, prices, primary labels |
| `--text-secondary` | `#9ca3af` | Secondary labels, captions, metadata |

---

## 🔌 4. API Cheatsheet (Connecting Frontend to Backend)

All endpoints return JSON and are available at `http://localhost:8502`:

### 1. Atomic Latest Prices (`GET /api/latest`)
* **Endpoint:** `/api/latest?symbols=BTC,ETH,SOL`
* **Response:**
  ```json
  [
    {
      "symbol": "BTC",
      "price": 67425.50,
      "price_usd": 67425.50,
      "price_inr": 5630029.00,
      "exchange_rate": 83.50,
      "volume": 12,
      "last_update": "2026-10-28 14:30:00"
    }
  ]
  ```

### 2. Historical Candles & Indicators (`GET /api/candles`)
* **Endpoint:** `/api/candles?symbol=BTC&range=1h&currency=USD`
* **Response:**
  ```json
  {
    "symbol": "BTC",
    "candles": [
      {
        "timestamp": "2026-10-28T14:00:00Z",
        "open": 67200.0,
        "high": 67550.0,
        "low": 67180.0,
        "close": 67425.5,
        "vwap": 67390.2,
        "volume": 450
      }
    ],
    "indicators": {
      "ema9": 67410.2,
      "bollinger_upper": 67600.0,
      "bollinger_lower": 67200.0
    }
  }
  ```

### 3. Order Flow Imbalance (`GET /api/order-flow`)
* **Endpoint:** `/api/order-flow?symbol=BTC`
* **Response:**
  ```json
  {
    "symbol": "BTC",
    "ofi_pct": 14.2,
    "buy_volume": 280,
    "sell_volume": 190,
    "dominant_side": "BUY"
  }
  ```

### 4. Cross-Market Arbitrage & India Premium
* **`/api/arbitrage`:** Real-time spot price spread between Coinbase Pro and Kraken.
* **`/api/india-premium`:** CoinDCX (INR) vs Implied Global Benchmark (Coinbase USD * ECB FX Rate).

### 5. Real-Time Server-Sent Events (SSE) Stream
* **Endpoint:** `/api/stream`
* **Frontend Usage:**
  ```javascript
  const eventSource = new EventSource("http://localhost:8502/api/stream");

  eventSource.onmessage = (event) => {
    const tick = JSON.parse(event.data);
    console.log("Live Tick:", tick.symbol, tick.price);
  };
  ```

---

## 🎯 5. Suggested Collaboration Tasks

### Task 1: Create the Landing Page (`landing.html`)
Build a high-converting, responsive landing page showcasing the CryptoPulse ecosystem:
- [ ] **Hero Section:** Engaging headline (e.g., *"Sub-Millisecond Financial Stream Processing"*), animated ticker marquee, and a bold **"Launch Trading Terminal"** button linking to `/app`.
- [ ] **Live Architecture Infographic:** Interactive visual explaining the data pipeline (Coinbase $\rightarrow$ Kafka $\rightarrow$ Spark/Flink $\rightarrow$ ClickHouse $\rightarrow$ UI).
- [ ] **Feature Highlights:** Dual-Currency (USD/INR) accounting, statistical $|Z| \ge 3.0\sigma$ anomaly alerts, and order flow imbalance analytics.
- [ ] **Live Mini-Ticker Component:** Embed a small widget calling `/api/latest` to display live BTC/ETH prices.

### Task 2: Polish the Trading Terminal (`crypto_index.html` & `dashboard.css`)
- [ ] **Mobile Responsiveness:** Refine grid breakpoints for tablets and mobile devices.
- [ ] **Modern Visual Polish:** Subtle glassmorphism, micro-animations on price updates, and clean tab transitions.
- [ ] **Watchlist / Search:** Quick keyboard shortcuts (`/` to search symbols).

