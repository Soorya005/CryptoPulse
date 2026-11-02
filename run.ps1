# =============================================================================
# Production Pipeline Runner (Windows PowerShell)
#
# Runs: Crypto Producer (Coinbase) -> Kafka -> Consumer -> ClickHouse -> Web Dashboard
#
# Usage:
#   .\run.ps1
#   .\run.ps1 spark_streaming
#   .\run.ps1 flink
# =============================================================================

param (
    [string]$Consumer = "spark_streaming"
)

# Validate consumer choice
if ($Consumer -ne "spark_streaming" -and $Consumer -ne "flink") {
    Write-Host "Error: Unknown consumer '$Consumer'" -ForegroundColor Red
    Write-Host "Valid options: spark_streaming, flink"
    exit 1
}

$env:DASHBOARD_MODE = "crypto"
$profileArgs = @("--profile", $Consumer, "--profile", "crypto", "--profile", "web")

Write-Host "==============================================" -ForegroundColor Green
Write-Host "  CryptoPulse Production Pipeline (PowerShell)" -ForegroundColor Green
Write-Host "  Consumer Engine: $Consumer" -ForegroundColor Cyan
Write-Host "  Data Source: Real-time Crypto (Coinbase)" -ForegroundColor Cyan
Write-Host "  Dashboard: http://localhost:8502" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green

# 1. Start core infrastructure (Kafka + ClickHouse)
Write-Host "`nStarting Kafka and ClickHouse..." -ForegroundColor Yellow
docker compose up -d kafka clickhouse

# 2. Wait for Kafka to become healthy
Write-Host "Waiting for Kafka to be ready..." -ForegroundColor Yellow
$kafkaReady = $false
while (-not $kafkaReady) {
    Start-Sleep -Seconds 2
    $null = docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
    }
}
Write-Host "Kafka is ready." -ForegroundColor Green

# 3. Wait for ClickHouse to become healthy
Write-Host "Waiting for ClickHouse to be ready..." -ForegroundColor Yellow
$chReady = $false
while (-not $chReady) {
    Start-Sleep -Seconds 2
    $null = docker exec clickhouse clickhouse-client --query "SELECT 1" 2>$null
    if ($LASTEXITCODE -eq 0) {
        $chReady = $true
    }
}
Write-Host "ClickHouse is ready." -ForegroundColor Green

# 4. Ensure Kafka topic exists
Write-Host "Ensuring Kafka topic 'stock-ticks' exists..." -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic stock-ticks --partitions 1 --replication-factor 1 --if-not-exists 2>$null
Write-Host "Kafka topic ready." -ForegroundColor Green

# 5. Create ClickHouse database & tables
Write-Host "Ensuring ClickHouse schema exists..." -ForegroundColor Yellow
docker exec clickhouse clickhouse-client --query "CREATE DATABASE IF NOT EXISTS stocks"

docker exec clickhouse clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS stocks.ticks (
        symbol String,
        price Float64,
        volume Float64,
        timestamp DateTime,
        price_usd Nullable(Float64) DEFAULT price,
        price_inr Nullable(Float64) DEFAULT NULL,
        exchange_rate Nullable(Float64) DEFAULT NULL,
        rate_timestamp Nullable(DateTime) DEFAULT NULL,
        event_id String DEFAULT ''
    ) ENGINE = MergeTree()
    ORDER BY (symbol, timestamp)
"

docker exec clickhouse clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS stocks.ticks_1m_agg (
        symbol String,
        window_start DateTime64(6),
        window_end DateTime64(6),
        vwap Float64,
        avg_price Float64,
        min_price Float64,
        max_price Float64,
        total_volume Float64,
        tick_count UInt32,
        vwap_inr Nullable(Float64) DEFAULT NULL,
        avg_price_inr Nullable(Float64) DEFAULT NULL,
        min_price_inr Nullable(Float64) DEFAULT NULL,
        max_price_inr Nullable(Float64) DEFAULT NULL
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (symbol, window_start)
"

docker exec clickhouse clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS stocks.market_alerts (
        symbol         String,
        alert_time     DateTime64(6),
        observed_price Float64,
        price_return   Float64,
        z_score        Float64,
        threshold      Float64,
        window_size    UInt32,
        sample_count   UInt32,
        source         String,
        created_at     DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (symbol, alert_time)
"
Write-Host "ClickHouse schema ready." -ForegroundColor Green

# 6. Launch all profiled services
Write-Host "`n==============================================" -ForegroundColor Green
Write-Host "  Launching all services..." -ForegroundColor Green
Write-Host "  Open http://localhost:8502 in your browser" -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host "==============================================`n" -ForegroundColor Green

docker compose @profileArgs up --build

