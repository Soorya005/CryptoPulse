"""
database.py — ClickHouse database integration layer for the Alert API.
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("api.database")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "stocks")

_client = None


def get_db_client():
    """Return a cached ClickHouse client instance, reconnecting if disconnected."""
    global _client
    if _client is not None:
        try:
            if _client.ping():
                return _client
        except Exception:
            _client = None

    try:
        import clickhouse_connect

        _client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            connect_timeout=5,
            send_receive_timeout=15,
        )
        return _client
    except Exception as exc:
        logger.warning("Could not establish connection to ClickHouse at %s:%d: %s", CLICKHOUSE_HOST, CLICKHOUSE_PORT, exc)
        return None


def check_clickhouse_health(client=None) -> bool:
    """Return True if ClickHouse is reachable and responsive."""
    cl = client or get_db_client()
    if cl is None:
        return False
    try:
        return bool(cl.ping())
    except Exception:
        return False


def get_alerts(
    client=None,
    symbol: Optional[str] = None,
    severity: Optional[str] = None,
    alert_type: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch paginated alerts from stocks.market_alerts with optional filters."""
    cl = client or get_db_client()
    if cl is None:
        return [], 0

    where_clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if symbol:
        where_clauses.append("symbol = {symbol:String}")
        params["symbol"] = symbol
    if severity:
        where_clauses.append("severity = {severity:String}")
        params["severity"] = severity.lower()
    if alert_type:
        where_clauses.append("alert_type = {alert_type:String}")
        params["alert_type"] = alert_type
    if start_time:
        where_clauses.append("timestamp >= {start_time:DateTime64(3, 'UTC')}")
        params["start_time"] = start_time
    if end_time:
        where_clauses.append("timestamp <= {end_time:DateTime64(3, 'UTC')}")
        params["end_time"] = end_time

    where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Total count query
    count_query = f"SELECT count(*) FROM stocks.market_alerts {where_str}"
    count_res = cl.query(count_query, parameters=params)
    total_count = count_res.result_rows[0][0] if count_res.result_rows else 0

    # Data fetch query
    data_query = (
        f"SELECT alert_id, symbol, alert_type, severity, message, metric_value, "
        f"       threshold, timestamp, created_at "
        f"FROM stocks.market_alerts {where_str} "
        f"ORDER BY timestamp DESC "
        f"LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}"
    )
    result = cl.query(data_query, parameters=params)
    cols = result.column_names

    rows = []
    for r in result.result_rows:
        d = dict(zip(cols, r))
        d["alert_id"] = str(d["alert_id"])
        rows.append(d)

    return rows, total_count


def get_alert_by_id(alert_id: str, client=None) -> Optional[dict[str, Any]]:
    """Retrieve a single alert record by alert_id."""
    cl = client or get_db_client()
    if cl is None:
        return None

    query = (
        "SELECT alert_id, symbol, alert_type, severity, message, metric_value, "
        "       threshold, timestamp, created_at "
        "FROM stocks.market_alerts "
        "WHERE alert_id = {alert_id:UUID} "
        "LIMIT 1"
    )
    try:
        result = cl.query(query, parameters={"alert_id": alert_id})
        if not result.result_rows:
            return None
        d = dict(zip(result.column_names, result.result_rows[0]))
        d["alert_id"] = str(d["alert_id"])
        return d
    except Exception as exc:
        logger.warning("Error fetching alert %s: %s", alert_id, exc)
        return None


def get_detection_delay_stats(client=None) -> dict[str, Any]:
    """Calculate operational detection delay statistics from stocks.market_alerts."""
    cl = client or get_db_client()
    default_stats = {
        "total_alerts": 0,
        "avg_delay_seconds": 0.0,
        "median_delay_seconds": 0.0,
        "min_delay_seconds": 0.0,
        "max_delay_seconds": 0.0,
        "p95_delay_seconds": 0.0,
    }
    if cl is None:
        return default_stats

    query = (
        "SELECT "
        "    count(*) AS total_alerts, "
        "    round(avg(dateDiff('millisecond', timestamp, created_at)) / 1000.0, 4) AS avg_delay, "
        "    round(min(dateDiff('millisecond', timestamp, created_at)) / 1000.0, 4) AS min_delay, "
        "    round(max(dateDiff('millisecond', timestamp, created_at)) / 1000.0, 4) AS max_delay, "
        "    round(quantile(0.50)(dateDiff('millisecond', timestamp, created_at)) / 1000.0, 4) AS median_delay, "
        "    round(quantile(0.95)(dateDiff('millisecond', timestamp, created_at)) / 1000.0, 4) AS p95_delay "
        "FROM stocks.market_alerts"
    )
    try:
        res = cl.query(query)
        if not res.result_rows or res.result_rows[0][0] == 0:
            return default_stats
        row = res.result_rows[0]
        return {
            "total_alerts": int(row[0]),
            "avg_delay_seconds": float(row[1] or 0.0),
            "min_delay_seconds": float(row[2] or 0.0),
            "max_delay_seconds": float(row[3] or 0.0),
            "median_delay_seconds": float(row[4] or 0.0),
            "p95_delay_seconds": float(row[5] or 0.0),
        }
    except Exception as exc:
        logger.warning("Error querying detection delay stats: %s", exc)
        return default_stats


def get_alert_stats(client=None) -> dict[str, Any]:
    """Compute summary statistics across all market alerts."""
    cl = client or get_db_client()
    if cl is None:
        return {
            "total_alerts": 0,
            "by_severity": {},
            "by_alert_type": {},
            "by_symbol": {},
            "recent_alerts_1h": 0,
            "detection_delay": {
                "total_alerts": 0,
                "avg_delay_seconds": 0.0,
                "median_delay_seconds": 0.0,
                "min_delay_seconds": 0.0,
                "max_delay_seconds": 0.0,
                "p95_delay_seconds": 0.0,
            },
        }

    # Total count
    tot_res = cl.query("SELECT count(*) FROM stocks.market_alerts")
    total_alerts = tot_res.result_rows[0][0] if tot_res.result_rows else 0

    # By severity
    sev_res = cl.query("SELECT severity, count(*) FROM stocks.market_alerts GROUP BY severity")
    by_severity = {row[0]: row[1] for row in sev_res.result_rows}

    # By alert_type
    type_res = cl.query("SELECT alert_type, count(*) FROM stocks.market_alerts GROUP BY alert_type")
    by_alert_type = {row[0]: row[1] for row in type_res.result_rows}

    # By symbol
    sym_res = cl.query("SELECT symbol, count(*) FROM stocks.market_alerts GROUP BY symbol")
    by_symbol = {row[0]: row[1] for row in sym_res.result_rows}

    # Recent (last hour)
    recent_res = cl.query(
        "SELECT count(*) FROM stocks.market_alerts WHERE timestamp >= now() - INTERVAL 1 HOUR"
    )
    recent_1h = recent_res.result_rows[0][0] if recent_res.result_rows else 0

    delay_stats = get_detection_delay_stats(client=cl)

    return {
        "total_alerts": total_alerts,
        "by_severity": by_severity,
        "by_alert_type": by_alert_type,
        "by_symbol": by_symbol,
        "recent_alerts_1h": recent_1h,
        "detection_delay": delay_stats,
    }


def get_latest_aggregates(symbol: Optional[str] = None, limit: int = 10, client=None) -> list[dict[str, Any]]:
    """Query 1-minute windowed metrics from deduplicated view stocks.v_ticks_1m_agg."""
    cl = client or get_db_client()
    if cl is None:
        return []

    where_clause = "WHERE symbol = {symbol:String}" if symbol else ""
    params = {"limit": limit}
    if symbol:
        params["symbol"] = symbol

    query = (
        f"SELECT symbol, window_start, window_end, event_count, avg_price, "
        f"       min_price, max_price, total_volume, avg_price_inr "
        f"FROM stocks.v_ticks_1m_agg {where_clause} "
        f"ORDER BY window_start DESC "
        f"LIMIT {{limit:UInt32}}"
    )
    result = cl.query(query, parameters=params)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def create_alert(
    symbol: str,
    alert_type: str,
    severity: str,
    message: str,
    metric_value: float,
    threshold: float,
    timestamp: Optional[datetime] = None,
    client=None,
) -> str:
    """Insert an alert directly into stocks.market_alerts and return its new UUID."""
    cl = client or get_db_client()
    if cl is None:
        raise RuntimeError("ClickHouse connection unavailable")

    alert_uuid = str(uuid.uuid4())
    ts = timestamp or datetime.now(timezone.utc)

    row = (
        alert_uuid,
        symbol,
        alert_type,
        severity,
        message,
        metric_value,
        threshold,
        ts,
    )
    cols = [
        "alert_id",
        "symbol",
        "alert_type",
        "severity",
        "message",
        "metric_value",
        "threshold",
        "timestamp",
    ]
    cl.insert("market_alerts", [row], column_names=cols)
    return alert_uuid
