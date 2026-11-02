"""
manage_db.py — ClickHouse Database & Table Initializer.

Connects to ClickHouse via its lightweight HTTP interface (port 8123)
using standard requests or urllib, reads init_db.sql, and verifies table schemas.
Configurable via environment variables for local host or Docker environments.
"""

from __future__ import annotations

import logging
import os
import pathlib
import sys
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clickhouse_init")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
BASE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"


def execute_sql(query: str, database: str | None = None) -> str:
    """Execute a single SQL statement against the ClickHouse HTTP interface."""
    params: dict[str, str] = {}
    if database:
        params["database"] = database
    if CLICKHOUSE_USER:
        params["user"] = CLICKHOUSE_USER
    if CLICKHOUSE_PASSWORD:
        params["password"] = CLICKHOUSE_PASSWORD

    try:
        response = requests.post(
            BASE_URL,
            params=params,
            data=query.strip().encode("utf-8"),
            timeout=10,
        )
        response.raise_for_status()
        return response.text.strip()
    except requests.RequestException as exc:
        logger.error("ClickHouse query execution failed: %s", exc)
        raise


def ping_clickhouse() -> bool:
    """Check if ClickHouse is up and responding to ping."""
    try:
        resp = requests.get(f"{BASE_URL}/ping", timeout=5)
        return resp.status_code == 200 and resp.text.strip() == "Ok."
    except Exception:
        return False


def apply_schema(sql_file_path: pathlib.Path | None = None) -> None:
    """Read and execute SQL statements from init_db.sql."""
    if sql_file_path is None:
        sql_file_path = pathlib.Path(__file__).parent / "init_db.sql"

    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found at: {sql_file_path}")

    logger.info("Connecting to ClickHouse at %s ...", BASE_URL)
    if not ping_clickhouse():
        logger.warning(
            "ClickHouse server at %s is not reachable yet. Please ensure the container is running.",
            BASE_URL,
        )

    raw_sql = sql_file_path.read_text(encoding="utf-8")
    # Separate statements by semicolon while ignoring comments and blank lines
    statements = []
    current: list[str] = []
    for line in raw_sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    for stmt in statements:
        # Extract table or database name for logging
        first_line = stmt.splitlines()[0]
        logger.info("Executing: %s", first_line)
        execute_sql(stmt)

    logger.info("All schemas applied successfully!")


def verify_tables() -> list[str]:
    """Query system.tables to confirm required tables exist in stocks database."""
    query = "SELECT name FROM system.tables WHERE database = 'stocks' ORDER BY name FORMAT TabSeparated"
    output = execute_sql(query)
    tables = [t.strip() for t in output.splitlines() if t.strip()]
    logger.info("Verified tables in 'stocks' database: %s", tables)
    return tables


if __name__ == "__main__":
    try:
        apply_schema()
        tables = verify_tables()
        print(f"\n[OK] ClickHouse schema initialized successfully. Tables: {', '.join(tables)}")
    except Exception as err:
        logger.error("Initialization error: %s", err)
        sys.exit(1)
