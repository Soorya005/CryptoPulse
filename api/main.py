"""
main.py — FastAPI Application Entrypoint for Crypto Analytics Alert API.
"""

from __future__ import annotations

import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("alert_api")

app = FastAPI(
    title="Crypto Market Anomaly Alert API",
    description=(
        "REST API service for Member 4 Anomaly Detection System in the Real-Time Crypto Analytics Platform. "
        "Provides endpoints to query real-time anomaly alerts, filter by symbol/severity/type, "
        "inspect statistical aggregate metrics from ClickHouse, and fetch telemetry."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Enable Cross-Origin Resource Sharing (CORS) for external web dashboards
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static assets for the Dashboard UI
static_dir = Path(__file__).resolve().parent / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Register routes
app.include_router(router, prefix="/api")
app.include_router(router, prefix="/api/v1")
app.include_router(router)  # also available at root level without prefix for convenience


@app.get("/", tags=["Root"])
def root_status() -> dict[str, str]:
    """Root endpoint welcoming consumers and pointing to interactive documentation."""
    return {
        "service": "Crypto Market Anomaly Alert API",
        "version": "1.0.0",
        "docs_url": "/docs",
        "dashboard_url": "/dashboard",
        "health_check": "/health",
        "alerts_endpoint": "/alerts",
        "detector_comparison": "/detectors/comparison",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
