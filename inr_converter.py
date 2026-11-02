"""
inr_converter.py — USD-to-INR exchange rate fetcher with caching.

Exchange-rate API
-----------------
Source  : https://open.er-api.com/v6/latest/USD
Provider: ExchangeRate-API (free tier, no API key required for the
          public endpoint).  The same data is available at
          https://www.exchangerate-api.com/docs/free (the ``/v6/latest``
          path). One call returns a JSON object containing conversion
          rates for ~160 currencies.

Refresh interval
----------------
``REFRESH_INTERVAL_S`` (default 300 s = 5 minutes).  The cached rate is
reused for all cryptocurrency events that arrive within the refresh
window, so the ingestion loop never blocks on an HTTP request per event.

Caching strategy
-----------------
A module-level ``_cache`` dict holds:
    ``rate``            — float or None
    ``rate_timestamp``  — UTC ISO-8601 string or None
    ``fetched_at``      — monotonic timestamp of the last successful fetch

On every call to ``get_usd_to_inr()``:
1.  If the cache is fresh (age < REFRESH_INTERVAL_S) return it immediately.
2.  Otherwise, make one HTTP GET to the exchange-rate API.
3.  On success, update the cache and return the new rate.
4.  On failure (network error, non-200 status, malformed JSON), log a
    warning, leave the stale rate in place, and return whatever is cached.
    If the cache has never been populated, return ``{"rate": None,
    "rate_timestamp": None}``.

Failure handling
-----------------
* Network / HTTP errors → warning logged; stale cache reused.
* Invalid JSON / missing ``rates.INR`` → warning logged; stale cache reused.
* If no valid rate has ever been fetched the caller receives
  ``{"rate": None, "rate_timestamp": None}`` and must publish USD-only
  events with INR fields set to null.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TypedDict

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EXCHANGE_RATE_URL = "https://open.er-api.com/v6/latest/USD"
REFRESH_INTERVAL_S: float = 300.0   # 5 minutes
REQUEST_TIMEOUT_S: float = 10.0


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class _RateCache(TypedDict):
    rate: float | None
    rate_timestamp: str | None
    fetched_at: float   # time.monotonic()


_cache: _RateCache = {
    "rate": None,
    "rate_timestamp": None,
    "fetched_at": 0.0,
}

# Thread-safety: multiple threads (e.g., asyncio + main) may call
# get_usd_to_inr() concurrently.  A lock ensures only one refresh runs
# at a time and the rest immediately get the refreshed value.
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class RateInfo(TypedDict):
    rate: float | None
    rate_timestamp: str | None


def get_usd_to_inr(force_refresh: bool = False) -> RateInfo:
    """
    Return the cached USD-to-INR exchange rate, refreshing if stale.

    Parameters
    ----------
    force_refresh : bool
        If True, bypass the cache age check and fetch immediately.
        Useful for unit tests and the first call at start-up.

    Returns
    -------
    RateInfo
        ``{"rate": <float or None>, "rate_timestamp": <str or None>}``
        The rate is None if no successful fetch has ever occurred.
    """
    with _lock:
        age = time.monotonic() - _cache["fetched_at"]
        if not force_refresh and age < REFRESH_INTERVAL_S and _cache["rate"] is not None:
            return {"rate": _cache["rate"], "rate_timestamp": _cache["rate_timestamp"]}

        _refresh_cache()

    return {"rate": _cache["rate"], "rate_timestamp": _cache["rate_timestamp"]}


FALLBACK_EXCHANGE_RATE_URL = "https://api.frankfurter.app/latest?from=USD&to=INR"


def _refresh_cache() -> None:
    """
    Fetch a fresh rate from the exchange-rate API and update ``_cache``.
    Must be called while ``_lock`` is held.
    """
    inr_rate: float | None = None

    # 1. Attempt primary API (open.er-api.com)
    try:
        resp = requests.get(EXCHANGE_RATE_URL, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
        val = data["rates"]["INR"]
        if isinstance(val, (int, float)) and val > 0:
            inr_rate = float(val)
    except Exception as exc:
        logger.warning("Primary exchange-rate API failed: %s — trying fallback", exc)

    # 2. Attempt fallback API (frankfurter.app) if primary failed
    if inr_rate is None:
        try:
            resp = requests.get(FALLBACK_EXCHANGE_RATE_URL, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            data = resp.json()
            val = data["rates"]["INR"]
            if isinstance(val, (int, float)) and val > 0:
                inr_rate = float(val)
        except Exception as exc:
            logger.warning("Fallback exchange-rate API failed: %s — using cached rate", exc)
            return

    rate_ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _cache["rate"] = inr_rate
    _cache["rate_timestamp"] = rate_ts
    _cache["fetched_at"] = time.monotonic()
    logger.info("USD→INR rate refreshed: %.4f (fetched at %s)", inr_rate, rate_ts)
