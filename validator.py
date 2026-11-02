"""
validator.py — Validates and normalizes market trade events.

Schema
------
Every event must carry the following fields:

Required (non-null)
    symbol          str     Trading pair, e.g. "BTC-USD"
    price           number  Last traded price (positive, finite)
    volume          number  Trade volume (positive, finite)
    timestamp       str     ISO-8601 timezone-aware timestamp
    price_usd       number  Price in US dollars (positive, finite)

Nullable (may be None / null when INR conversion is unavailable)
    price_inr       number | null   Price in Indian rupees
    exchange_rate   number | null   USD→INR rate used
    rate_timestamp  str    | null   When the rate was fetched (ISO-8601 + tz)

Numeric representation
-----------------------
Numeric values are preserved as JSON numbers (float) rather than
converted to strings.  This avoids downstream parsing overhead in Spark
and ClickHouse, both of which expect native numeric types.

Timestamps
----------
All timestamp strings are parsed, validated to be timezone-aware, and
normalised to UTC ISO-8601 format: ``YYYY-MM-DDTHH:MM:SSZ``.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


# ---------------------------------------------------------------------------
# Field lists
# ---------------------------------------------------------------------------

# Fields that must always be present and non-null.
REQUIRED_NON_NULL_FIELDS = ["symbol", "price", "volume", "timestamp", "price_usd"]

# Fields that must be present (key must exist) but are allowed to be null.
NULLABLE_FIELDS = ["price_inr", "exchange_rate", "rate_timestamp"]

# All fields that must be present as keys in the event dict.
REQUIRED_FIELDS = REQUIRED_NON_NULL_FIELDS + NULLABLE_FIELDS

# Non-null numeric fields (must be positive finite numbers).
NON_NULL_NUMERIC_FIELDS = ["price", "volume", "price_usd"]

# Nullable numeric fields (validated only when not None).
NULLABLE_NUMERIC_FIELDS = ["price_inr", "exchange_rate"]

# Non-null timestamp fields.
NON_NULL_TIMESTAMP_FIELDS = ["timestamp"]

# Nullable timestamp fields (validated only when not None).
NULLABLE_TIMESTAMP_FIELDS = ["rate_timestamp"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_numeric(field: str, value) -> float:
    """
    Return *value* as a float if it is a positive, finite number.
    Booleans are rejected even though Python treats them as ints.
    Raises ValueError with a descriptive message on any problem.
    """
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric, not a boolean")

    try:
        fval = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be numeric")

    if math.isnan(fval) or math.isinf(fval) or fval <= 0:
        raise ValueError(f"{field} must be positive and finite")

    return fval


def _parse_timestamp(field: str, value) -> datetime:
    """
    Parse *value* as an ISO-8601 timestamp and return a timezone-aware
    datetime. Raises ValueError with a descriptive message on failure.
    """
    if not isinstance(value, str):
        raise ValueError(f"Invalid {field}: must be a string")

    normalised = value.strip().replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(normalised)
    except ValueError:
        raise ValueError(f"Invalid {field}: '{value}' is not a valid ISO-8601 timestamp")

    if dt.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")

    return dt


def _to_utc_str(dt: datetime) -> str:
    """Convert a timezone-aware datetime to a UTC ISO-8601 string ending in 'Z'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_trade(event: dict) -> dict:
    """
    Validate and normalize a market trade event.

    Parameters
    ----------
    event : dict
        Raw event dict.  Required fields are listed in REQUIRED_FIELDS.
        ``price_inr``, ``exchange_rate``, and ``rate_timestamp`` may be
        None / null when USD-to-INR conversion data is unavailable.

    Returns
    -------
    dict
        A normalized copy of the event:
        - ``symbol`` is stripped and upper-cased.
        - Numeric values are stored as floats (JSON numbers).
        - Timestamps are converted to UTC ISO-8601 strings ending with 'Z'.
        - Nullable fields that are None remain None.

    Raises
    ------
    ValueError
        On any validation failure.  The message identifies the failing
        field and reason.
    """
    if not isinstance(event, dict):
        raise ValueError("Event must be a dictionary")

    # --- Required keys must be present -------------------------------------
    for field in REQUIRED_FIELDS:
        if field not in event:
            raise ValueError(f"Missing required field: {field}")

    # --- Non-null required fields must not be None -------------------------
    for field in REQUIRED_NON_NULL_FIELDS:
        if event[field] is None:
            raise ValueError(f"Missing required field: {field}")

    # --- Symbol ------------------------------------------------------------
    if not isinstance(event["symbol"], str) or not event["symbol"].strip():
        raise ValueError("symbol must be a non-empty string")

    # --- Non-null numeric fields -------------------------------------------
    validated_numerics: dict[str, float] = {}
    for field in NON_NULL_NUMERIC_FIELDS:
        validated_numerics[field] = _validate_numeric(field, event[field])

    # --- Nullable numeric fields (only when not None) ----------------------
    nullable_numerics: dict[str, float | None] = {}
    for field in NULLABLE_NUMERIC_FIELDS:
        val = event[field]
        if val is None:
            nullable_numerics[field] = None
        else:
            nullable_numerics[field] = _validate_numeric(field, val)

    # --- Non-null timestamp fields -----------------------------------------
    validated_timestamps: dict[str, datetime] = {}
    for field in NON_NULL_TIMESTAMP_FIELDS:
        validated_timestamps[field] = _parse_timestamp(field, event[field])

    # --- Nullable timestamp fields (only when not None) --------------------
    nullable_timestamps: dict[str, datetime | None] = {}
    for field in NULLABLE_TIMESTAMP_FIELDS:
        val = event[field]
        if val is None:
            nullable_timestamps[field] = None
        else:
            nullable_timestamps[field] = _parse_timestamp(field, val)

    # --- Build normalized output -------------------------------------------
    normalized = event.copy()

    normalized["symbol"] = event["symbol"].strip().upper()

    # Store numeric values as floats (JSON numbers), not strings.
    for field, num in validated_numerics.items():
        normalized[field] = num

    for field, num in nullable_numerics.items():
        normalized[field] = num  # may be None

    for field, dt in validated_timestamps.items():
        normalized[field] = _to_utc_str(dt)

    for field, dt in nullable_timestamps.items():
        normalized[field] = _to_utc_str(dt) if dt is not None else None

    return normalized
