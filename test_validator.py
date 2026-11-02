"""
test_validator.py — Unit tests for validate_trade().

All tests are pure Python and do not require a running Kafka broker.
The fixture valid_trade provides a correct baseline event; individual
tests mutate a copy of it to exercise each validation and normalisation
path.

Schema changes reflected here
------------------------------
* price_inr, exchange_rate, and rate_timestamp are nullable: the key must
  be present but the value may be None.  When None, they pass through
  unchanged.
* Numeric fields are stored as floats (JSON numbers) in the normalized
  output, not strings.  Tests updated accordingly.
"""

from __future__ import annotations

import math
import re

import pytest

from validator import validate_trade


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_trade():
    """Return a complete, valid trade event with all INR fields populated."""
    return {
        "symbol": "BTC-USD",
        "price": 81562.81,
        "volume": 0.025,
        "timestamp": "2026-10-16T10:30:15Z",
        "price_usd": 81562.81,
        "price_inr": 7745204.44,
        "exchange_rate": 94.96,
        "rate_timestamp": "2026-10-16T00:00:00Z",
    }


@pytest.fixture
def valid_trade_null_inr():
    """Valid event where INR-related fields are all None (conversion unavailable)."""
    return {
        "symbol": "ETH-USD",
        "price": 2500.00,
        "volume": 1.5,
        "timestamp": "2026-10-16T10:30:15Z",
        "price_usd": 2500.00,
        "price_inr": None,
        "exchange_rate": None,
        "rate_timestamp": None,
    }


# ---------------------------------------------------------------------------
# Valid event — smoke test and normalisation checks
# ---------------------------------------------------------------------------

class TestValidEvent:
    def test_returns_dict(self, valid_trade):
        result = validate_trade(valid_trade)
        assert isinstance(result, dict)

    def test_symbol_preserved(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"

    def test_timestamp_ends_with_z(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["timestamp"].endswith("Z")

    def test_rate_timestamp_ends_with_z(self, valid_trade):
        result = validate_trade(valid_trade)
        assert result["rate_timestamp"].endswith("Z")

    def test_numeric_fields_are_floats(self, valid_trade):
        """Numeric values must be floats (JSON numbers), not strings."""
        result = validate_trade(valid_trade)
        for field in ("price", "volume", "price_usd", "price_inr", "exchange_rate"):
            assert isinstance(result[field], float), (
                f"{field} should be a float after normalisation, got {type(result[field])}"
            )

    def test_original_event_not_mutated(self, valid_trade):
        original = valid_trade.copy()
        validate_trade(valid_trade)
        assert valid_trade == original

    def test_all_required_fields_present(self, valid_trade):
        result = validate_trade(valid_trade)
        for field in (
            "symbol", "price", "volume", "timestamp",
            "price_usd", "price_inr", "exchange_rate", "rate_timestamp",
        ):
            assert field in result


# ---------------------------------------------------------------------------
# Nullable INR fields
# ---------------------------------------------------------------------------

class TestNullableInrFields:
    def test_null_price_inr_accepted(self, valid_trade):
        valid_trade["price_inr"] = None
        result = validate_trade(valid_trade)
        assert result["price_inr"] is None

    def test_null_exchange_rate_accepted(self, valid_trade):
        valid_trade["exchange_rate"] = None
        result = validate_trade(valid_trade)
        assert result["exchange_rate"] is None

    def test_null_rate_timestamp_accepted(self, valid_trade):
        valid_trade["rate_timestamp"] = None
        result = validate_trade(valid_trade)
        assert result["rate_timestamp"] is None

    def test_all_inr_fields_null_accepted(self, valid_trade_null_inr):
        result = validate_trade(valid_trade_null_inr)
        assert result["price_inr"] is None
        assert result["exchange_rate"] is None
        assert result["rate_timestamp"] is None

    def test_non_null_inr_still_validated(self, valid_trade):
        """When price_inr is provided it must still be a positive number."""
        valid_trade["price_inr"] = -100.0
        with pytest.raises(ValueError, match="price_inr must be positive and finite"):
            validate_trade(valid_trade)

    def test_non_null_exchange_rate_still_validated(self, valid_trade):
        valid_trade["exchange_rate"] = 0
        with pytest.raises(ValueError, match="exchange_rate must be positive and finite"):
            validate_trade(valid_trade)

    def test_non_null_rate_timestamp_still_validated(self, valid_trade):
        valid_trade["rate_timestamp"] = "not-a-timestamp"
        with pytest.raises(ValueError, match="Invalid rate_timestamp"):
            validate_trade(valid_trade)


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------

class TestMissingFields:
    @pytest.mark.parametrize("field", [
        "symbol", "price", "volume", "timestamp",
        "price_usd", "price_inr", "exchange_rate", "rate_timestamp",
    ])
    def test_missing_key_raises(self, valid_trade, field):
        """Key entirely absent → ValueError."""
        del valid_trade[field]
        with pytest.raises(ValueError, match=f"Missing required field: {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", [
        "symbol", "price", "volume", "timestamp", "price_usd",
    ])
    def test_none_in_non_nullable_field_raises(self, valid_trade, field):
        """Non-nullable fields set to None → ValueError."""
        valid_trade[field] = None
        with pytest.raises(ValueError, match=f"Missing required field: {field}"):
            validate_trade(valid_trade)

    def test_non_dict_event_raises(self):
        with pytest.raises(ValueError, match="Event must be a dictionary"):
            validate_trade("not a dict")

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="Missing required field: symbol"):
            validate_trade({})


# ---------------------------------------------------------------------------
# Symbol validation
# ---------------------------------------------------------------------------

class TestSymbol:
    def test_empty_string_raises(self, valid_trade):
        valid_trade["symbol"] = ""
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_whitespace_only_raises(self, valid_trade):
        valid_trade["symbol"] = "   "
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_non_string_raises(self, valid_trade):
        valid_trade["symbol"] = 12345
        with pytest.raises(ValueError, match="symbol"):
            validate_trade(valid_trade)

    def test_symbol_uppercased(self, valid_trade):
        valid_trade["symbol"] = "btc-usd"
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"

    def test_symbol_stripped(self, valid_trade):
        valid_trade["symbol"] = "  BTC-USD  "
        result = validate_trade(valid_trade)
        assert result["symbol"] == "BTC-USD"


# ---------------------------------------------------------------------------
# Numeric field validation (non-null fields)
# ---------------------------------------------------------------------------

NON_NULL_NUMERIC_FIELDS = ["price", "volume", "price_usd"]
NULLABLE_NUMERIC_FIELDS_LIST = ["price_inr", "exchange_rate"]
ALL_NUMERIC_FIELDS = NON_NULL_NUMERIC_FIELDS + NULLABLE_NUMERIC_FIELDS_LIST


class TestNumericFields:
    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_string_non_numeric_raises(self, valid_trade, field):
        valid_trade[field] = "abc"
        with pytest.raises(ValueError, match=f"{field} must be numeric"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_negative_raises(self, valid_trade, field):
        valid_trade[field] = -1.0
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_zero_raises(self, valid_trade, field):
        valid_trade[field] = 0
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_nan_raises(self, valid_trade, field):
        valid_trade[field] = math.nan
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_positive_infinity_raises(self, valid_trade, field):
        valid_trade[field] = math.inf
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_negative_infinity_raises(self, valid_trade, field):
        valid_trade[field] = -math.inf
        with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_boolean_true_raises(self, valid_trade, field):
        valid_trade[field] = True
        with pytest.raises(ValueError, match="must be numeric, not a boolean"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_boolean_false_raises(self, valid_trade, field):
        valid_trade[field] = False
        with pytest.raises(ValueError, match="must be numeric, not a boolean"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_list_value_raises(self, valid_trade, field):
        valid_trade[field] = [100]
        with pytest.raises(ValueError, match=f"{field} must be numeric"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", ALL_NUMERIC_FIELDS)
    def test_numeric_string_accepted_and_stored_as_float(self, valid_trade, field):
        valid_trade[field] = "100.5"
        result = validate_trade(valid_trade)
        assert isinstance(result[field], float)
        assert result[field] == pytest.approx(100.5)

    def test_price_small_positive_accepted(self, valid_trade):
        valid_trade["price"] = 0.0001
        result = validate_trade(valid_trade)
        assert result["price"] == pytest.approx(0.0001)


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------

TIMESTAMP_FIELDS = ["timestamp"]
NULLABLE_TS_FIELDS = ["rate_timestamp"]


class TestTimestamps:
    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_invalid_string_raises(self, valid_trade, field):
        valid_trade[field] = "not-a-timestamp"
        with pytest.raises(ValueError, match=f"Invalid {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_date_only_raises(self, valid_trade, field):
        valid_trade[field] = "2026-10-16"
        with pytest.raises(ValueError, match=f"{field} must include a timezone"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_datetime_without_tz_raises(self, valid_trade, field):
        valid_trade[field] = "2026-10-16T10:30:15"
        with pytest.raises(ValueError, match=f"{field} must include a timezone"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS)
    def test_non_string_raises(self, valid_trade, field):
        valid_trade[field] = 1729072215
        with pytest.raises(ValueError, match=f"Invalid {field}"):
            validate_trade(valid_trade)

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS + NULLABLE_TS_FIELDS)
    def test_z_suffix_accepted(self, valid_trade, field):
        valid_trade[field] = "2026-10-16T10:30:15Z"
        result = validate_trade(valid_trade)
        assert result[field].endswith("Z")

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS + NULLABLE_TS_FIELDS)
    def test_offset_tz_accepted_and_converted_to_utc(self, valid_trade, field):
        valid_trade[field] = "2026-10-16T16:00:15+05:30"
        result = validate_trade(valid_trade)
        assert result[field] == "2026-10-16T10:30:15Z"

    @pytest.mark.parametrize("field", TIMESTAMP_FIELDS + NULLABLE_TS_FIELDS)
    def test_normalised_timestamp_format(self, valid_trade, field):
        result = validate_trade(valid_trade)
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", result[field])

    def test_null_rate_timestamp_passes_through(self, valid_trade):
        valid_trade["rate_timestamp"] = None
        result = validate_trade(valid_trade)
        assert result["rate_timestamp"] is None
