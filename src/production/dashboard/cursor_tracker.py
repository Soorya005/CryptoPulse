"""
Keyset Pagination & Starvation-Free Draining for Real-Time SSE Streams.

Provides robust event cursor tracking with bounded lookback windows and
CityHash64 fallback fingerprinting to ensure zero dropped records and
prevent starvation under high-frequency tick ingestion.
"""

from datetime import datetime, timedelta


class SSECursorTracker:
    """
    Keyset pagination tracker for SSE streaming with bounded overlap lookback
    and multi-page draining to prevent starvation.
    """

    def __init__(
        self,
        lookback_seconds: int = 5,
        batch_size: int = 100,
        max_pages_per_poll: int = 5,
    ):
        self.lookback_seconds = lookback_seconds
        self.batch_size = batch_size
        self.max_pages_per_poll = max_pages_per_poll
        self.is_primed = False
        self.high_watermark_ts: datetime | None = None
        self.drain_cursor: tuple[str, str] | None = None
        self.seen_signatures: dict[str, datetime] = {}

    def fetch_next_events(self, ch_client, symbol_filter: str = "") -> list[dict]:
        """Fetch next batch of events using keyset pagination and multi-page draining."""
        if not self.is_primed:
            prime_query = f"""
            WITH enriched AS (
                SELECT
                    timestamp, symbol, price, volume,
                    price_usd, price_inr, exchange_rate, rate_timestamp,
                    COALESCE(
                        NULLIF(event_id, ''),
                        lower(hex(cityHash64(symbol, toString(price), toString(volume), toString(timestamp))))
                    ) AS effective_event_id
                FROM stocks.ticks
                WHERE timestamp >= now() - INTERVAL 10 MINUTE {symbol_filter}
            )
            SELECT
                timestamp, symbol, price, volume,
                price_usd, price_inr, exchange_rate, rate_timestamp,
                effective_event_id
            FROM enriched
            ORDER BY timestamp DESC, effective_event_id DESC
            LIMIT {self.batch_size}
            """
            rows = ch_client.query(prime_query).result_rows
            if not rows:
                fallback_query = f"""
                WITH enriched AS (
                    SELECT
                        timestamp, symbol, price, volume,
                        price_usd, price_inr, exchange_rate, rate_timestamp,
                        COALESCE(
                            NULLIF(event_id, ''),
                            lower(hex(cityHash64(symbol, toString(price), toString(volume), toString(timestamp))))
                        ) AS effective_event_id
                    FROM stocks.ticks
                    WHERE 1=1 {symbol_filter}
                )
                SELECT
                    timestamp, symbol, price, volume,
                    price_usd, price_inr, exchange_rate, rate_timestamp,
                    effective_event_id
                FROM enriched
                ORDER BY timestamp DESC, effective_event_id DESC
                LIMIT {self.batch_size}
                """
                rows = ch_client.query(fallback_query).result_rows
            self.is_primed = True
            if not rows:
                return []

            primed_rows = list(reversed(rows))
            for r in primed_rows:
                r_ts = r[0] if isinstance(r[0], datetime) else datetime.fromisoformat(str(r[0]))
                r_id = str(r[8])
                self.seen_signatures[r_id] = r_ts
                if self.high_watermark_ts is None or r_ts > self.high_watermark_ts:
                    self.high_watermark_ts = r_ts
            return [self._format_row(r) for r in primed_rows]

        if self.high_watermark_ts is None:
            return []

        if self.drain_cursor is None:
            lookback_boundary = self.high_watermark_ts - timedelta(seconds=self.lookback_seconds)
            cur_ts = lookback_boundary.strftime("%Y-%m-%d %H:%M:%S")
            cur_id = ""
        else:
            cur_ts, cur_id = self.drain_cursor

        new_events = []
        pages_fetched = 0

        while pages_fetched < self.max_pages_per_poll:
            query = f"""
            WITH enriched AS (
                SELECT
                    timestamp, symbol, price, volume,
                    price_usd, price_inr, exchange_rate, rate_timestamp,
                    COALESCE(
                        NULLIF(event_id, ''),
                        lower(hex(cityHash64(symbol, toString(price), toString(volume), toString(timestamp))))
                    ) AS effective_event_id
                FROM stocks.ticks
                WHERE timestamp >= toDateTime('{cur_ts}') {symbol_filter}
            )
            SELECT
                timestamp, symbol, price, volume,
                price_usd, price_inr, exchange_rate, rate_timestamp,
                effective_event_id
            FROM enriched
            WHERE (timestamp, effective_event_id) > (toDateTime('{cur_ts}'), '{cur_id}')
            ORDER BY timestamp ASC, effective_event_id ASC
            LIMIT {self.batch_size}
            """
            rows = ch_client.query(query).result_rows
            pages_fetched += 1

            if not rows:
                self.drain_cursor = None
                break

            for r in rows:
                r_ts = r[0] if isinstance(r[0], datetime) else datetime.fromisoformat(str(r[0]))
                r_id = str(r[8])
                if r_id not in self.seen_signatures:
                    self.seen_signatures[r_id] = r_ts
                    new_events.append(self._format_row(r))
                if self.high_watermark_ts is None or r_ts > self.high_watermark_ts:
                    self.high_watermark_ts = r_ts

            last_ts = rows[-1][0] if isinstance(rows[-1][0], datetime) else datetime.fromisoformat(str(rows[-1][0]))
            cur_ts = last_ts.strftime("%Y-%m-%d %H:%M:%S")
            cur_id = str(rows[-1][8])

            if len(rows) < self.batch_size:
                self.drain_cursor = None
                break
            else:
                self.drain_cursor = (cur_ts, cur_id)

        # Cleanup deduplication signatures only after current window is fully drained
        if self.drain_cursor is None and self.high_watermark_ts is not None:
            prune_cutoff = self.high_watermark_ts - timedelta(seconds=self.lookback_seconds)
            self.seen_signatures = {
                sig: ts for sig, ts in self.seen_signatures.items()
                if ts >= prune_cutoff
            }

        return new_events

    def _format_row(self, r) -> dict:
        return {
            "timestamp": str(r[0]),
            "symbol": r[1],
            "price": r[2],
            "volume": r[3],
            "price_usd": r[4],
            "price_inr": r[5],
            "exchange_rate": r[6],
            "rate_timestamp": str(r[7]) if r[7] else None,
            "event_id": str(r[8]),
        }
