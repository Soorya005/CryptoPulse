"""
producer.py — Creates a Kafka producer and publishes validated trade events.

Uses a proper Serializer subclass to avoid the kafka-python 3.x deprecation
warning that occurs when a plain callable is passed as value_serializer.

Numeric values are serialized as JSON numbers (not strings) so that
downstream consumers (Spark, ClickHouse) receive native numeric types
without any additional parsing step.
"""

from __future__ import annotations

import json
import logging
from typing import Any, List, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer

from validator import validate_trade

logger = logging.getLogger(__name__)

TOPIC = "stock-ticks"
BROKER = "localhost:9092"


class JsonSerializer(Serializer):
    """Serialize a Python dict to UTF-8 encoded JSON bytes.

    Numeric values (int, float) are serialized as JSON numbers.
    None values are serialized as JSON null.
    """

    def serialize(self, topic: str, headers: List[Tuple[str, bytes]], data: Any) -> bytes:
        return json.dumps(data, allow_nan=False).encode("utf-8")


def create_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer configured for reliable delivery.

    The producer uses acks='all' so the broker confirms that all in-sync
    replicas have received the message before acknowledging it, and retries
    up to five times on transient network errors.
    """
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=JsonSerializer(),
        acks="all",
        retries=5,
    )


def publish_trade(producer: KafkaProducer, event: dict) -> None:
    """
    Validate *event* and publish it to the stock-ticks Kafka topic.

    Parameters
    ----------
    producer : KafkaProducer
        An active producer returned by create_producer().
    event : dict
        Raw trade event.  Must pass validate_trade() before sending.
        ``price_inr``, ``exchange_rate``, and ``rate_timestamp`` may be
        None if USD-to-INR conversion data is unavailable.

    Raises
    ------
    ValueError
        If the event fails validation.
    KafkaError
        If the broker cannot be reached or delivery fails after retries.
    """
    # Validate and normalise before touching the network.
    normalized_event = validate_trade(event)

    try:
        future = producer.send(TOPIC, value=normalized_event)
        # Block until the broker confirms delivery (or the timeout expires).
        metadata = future.get(timeout=10)
    except KafkaError as exc:
        raise KafkaError(
            f"Failed to publish {normalized_event.get('symbol', '?')} "
            f"to {TOPIC}: {exc}"
        ) from exc

    logger.info(
        "Published %s to %s, partition %s, offset %s",
        normalized_event["symbol"],
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
    print(
        f"Published {normalized_event['symbol']} "
        f"to {metadata.topic}, partition {metadata.partition}, "
        f"offset {metadata.offset}"
    )
