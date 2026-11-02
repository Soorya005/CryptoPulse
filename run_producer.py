"""
run_producer.py — Entry point for the market-data ingestion pipeline.

Usage
-----
Live mode (default) — connect to Coinbase WebSocket and publish continuously:

    python run_producer.py

Single-event test mode — load one event from a JSON file and publish it:

    python run_producer.py --file sample_trade.json
    python run_producer.py --file path/to/my_event.json

Options
-------
--file  PATH    Publish a single event from PATH, then exit.
                When omitted, the WebSocket live-feed runs until Ctrl-C.
--log-level     Logging level (DEBUG, INFO, WARNING, ERROR).  Default: INFO.
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import sys

from kafka.errors import KafkaError


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coinbase market-data ingestion pipeline.",
    )
    parser.add_argument(
        "--file",
        metavar="PATH",
        type=pathlib.Path,
        default=None,
        help="Publish a single event from this JSON file, then exit.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and log events to console without publishing to Kafka.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after processing this many events (useful for verification).",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def _load_event(path: pathlib.Path) -> dict:
    """
    Read and parse a JSON trade event from *path*.

    Raises SystemExit with a descriptive message if the file is missing
    or contains invalid JSON.
    """
    if not path.exists():
        sys.exit(f"Error: data file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        sys.exit(f"Error: {path} contains invalid JSON — {exc}")


def _run_file_mode(path: pathlib.Path) -> None:
    """Load one event from *path* and publish it to Kafka, then exit."""
    from producer import create_producer, publish_trade

    event = _load_event(path)
    try:
        producer = create_producer()
        try:
            publish_trade(producer, event)
        finally:
            producer.close()
    except ValueError as exc:
        sys.exit(f"Validation error: {exc}")
    except KafkaError as exc:
        sys.exit(f"Kafka error: {exc}")


def _run_live_mode(dry_run: bool = False, max_events: int | None = None) -> None:
    """Connect to Coinbase WebSocket and publish events continuously."""
    from coinbase_ws import CoinbaseWebSocketClient

    client = CoinbaseWebSocketClient(dry_run=dry_run, max_events=max_events)
    try:
        client.run_forever()
    except KeyboardInterrupt:
        print("\nShutting down — waiting for producer to flush…")
        client.stop()


def main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

    if args.file is not None:
        _run_file_mode(args.file)
    else:
        _run_live_mode(dry_run=args.dry_run, max_events=args.max_events)


if __name__ == "__main__":
    main()
