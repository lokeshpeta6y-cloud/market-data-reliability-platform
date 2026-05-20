"""
Redis writer service entry point.

Consumes CurveEvents from ``market.events.normalized`` and persists them to
Redis, maintaining:
  - Latest tenor data per instrument (Hash)
  - Event history sorted by timestamp (Sorted Set, capped at N entries)
  - Provider health information (Hash)
  - Forward curve snapshots when completeness threshold is met (String/JSON)
"""

from __future__ import annotations

import signal
import sys
from typing import Any

import redis

from mdrp_common.kafka_client import (
    MdrpConsumer,
    MdrpProducer,
    Topics,
    deserialise,
    ensure_topics,
)
from mdrp_common.logging import configure_logging, get_logger, set_trace_id
from mdrp_common.metrics import register_metrics
from mdrp_common.models import CurveEvent

from redis_writer.curve_store import CurveStore
from redis_writer.settings import RedisWriterSettings
from redis_writer.writer import RedisWriter

_SERVICE_NAME = "redis-writer"


def main() -> None:
    settings = RedisWriterSettings()

    configure_logging(_SERVICE_NAME, level=settings.log_level)
    log = get_logger(_SERVICE_NAME)

    register_metrics(_SERVICE_NAME, port=settings.metrics_port, version=settings.service_version)
    log.info("metrics_server_started", port=settings.metrics_port)

    # Ensure all platform topics exist
    ensure_topics(settings.kafka_bootstrap_servers)
    log.info("topics_verified")

    # Redis client
    redis_client: redis.Redis[Any] = redis.Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=settings.redis_connect_timeout,
        decode_responses=False,
    )
    try:
        redis_client.ping()
    except redis.exceptions.ConnectionError as exc:
        log.error("redis_connection_failed", error=str(exc))
        sys.exit(1)
    log.info("redis_connected", url=settings.redis_url)

    curve_store = CurveStore(
        redis_client=redis_client,
        curve_history_max_entries=settings.curve_history_max_entries,
        expected_tenors=settings.expected_tenors_per_instrument,
        snapshot_completeness_threshold=settings.snapshot_completeness_threshold,
        staleness_threshold_seconds=settings.staleness_threshold_seconds,
    )

    writer = RedisWriter(curve_store=curve_store)

    consumer = MdrpConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        topics=[Topics.NORMALIZED_EVENTS],
    )

    log.info(
        "consumer_started",
        topic=Topics.NORMALIZED_EVENTS,
        group=settings.kafka_consumer_group,
    )

    # Graceful shutdown on SIGTERM / SIGINT
    def _shutdown(signum: int, frame: Any) -> None:
        log.info("shutdown_signal_received", signum=signum)
        consumer.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        for msg in consumer.messages():
            _process_message(msg, writer, consumer, log)
    finally:
        # Final staleness check before exit
        writer.check_staleness_now()
        redis_client.close()
        log.info("shutdown_complete")


def _process_message(
    msg: Any,
    writer: RedisWriter,
    consumer: MdrpConsumer,
    log: Any,
) -> None:
    """Deserialise and persist one Kafka message, then commit offset."""
    try:
        event: CurveEvent = deserialise(msg, CurveEvent)  # type: ignore[assignment]
    except Exception as exc:
        log.error(
            "deserialisation_failed",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=str(exc),
        )
        # Commit the bad offset so we don't get stuck replaying a corrupt message
        consumer.commit(msg)
        return

    set_trace_id(event.trace_id)

    try:
        writer.handle_event(event)
    except Exception as exc:
        log.exception(
            "write_error",
            curve_name=event.curve_name,
            source_event_id=event.source_event_id,
            error=str(exc),
        )
        # Do not commit — allow retry on restart
        return

    consumer.commit(msg)


if __name__ == "__main__":
    main()
