"""
Normalization service entry point.

Consumes ValidatedMarketEvents from ``market.events.validated``, normalises
each event to the canonical CurveEvent schema, and publishes the result to
``market.events.normalized``.

Events that cannot be normalised (unknown instrument, missing price, or
unrecognised tenor) are logged and silently dropped — they already passed
validation, so DLQ routing is not appropriate here.
"""

from __future__ import annotations

import signal
import sys
from datetime import datetime, timezone
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
from mdrp_common.metrics import (
    EVENT_PROCESSING_LATENCY_SECONDS,
    EVENTS_NORMALIZED_TOTAL,
    QUALITY_SCORE,
    register_metrics,
)
from mdrp_common.models import ValidatedMarketEvent

from normalization_service.normalizer import Normalizer
from normalization_service.settings import NormalizationServiceSettings

_SERVICE_NAME = "normalization-service"


def main() -> None:
    settings = NormalizationServiceSettings()

    configure_logging(_SERVICE_NAME, level=settings.log_level)
    log = get_logger(_SERVICE_NAME)

    register_metrics(_SERVICE_NAME, port=settings.metrics_port, version=settings.service_version)
    log.info("metrics_server_started", port=settings.metrics_port)

    # Ensure all platform topics exist before subscribing
    ensure_topics(settings.kafka_bootstrap_servers)
    log.info("topics_verified")

    # Redis client for version counters
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

    normalizer = Normalizer(
        redis_client=redis_client,
        version_counter_ttl=settings.redis_version_counter_ttl,
    )

    producer = MdrpProducer(settings.kafka_bootstrap_servers)
    consumer = MdrpConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        topics=[Topics.VALIDATED_EVENTS],
    )

    log.info(
        "consumer_started",
        topic=Topics.VALIDATED_EVENTS,
        group=settings.kafka_consumer_group,
    )

    # Register a graceful SIGTERM/SIGINT handler that also stops the consumer
    def _shutdown(signum: int, frame: Any) -> None:
        log.info("shutdown_signal_received", signum=signum)
        consumer.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        for msg in consumer.messages():
            _process_message(msg, normalizer, producer, log)
            consumer.commit(msg)
    finally:
        log.info("flushing_producer")
        producer.flush()
        redis_client.close()
        log.info("shutdown_complete")


def _process_message(
    msg: Any,
    normalizer: Normalizer,
    producer: MdrpProducer,
    log: Any,
) -> None:
    """Deserialise, normalise, and publish one Kafka message."""
    try:
        event: ValidatedMarketEvent = deserialise(msg, ValidatedMarketEvent)  # type: ignore[assignment]
    except Exception as exc:
        log.error(
            "deserialisation_failed",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=str(exc),
        )
        return

    # Propagate trace ID into the logging context for this request
    set_trace_id(event.trace_id)

    try:
        curve_event = normalizer.normalise(event)
    except Exception as exc:
        log.exception(
            "normalisation_error",
            event_id=event.event_id,
            provider=event.provider,
            instrument=event.instrument,
            error=str(exc),
        )
        return

    if curve_event is None:
        # Already logged inside normaliser; nothing more to do
        return

    # Publish
    try:
        producer.produce(
            topic=Topics.NORMALIZED_EVENTS,
            value=curve_event,
            key=curve_event.curve_name,
            headers={
                "trace_id": event.trace_id,
                "source_event_id": event.event_id,
            },
        )
    except Exception as exc:
        log.error(
            "publish_failed",
            curve_name=curve_event.curve_name,
            source_event_id=event.event_id,
            error=str(exc),
        )
        return

    # Metrics
    EVENTS_NORMALIZED_TOTAL.labels(
        provider=curve_event.provider,
        instrument=curve_event.instrument,
    ).inc()

    QUALITY_SCORE.labels(provider=curve_event.provider).observe(
        curve_event.quality_score
    )

    latency = (
        datetime.now(timezone.utc) - event.event_timestamp
    ).total_seconds()
    EVENT_PROCESSING_LATENCY_SECONDS.labels(
        service=_SERVICE_NAME,
        provider=curve_event.provider,
    ).observe(latency)

    log.info(
        "curve_event_published",
        curve_name=curve_event.curve_name,
        tenor=curve_event.tenor,
        provider=curve_event.provider,
        instrument=curve_event.instrument,
        quality_score=curve_event.quality_score,
        version=curve_event.version,
        source_event_id=event.event_id,
    )


if __name__ == "__main__":
    main()
