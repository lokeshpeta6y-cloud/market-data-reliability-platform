"""
Entry point for the validation-service.

Start-up sequence
-----------------
1. Load settings from environment / .env file.
2. Configure structured logging.
3. Start Prometheus metrics HTTP server.
4. Ensure Kafka topics exist (idempotent).
5. Connect to Redis.
6. Build ValidationService with Deduplicator and QualityScorer.
7. Subscribe to market.events.raw and enter the consume loop.
8. On SIGTERM: stop the consumer loop, flush the producer, close Redis.

Offset commit strategy
----------------------
Manual commits are used so that a crash between consuming a message and
producing the outcome does not silently discard the event.  We commit only
after both the outcome (validated or DLQ) has been produced to Kafka.

Consumer lag is sampled every ``LAG_SAMPLE_INTERVAL`` messages and exposed
as a Prometheus Gauge.
"""

from __future__ import annotations

import signal
import sys
import threading

import redis

from mdrp_common.kafka_client import (
    MdrpConsumer,
    MdrpProducer,
    Topics,
    deserialise,
    ensure_topics,
)
from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import CONSUMER_LAG, register_metrics
from mdrp_common.models import RawMarketEvent

from .deduplicator import Deduplicator
from .quality_scorer import QualityScorer
from .settings import ValidationServiceSettings
from .validator import ValidationService

logger = get_logger(__name__)

# How often (in messages processed) to sample consumer lag
LAG_SAMPLE_INTERVAL = 100


def build_redis_client(settings: ValidationServiceSettings) -> redis.Redis:
    """Return a connected Redis client with sensible timeout defaults."""
    client: redis.Redis = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )
    client.ping()  # Fast-fail at startup if Redis is unreachable
    return client


def run_consumer_loop(
    consumer: MdrpConsumer,
    producer: MdrpProducer,
    validation_service: ValidationService,
    shutdown_event: threading.Event,
) -> None:
    """
    Main consume-validate-produce loop.

    Each message is processed synchronously:
      - Deserialise to RawMarketEvent
      - Run all validation rules
      - Produce to VALIDATED_EVENTS or DLQ_EVENTS
      - Commit the offset

    On deserialisation failure (malformed JSON / missing fields) the message
    is sent to the DLQ as a raw bytes dict to preserve evidence.
    """
    processed = 0

    for msg in consumer.messages():
        if shutdown_event.is_set():
            break

        try:
            raw_event = deserialise(msg, RawMarketEvent)
        except Exception as exc:
            # Deserialisation failed — produce a minimal DLQ entry and commit
            logger.error(
                "raw_event_deserialise_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=str(exc),
            )
            _produce_deserialise_failure_dlq(producer, msg, exc)
            consumer.commit(msg)
            continue

        assert isinstance(raw_event, RawMarketEvent)  # narrow type for mypy

        try:
            validated_event, dlq_event = validation_service.validate(raw_event)
        except Exception as exc:
            # Unexpected validation error — send to DLQ to avoid message loss
            logger.exception(
                "validation_unexpected_error",
                event_id=raw_event.event_id,
                provider=raw_event.provider,
                error=str(exc),
            )
            _produce_unexpected_failure_dlq(producer, raw_event, exc)
            consumer.commit(msg)
            continue

        if validated_event is not None:
            producer.produce(
                topic=Topics.VALIDATED_EVENTS,
                value=validated_event,
                key=raw_event.provider,
                headers={"trace_id": raw_event.trace_id},
            )
        elif dlq_event is not None:
            producer.produce(
                topic=Topics.DLQ_EVENTS,
                value=dlq_event,
                key=raw_event.provider,
                headers={
                    "trace_id": raw_event.trace_id,
                    "failure_category": dlq_event.failure_category.value,
                },
            )
        # (None, None) → duplicate, silently discard — no produce needed

        # Commit offset after successful produce
        consumer.commit(msg)

        processed += 1
        if processed % LAG_SAMPLE_INTERVAL == 0:
            _update_lag_metrics(consumer)


def _produce_deserialise_failure_dlq(
    producer: MdrpProducer,
    msg: object,
    exc: Exception,
) -> None:
    """Produce a minimal DLQ entry when we cannot even parse the raw message."""
    from datetime import datetime, timezone

    from mdrp_common.models import DLQEvent, DLQFailureCategory

    raw_value: bytes | None = msg.value() if hasattr(msg, "value") else None  # type: ignore[union-attr]
    producer.produce(
        topic=Topics.DLQ_EVENTS,
        value=DLQEvent(
            original_event_id="unknown",
            provider="unknown",
            instrument=None,
            failure_reason=f"Deserialisation error: {exc}",
            failure_category=DLQFailureCategory.SCHEMA_VIOLATION,
            raw_payload={"raw_bytes": (raw_value or b"").decode("utf-8", errors="replace")},
            original_received_at=datetime.now(timezone.utc),
            trace_id="unknown",
        ),
    )


def _produce_unexpected_failure_dlq(
    producer: MdrpProducer,
    event: RawMarketEvent,
    exc: Exception,
) -> None:
    """Produce a DLQ entry when validation raises an unexpected exception."""
    from mdrp_common.models import DLQEvent, DLQFailureCategory

    producer.produce(
        topic=Topics.DLQ_EVENTS,
        value=DLQEvent(
            original_event_id=event.event_id,
            provider=event.provider,
            instrument=event.instrument,
            failure_reason=f"Unexpected validation error: {exc}",
            failure_category=DLQFailureCategory.UNKNOWN,
            raw_payload=event.model_dump(mode="python"),
            original_received_at=event.received_at,
            trace_id=event.trace_id,
        ),
    )


def _update_lag_metrics(consumer: MdrpConsumer) -> None:
    """Sample consumer lag and update the Prometheus Gauge."""
    try:
        lag = consumer.get_lag()
        for partition_key, lag_value in lag.items():
            # partition_key format: "topic[partition_number]"
            topic_part, _, rest = partition_key.partition("[")
            partition_num = rest.rstrip("]")
            CONSUMER_LAG.labels(
                topic=topic_part,
                partition=partition_num,
                consumer_group="validation-service",
            ).set(lag_value)
    except Exception as exc:
        logger.warning("lag_metrics_update_failed", error=str(exc))


def main() -> None:
    settings = ValidationServiceSettings()

    configure_logging("validation-service", level=settings.log_level)

    logger.info(
        "validation_service_starting",
        kafka=settings.kafka_bootstrap_servers,
        redis=settings.redis_url,
        metrics_port=settings.metrics_port,
    )

    # Prometheus metrics
    register_metrics(
        service_name="validation-service",
        port=settings.metrics_port,
        version=settings.service_version,
    )

    # Kafka topic provisioning
    ensure_topics(settings.kafka_bootstrap_servers)

    # Redis
    redis_client = build_redis_client(settings)
    logger.info("redis_connected", url=settings.redis_url)

    # Build service components
    deduplicator = Deduplicator(redis_client, ttl_seconds=settings.dedup_ttl_seconds)
    quality_scorer = QualityScorer(
        redis_client, rolling_window=settings.quality_rolling_window
    )
    validation_service = ValidationService(settings, deduplicator, quality_scorer)

    # Kafka producer and consumer
    producer = MdrpProducer(settings.kafka_bootstrap_servers)
    consumer = MdrpConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        topics=[Topics.RAW_EVENTS],
    )

    # Shutdown coordination
    shutdown_event = threading.Event()

    def _handle_sigterm(signum: int, frame: object) -> None:
        logger.info("sigterm_received_initiating_graceful_shutdown")
        shutdown_event.set()
        consumer.shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    logger.info("validation_service_ready", topic=Topics.RAW_EVENTS)

    try:
        run_consumer_loop(consumer, producer, validation_service, shutdown_event)
    finally:
        logger.info("validation_service_shutting_down")
        producer.flush(timeout_s=30.0)
        redis_client.close()
        logger.info("validation_service_stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
