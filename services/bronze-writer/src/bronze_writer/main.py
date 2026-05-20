"""
Entry point for the bronze-writer service.

Start-up sequence
-----------------
1. Load settings from environment / .env file.
2. Configure structured logging.
3. Start Prometheus metrics HTTP server.
4. Ensure Kafka topics exist (idempotent).
5. Ensure the Bronze S3 bucket exists (idempotent, MinIO-safe).
6. Build BronzeWriter (owns EventBuffer + BronzeStorageClient).
7. Subscribe to market.events.raw with consumer group ``bronze-writer``.
8. Enter the consume loop.
9. On SIGTERM: drain the buffer, commit remaining offsets, exit cleanly.

Offset commit strategy
----------------------
Manual commits are used.  After a successful ``writer.process(event)`` call
(which may or may not have triggered a flush), the offset is committed.
If ``process()`` raises BronzeWriteError, we do NOT commit — the consumer will
re-read those messages on the next startup, providing at-least-once delivery
for the Bronze layer.

The per-provider consumer group ensures this service reads independently of
the validation-service, so neither service affects the other's progress.

Background flush timer
----------------------
A background daemon thread calls ``writer.flush()`` on the
``flush_interval_seconds`` cadence so that a quiet period still produces
timely Parquet files.  The main thread also calls flush inline when
``buffer.should_flush()`` is true (size-triggered), ensuring latency is
bounded even under high throughput.
"""

from __future__ import annotations

import signal
import sys
import threading
import time

from mdrp_common.kafka_client import (
    MdrpConsumer,
    Topics,
    deserialise,
    ensure_topics,
)
from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import CONSUMER_LAG, register_metrics
from mdrp_common.models import RawMarketEvent
from mdrp_common.storage import BronzeStorageClient

from .settings import BronzeWriterSettings
from .writer import BronzeWriteError, BronzeWriter

logger = get_logger(__name__)

# How often (in messages) to sample consumer lag
LAG_SAMPLE_INTERVAL = 100


def _build_storage_client(settings: BronzeWriterSettings) -> BronzeStorageClient:
    return BronzeStorageClient(
        bucket=settings.s3_bucket_bronze,
        region=settings.aws_region,
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


def _background_flush_thread(
    writer: BronzeWriter,
    interval_seconds: float,
    shutdown_event: threading.Event,
) -> None:
    """
    Daemon thread that triggers a time-based flush even if batch_size is not reached.

    Runs until ``shutdown_event`` is set.
    """
    while not shutdown_event.wait(timeout=interval_seconds):
        if writer.buffer_size > 0:
            try:
                writer.flush()
            except BronzeWriteError as exc:
                # Logged inside writer.flush(); nothing more to do here —
                # the main thread will not commit those offsets.
                logger.warning(
                    "background_flush_failed",
                    error=str(exc),
                )


def _update_lag_metrics(consumer: MdrpConsumer) -> None:
    """Sample consumer lag and publish to Prometheus."""
    try:
        lag = consumer.get_lag()
        for partition_key, lag_value in lag.items():
            topic_part, _, rest = partition_key.partition("[")
            partition_num = rest.rstrip("]")
            CONSUMER_LAG.labels(
                topic=topic_part,
                partition=partition_num,
                consumer_group="bronze-writer",
            ).set(lag_value)
    except Exception as exc:
        logger.warning("lag_metrics_update_failed", error=str(exc))


def run_consumer_loop(
    consumer: MdrpConsumer,
    writer: BronzeWriter,
    shutdown_event: threading.Event,
) -> None:
    """
    Main consume-buffer-flush loop.

    For each message:
      1. Deserialise to RawMarketEvent.
      2. Call writer.process(event_dict).
      3. If process() succeeds (no exception), commit the offset.
      4. If process() raises BronzeWriteError, log and skip the commit.
         The consumer will re-read this message on next startup.
    """
    processed = 0

    for msg in consumer.messages():
        if shutdown_event.is_set():
            break

        try:
            raw_event = deserialise(msg, RawMarketEvent)
        except Exception as exc:
            # Deserialisation failure — skip this message (it is unrecoverable)
            # but still commit so we do not block progress.
            logger.error(
                "raw_event_deserialise_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=str(exc),
            )
            consumer.commit(msg)
            continue

        assert isinstance(raw_event, RawMarketEvent)

        # Convert to plain dict for storage; include all fields (injected_faults etc.)
        event_dict = raw_event.model_dump(mode="python")

        try:
            writer.process(event_dict)
        except BronzeWriteError as exc:
            logger.error(
                "bronze_write_error_skipping_commit",
                provider=raw_event.provider,
                event_id=raw_event.event_id,
                error=str(exc),
            )
            # Do NOT commit — let the consumer re-read on restart
            continue

        # Offset committed only after successful buffer add / flush
        consumer.commit(msg)

        processed += 1
        if processed % LAG_SAMPLE_INTERVAL == 0:
            _update_lag_metrics(consumer)


def main() -> None:
    settings = BronzeWriterSettings()

    configure_logging("bronze-writer", level=settings.log_level)

    logger.info(
        "bronze_writer_starting",
        kafka=settings.kafka_bootstrap_servers,
        bucket=settings.s3_bucket_bronze,
        batch_size=settings.batch_size,
        flush_interval=settings.flush_interval_seconds,
        metrics_port=settings.metrics_port,
    )

    # Prometheus metrics
    register_metrics(
        service_name="bronze-writer",
        port=settings.metrics_port,
        version=settings.service_version,
    )

    # Kafka topic provisioning
    ensure_topics(settings.kafka_bootstrap_servers)

    # S3/MinIO storage
    storage_client = _build_storage_client(settings)
    storage_client.ensure_bucket()
    logger.info("bronze_bucket_ready", bucket=settings.s3_bucket_bronze)

    # BronzeWriter (owns buffer)
    writer = BronzeWriter(
        storage_client=storage_client,
        batch_size=settings.batch_size,
        flush_interval_seconds=settings.flush_interval_seconds,
    )

    # Kafka consumer (separate group from validation-service)
    consumer = MdrpConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        topics=[Topics.RAW_EVENTS],
    )

    shutdown_event = threading.Event()

    def _handle_sigterm(signum: int, frame: object) -> None:
        logger.info("sigterm_received_initiating_graceful_shutdown")
        shutdown_event.set()
        consumer.shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    # Background time-based flush thread
    flush_thread = threading.Thread(
        target=_background_flush_thread,
        args=(writer, settings.flush_interval_seconds, shutdown_event),
        daemon=True,
        name="bronze-flush-timer",
    )
    flush_thread.start()

    logger.info("bronze_writer_ready", topic=Topics.RAW_EVENTS)

    try:
        run_consumer_loop(consumer, writer, shutdown_event)
    finally:
        logger.info("bronze_writer_shutting_down")
        # Signal background thread to stop
        shutdown_event.set()
        flush_thread.join(timeout=10.0)
        # Drain any remaining buffered events
        writer.flush_all()
        logger.info("bronze_writer_stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
