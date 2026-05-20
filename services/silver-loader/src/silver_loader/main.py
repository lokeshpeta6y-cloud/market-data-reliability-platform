"""
Entry point for the silver-loader service.

Start-up sequence
-----------------
1.  Load settings from environment / .env file.
2.  Configure structured logging.
3.  Start Prometheus metrics HTTP server.
4.  Ensure Kafka topics exist (idempotent).
5.  If Snowflake is configured: build SnowflakeClient and connect.
    Otherwise: log a warning and continue with client=None (no-op flushes).
6.  Build SilverLoader (owns buffer + SnowflakeClient).
7.  Subscribe to market.events.normalized with consumer group ``silver-loader``.
8.  Start background flush timer thread.
9.  Enter the consume-buffer-flush loop.
10. On SIGTERM / SIGINT: drain the buffer, commit remaining offsets, exit cleanly.

Offset commit strategy
----------------------
Manual commits are used.  The offset for a message is committed only after
``loader.process(event)`` succeeds.  If a flush to Snowflake raises
``SnowflakeLoadError``, the offset is NOT committed so the message will be
re-delivered on the next startup (at-least-once delivery guarantee).

Background flush timer
----------------------
A daemon thread calls ``loader.flush()`` on the ``flush_interval_seconds``
cadence so that time-based flushing works even under low throughput.
"""

from __future__ import annotations

import signal
import sys
import threading

from mdrp_common.kafka_client import (
    MdrpConsumer,
    Topics,
    deserialise,
    ensure_topics,
)
from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import CONSUMER_LAG, register_metrics
from mdrp_common.models import CurveEvent

from .loader import SilverLoader
from .settings import SilverLoaderSettings
from .snowflake_client import SnowflakeClient, SnowflakeLoadError

logger = get_logger(__name__)

# Sample consumer lag every N messages
LAG_SAMPLE_INTERVAL = 100


def _build_snowflake_client(settings: SilverLoaderSettings) -> SnowflakeClient | None:
    """Return a connected SnowflakeClient, or None if Snowflake is not configured."""
    if not settings.snowflake_configured:
        logger.warning(
            "snowflake_not_configured",
            hint="Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD to enable Silver loading",
        )
        return None

    client = SnowflakeClient(
        account=settings.snowflake_account,  # type: ignore[arg-type]
        user=settings.snowflake_user,  # type: ignore[arg-type]
        password=settings.snowflake_password,  # type: ignore[arg-type]
        database=settings.snowflake_database,
        schema=settings.snowflake_schema_silver,
        warehouse=settings.snowflake_warehouse,
        stage_name=settings.snowflake_stage_name,
        max_reconnect_attempts=settings.snowflake_load_retries,
    )
    # Eagerly connect so we catch mis-configuration at startup, not on first flush
    try:
        client.connect()
    except Exception as exc:
        logger.error(
            "snowflake_initial_connect_failed",
            error=str(exc),
            hint="Service will continue but Silver loads are disabled until connection succeeds",
        )
        # Return the client anyway — it will attempt reconnection on load_batch
    return client


def _background_flush_thread(
    loader: SilverLoader,
    interval_seconds: float,
    shutdown_event: threading.Event,
) -> None:
    """
    Daemon thread that triggers a time-based flush even if batch_size is not reached.

    Runs until *shutdown_event* is set.
    """
    while not shutdown_event.wait(timeout=interval_seconds):
        if loader.buffer_size > 0:
            try:
                loader.flush()
            except SnowflakeLoadError as exc:
                logger.warning("background_flush_failed", error=str(exc))


def _update_lag_metrics(consumer: MdrpConsumer, group_id: str) -> None:
    """Sample consumer lag and publish to Prometheus."""
    try:
        lag = consumer.get_lag()
        for partition_key, lag_value in lag.items():
            topic_part, _, rest = partition_key.partition("[")
            partition_num = rest.rstrip("]")
            CONSUMER_LAG.labels(
                topic=topic_part,
                partition=partition_num,
                consumer_group=group_id,
            ).set(lag_value)
    except Exception as exc:
        logger.warning("lag_metrics_update_failed", error=str(exc))


def run_consumer_loop(
    consumer: MdrpConsumer,
    loader: SilverLoader,
    group_id: str,
    shutdown_event: threading.Event,
) -> None:
    """
    Main consume-buffer-flush loop.

    For each message:
      1. Deserialise to CurveEvent.
      2. Call loader.process(event) — may trigger an inline size-based flush.
      3. If process() succeeds (no SnowflakeLoadError raised), commit the offset.
      4. On SnowflakeLoadError: log and skip commit so the message is re-delivered.
      5. On deserialisation failure: log, commit anyway (message is unrecoverable).
    """
    processed = 0

    for msg in consumer.messages():
        if shutdown_event.is_set():
            break

        try:
            event = deserialise(msg, CurveEvent)
        except Exception as exc:
            logger.error(
                "curve_event_deserialise_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=str(exc),
            )
            # Unrecoverable — commit and move on
            consumer.commit(msg)
            continue

        assert isinstance(event, CurveEvent)

        try:
            loader.process(event)
        except SnowflakeLoadError as exc:
            logger.error(
                "silver_load_error_skipping_commit",
                event_id=event.event_id,
                provider=event.provider,
                error=str(exc),
            )
            # Do NOT commit — the buffer was already restored by loader.flush()
            continue

        consumer.commit(msg)
        processed += 1

        if processed % LAG_SAMPLE_INTERVAL == 0:
            _update_lag_metrics(consumer, group_id)


def main() -> None:
    settings = SilverLoaderSettings()

    configure_logging("silver-loader", level=settings.log_level)

    logger.info(
        "silver_loader_starting",
        kafka=settings.kafka_bootstrap_servers,
        batch_size=settings.batch_size,
        flush_interval=settings.flush_interval_seconds,
        metrics_port=settings.metrics_port,
        snowflake_configured=settings.snowflake_configured,
    )

    # Prometheus metrics
    register_metrics(
        service_name="silver-loader",
        port=settings.metrics_port,
        version=settings.service_version,
    )

    # Kafka topic provisioning (idempotent)
    ensure_topics(settings.kafka_bootstrap_servers)

    # Snowflake (optional)
    snowflake_client = _build_snowflake_client(settings)

    # SilverLoader owns the buffer
    loader = SilverLoader(
        snowflake_client=snowflake_client,
        batch_size=settings.batch_size,
        flush_interval_seconds=settings.flush_interval_seconds,
    )

    # Kafka consumer
    consumer = MdrpConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        topics=[Topics.NORMALIZED_EVENTS],
    )

    shutdown_event = threading.Event()

    def _handle_shutdown(signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        shutdown_event.set()
        consumer.shutdown()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    # Background time-based flush thread
    flush_thread = threading.Thread(
        target=_background_flush_thread,
        args=(loader, settings.flush_interval_seconds, shutdown_event),
        daemon=True,
        name="silver-flush-timer",
    )
    flush_thread.start()

    logger.info(
        "silver_loader_ready",
        topic=Topics.NORMALIZED_EVENTS,
        consumer_group=settings.kafka_consumer_group,
    )

    try:
        run_consumer_loop(
            consumer=consumer,
            loader=loader,
            group_id=settings.kafka_consumer_group,
            shutdown_event=shutdown_event,
        )
    finally:
        logger.info("silver_loader_shutting_down")
        shutdown_event.set()
        flush_thread.join(timeout=15.0)

        # Final drain — best-effort; errors are logged inside loader.close()
        loader.close()

        logger.info("silver_loader_stopped")
        sys.exit(0)


if __name__ == "__main__":
    main()
