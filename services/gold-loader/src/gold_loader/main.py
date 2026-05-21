"""
Entry point for the gold-loader service.

Start-up sequence
-----------------
1.  Load settings from environment / .env file.
2.  Configure structured logging.
3.  Start Prometheus metrics HTTP server.
4.  Ensure Kafka topics exist (idempotent).
5.  If Snowflake is configured: build SnowflakeClient and eagerly connect.
    Otherwise: log a warning and continue with client=None (no-op loads).
6.  Build SnapshotAssembler and GoldLoader.
7.  Subscribe to market.events.normalized with consumer group ``gold-loader``.
8.  Start background snapshot-flush polling thread.
9.  Enter the consume loop.
10. On SIGTERM / SIGINT: signal the shutdown event, drain pending snapshots,
    close Snowflake, exit cleanly.

Offset commit strategy
----------------------
Manual commits are used.  After ``loader.add(event)`` the offset is committed
immediately — buffering into the assembler is infallible.  Only if
``loader.flush_ready()`` raises ``SnowflakeLoadError`` do we halt progress
(that error originates in the background polling thread and is logged without
crashing; the next poll cycle will retry).

Background flush thread
-----------------------
A daemon thread calls ``loader.flush_ready()`` every ``poll_interval_seconds``.
Expired windows are loaded to Snowflake Gold.  The main consume thread never
blocks on a Snowflake call.
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

from .loader import GoldLoader
from .settings import GoldLoaderSettings
from .snapshot_assembler import SnapshotAssembler
from .snowflake_client import SnowflakeClient, SnowflakeLoadError

logger = get_logger(__name__)

LAG_SAMPLE_INTERVAL = 100


def _build_snowflake_client(settings: GoldLoaderSettings) -> SnowflakeClient | None:
    """Return a connected SnowflakeClient, or None if Snowflake is not configured."""
    if not settings.snowflake_configured:
        logger.warning(
            "snowflake_not_configured",
            hint=(
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PAT_TOKEN "
                "(or SNOWFLAKE_PASSWORD) to enable Gold loading"
            ),
        )
        return None

    client = SnowflakeClient(
        account=settings.snowflake_account,  # type: ignore[arg-type]
        user=settings.snowflake_user,  # type: ignore[arg-type]
        password=settings.snowflake_password,
        pat_token=settings.snowflake_pat_token,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema_gold,
        warehouse=settings.snowflake_warehouse,
        stage_name=settings.snowflake_stage_name,
        max_reconnect_attempts=settings.snowflake_load_retries,
    )
    try:
        client.connect()
    except Exception as exc:
        logger.error(
            "snowflake_initial_connect_failed",
            error=str(exc),
            hint="Service will continue but Gold loads are disabled until connection succeeds",
        )
    return client


def _background_flush_thread(
    loader: GoldLoader,
    poll_interval_seconds: float,
    shutdown_event: threading.Event,
) -> None:
    """
    Daemon thread that polls the assembler for expired windows and loads them
    to Snowflake Gold.

    Runs until *shutdown_event* is set.
    """
    while not shutdown_event.wait(timeout=poll_interval_seconds):
        try:
            rows = loader.flush_ready()
            if rows > 0:
                logger.debug("background_flush_loaded_rows", rows=rows)
        except SnowflakeLoadError as exc:
            # Log the error but do not crash — the next poll cycle will retry
            logger.error("background_gold_flush_failed", error=str(exc))

    # Final drain on shutdown
    logger.info("gold_loader_background_thread_final_drain")
    try:
        loader.flush_ready()
    except SnowflakeLoadError as exc:
        logger.error("gold_loader_final_drain_failed", error=str(exc))


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
    loader: GoldLoader,
    group_id: str,
    shutdown_event: threading.Event,
) -> None:
    """
    Main consume loop.

    For each message:
      1. Deserialise to CurveEvent.
      2. Call loader.add(event) — buffers into the assembler (infallible).
      3. Commit the offset.
      4. On deserialisation failure: log and commit anyway (unrecoverable).

    Snapshot assembly and Snowflake loading happen in the background thread.
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
            consumer.commit(msg)
            continue

        assert isinstance(event, CurveEvent)

        loader.add(event)
        consumer.commit(msg)
        processed += 1

        if processed % LAG_SAMPLE_INTERVAL == 0:
            _update_lag_metrics(consumer, group_id)


def main() -> None:
    settings = GoldLoaderSettings()

    configure_logging("gold-loader", level=settings.log_level)

    logger.info(
        "gold_loader_starting",
        kafka=settings.kafka_bootstrap_servers,
        snapshot_window_minutes=settings.snapshot_window_minutes,
        min_completeness=settings.min_completeness,
        min_quality_score=settings.min_quality_score,
        metrics_port=settings.metrics_port,
        snowflake_configured=settings.snowflake_configured,
    )

    # Prometheus metrics
    register_metrics(
        service_name="gold-loader",
        port=settings.metrics_port,
        version=settings.service_version,
    )

    # Kafka topic provisioning (idempotent)
    ensure_topics(settings.kafka_bootstrap_servers)

    # Snowflake (optional)
    snowflake_client = _build_snowflake_client(settings)

    # Snapshot assembler
    assembler = SnapshotAssembler(
        snapshot_window_minutes=settings.snapshot_window_minutes,
        min_completeness=settings.min_completeness,
        min_quality_score=settings.min_quality_score,
        expected_tenors_per_curve=settings.expected_tenors_per_curve,
    )

    # GoldLoader coordinates assembler + Snowflake
    loader = GoldLoader(
        assembler=assembler,
        snowflake_client=snowflake_client,
    )

    # Kafka consumer (separate group from silver-loader)
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

    # Background snapshot flush thread
    flush_thread = threading.Thread(
        target=_background_flush_thread,
        args=(loader, settings.poll_interval_seconds, shutdown_event),
        daemon=True,
        name="gold-flush-poller",
    )
    flush_thread.start()

    logger.info(
        "gold_loader_ready",
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
        logger.info("gold_loader_shutting_down")
        shutdown_event.set()
        # Wait for background thread to run its final drain
        flush_thread.join(timeout=30.0)
        loader.close()
        logger.info(
            "gold_loader_stopped",
            total_loaded=loader.total_loaded,
            total_snapshots=loader.total_snapshots_assembled,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
