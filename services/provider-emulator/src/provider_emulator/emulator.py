"""
Provider emulator orchestration.

ProviderEmulator is the central loop of the service.  On each tick it:

1. Generates (or ingests from Databento) a fresh batch of forward-curve events.
2. Passes events through the FaultInjector, which:
   - Immediately returns events that are not held in the delay queue.
   - May suppress events into the delay / OOO queue for later release.
3. Drains any previously-held events whose release time has arrived.
4. Publishes all ready events to the ``market.events.raw`` Redpanda topic.
5. Updates Prometheus counters and the structured log.

Graceful shutdown
-----------------
ProviderEmulator listens for SIGTERM and SIGINT.  When triggered, the loop
exits cleanly, any in-flight batches are flushed, and the Kafka producer is
closed with a 30-second flush timeout.

Databento vs. synthetic
-----------------------
If ``settings.databento_configured`` is True and the ``databento`` package is
importable, the emulator uses DatabentoAdapter for the configured instruments.
Instruments not mapped in Databento fall back to MarketDataGenerator.  If
Databento is not configured at all, the synthetic generator handles everything.
"""

from __future__ import annotations

import signal
import threading
import time
from datetime import datetime, timezone

from mdrp_common.kafka_client import MdrpProducer, Topics
from mdrp_common.logging import get_logger
from mdrp_common.metrics import (
    EVENTS_INGESTED_TOTAL,
    EVENTS_PUBLISHED_TOTAL,
    FAULTS_INJECTED_TOTAL,
    PROVIDER_LAST_EVENT_TIMESTAMP,
)
from mdrp_common.models import RawMarketEvent

from provider_emulator.databento_adapter import DatabentoAdapter, DatabentoAdapterError
from provider_emulator.fault_injector import FaultInjector
from provider_emulator.market_data_generator import MarketDataGenerator
from provider_emulator.settings import EmulatorSettings

logger = get_logger(__name__)

# Prometheus gauge for delay queue depth (emulator-specific)
from prometheus_client import Gauge

DELAY_QUEUE_DEPTH = Gauge(
    "mdrp_emulator_delay_queue_depth",
    "Number of events currently held in the delay queue",
)
OOO_QUEUE_DEPTH = Gauge(
    "mdrp_emulator_ooo_queue_depth",
    "Number of events currently held in the out-of-order queue",
)
PUBLISH_CYCLE_DURATION = Gauge(
    "mdrp_emulator_publish_cycle_duration_seconds",
    "Wall-clock time of the last publish cycle",
)


class ProviderEmulator:
    """
    Orchestrates synthetic + real data generation, fault injection, and Kafka publishing.

    Parameters
    ----------
    settings : EmulatorSettings
        Fully resolved service settings (from environment / .env file).
    """

    def __init__(self, settings: EmulatorSettings) -> None:
        self._settings = settings
        self._shutdown_event = threading.Event()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Kafka producer
        self._producer = MdrpProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        logger.info(
            "kafka_producer_created",
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )

        # Fault injector
        self._fault_injector = FaultInjector(
            fault_rate_duplicate=settings.fault_rate_duplicate,
            fault_rate_malformed=settings.fault_rate_malformed,
            fault_rate_delayed=settings.fault_rate_delayed,
            fault_rate_out_of_order=settings.fault_rate_out_of_order,
            fault_rate_schema_drift=settings.fault_rate_schema_drift,
            fault_rate_stale=settings.fault_rate_stale,
            fault_rate_partial_curve=settings.fault_rate_partial_curve,
            delay_min_seconds=settings.delay_min_seconds,
            delay_max_seconds=settings.delay_max_seconds,
            delay_queue_max_size=settings.delay_queue_max_size,
        )

        # Data source: Databento (optional) + synthetic fallback
        self._databento_adapter: DatabentoAdapter | None = None
        self._generator = MarketDataGenerator(
            instruments=settings.instruments,
            provider_name=settings.provider_name,
        )

        if settings.databento_configured:
            try:
                self._databento_adapter = DatabentoAdapter(
                    api_key=settings.databento_api_key,  # type: ignore[arg-type]
                    dataset=settings.databento_dataset,
                    lookback_days=settings.databento_lookback_days,
                    provider_name=settings.provider_name,
                    instruments=settings.instruments,
                )
                logger.info("databento_adapter_enabled")
            except DatabentoAdapterError as exc:
                logger.warning(
                    "databento_adapter_unavailable_falling_back_to_synthetic",
                    error=str(exc),
                )

        # Cycle counter — used only for structured logging; wraps at 2^63.
        self._cycle: int = 0

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """
        Run the emulator loop until shutdown is requested.

        On each iteration:
        1. Generate / ingest clean events.
        2. Apply fault injection.
        3. Drain the hold queues.
        4. Publish all ready events.
        5. Sleep for the configured interval (minus elapsed time).
        """
        logger.info(
            "emulator_starting",
            instruments=self._settings.instruments,
            publish_interval_seconds=self._settings.publish_interval_seconds,
            databento_enabled=self._databento_adapter is not None,
        )

        # On the first cycle, pre-warm the Databento adapter (if configured)
        # so we have real data immediately.
        databento_events: list[RawMarketEvent] = []
        if self._databento_adapter is not None:
            databento_events = self._fetch_databento_safe()

        while not self._shutdown_event.is_set():
            cycle_start = time.monotonic()
            self._cycle += 1

            try:
                self._run_cycle(databento_events)
            except Exception as exc:
                logger.exception(
                    "publish_cycle_error",
                    cycle=self._cycle,
                    error=str(exc),
                )

            # Refresh Databento data every 10 cycles (avoids hammering the API)
            if self._databento_adapter is not None and self._cycle % 10 == 0:
                databento_events = self._fetch_databento_safe()

            elapsed = time.monotonic() - cycle_start
            PUBLISH_CYCLE_DURATION.set(elapsed)

            sleep_for = max(0.0, self._settings.publish_interval_seconds - elapsed)
            if sleep_for > 0:
                self._shutdown_event.wait(timeout=sleep_for)

        self._shutdown()

    # ------------------------------------------------------------------ #
    # Single cycle
    # ------------------------------------------------------------------ #

    def _run_cycle(self, databento_events: list[RawMarketEvent]) -> None:
        """Execute one publish cycle."""
        # Step 1: collect clean events
        clean_events = self._collect_events(databento_events)

        # Step 2: per-instrument fault injection (batch-level faults like
        # PARTIAL_CURVE require the events to be grouped by instrument).
        ready_events: list[RawMarketEvent] = []
        for instrument in self._settings.instruments:
            instrument_batch = [e for e in clean_events if e.instrument == instrument]
            if not instrument_batch:
                continue

            injected_batch = self._fault_injector.inject(instrument_batch)
            ready_events.extend(injected_batch)

            # Count ingested (pre-fault) events
            for event in instrument_batch:
                EVENTS_INGESTED_TOTAL.labels(
                    provider=event.provider,
                    instrument=event.instrument,
                ).inc()

        # Step 3: drain held events
        drained = self._fault_injector.drain_ready()
        ready_events.extend(drained)

        # Step 4: publish
        published_count = 0
        for event in ready_events:
            self._publish_event(event)
            published_count += 1

        # Step 5: update queue depth gauges
        DELAY_QUEUE_DEPTH.set(self._fault_injector.delay_queue_depth)
        OOO_QUEUE_DEPTH.set(self._fault_injector.ooo_queue_depth)

        logger.info(
            "publish_cycle_complete",
            cycle=self._cycle,
            clean_events=len(clean_events),
            published=published_count,
            drained=len(drained),
            delay_queue=self._fault_injector.delay_queue_depth,
            ooo_queue=self._fault_injector.ooo_queue_depth,
        )

    # ------------------------------------------------------------------ #
    # Event collection
    # ------------------------------------------------------------------ #

    def _collect_events(
        self, databento_events: list[RawMarketEvent]
    ) -> list[RawMarketEvent]:
        """
        Combine Databento events (if any) with synthetic generator output.

        For instruments that have a Databento event in the current cache, use
        the real data.  For all others, use the synthetic generator.
        """
        if not databento_events:
            return self._generator.generate_all_batches()

        # Determine which instruments are covered by Databento
        databento_instruments = {e.instrument for e in databento_events}
        synthetic_instruments = [
            inst
            for inst in self._settings.instruments
            if inst not in databento_instruments
        ]

        events: list[RawMarketEvent] = list(databento_events)
        for instrument in synthetic_instruments:
            events.extend(self._generator.generate_curve_batch(instrument))

        return events

    # ------------------------------------------------------------------ #
    # Databento refresh
    # ------------------------------------------------------------------ #

    def _fetch_databento_safe(self) -> list[RawMarketEvent]:
        """
        Fetch from Databento, returning an empty list on any error.

        Errors are logged at WARNING level so we fall back to synthetic data
        rather than crashing the emulator.
        """
        if self._databento_adapter is None:
            return []
        try:
            events = self._databento_adapter.fetch_events()
            logger.info(
                "databento_refresh_complete",
                event_count=len(events),
            )
            return events
        except DatabentoAdapterError as exc:
            logger.warning(
                "databento_refresh_failed",
                error=str(exc),
            )
            return []

    # ------------------------------------------------------------------ #
    # Publishing
    # ------------------------------------------------------------------ #

    def _publish_event(self, event: RawMarketEvent) -> None:
        """Publish a single RawMarketEvent to the raw events topic."""
        # Kafka key = instrument so events for the same instrument land on
        # the same partition (preserves ordering within an instrument).
        self._producer.produce(
            topic=Topics.RAW_EVENTS,
            value=event,
            key=event.instrument,
            headers={
                "trace_id": event.trace_id,
                "provider": event.provider,
                "has_faults": "1" if event.injected_faults else "0",
            },
        )

        EVENTS_PUBLISHED_TOTAL.labels(
            topic=Topics.RAW_EVENTS,
            provider=event.provider,
        ).inc()

        PROVIDER_LAST_EVENT_TIMESTAMP.labels(provider=event.provider).set(
            datetime.now(timezone.utc).timestamp()
        )

        if event.injected_faults:
            logger.debug(
                "event_published_with_faults",
                event_id=event.event_id,
                instrument=event.instrument,
                faults=[f.value for f in event.injected_faults],
            )

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #

    def _shutdown(self) -> None:
        """Flush the producer and release resources."""
        logger.info("emulator_shutting_down")
        try:
            self._producer.flush(timeout_s=30.0)
        except Exception as exc:
            logger.error("producer_flush_error_during_shutdown", error=str(exc))
        logger.info("emulator_stopped")

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info("shutdown_signal_received", signum=signum)
        self._shutdown_event.set()
