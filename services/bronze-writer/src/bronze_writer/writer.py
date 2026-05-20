"""
BronzeWriter — coordinates the EventBuffer and BronzeStorageClient.

Responsibility:
  - Accept raw event dicts via ``process()``.
  - Add each event to the in-memory EventBuffer.
  - When the buffer triggers a flush (size or age), serialise the batch to
    Parquet and write it to S3/MinIO via BronzeStorageClient.
  - Track per-provider batches so that a single flush containing events from
    multiple providers writes one Parquet file per provider (matching the
    Bronze partition scheme: bronze/{provider}/{YYYY-MM-DD}/{HH}/events_{id}.parquet).
  - Expose a ``flush_all()`` method that the consumer loop calls before
    shutdown to drain remaining events.

Commit safety
-------------
``process()`` returns True if the event was safely flushed (all writes
succeeded) or added to the buffer (no flush needed yet).  It raises
BronzeWriteError if a flush was triggered and the S3 write failed, which
signals to the consumer loop that it must NOT commit the offset.

On a failed flush, the buffer events are put back so they can be retried
when the service restarts and re-reads uncommitted offsets.

Metrics
-------
  mdrp_bronze_writes_total          – success / failed counter per provider
  mdrp_bronze_write_duration_seconds – histogram of write wall-clock time
  mdrp_bronze_bytes_written_total    – bytes written counter per provider
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from mdrp_common.logging import get_logger
from mdrp_common.metrics import (
    BRONZE_BYTES_WRITTEN_TOTAL,
    BRONZE_WRITES_TOTAL,
    BRONZE_WRITE_DURATION_SECONDS,
)
from mdrp_common.storage import BronzeStorageClient

from .buffer import EventBuffer

logger = get_logger(__name__)


class BronzeWriteError(Exception):
    """Raised when a Parquet write to S3/MinIO fails."""


class BronzeWriter:
    """
    Owns an EventBuffer and a BronzeStorageClient.

    Parameters
    ----------
    storage_client:
        Configured BronzeStorageClient pointed at the Bronze bucket.
    batch_size:
        Forwarded to EventBuffer.
    flush_interval_seconds:
        Forwarded to EventBuffer.
    """

    def __init__(
        self,
        storage_client: BronzeStorageClient,
        batch_size: int = 500,
        flush_interval_seconds: float = 30.0,
    ) -> None:
        self._storage = storage_client
        self._buffer = EventBuffer(
            batch_size=batch_size,
            flush_interval_seconds=flush_interval_seconds,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, event: dict[str, Any]) -> None:
        """
        Add *event* to the buffer and flush if the threshold is reached.

        Parameters
        ----------
        event:
            Plain dict from RawMarketEvent.model_dump(mode="python").

        Raises
        ------
        BronzeWriteError
            If a flush was triggered and one or more S3 writes failed.
            In this case the buffer contents are restored so the caller
            can avoid committing offsets.
        """
        self._buffer.add(event)

        if self._buffer.should_flush():
            self.flush()

    def flush(self) -> None:
        """
        Drain the buffer and write all events to S3.

        Events are grouped by provider — each provider gets its own Parquet
        file per flush so that the Bronze partition scheme is respected.

        Raises
        ------
        BronzeWriteError
            If any S3 write fails.  All failed events are restored to the
            buffer (prepended) so they will be retried on the next flush.
        """
        batch = self._buffer.drain()
        if not batch:
            return

        logger.info(
            "bronze_flush_started",
            batch_size=len(batch),
            buffer_age_seconds=self._buffer.age_seconds(),
        )

        # Group by provider
        by_provider: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in batch:
            provider = event.get("provider", "unknown")
            by_provider[provider].append(event)

        failed_events: list[dict[str, Any]] = []

        for provider, provider_events in by_provider.items():
            try:
                self._write_provider_batch(provider, provider_events)
            except Exception as exc:
                logger.error(
                    "bronze_write_failed",
                    provider=provider,
                    batch_size=len(provider_events),
                    error=str(exc),
                )
                BRONZE_WRITES_TOTAL.labels(provider=provider, outcome="failed").inc()
                failed_events.extend(provider_events)

        if failed_events:
            # Restore failed events to the front of the buffer for retry
            self._restore_events(failed_events)
            raise BronzeWriteError(
                f"{len(failed_events)} events failed to write to S3 across "
                f"{len(set(e.get('provider', 'unknown') for e in failed_events))} provider(s)"
            )

    def flush_all(self) -> None:
        """
        Flush all remaining buffered events.

        Called during graceful shutdown.  Unlike ``flush()``, this does not
        raise on failure — it logs the error and returns so the process can
        exit cleanly.  Uncommitted offsets will be re-read on next startup.
        """
        if self._buffer.size() == 0:
            return

        logger.info(
            "bronze_flush_all_on_shutdown",
            remaining_events=self._buffer.size(),
        )
        try:
            self.flush()
        except BronzeWriteError as exc:
            logger.error(
                "bronze_flush_all_failed_on_shutdown",
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Properties (for observability from the consumer loop)
    # ------------------------------------------------------------------

    @property
    def buffer_size(self) -> int:
        return self._buffer.size()

    @property
    def buffer_age_seconds(self) -> float:
        return self._buffer.age_seconds()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _write_provider_batch(
        self, provider: str, events: list[dict[str, Any]]
    ) -> None:
        """Write a single provider's events to Parquet and record metrics."""
        # Use the timestamp of the first event in the batch for partitioning
        flush_ts = self._extract_batch_timestamp(events)

        start = time.perf_counter()
        key = self._storage.write_parquet_batch(
            records=events,
            provider=provider,
            timestamp=flush_ts,
        )
        elapsed = time.perf_counter() - start

        # Estimate written bytes: re-read the object metadata is expensive,
        # so we approximate using the serialised dict sizes.
        approx_bytes = sum(len(str(e)) for e in events)

        BRONZE_WRITES_TOTAL.labels(provider=provider, outcome="success").inc()
        BRONZE_WRITE_DURATION_SECONDS.observe(elapsed)
        BRONZE_BYTES_WRITTEN_TOTAL.labels(provider=provider).inc(approx_bytes)

        logger.info(
            "bronze_batch_written",
            provider=provider,
            key=key,
            record_count=len(events),
            approx_bytes=approx_bytes,
            duration_seconds=round(elapsed, 3),
        )

    def _restore_events(self, events: list[dict[str, Any]]) -> None:
        """Put events back into the buffer (used after a failed flush)."""
        # Re-add in original order — they go to the back but that is
        # acceptable since the offset will not be committed anyway.
        for event in events:
            self._buffer.add(event)

    @staticmethod
    def _extract_batch_timestamp(events: list[dict[str, Any]]) -> datetime:
        """
        Return a representative timestamp for the batch, used for S3 partitioning.

        Uses the received_at of the first event; falls back to UTC now.
        """
        first = events[0]
        received_at = first.get("received_at")

        if isinstance(received_at, datetime):
            return received_at if received_at.tzinfo else received_at.replace(tzinfo=timezone.utc)

        if isinstance(received_at, str):
            try:
                dt = datetime.fromisoformat(received_at)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        return datetime.now(timezone.utc)
