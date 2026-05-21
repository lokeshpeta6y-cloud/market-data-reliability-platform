"""
Bronze S3 Replayer.

Reads Parquet files from the Bronze layer for a specified provider/time window,
reconstructs RawMarketEvent objects, sets replay flags, and publishes to the
REPLAY_EVENTS topic at a configurable rate limit.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from mdrp_common.kafka_client import MdrpProducer, Topics
from mdrp_common.logging import get_logger
from mdrp_common.metrics import REPLAY_EVENTS_TOTAL
from mdrp_common.models import RawMarketEvent, ReplayJob, ReplaySource
from mdrp_common.storage import BronzeStorageClient

log = get_logger(__name__)


def _parse_json_strings(record: dict[str, Any]) -> dict[str, Any]:
    """
    Convert JSON-string values back to dict/list.

    storage.py serialises dict/list columns to JSON strings so PyArrow can
    handle mixed-type columns (e.g. fault-injected ~~CORRUPTED~~ strings
    alongside numeric prices).  This reverses that serialisation so Pydantic
    can validate the reconstructed RawMarketEvent correctly.
    """
    out: dict[str, Any] = {}
    for k, v in record.items():
        if isinstance(v, str):
            stripped = v.strip()
            if stripped and stripped[0] in ("{", "["):
                try:
                    out[k] = json.loads(v)
                    continue
                except json.JSONDecodeError:
                    pass
        out[k] = v
    return out


class BronzeReplayer:
    """
    Replays market events stored in the Bronze (S3/MinIO) layer.

    Each Parquet record is reconstructed as a RawMarketEvent with
    ``is_replay=True`` and ``replay_source=ReplaySource.BRONZE_S3``,
    then published to the REPLAY_EVENTS topic.

    Rate limiting is implemented with a simple token-bucket style approach:
    after each batch of events we sleep long enough to stay at or below
    ``rate_limit_per_second``.
    """

    def __init__(
        self,
        storage_client: BronzeStorageClient,
        rate_limit_per_second: int = 1000,
    ) -> None:
        self._storage = storage_client
        self._rate_limit = rate_limit_per_second

    def replay(self, job: ReplayJob, producer: MdrpProducer) -> int:
        """
        Execute a Bronze replay job.

        Parameters
        ----------
        job:
            The replay job describing provider, instrument, and time window.
        producer:
            Kafka producer to publish reconstructed events onto.

        Returns
        -------
        int
            Total number of events published.
        """
        provider = job.provider or ""
        start_time = job.start_time
        end_time = job.end_time

        log.info(
            "bronze_replay_start",
            job_id=job.job_id,
            provider=provider,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
        )

        # Enumerate all Parquet partition keys in the time window
        partition_keys = self._storage.list_partitions(
            provider=provider,
            start=start_time,
            end=end_time,
        )

        if not partition_keys:
            log.warning(
                "bronze_replay_no_partitions",
                job_id=job.job_id,
                provider=provider,
            )
            return 0

        log.info(
            "bronze_replay_partitions_found",
            job_id=job.job_id,
            partition_count=len(partition_keys),
        )

        total_events = 0
        batch_start = time.monotonic()
        events_in_window = 0

        for key in partition_keys:
            records = self._storage.read_parquet_batch(key)

            for record in records:
                # Filter by instrument if specified
                if job.instrument and record.get("instrument") != job.instrument:
                    continue

                # Filter by event_timestamp within the job window
                event_ts = self._parse_datetime(record.get("event_timestamp"))
                if event_ts is not None:
                    if not (start_time <= event_ts <= end_time):
                        continue

                event = self._reconstruct_event(record)
                producer.produce(
                    topic=Topics.REPLAY_EVENTS,
                    value=event,
                    key=f"{event.provider}:{event.instrument}",
                )

                total_events += 1
                events_in_window += 1
                REPLAY_EVENTS_TOTAL.labels(source=ReplaySource.BRONZE_S3.value).inc()

                # Rate limiting: check elapsed time every 100 events
                if events_in_window >= 100:
                    elapsed = time.monotonic() - batch_start
                    expected_elapsed = events_in_window / self._rate_limit
                    if expected_elapsed > elapsed:
                        sleep_s = expected_elapsed - elapsed
                        time.sleep(sleep_s)
                    events_in_window = 0
                    batch_start = time.monotonic()

            log.debug(
                "bronze_replay_partition_done",
                job_id=job.job_id,
                key=key,
                total_so_far=total_events,
            )

        # Final flush to ensure all events are delivered
        producer.flush()

        log.info(
            "bronze_replay_complete",
            job_id=job.job_id,
            total_events=total_events,
        )
        return total_events

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _reconstruct_event(record: dict[str, Any]) -> RawMarketEvent:
        """
        Build a RawMarketEvent from a Parquet record dict.

        The record is expected to contain the fields written by the Bronze
        writer (which stores RawMarketEvent.model_dump() rows).  We override
        the replay-related fields and generate a fresh trace_id so downstream
        services can distinguish this event from the original.
        """
        import uuid

        # Strip replay metadata from original and override
        data = _parse_json_strings(dict(record))
        data["is_replay"] = True
        data["replay_source"] = ReplaySource.BRONZE_S3.value
        # New event_id so this replay event is distinct from the original
        data["event_id"] = str(uuid.uuid4())
        data["trace_id"] = str(uuid.uuid4())
        # Reset received_at to now so downstream latency metrics are meaningful
        data["received_at"] = datetime.now(UTC).isoformat()

        return RawMarketEvent.model_validate(data)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        try:
            dt = datetime.fromisoformat(str(value))
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except (ValueError, TypeError):
            return None
