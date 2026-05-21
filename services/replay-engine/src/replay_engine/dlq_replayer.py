"""
DLQ Replayer.

Consumes events from the Dead-Letter Queue for a specified time window and
re-emits them to the RAW_EVENTS topic so they are processed through the full
validation/normalisation pipeline again.

This mode is used after a bug-fix deployment to recover events that previously
failed validation or normalisation.

Offset seeking strategy
-----------------------
confluent-kafka does not expose a timestamp-based seek via the high-level
consumer directly, so we use ``offsets_for_times`` to find the partition
offsets corresponding to the replay window's start timestamp, assign those
partitions manually (bypassing group rebalancing), seek to the correct offset,
and consume until we reach a message whose timestamp exceeds end_time.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from mdrp_common.kafka_client import MdrpProducer, Topics
from mdrp_common.logging import get_logger
from mdrp_common.metrics import REPLAY_EVENTS_TOTAL
from mdrp_common.models import DLQEvent, RawMarketEvent, ReplayJob, ReplaySource

log = get_logger(__name__)


class DLQReplayer:
    """
    Time-bounded DLQ consumer that re-publishes events to RAW_EVENTS.

    Re-uses the raw payload from DLQEvent so that the re-processed event
    goes through the exact same validation path as a live event would.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        rate_limit_per_second: int = 1000,
        max_messages: int = 500_000,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group = consumer_group
        self._rate_limit = rate_limit_per_second
        self._max_messages = max_messages

    def replay(self, job: ReplayJob, producer: MdrpProducer) -> int:
        """
        Execute a DLQ replay job.

        Parameters
        ----------
        job:
            Job containing the time window to replay from the DLQ.
        producer:
            Kafka producer targeting RAW_EVENTS.

        Returns
        -------
        int
            Number of events re-published.
        """
        start_ts_ms = int(job.start_time.timestamp() * 1000)
        end_ts_ms = int(job.end_time.timestamp() * 1000)

        log.info(
            "dlq_replay_start",
            job_id=job.job_id,
            start_time=job.start_time.isoformat(),
            end_time=job.end_time.isoformat(),
        )

        # Build a low-level consumer for manual partition assignment
        consumer_cfg: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": f"{self._consumer_group}-{job.job_id}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 600_000,
            "session.timeout.ms": 30_000,
        }
        consumer = Consumer(consumer_cfg)

        try:
            total_events = self._consume_window(
                consumer=consumer,
                producer=producer,
                job=job,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
            )
        finally:
            consumer.close()

        producer.flush()
        log.info(
            "dlq_replay_complete",
            job_id=job.job_id,
            total_events=total_events,
        )
        return total_events

    # ------------------------------------------------------------------
    # Internal implementation
    # ------------------------------------------------------------------

    def _consume_window(
        self,
        consumer: Consumer,
        producer: MdrpProducer,
        job: ReplayJob,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> int:
        """Seek to start_ts on all DLQ partitions, consume until end_ts."""
        # Discover partitions for the DLQ topic
        admin = AdminClient({"bootstrap.servers": self._bootstrap_servers})
        cluster_meta = admin.list_topics(topic=Topics.DLQ_EVENTS, timeout=10)
        topic_meta = cluster_meta.topics.get(Topics.DLQ_EVENTS)
        if topic_meta is None:
            log.warning("dlq_topic_not_found", job_id=job.job_id)
            return 0

        partitions = [
            TopicPartition(Topics.DLQ_EVENTS, p)
            for p in topic_meta.partitions
        ]

        # Get offsets for the start timestamp
        ts_partitions = [
            TopicPartition(Topics.DLQ_EVENTS, tp.partition, start_ts_ms)
            for tp in partitions
        ]
        offsets = consumer.offsets_for_times(ts_partitions, timeout=10)

        # Build assignment with resolved offsets; skip partitions with no data
        assignment: list[TopicPartition] = []
        for tp in offsets:
            if tp.offset >= 0:
                assignment.append(TopicPartition(tp.topic, tp.partition, tp.offset))

        if not assignment:
            log.warning(
                "dlq_replay_no_offsets_for_window",
                job_id=job.job_id,
                start_ts_ms=start_ts_ms,
            )
            return 0

        consumer.assign(assignment)

        total_events = 0
        rate_window_start = time.monotonic()
        events_in_window = 0

        while total_events < self._max_messages:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                # Check whether we have consumed all assigned partitions
                if self._all_partitions_exhausted(consumer, assignment):
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Partition EOF — check if all are done
                    if self._all_partitions_exhausted(consumer, assignment):
                        break
                    continue
                log.error(
                    "dlq_replay_consumer_error",
                    job_id=job.job_id,
                    error=str(msg.error()),
                )
                continue

            # Stop if we've passed the end of the replay window
            msg_ts_type, msg_ts_ms = msg.timestamp()
            if msg_ts_ms > end_ts_ms:
                # Timestamp is past the window — we can stop consuming this partition
                # but other partitions may still have messages in range.
                # For simplicity we stop consuming entirely when any message is past end.
                # A more sophisticated approach would track per-partition end state.
                continue

            try:
                dlq_event = DLQEvent.model_validate_json(msg.value())
            except Exception as exc:
                log.warning(
                    "dlq_replay_deserialise_error",
                    job_id=job.job_id,
                    error=str(exc),
                )
                continue

            # Filter by provider/instrument if specified
            if job.provider and dlq_event.provider != job.provider:
                continue
            if job.instrument and dlq_event.instrument != job.instrument:
                continue

            raw_event = self._reconstruct_raw_event(dlq_event)
            producer.produce(
                topic=Topics.RAW_EVENTS,
                value=raw_event,
                key=f"{raw_event.provider}:{raw_event.instrument}",
            )

            total_events += 1
            events_in_window += 1
            REPLAY_EVENTS_TOTAL.labels(source=ReplaySource.DLQ.value).inc()

            # Rate limiting
            if events_in_window >= 100:
                elapsed = time.monotonic() - rate_window_start
                expected = events_in_window / self._rate_limit
                if expected > elapsed:
                    time.sleep(expected - elapsed)
                events_in_window = 0
                rate_window_start = time.monotonic()

        return total_events

    @staticmethod
    def _all_partitions_exhausted(
        consumer: Consumer,
        assignment: list[TopicPartition],
    ) -> bool:
        """
        Return True if the consumer's current position has reached or exceeded
        the high-water mark for every assigned partition.
        """
        try:
            positions = consumer.position(assignment)
            for tp in positions:
                try:
                    _low, high = consumer.get_watermark_offsets(tp, timeout=2.0)
                    if tp.offset < high:
                        return False
                except Exception:
                    return False
            return True
        except Exception:
            return False

    @staticmethod
    def _reconstruct_raw_event(dlq_event: DLQEvent) -> RawMarketEvent:
        """
        Re-build a RawMarketEvent from a DLQ event's raw_payload.

        A fresh event_id and trace_id are generated so the replayed event
        can be distinguished from the original in audit logs.
        """
        import uuid

        data = dict(dlq_event.raw_payload)
        data["is_replay"] = True
        data["replay_source"] = ReplaySource.DLQ.value
        data["event_id"] = str(uuid.uuid4())
        data["trace_id"] = str(uuid.uuid4())
        data["received_at"] = datetime.now(UTC).isoformat()
        # Preserve original provider/instrument from DLQ metadata if missing
        if "provider" not in data or not data["provider"]:
            data["provider"] = dlq_event.provider
        if "instrument" not in data or not data["instrument"]:
            data["instrument"] = dlq_event.instrument or "unknown"

        return RawMarketEvent.model_validate(data)
