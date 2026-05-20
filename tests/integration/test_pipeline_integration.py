"""
Integration tests for the full validation → normalisation pipeline.

These tests require a running Redpanda (Kafka-compatible) broker.
They are skipped unless the environment variable INTEGRATION_TESTS=true is set.

To run:
    INTEGRATION_TESTS=true pytest -m integration tests/integration/

To run with a local stack:
    make up
    INTEGRATION_TESTS=true make test-integration
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Generator

import pytest

# ---------------------------------------------------------------------------
# Skip guard — skip the entire module unless INTEGRATION_TESTS=true
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.integration

_INTEGRATION_ENABLED = os.getenv("INTEGRATION_TESTS", "false").lower() == "true"


def _require_integration():
    if not _INTEGRATION_ENABLED:
        pytest.skip(
            "Integration tests disabled. Set INTEGRATION_TESTS=true to enable."
        )


# ---------------------------------------------------------------------------
# Optional imports — only needed when tests actually run
# ---------------------------------------------------------------------------

if _INTEGRATION_ENABLED:
    from confluent_kafka import Consumer, Producer, KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC_RAW = "market.events.raw"
TOPIC_VALIDATED = "market.events.validated"
TOPIC_DLQ = "market.events.dlq"
CONSUMER_TIMEOUT_S = 30  # Maximum seconds to wait for all events to flow through


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def kafka_admin():
    _require_integration()
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    return admin


@pytest.fixture(scope="module")
def kafka_producer():
    _require_integration()
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "retries": 3,
        }
    )
    yield producer
    producer.flush(timeout=10)


def _make_consumer(group_id: str, topics: list[str]) -> "Consumer":
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    return consumer


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_valid_raw_event(event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "provider": "integration-test-provider",
        "instrument": "TTF_CAL25",
        "received_at": _utc_now_iso(),
        "event_timestamp": _utc_now_iso(),
        "payload": {
            "price": 45.50,
            "tenor": "2025-CAL",
            "currency": "EUR",
            "unit": "MWh",
            "curve_name": "TTF_FORWARD",
        },
        "injected_faults": [],
        "is_replay": False,
        "replay_source": None,
        "trace_id": str(uuid.uuid4()),
    }


def _make_malformed_raw_event(event_id: str | None = None) -> dict:
    """An event with a price well outside the valid range."""
    event = _make_valid_raw_event(event_id)
    event["payload"]["price"] = -9999.0
    event["injected_faults"] = ["malformed"]
    return event


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


class TestFullPipelineIntegration:
    """
    End-to-end test: produce 100 events to market.events.raw, then assert
    that valid events appear on market.events.validated and malformed events
    appear on market.events.dlq.
    """

    def test_pipeline_routes_events_correctly(self, kafka_producer):
        _require_integration()

        # Use a unique test run ID so consumers see only our events
        run_id = str(uuid.uuid4())[:8]
        consumer_group = f"integration-test-{run_id}"

        # -----------------------------------------------------------------
        # Produce 100 events: 70 valid, 30 malformed
        # -----------------------------------------------------------------
        valid_ids: set[str] = set()
        malformed_ids: set[str] = set()

        for i in range(70):
            eid = str(uuid.uuid4())
            valid_ids.add(eid)
            msg = json.dumps(_make_valid_raw_event(eid)).encode()
            kafka_producer.produce(TOPIC_RAW, value=msg, key=eid.encode())

        for i in range(30):
            eid = str(uuid.uuid4())
            malformed_ids.add(eid)
            msg = json.dumps(_make_malformed_raw_event(eid)).encode()
            kafka_producer.produce(TOPIC_RAW, value=msg, key=eid.encode())

        kafka_producer.flush(timeout=15)

        # -----------------------------------------------------------------
        # Consume from validated and DLQ topics
        # -----------------------------------------------------------------
        validated_original_ids: set[str] = set()
        dlq_original_ids: set[str] = set()

        consumer = _make_consumer(
            group_id=consumer_group,
            topics=[TOPIC_VALIDATED, TOPIC_DLQ],
        )

        deadline = time.monotonic() + CONSUMER_TIMEOUT_S
        try:
            while time.monotonic() < deadline:
                total_seen = len(validated_original_ids) + len(dlq_original_ids)
                if total_seen >= 100:
                    break

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Kafka consumer error: {msg.error()}")

                try:
                    payload = json.loads(msg.value().decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                topic = msg.topic()
                if topic == TOPIC_VALIDATED:
                    orig_id = payload.get("original_event_id")
                    if orig_id and (orig_id in valid_ids or orig_id in malformed_ids):
                        validated_original_ids.add(orig_id)
                elif topic == TOPIC_DLQ:
                    orig_id = payload.get("original_event_id")
                    if orig_id and (orig_id in valid_ids or orig_id in malformed_ids):
                        dlq_original_ids.add(orig_id)
        finally:
            consumer.close()

        # -----------------------------------------------------------------
        # Assertions
        # -----------------------------------------------------------------

        # Valid events should appear on the validated topic
        valid_on_validated = valid_ids & validated_original_ids
        assert len(valid_on_validated) > 0, (
            "Expected valid events on market.events.validated; none found"
        )

        # Malformed events should appear on the DLQ
        malformed_on_dlq = malformed_ids & dlq_original_ids
        assert len(malformed_on_dlq) > 0, (
            "Expected malformed events on market.events.dlq; none found"
        )

        # No valid event should be on the DLQ
        valid_on_dlq = valid_ids & dlq_original_ids
        assert len(valid_on_dlq) == 0, (
            f"Valid events incorrectly sent to DLQ: {valid_on_dlq}"
        )

        # No malformed event should be on validated
        malformed_on_validated = malformed_ids & validated_original_ids
        assert len(malformed_on_validated) == 0, (
            f"Malformed events incorrectly validated: {malformed_on_validated}"
        )

        # Total processed should account for all 100 events
        # (some may be deduplicated or still in-flight; allow a margin)
        total_seen = len(validated_original_ids) + len(dlq_original_ids)
        assert total_seen == 100, (
            f"Expected 100 total events, got {total_seen} "
            f"(validated={len(validated_original_ids)}, dlq={len(dlq_original_ids)})"
        )


class TestConsumerLagAfterBurst:
    """Verify that a burst of events is fully drained within the timeout window."""

    def test_burst_fully_processed(self, kafka_producer):
        _require_integration()

        run_id = str(uuid.uuid4())[:8]
        consumer_group = f"integration-burst-{run_id}"
        event_ids: set[str] = set()

        # Produce 100 valid events in rapid succession
        for _ in range(100):
            eid = str(uuid.uuid4())
            event_ids.add(eid)
            msg = json.dumps(_make_valid_raw_event(eid)).encode()
            kafka_producer.produce(TOPIC_RAW, value=msg)

        kafka_producer.flush(timeout=15)

        # Wait for all to appear on validated topic
        seen_ids: set[str] = set()
        consumer = _make_consumer(
            group_id=consumer_group,
            topics=[TOPIC_VALIDATED],
        )
        deadline = time.monotonic() + CONSUMER_TIMEOUT_S
        try:
            while time.monotonic() < deadline and len(seen_ids) < 100:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error():
                    continue
                try:
                    payload = json.loads(msg.value().decode())
                    orig_id = payload.get("original_event_id")
                    if orig_id in event_ids:
                        seen_ids.add(orig_id)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
        finally:
            consumer.close()

        assert len(seen_ids) == 100, (
            f"Only {len(seen_ids)}/100 events processed within {CONSUMER_TIMEOUT_S}s"
        )
