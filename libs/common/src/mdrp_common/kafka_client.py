"""
Kafka producer and consumer wrappers for the Market Data Reliability Platform.

Built on top of confluent-kafka. Every service uses these wrappers rather than
instantiating Confluent clients directly — this centralises retry logic, serialisation,
metrics instrumentation, and graceful shutdown handling.

Topic constants are defined here so that a rename is a one-line change, not a grep.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic registry — single source of truth for all topic names
# ---------------------------------------------------------------------------


class Topics:
    RAW_EVENTS = "market.events.raw"
    VALIDATED_EVENTS = "market.events.validated"
    NORMALIZED_EVENTS = "market.events.normalized"
    REPLAY_EVENTS = "market.events.replay"
    DLQ_EVENTS = "market.events.dlq"

    ALL = [
        RAW_EVENTS,
        VALIDATED_EVENTS,
        NORMALIZED_EVENTS,
        REPLAY_EVENTS,
        DLQ_EVENTS,
    ]


# ---------------------------------------------------------------------------
# Topic configuration — partition count and retention per topic
# ---------------------------------------------------------------------------


TOPIC_CONFIGS: dict[str, dict[str, Any]] = {
    Topics.RAW_EVENTS: {
        "num_partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": str(7 * 24 * 60 * 60 * 1000)},  # 7 days
    },
    Topics.VALIDATED_EVENTS: {
        "num_partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
    },
    Topics.NORMALIZED_EVENTS: {
        "num_partitions": 6,
        "replication_factor": 1,
        "config": {"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
    },
    Topics.REPLAY_EVENTS: {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": str(3 * 24 * 60 * 60 * 1000)},  # 3 days
    },
    Topics.DLQ_EVENTS: {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": str(30 * 24 * 60 * 60 * 1000)},  # 30 days
    },
}


# ---------------------------------------------------------------------------
# Topic provisioning — idempotent, called at service startup
# ---------------------------------------------------------------------------


def ensure_topics(bootstrap_servers: str, timeout_s: int = 30) -> None:
    """Create all platform topics if they do not already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=timeout_s).topics.keys())

    to_create = [
        NewTopic(
            name,
            num_partitions=cfg["num_partitions"],
            replication_factor=cfg["replication_factor"],
            config=cfg.get("config", {}),
        )
        for name, cfg in TOPIC_CONFIGS.items()
        if name not in existing
    ]

    if not to_create:
        logger.info("all_topics_exist", extra={"topic_count": len(TOPIC_CONFIGS)})
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("topic_created", extra={"topic": topic})
        except KafkaException as exc:
            # TOPIC_ALREADY_EXISTS is not an error in our context
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


class MdrpProducer:
    """
    Thread-safe Kafka producer with JSON serialisation, delivery callbacks,
    and graceful flush on shutdown.
    """

    def __init__(self, bootstrap_servers: str, **extra_config: Any) -> None:
        config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.size": 65536,
            **extra_config,
        }
        self._producer = Producer(config)
        self._lock = threading.Lock()

    def produce(
        self,
        topic: str,
        value: BaseModel | dict[str, Any],
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        if isinstance(value, BaseModel):
            serialised = value.model_dump_json().encode()
        else:
            serialised = json.dumps(value, default=str).encode()

        kafka_headers = list((k, v.encode()) for k, v in (headers or {}).items())

        with self._lock:
            self._producer.produce(
                topic=topic,
                value=serialised,
                key=key.encode() if key else None,
                headers=kafka_headers,
                on_delivery=self._on_delivery,
            )
            # Poll to trigger delivery callbacks without blocking
            self._producer.poll(0)

    def flush(self, timeout_s: float = 30.0) -> None:
        remaining = self._producer.flush(timeout=timeout_s)
        if remaining > 0:
            logger.warning(
                "producer_flush_incomplete",
                extra={"remaining_messages": remaining},
            )

    @staticmethod
    def _on_delivery(err: KafkaError | None, msg: Message) -> None:
        if err:
            logger.error(
                "kafka_delivery_failed",
                extra={"topic": msg.topic(), "error": str(err)},
            )


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class MdrpConsumer:
    """
    Kafka consumer with manual offset commits, graceful shutdown via SIGTERM,
    and structured error logging.

    Usage:
        consumer = MdrpConsumer(bootstrap_servers, group_id, [Topics.RAW_EVENTS])
        for message in consumer.messages():
            process(message)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        auto_offset_reset: str = "earliest",
        **extra_config: Any,
    ) -> None:
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
            **extra_config,
        }
        self._consumer = Consumer(config)
        self._consumer.subscribe(topics)
        self._shutdown = threading.Event()
        self._topics = topics

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def messages(self, poll_timeout_s: float = 1.0) -> Iterator[Message]:
        """Yield messages until shutdown is requested."""
        try:
            while not self._shutdown.is_set():
                msg = self._consumer.poll(timeout=poll_timeout_s)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(
                        "kafka_consumer_error",
                        extra={"error": str(msg.error()), "topics": self._topics},
                    )
                    continue
                yield msg
        finally:
            self._consumer.close()
            logger.info("consumer_closed", extra={"topics": self._topics})

    def commit(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def get_lag(self) -> dict[str, int]:
        """Return per-partition consumer lag. Used by metrics collection."""
        lag: dict[str, int] = {}
        assignment = self._consumer.assignment()
        if not assignment:
            return lag

        for tp in assignment:
            low, high = self._consumer.get_watermark_offsets(tp, timeout=5.0)
            committed = self._consumer.committed([tp], timeout=5.0)
            current_offset = committed[0].offset if committed[0].offset >= 0 else low
            lag[f"{tp.topic}[{tp.partition}]"] = max(0, high - current_offset)

        return lag

    def shutdown(self) -> None:
        self._shutdown.set()

    def _handle_sigterm(self, _signum: int, _frame: Any) -> None:
        logger.info("sigterm_received_initiating_graceful_shutdown")
        self.shutdown()


# ---------------------------------------------------------------------------
# Deserialisation helpers
# ---------------------------------------------------------------------------


def deserialise(msg: Message, model_class: type[BaseModel]) -> BaseModel:
    """Deserialise a Kafka message value into a Pydantic model."""
    raw = msg.value()
    if raw is None:
        raise ValueError(f"received null message on topic {msg.topic()}")
    data = json.loads(raw)
    return model_class.model_validate(data)


@contextmanager
def producer_context(bootstrap_servers: str, **kwargs: Any) -> Iterator[MdrpProducer]:
    """Context manager that flushes on exit."""
    p = MdrpProducer(bootstrap_servers, **kwargs)
    try:
        yield p
    finally:
        p.flush()
