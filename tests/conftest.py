"""
Shared pytest fixtures for the Market Data Reliability Platform test suite.

Available fixtures:
    raw_market_event  — factory producing a valid RawMarketEvent
    curve_event       — factory producing a valid CurveEvent
    fake_redis        — fakeredis.FakeRedis instance (in-memory, no server needed)
    mock_producer     — MagicMock with a .produce() method (simulates Kafka producer)
    settings          — ValidationServiceSettings with localhost defaults
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import fakeredis
import pytest

from mdrp_common.models import (
    CurveEvent,
    DeliveryPeriod,
    FaultType,
    RawMarketEvent,
    ReplaySource,
)
from validation_service.settings import ValidationServiceSettings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# raw_market_event — factory fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def raw_market_event():
    """
    Factory fixture that returns a callable producing valid RawMarketEvents.

    Usage::

        def test_something(raw_market_event):
            event = raw_market_event()
            event_with_overrides = raw_market_event(provider="ice-endex", instrument="NBP")
    """

    def _factory(
        provider: str = "test-provider",
        instrument: str = "TTF_CAL25",
        event_timestamp: datetime | None = None,
        payload: dict | None = None,
        injected_faults: list[FaultType] | None = None,
        is_replay: bool = False,
        replay_source: ReplaySource | None = None,
        event_id: str | None = None,
        trace_id: str | None = None,
    ) -> RawMarketEvent:
        return RawMarketEvent(
            event_id=event_id or str(uuid.uuid4()),
            provider=provider,
            instrument=instrument,
            received_at=_utc_now(),
            event_timestamp=event_timestamp or _utc_now(),
            payload=payload
            or {
                "price": 42.50,
                "tenor": "2025-CAL",
                "currency": "EUR",
                "unit": "MWh",
                "curve_name": "TTF_FORWARD",
                "bid": 42.0,
                "ask": 43.0,
            },
            injected_faults=injected_faults or [],
            is_replay=is_replay,
            replay_source=replay_source,
            trace_id=trace_id or str(uuid.uuid4()),
        )

    return _factory


# ---------------------------------------------------------------------------
# curve_event — factory fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def curve_event():
    """
    Factory fixture that returns a callable producing valid CurveEvents.

    Usage::

        def test_something(curve_event):
            evt = curve_event()
            evt_custom = curve_event(quality_score=0.75, delivery_period=DeliveryPeriod.QUARTERLY)
    """

    def _factory(
        source_event_id: str | None = None,
        curve_name: str = "TTF_FORWARD",
        instrument: str = "TTF",
        tenor: str = "2025-03",
        delivery_period: DeliveryPeriod = DeliveryPeriod.MONTHLY,
        price: Decimal | float | str = Decimal("42.500"),
        currency: str = "EUR",
        unit: str = "MWh",
        provider: str = "test-provider",
        version: int = 1,
        event_timestamp: datetime | None = None,
        quality_score: float = 1.0,
        is_replay: bool = False,
        replay_source: ReplaySource | None = None,
        trace_id: str | None = None,
    ) -> CurveEvent:
        return CurveEvent(
            source_event_id=source_event_id or str(uuid.uuid4()),
            curve_name=curve_name,
            instrument=instrument,
            tenor=tenor,
            delivery_period=delivery_period,
            price=Decimal(str(price)),
            currency=currency,
            unit=unit,
            provider=provider,
            version=version,
            event_timestamp=event_timestamp or _utc_now(),
            quality_score=quality_score,
            is_replay=is_replay,
            replay_source=replay_source,
            trace_id=trace_id or str(uuid.uuid4()),
        )

    return _factory


# ---------------------------------------------------------------------------
# fake_redis — fakeredis instance
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    """
    Provide a fresh fakeredis.FakeRedis instance for each test.

    The instance uses decode_responses=True to match the behaviour expected by
    Deduplicator and QualityScorer (both of which use string keys and values).
    """
    client = fakeredis.FakeRedis(decode_responses=True)
    yield client
    client.flushall()
    client.close()


# ---------------------------------------------------------------------------
# mock_producer — MagicMock Kafka producer
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_producer() -> MagicMock:
    """
    Return a MagicMock that mimics a Confluent Kafka Producer.

    The mock has the following pre-configured methods:
        .produce(topic, value, key, on_delivery)  — tracked call
        .flush(timeout)                            — no-op
        .poll(timeout)                             — returns 0

    Usage::

        def test_something(mock_producer):
            service.produce_event(event, producer=mock_producer)
            mock_producer.produce.assert_called_once()
            call_kwargs = mock_producer.produce.call_args.kwargs
            assert call_kwargs["topic"] == "market.events.validated"
    """
    producer = MagicMock()
    producer.produce = MagicMock(return_value=None)
    producer.flush = MagicMock(return_value=0)
    producer.poll = MagicMock(return_value=0)
    return producer


# ---------------------------------------------------------------------------
# settings — test ValidationServiceSettings
# ---------------------------------------------------------------------------


@pytest.fixture
def settings() -> ValidationServiceSettings:
    """
    Return ValidationServiceSettings configured with safe localhost defaults
    suitable for unit tests (no actual Redis or Kafka connections required).
    """
    return ValidationServiceSettings(
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        REDIS_URL="redis://localhost:6379/0",
        LOG_LEVEL="WARNING",
        METRICS_PORT=9999,
        MAX_EVENT_AGE_HOURS=24,
        MAX_FUTURE_MINUTES=5,
        MIN_PRICE=0.0,
        MAX_PRICE=1_000_000.0,
        DEDUP_TTL_SECONDS=3600,
        QUALITY_ROLLING_WINDOW=100,
        OTEL_ENABLED=False,
    )
