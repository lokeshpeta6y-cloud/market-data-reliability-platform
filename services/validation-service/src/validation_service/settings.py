"""
Settings for the validation-service.

Extends BaseServiceSettings with Redis and deduplication configuration.
All values can be overridden via environment variables.
"""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class ValidationServiceSettings(BaseServiceSettings):
    """Runtime configuration for the validation-service."""

    # Kafka consumer group
    kafka_consumer_group: str = Field(
        default="validation-service",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Prometheus metrics port (unique per service)
    metrics_port: int = Field(default=8002, alias="METRICS_PORT")

    # Redis connection — inherited from BaseServiceSettings (redis_url)
    # Overriding the default here for explicitness
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        alias="REDIS_URL",
    )

    # How long a seen event_id is remembered for deduplication
    dedup_ttl_seconds: int = Field(
        default=3600,
        alias="DEDUP_TTL_SECONDS",
        ge=60,
        le=86400,
    )

    # Maximum age of an event before it is considered stale
    max_event_age_hours: int = Field(
        default=24,
        alias="MAX_EVENT_AGE_HOURS",
        ge=1,
        le=168,
    )

    # Maximum drift into the future allowed before an event is out-of-order
    max_future_minutes: int = Field(
        default=5,
        alias="MAX_FUTURE_MINUTES",
        ge=1,
        le=60,
    )

    # Price sanity bounds
    min_price: float = Field(default=0.0, alias="MIN_PRICE")
    max_price: float = Field(default=1_000_000.0, alias="MAX_PRICE")

    # Rolling quality average — how many recent scores to average
    quality_rolling_window: int = Field(
        default=100,
        alias="QUALITY_ROLLING_WINDOW",
        ge=10,
        le=10_000,
    )
