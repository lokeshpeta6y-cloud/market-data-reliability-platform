"""
Settings for the bronze-writer service.

Extends BaseServiceSettings with S3 batching configuration.
All values can be overridden via environment variables.
"""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class BronzeWriterSettings(BaseServiceSettings):
    """Runtime configuration for the bronze-writer service."""

    # Kafka consumer group — separate from validation-service so both
    # consume the full market.events.raw stream independently
    kafka_consumer_group: str = Field(
        default="bronze-writer",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Prometheus metrics port (unique per service)
    metrics_port: int = Field(default=8003, alias="METRICS_PORT")

    # Flush when batch reaches this many events
    batch_size: int = Field(
        default=500,
        alias="BATCH_SIZE",
        ge=1,
        le=100_000,
    )

    # Flush when this many seconds have elapsed since the last flush,
    # even if batch_size has not been reached
    flush_interval_seconds: float = Field(
        default=30.0,
        alias="FLUSH_INTERVAL_SECONDS",
        ge=1.0,
        le=300.0,
    )
