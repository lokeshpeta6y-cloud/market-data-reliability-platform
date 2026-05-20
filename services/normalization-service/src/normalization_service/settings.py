"""Settings for the normalization-service."""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class NormalizationServiceSettings(BaseServiceSettings):
    metrics_port: int = Field(default=8004, alias="METRICS_PORT")

    # Kafka consumer group
    kafka_consumer_group: str = Field(
        default="normalization-service",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Redis version counter TTL (seconds). 0 = no TTL (keys persist).
    redis_version_counter_ttl: int = Field(
        default=0,
        alias="REDIS_VERSION_COUNTER_TTL",
    )

    # How many seconds to wait for Redis on startup before failing
    redis_connect_timeout: int = Field(
        default=10,
        alias="REDIS_CONNECT_TIMEOUT",
    )
