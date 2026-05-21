"""Settings for the redis-writer service."""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class RedisWriterSettings(BaseServiceSettings):
    metrics_port: int = Field(default=8005, alias="METRICS_PORT")

    # Kafka consumer group
    kafka_consumer_group: str = Field(
        default="redis-writer",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Maximum history entries kept per instrument in the sorted set
    curve_history_max_entries: int = Field(
        default=1000,
        alias="CURVE_HISTORY_MAX_ENTRIES",
    )

    # How old a last_event_at must be (seconds) before we log a staleness warning
    staleness_threshold_seconds: int = Field(
        default=600,
        alias="STALENESS_THRESHOLD_SECONDS",
    )

    # Redis connect timeout on startup (seconds)
    redis_connect_timeout: int = Field(
        default=10,
        alias="REDIS_CONNECT_TIMEOUT",
    )

    # Minimum fraction of expected tenors required to assemble a snapshot
    snapshot_completeness_threshold: float = Field(
        default=0.80,
        alias="SNAPSHOT_COMPLETENESS_THRESHOLD",
    )

    # Expected number of tenors per canonical instrument.
    # Used to calculate snapshot completeness.  Override via env if needed.
    expected_tenors_per_instrument: dict[str, int] = Field(
        default={
            "TTF": 24,
            "NBP": 24,
            "BRENT": 24,
            "WTI": 24,
            "EU_ETS": 5,
            "TTF_POWER": 32,
        },
        alias="EXPECTED_TENORS_PER_INSTRUMENT",
    )
