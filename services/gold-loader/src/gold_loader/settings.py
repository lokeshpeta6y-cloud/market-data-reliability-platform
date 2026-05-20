"""
Settings for the gold-loader service.

Extends BaseServiceSettings with snapshot assembly and Snowflake configuration.
All values can be overridden via environment variables.
"""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class GoldLoaderSettings(BaseServiceSettings):
    """Runtime configuration for the gold-loader service."""

    # Kafka consumer group — independent of silver-loader; both read the same topic
    kafka_consumer_group: str = Field(
        default="gold-loader",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Prometheus metrics port (unique per service)
    metrics_port: int = Field(default=8009, alias="METRICS_PORT")

    # Width of the tumbling time window used to group events into a snapshot
    snapshot_window_minutes: int = Field(
        default=5,
        alias="SNAPSHOT_WINDOW_MINUTES",
        ge=1,
        le=60,
    )

    # Minimum completeness (tenors_received / expected_tenors) for a snapshot
    # to be considered authoritative and written to Gold
    min_completeness: float = Field(
        default=0.80,
        alias="MIN_COMPLETENESS",
        ge=0.0,
        le=1.0,
    )

    # Minimum quality score (across all received tenors) for a snapshot to be
    # considered authoritative
    min_quality_score: float = Field(
        default=0.70,
        alias="MIN_QUALITY_SCORE",
        ge=0.0,
        le=1.0,
    )

    # Expected number of tenors per curve.  Used for completeness calculation.
    # Set to 0 to derive expected count from the first snapshot seen for each curve.
    expected_tenors_per_curve: int = Field(
        default=0,
        alias="EXPECTED_TENORS_PER_CURVE",
        ge=0,
    )

    # Name of the Snowflake internal stage
    snowflake_stage_name: str = Field(
        default="MDRP_STAGE",
        alias="SNOWFLAKE_STAGE_NAME",
    )

    # Maximum retries for a failed Snowflake load
    snowflake_load_retries: int = Field(
        default=3,
        alias="SNOWFLAKE_LOAD_RETRIES",
        ge=0,
        le=10,
    )

    # How often the main loop polls for ready snapshots (seconds)
    poll_interval_seconds: float = Field(
        default=5.0,
        alias="POLL_INTERVAL_SECONDS",
        ge=1.0,
        le=60.0,
    )
