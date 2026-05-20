"""
Settings for the silver-loader service.

Extends BaseServiceSettings with Snowflake staging and batch configuration.
All values can be overridden via environment variables.
"""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class SilverLoaderSettings(BaseServiceSettings):
    """Runtime configuration for the silver-loader service."""

    # Kafka consumer group — independent from other services on the same topic
    kafka_consumer_group: str = Field(
        default="silver-loader",
        alias="KAFKA_CONSUMER_GROUP",
    )

    # Prometheus metrics port (unique per service)
    metrics_port: int = Field(default=8008, alias="METRICS_PORT")

    # Flush when batch reaches this many CurveEvents
    batch_size: int = Field(
        default=1000,
        alias="BATCH_SIZE",
        ge=1,
        le=100_000,
    )

    # Flush when this many seconds have elapsed since the last flush,
    # even if batch_size has not been reached
    flush_interval_seconds: float = Field(
        default=60.0,
        alias="FLUSH_INTERVAL_SECONDS",
        ge=1.0,
        le=600.0,
    )

    # Name of the Snowflake internal stage used for COPY INTO staging
    snowflake_stage_name: str = Field(
        default="MDRP_STAGE",
        alias="SNOWFLAKE_STAGE_NAME",
    )

    # Maximum retries for a failed Snowflake load attempt before giving up
    snowflake_load_retries: int = Field(
        default=3,
        alias="SNOWFLAKE_LOAD_RETRIES",
        ge=0,
        le=10,
    )
