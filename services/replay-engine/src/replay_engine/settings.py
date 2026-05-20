"""Replay Engine service settings."""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class ReplayEngineSettings(BaseServiceSettings):
    """
    Settings for the replay-engine service.

    Extends BaseServiceSettings with replay-specific configuration.
    All fields are overridable via environment variables.
    """

    # Override default metrics port
    metrics_port: int = Field(default=8006, alias="METRICS_PORT")

    # Rate limiting — events per second emitted to Kafka during replay.
    # This protects downstream consumers from being overwhelmed.
    replay_rate_limit_per_second: int = Field(
        default=1000,
        alias="REPLAY_RATE_LIMIT_PER_SECOND",
        ge=1,
        le=100_000,
    )

    # How often the engine polls Redis for new pending jobs.
    job_poll_interval_seconds: float = Field(
        default=5.0,
        alias="JOB_POLL_INTERVAL_SECONDS",
        ge=0.5,
    )

    # Databento-specific — dataset to pull from when doing historical replay.
    # Only active when DATABENTO_API_KEY is also set (inherited from BaseServiceSettings).
    databento_dataset: str = Field(
        default="DBEQ.BASIC",
        alias="DATABENTO_DATASET",
    )

    # DLQ consumer group for DLQ replay mode.
    dlq_replay_consumer_group: str = Field(
        default="replay-engine-dlq",
        alias="DLQ_REPLAY_CONSUMER_GROUP",
    )

    # Maximum messages to consume per DLQ replay job (safety cap).
    dlq_replay_max_messages: int = Field(
        default=500_000,
        alias="DLQ_REPLAY_MAX_MESSAGES",
        ge=1,
    )
