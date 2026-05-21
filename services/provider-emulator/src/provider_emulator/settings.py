"""
Settings for the provider-emulator service.

All configuration is read from environment variables (or a .env file).
Fault rates are independently configurable so that integration tests can
dial individual fault types to 0 or 1 without touching the rest.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict

from mdrp_common.settings import BaseServiceSettings


class EmulatorSettings(BaseServiceSettings):
    """
    Extends BaseServiceSettings with emulator-specific configuration.

    All fault rates are floats in [0.0, 1.0] representing the probability
    that the fault is applied to any given event (or batch for PARTIAL_CURVE).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Override the default metrics port — emulator runs on 8001
    metrics_port: int = Field(default=8001, alias="METRICS_PORT")

    # ------------------------------------------------------------------ #
    # Publish behaviour
    # ------------------------------------------------------------------ #

    # How frequently (in seconds) to generate and publish a full round of
    # forward curve snapshots for all configured instruments.
    publish_interval_seconds: float = Field(
        default=5.0,
        alias="PUBLISH_INTERVAL_SECONDS",
        gt=0,
    )

    # Maximum number of events that can be held in the delayed / out-of-order
    # queues at any one time.  Prevents unbounded memory growth.
    delay_queue_max_size: int = Field(
        default=500,
        alias="DELAY_QUEUE_MAX_SIZE",
        gt=0,
    )

    # Minimum and maximum seconds a DELAYED event is held before release.
    delay_min_seconds: float = Field(default=2.0, alias="DELAY_MIN_SECONDS", ge=0)
    delay_max_seconds: float = Field(default=30.0, alias="DELAY_MAX_SECONDS", ge=0)

    # ------------------------------------------------------------------ #
    # Fault injection rates — all default to the spec values
    # ------------------------------------------------------------------ #

    fault_rate_duplicate: float = Field(
        default=0.02,
        alias="FAULT_RATE_DUPLICATE",
        ge=0.0,
        le=1.0,
    )
    fault_rate_malformed: float = Field(
        default=0.01,
        alias="FAULT_RATE_MALFORMED",
        ge=0.0,
        le=1.0,
    )
    fault_rate_delayed: float = Field(
        default=0.05,
        alias="FAULT_RATE_DELAYED",
        ge=0.0,
        le=1.0,
    )
    fault_rate_out_of_order: float = Field(
        default=0.03,
        alias="FAULT_RATE_OUT_OF_ORDER",
        ge=0.0,
        le=1.0,
    )
    fault_rate_schema_drift: float = Field(
        default=0.005,
        alias="FAULT_RATE_SCHEMA_DRIFT",
        ge=0.0,
        le=1.0,
    )
    fault_rate_stale: float = Field(
        default=0.01,
        alias="FAULT_RATE_STALE",
        ge=0.0,
        le=1.0,
    )
    fault_rate_partial_curve: float = Field(
        default=0.02,
        alias="FAULT_RATE_PARTIAL_CURVE",
        ge=0.0,
        le=1.0,
    )

    # ------------------------------------------------------------------ #
    # Instrument selection — defaults to all supported instruments
    # ------------------------------------------------------------------ #

    # Comma-separated list of instrument codes to simulate.
    # If empty, all instruments are simulated.
    instruments: list[str] = Field(
        default=[
            "TTF",
            "NBP",
            "TTF_POWER",
            "BRENT",
            "WTI",
            "EU_ETS",
        ],
        alias="INSTRUMENTS",
    )

    @field_validator("instruments", mode="before")
    @classmethod
    def _parse_instruments(cls, v: object) -> object:
        if isinstance(v, str):
            return [i.strip() for i in v.split(",") if i.strip()]
        return v

    # Provider label stamped on every emitted event
    provider_name: str = Field(
        default="provider-emulator",
        alias="PROVIDER_NAME",
    )

    # ------------------------------------------------------------------ #
    # Databento adapter config (supplements base settings)
    # ------------------------------------------------------------------ #

    # Number of historical trading days to pull when the adapter initialises.
    databento_lookback_days: int = Field(
        default=5,
        alias="DATABENTO_LOOKBACK_DAYS",
        gt=0,
        le=365,
    )
