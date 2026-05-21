"""
Base settings shared across all platform services.

Each service extends BaseServiceSettings and adds its own fields.
Settings are loaded from environment variables — pydantic-settings handles
dotenv files, environment variable precedence, and type coercion automatically.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Kafka / Redpanda
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    # Observability
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    metrics_port: int = Field(default=8000, alias="METRICS_PORT")
    service_version: str = Field(default="0.1.0", alias="SERVICE_VERSION")

    # OpenTelemetry
    otel_exporter_otlp_endpoint: str = Field(
        default="http://jaeger:4317",
        alias="OTEL_EXPORTER_OTLP_ENDPOINT",
    )
    otel_enabled: bool = Field(default=True, alias="OTEL_ENABLED")

    # Redis
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    # S3 / MinIO
    s3_endpoint_url: str | None = Field(default=None, alias="S3_ENDPOINT_URL")
    s3_bucket_bronze: str = Field(
        default="mdrp-bronze", alias="S3_BUCKET_BRONZE"
    )
    aws_access_key_id: str | None = Field(default=None, alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str | None = Field(
        default=None, alias="AWS_SECRET_ACCESS_KEY"
    )
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")

    # Snowflake (optional — skipped if not configured)
    snowflake_account: str | None = Field(default=None, alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str | None = Field(default=None, alias="SNOWFLAKE_USER")
    snowflake_password: str | None = Field(default=None, alias="SNOWFLAKE_PASSWORD")
    # PAT token takes precedence over password; bypasses MFA
    snowflake_pat_token: str | None = Field(default=None, alias="SNOWFLAKE_PAT_TOKEN")
    snowflake_database: str = Field(
        default="MARKET_DATA", alias="SNOWFLAKE_DATABASE"
    )
    snowflake_schema_silver: str = Field(
        default="SILVER_EVENTS", alias="SNOWFLAKE_SCHEMA_SILVER"
    )
    snowflake_schema_gold: str = Field(
        default="GOLD_CURVES", alias="SNOWFLAKE_SCHEMA_GOLD"
    )
    snowflake_warehouse: str = Field(
        default="INGESTION_WH", alias="SNOWFLAKE_WAREHOUSE"
    )

    # Databento (optional — emulator is used when not set)
    databento_api_key: str | None = Field(default=None, alias="DATABENTO_API_KEY")
    databento_dataset: str = Field(
        default="DBEQ.BASIC", alias="DATABENTO_DATASET"
    )

    @property
    def snowflake_configured(self) -> bool:
        has_account_and_user = bool(self.snowflake_account and self.snowflake_user)
        has_auth = bool(self.snowflake_pat_token or self.snowflake_password)
        return has_account_and_user and has_auth

    @property
    def databento_configured(self) -> bool:
        return self.databento_api_key is not None

    @property
    def s3_is_minio(self) -> bool:
        """True when pointing at a local MinIO instance rather than real AWS S3."""
        return self.s3_endpoint_url is not None
