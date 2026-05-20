"""Ops API service settings."""

from __future__ import annotations

from pydantic import Field

from mdrp_common.settings import BaseServiceSettings


class OpsApiSettings(BaseServiceSettings):
    """
    Settings for the ops-api service.

    Extends BaseServiceSettings with API-specific configuration for alerting,
    CORS, and observability.
    """

    # Override default metrics port
    metrics_port: int = Field(default=8007, alias="METRICS_PORT")

    # FastAPI server
    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8080, alias="PORT")
    workers: int = Field(default=1, alias="WORKERS")

    # -----------------------------------------------------------------------
    # Alert routing — Microsoft Teams
    # -----------------------------------------------------------------------
    teams_webhook_url: str | None = Field(
        default=None,
        alias="TEAMS_WEBHOOK_URL",
    )

    alert_teams_enabled: bool = Field(
        default=False,
        alias="ALERT_TEAMS_ENABLED",
    )

    # -----------------------------------------------------------------------
    # Alert routing — SMTP email
    # -----------------------------------------------------------------------
    smtp_host: str | None = Field(default=None, alias="SMTP_HOST")
    smtp_port: int = Field(default=587, alias="SMTP_PORT")
    smtp_user: str | None = Field(default=None, alias="SMTP_USER")
    smtp_password: str | None = Field(default=None, alias="SMTP_PASSWORD")
    smtp_from: str | None = Field(default=None, alias="SMTP_FROM")

    # Comma-separated list of recipient addresses, e.g. "ops@example.com,backup@example.com"
    smtp_to: str | None = Field(default=None, alias="SMTP_TO")

    alert_email_enabled: bool = Field(
        default=False,
        alias="ALERT_EMAIL_ENABLED",
    )

    # -----------------------------------------------------------------------
    # CORS
    # -----------------------------------------------------------------------
    cors_origins: str = Field(
        default="*",
        alias="CORS_ORIGINS",
        description="Comma-separated list of allowed CORS origins, or * for all.",
    )

    # -----------------------------------------------------------------------
    # Operational limits
    # -----------------------------------------------------------------------
    # Maximum number of DLQ entries to return in a single request
    dlq_page_size: int = Field(default=100, alias="DLQ_PAGE_SIZE", ge=1, le=1000)

    # Max recent replay jobs returned by the list endpoint
    replay_list_limit: int = Field(
        default=50, alias="REPLAY_LIST_LIMIT", ge=1, le=500
    )

    @property
    def smtp_recipients(self) -> list[str]:
        """Parse the comma-separated SMTP_TO value into a list of addresses."""
        if not self.smtp_to:
            return []
        return [addr.strip() for addr in self.smtp_to.split(",") if addr.strip()]

    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS_ORIGINS into a list."""
        if self.cors_origins == "*":
            return ["*"]
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]
