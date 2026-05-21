"""
Alert webhook receiver.

POST /api/v1/alerts/webhook — receive AlertManager webhook, dispatch async
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Request, status
from pydantic import BaseModel, Field

from mdrp_common.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


# ---------------------------------------------------------------------------
# AlertManager webhook payload models (Pydantic v2)
# ---------------------------------------------------------------------------


class AlertLabel(BaseModel):
    """Free-form key/value labels on an alert."""

    model_config = {"extra": "allow"}


class AlertAnnotation(BaseModel):
    """Free-form key/value annotations on an alert."""

    model_config = {"extra": "allow"}


class Alert(BaseModel):
    status: str = "firing"
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: str = ""
    endsAt: str = ""
    generatorURL: str = ""
    fingerprint: str = ""


class AlertManagerWebhook(BaseModel):
    """
    AlertManager webhook payload schema.
    https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
    """

    version: str = "4"
    groupKey: str = ""
    truncatedAlerts: int = 0
    status: str = "firing"
    receiver: str = ""
    groupLabels: dict[str, str] = Field(default_factory=dict)
    commonLabels: dict[str, str] = Field(default_factory=dict)
    commonAnnotations: dict[str, str] = Field(default_factory=dict)
    externalURL: str = ""
    alerts: list[Alert] = Field(default_factory=list)


class WebhookResponse(BaseModel):
    accepted: bool = True
    message: str = "Alert received"
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.post(
    "/webhook",
    response_model=WebhookResponse,
    status_code=status.HTTP_200_OK,
    summary="Receive AlertManager webhook",
    description=(
        "Receives a Prometheus AlertManager webhook payload. "
        "Returns 200 immediately and dispatches notifications asynchronously."
    ),
)
async def receive_alert_webhook(
    payload: AlertManagerWebhook,
    request: Request,
    background_tasks: BackgroundTasks,
) -> WebhookResponse:
    """
    Accept an AlertManager webhook and immediately return 200.

    Alert routing (Teams, email) happens in a background task so we never
    make AlertManager wait for external HTTP/SMTP calls.
    """
    alert_router = request.app.state.alert_router

    # Serialise to dict for the router (which accepts dict[str, Any])
    payload_dict = payload.model_dump()

    log.info(
        "alert_webhook_received",
        status=payload.status,
        receiver=payload.receiver,
        alert_count=len(payload.alerts),
        fingerprints=[a.fingerprint for a in payload.alerts],
    )

    background_tasks.add_task(_route_alert, alert_router, payload_dict)

    return WebhookResponse(
        accepted=True,
        message=f"Received {len(payload.alerts)} alert(s). Routing in background.",
    )


async def _route_alert(alert_router: Any, payload: dict[str, Any]) -> None:
    """Background task wrapper — catches and logs any routing errors."""
    try:
        await alert_router.route(payload)
    except Exception as exc:
        log.error("alert_routing_background_error", error=str(exc), exc_info=True)
