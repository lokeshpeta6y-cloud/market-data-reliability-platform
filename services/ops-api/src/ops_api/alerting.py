"""
Alert routing for the ops-api.

Receives a parsed AlertManager webhook payload and dispatches human-readable
notifications to:
  - Microsoft Teams (via incoming webhook)
  - SMTP email

Both channels are optional and independently enabled via settings.
All alerts are logged as structured JSON regardless of channel availability.
"""

from __future__ import annotations

import asyncio
import smtplib
import ssl
from datetime import UTC, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import httpx

from mdrp_common.logging import get_logger

from .settings import OpsApiSettings

log = get_logger(__name__)

# Severity colours for Teams adaptive cards
_SEVERITY_COLOURS = {
    "critical": "FF0000",  # red
    "warning": "FFA500",  # orange
    "info": "0078D4",  # Microsoft blue
}


class AlertRouter:
    """
    Routes AlertManager webhook payloads to configured notification channels.

    Usage::

        router = AlertRouter(settings)
        await router.route(webhook_payload)
    """

    def __init__(self, settings: OpsApiSettings) -> None:
        self._settings = settings

    async def route(self, alert_payload: dict[str, Any]) -> None:
        """
        Process an AlertManager webhook payload.

        Dispatches to Teams and/or SMTP concurrently, then returns.
        Errors in individual channels are logged but do not raise.

        Parameters
        ----------
        alert_payload:
            Parsed JSON body from the AlertManager webhook POST.
        """
        # Always log the incoming alert
        log.info(
            "alert_received",
            receiver=alert_payload.get("receiver"),
            status=alert_payload.get("status"),
            alert_count=len(alert_payload.get("alerts", [])),
        )

        tasks: list[asyncio.Task[None]] = []

        if self._settings.alert_teams_enabled and self._settings.teams_webhook_url:
            tasks.append(asyncio.create_task(self._send_teams(alert_payload)))

        if (
            self._settings.alert_email_enabled
            and self._settings.smtp_host
            and self._settings.smtp_recipients
        ):
            tasks.append(asyncio.create_task(self._send_email(alert_payload)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    log.error("alert_dispatch_error", error=str(result))

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    async def _send_teams(self, payload: dict[str, Any]) -> None:
        """POST a formatted message to the Teams incoming webhook."""
        message = self._format_teams_message(payload)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self._settings.teams_webhook_url,  # type: ignore[arg-type]
                    json=message,
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()
            log.info("teams_alert_sent", status_code=response.status_code)
        except httpx.HTTPStatusError as exc:
            log.error(
                "teams_alert_http_error",
                status_code=exc.response.status_code,
                body=exc.response.text[:500],
            )
            raise
        except httpx.RequestError as exc:
            log.error("teams_alert_request_error", error=str(exc))
            raise

    @staticmethod
    def _format_teams_message(payload: dict[str, Any]) -> dict[str, Any]:
        """
        Build a Teams MessageCard payload from AlertManager data.

        Uses the legacy MessageCard format (not Adaptive Card) for maximum
        compatibility with incoming webhook connectors.
        """
        alerts: list[dict[str, Any]] = payload.get("alerts", [])
        status: str = payload.get("status", "unknown").upper()

        # Determine severity from the first alert's labels
        first_alert = alerts[0] if alerts else {}
        severity = first_alert.get("labels", {}).get("severity", "info").lower()
        colour = _SEVERITY_COLOURS.get(severity, "0078D4")

        # Summary line
        alert_names = [a.get("labels", {}).get("alertname", "Unknown") for a in alerts]
        summary = f"[{status}] {', '.join(alert_names)}"

        # Build sections — one per alert
        sections: list[dict[str, Any]] = []
        for alert in alerts:
            labels = alert.get("labels", {})
            annotations = alert.get("annotations", {})
            starts_at = alert.get("startsAt", "")
            ends_at = alert.get("endsAt", "")

            facts: list[dict[str, str]] = [{"name": k, "value": str(v)} for k, v in labels.items()]
            if starts_at:
                facts.append({"name": "Started", "value": starts_at})
            if ends_at and ends_at != "0001-01-01T00:00:00Z":
                facts.append({"name": "Ended", "value": ends_at})

            sections.append(
                {
                    "activityTitle": labels.get("alertname", "Alert"),
                    "activitySubtitle": annotations.get("summary", ""),
                    "text": annotations.get("description", ""),
                    "facts": facts,
                }
            )

        return {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": colour,
            "summary": summary,
            "title": summary,
            "sections": sections,
        }

    # ------------------------------------------------------------------
    # SMTP email
    # ------------------------------------------------------------------

    async def _send_email(self, payload: dict[str, Any]) -> None:
        """Send an alert email via SMTP in a thread-pool executor."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._send_email_sync, payload)

    def _send_email_sync(self, payload: dict[str, Any]) -> None:
        """Synchronous SMTP send — runs in a thread-pool executor."""
        subject, body_text, body_html = self._format_email(payload)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = self._settings.smtp_from or "mdrp-alerts@localhost"
        msg["To"] = ", ".join(self._settings.smtp_recipients)

        msg.attach(MIMEText(body_text, "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))

        smtp_host = self._settings.smtp_host
        smtp_port = self._settings.smtp_port

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:  # type: ignore[arg-type]
                server.ehlo()
                if smtp_port == 587:
                    server.starttls(context=context)
                    server.ehlo()
                if self._settings.smtp_user and self._settings.smtp_password:
                    server.login(self._settings.smtp_user, self._settings.smtp_password)
                server.sendmail(
                    msg["From"],
                    self._settings.smtp_recipients,
                    msg.as_string(),
                )
            log.info(
                "email_alert_sent",
                recipients=self._settings.smtp_recipients,
                subject=subject,
            )
        except smtplib.SMTPException as exc:
            log.error("email_alert_smtp_error", error=str(exc))
            raise

    @staticmethod
    def _format_email(
        payload: dict[str, Any],
    ) -> tuple[str, str, str]:
        """Return (subject, plain_text, html) for the alert email."""
        alerts: list[dict[str, Any]] = payload.get("alerts", [])
        status: str = payload.get("status", "unknown").upper()

        alert_names = [a.get("labels", {}).get("alertname", "Unknown") for a in alerts]
        subject = f"[MDRP Alert] [{status}] {', '.join(alert_names)}"

        # Plain text body
        lines = [
            "MDRP Alert Notification",
            "========================",
            f"Status: {status}",
            f"Time:   {datetime.now(UTC).isoformat()}",
            f"Alerts: {len(alerts)}",
            "",
        ]
        for i, alert in enumerate(alerts, start=1):
            labels = alert.get("labels", {})
            annotations = alert.get("annotations", {})
            lines += [
                f"Alert #{i}: {labels.get('alertname', 'Unknown')}",
                f"  Severity:    {labels.get('severity', 'unknown')}",
                f"  Summary:     {annotations.get('summary', '')}",
                f"  Description: {annotations.get('description', '')}",
                f"  Started:     {alert.get('startsAt', '')}",
            ]
            for k, v in labels.items():
                if k not in ("alertname", "severity"):
                    lines.append(f"  {k}: {v}")
            lines.append("")

        plain_text = "\n".join(lines)

        # HTML body
        alert_rows = ""
        for alert in alerts:
            labels = alert.get("labels", {})
            annotations = alert.get("annotations", {})
            severity = labels.get("severity", "info")
            bg = {"critical": "#ffe0e0", "warning": "#fff3cd", "info": "#e7f3ff"}.get(
                severity, "#f8f9fa"
            )
            alert_rows += f"""
            <tr style="background:{bg}">
              <td style="padding:8px;font-weight:bold">
                {labels.get("alertname", "Unknown")}
              </td>
              <td style="padding:8px">{labels.get("severity", "").upper()}</td>
              <td style="padding:8px">{annotations.get("summary", "")}</td>
              <td style="padding:8px">{alert.get("startsAt", "")}</td>
            </tr>"""

        html = f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;margin:20px">
  <h2 style="color:#333">MDRP Alert — {status}</h2>
  <p>Generated: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}</p>
  <table border="1" cellpadding="0" cellspacing="0"
         style="border-collapse:collapse;width:100%;font-size:13px">
    <thead>
      <tr style="background:#0078d4;color:white">
        <th style="padding:8px;text-align:left">Alert</th>
        <th style="padding:8px;text-align:left">Severity</th>
        <th style="padding:8px;text-align:left">Summary</th>
        <th style="padding:8px;text-align:left">Started</th>
      </tr>
    </thead>
    <tbody>
      {alert_rows}
    </tbody>
  </table>
</body>
</html>"""

        return subject, plain_text, html
