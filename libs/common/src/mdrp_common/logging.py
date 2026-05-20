"""
Structured JSON logging for the Market Data Reliability Platform.

Every service calls configure_logging() at startup. All log records are emitted
as newline-delimited JSON — compatible with CloudWatch Logs Insights, Datadog,
and any log aggregation pipeline that understands JSON.

Fields on every log record:
  timestamp, level, service, message, trace_id (if present in context)
"""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Any

import structlog

# Per-request trace ID propagated through async/sync context
_trace_id_var: ContextVar[str | None] = ContextVar("trace_id", default=None)


def set_trace_id(trace_id: str) -> None:
    _trace_id_var.set(trace_id)


def get_trace_id() -> str | None:
    return _trace_id_var.get()


def _add_trace_id(
    logger: Any, method: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    trace_id = get_trace_id()
    if trace_id:
        event_dict["trace_id"] = trace_id
    return event_dict


def configure_logging(service_name: str, level: str = "INFO") -> None:
    """
    Configure structlog for JSON output. Call once at process startup.

    In development (when stdout is a TTY), emits colourised key=value output
    for readability. In production (non-TTY), emits compact JSON.
    """
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_trace_id,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    is_tty = sys.stdout.isatty()

    if is_tty:
        renderer: Any = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level.upper())
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging so that libraries (confluent-kafka, boto3, etc.)
    # route through structlog.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.getLevelName(level.upper()),
    )

    # Bind service name to every log record for this process
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)
