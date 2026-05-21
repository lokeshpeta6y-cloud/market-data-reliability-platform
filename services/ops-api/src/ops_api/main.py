"""
Ops API — FastAPI application factory.

Creates the FastAPI app, wires up all middleware, mounts routers,
handles startup/shutdown lifecycle, and exposes Prometheus /metrics.
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from mdrp_common.kafka_client import MdrpProducer, ensure_topics
from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import register_metrics
from mdrp_common.storage import BronzeStorageClient
from .alerting import AlertRouter
from .routers import alerts, curves, dlq, health, replay
from .settings import OpsApiSettings

_SERVICE_NAME = "ops-api"


# ---------------------------------------------------------------------------
# Application lifespan — startup + shutdown
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan context manager.

    Resources created here are stored on ``app.state`` so route handlers
    can retrieve them via dependency injection (see dependencies.py).
    """
    settings: OpsApiSettings = app.state.settings
    log = get_logger(_SERVICE_NAME)

    log.info("ops_api_starting", version=settings.service_version)

    # -----------------------------------------------------------------------
    # Async Redis client
    # -----------------------------------------------------------------------
    redis_client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=False,
        socket_timeout=5,
        socket_connect_timeout=5,
        max_connections=20,
    )
    app.state.redis = redis_client
    log.info("redis_connected", url=settings.redis_url)

    # -----------------------------------------------------------------------
    # Kafka producer (sync, thread-safe)
    # -----------------------------------------------------------------------
    producer = MdrpProducer(settings.kafka_bootstrap_servers)
    app.state.producer = producer
    log.info("kafka_producer_created", brokers=settings.kafka_bootstrap_servers)

    # Ensure topics exist (best-effort; don't fail startup if Kafka is down)
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, ensure_topics, settings.kafka_bootstrap_servers
        )
    except Exception as exc:
        log.warning("kafka_topic_ensure_failed", error=str(exc))

    # -----------------------------------------------------------------------
    # S3/MinIO storage client
    # -----------------------------------------------------------------------
    storage = BronzeStorageClient(
        bucket=settings.s3_bucket_bronze,
        region=settings.aws_region,
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    app.state.storage = storage
    log.info("storage_client_created", bucket=settings.s3_bucket_bronze)

    # -----------------------------------------------------------------------
    # Alert router
    # -----------------------------------------------------------------------
    app.state.alert_router = AlertRouter(settings)
    log.info(
        "alert_router_configured",
        teams_enabled=settings.alert_teams_enabled,
        email_enabled=settings.alert_email_enabled,
    )

    # -----------------------------------------------------------------------
    # Redis-backed job store (sync, used in replay router via app.state)
    #
    # The ops-api imports JobStore from the replay-engine package so both
    # services share exactly the same Redis key schema without duplicating
    # code.  The replay-engine is declared as a dependency in pyproject.toml.
    # -----------------------------------------------------------------------
    import redis as sync_redis
    from replay_engine.job_store import JobStore  # type: ignore[import]

    sync_redis_client = sync_redis.from_url(
        settings.redis_url,
        decode_responses=False,
        socket_timeout=5,
        socket_connect_timeout=5,
    )
    app.state.job_store = JobStore(sync_redis_client)
    log.info("job_store_initialised")

    log.info("ops_api_started")
    yield  # ← application is running

    # -----------------------------------------------------------------------
    # Shutdown
    # -----------------------------------------------------------------------
    log.info("ops_api_shutting_down")
    producer.flush(timeout_s=10.0)
    await redis_client.aclose()
    log.info("ops_api_shutdown_complete")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app(settings: OpsApiSettings | None = None) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Accepts an optional pre-built settings instance (useful for tests).
    When not provided, settings are loaded from environment variables.
    """
    if settings is None:
        settings = OpsApiSettings()

    configure_logging(service_name=_SERVICE_NAME, level=settings.log_level)

    # Start Prometheus metrics HTTP server on a separate port
    register_metrics(
        service_name=_SERVICE_NAME,
        port=settings.metrics_port,
        version=settings.service_version,
    )

    app = FastAPI(
        title="MDRP Ops API",
        description=(
            "Operational control plane for the Market Data Reliability Platform. "
            "Exposes pipeline health, provider metrics, curve snapshots, "
            "replay job management, DLQ inspection, and alert webhook receiver."
        ),
        version=settings.service_version,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Store settings on app.state for access in lifespan and dependencies
    app.state.settings = settings

    # -----------------------------------------------------------------------
    # CORS
    # -----------------------------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------------
    # OpenTelemetry instrumentation (optional — only if SDK is installed)
    # -----------------------------------------------------------------------
    if settings.otel_enabled:
        _setup_otel(app, settings)

    # -----------------------------------------------------------------------
    # Routers
    # -----------------------------------------------------------------------
    app.include_router(health.router)
    app.include_router(curves.router)
    app.include_router(replay.router)
    app.include_router(dlq.router)
    app.include_router(alerts.router)

    # -----------------------------------------------------------------------
    # Prometheus metrics scrape endpoint
    # (In addition to the separate HTTP server started by register_metrics,
    #  expose /metrics on the main port for convenience.)
    # -----------------------------------------------------------------------
    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)

    # -----------------------------------------------------------------------
    # Global exception handler — structured error logging
    # -----------------------------------------------------------------------
    log = get_logger(_SERVICE_NAME)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        log.error(
            "unhandled_exception",
            path=request.url.path,
            method=request.method,
            error=str(exc),
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "An unexpected error occurred."},
        )

    return app


# ---------------------------------------------------------------------------
# OTel setup helper
# ---------------------------------------------------------------------------


def _setup_otel(app: FastAPI, settings: OpsApiSettings) -> None:
    """
    Wire up OpenTelemetry tracing via FastAPI instrumentation.

    Silently skips if the opentelemetry packages are not installed — this
    keeps the service startable in environments without OTel.
    """
    log = get_logger(_SERVICE_NAME)
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource(attributes={SERVICE_NAME: _SERVICE_NAME})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)
        log.info(
            "otel_instrumentation_enabled",
            endpoint=settings.otel_exporter_otlp_endpoint,
        )
    except ImportError:
        log.info(
            "otel_packages_not_installed_skipping",
            detail="Install opentelemetry-sdk and opentelemetry-instrumentation-fastapi",
        )


# ---------------------------------------------------------------------------
# Module-level app instance for uvicorn
# ---------------------------------------------------------------------------

app = create_app()
