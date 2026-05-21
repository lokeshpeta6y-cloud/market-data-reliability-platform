"""
FastAPI dependency injection for shared resources.

All shared clients (Redis, Kafka producer, S3 storage) are created once
at application startup and injected via FastAPI's dependency system.
This avoids re-creating connections on every request.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Depends, Request

from mdrp_common.kafka_client import MdrpProducer
from mdrp_common.storage import BronzeStorageClient

from .settings import OpsApiSettings


@lru_cache(maxsize=1)
def get_settings() -> OpsApiSettings:
    """Return the singleton settings instance (cached after first call)."""
    return OpsApiSettings()


# ---------------------------------------------------------------------------
# Redis dependency
# ---------------------------------------------------------------------------


async def get_redis(
    request: Request,
) -> aioredis.Redis:
    """
    Yield the async Redis client stored on ``app.state.redis``.

    The client is created once during application startup (see main.py) and
    closed on shutdown.  Injecting via app.state keeps the connection pool
    shared across all requests.
    """
    return request.app.state.redis


# ---------------------------------------------------------------------------
# Kafka producer dependency
# ---------------------------------------------------------------------------


def get_producer(request: Request) -> MdrpProducer:
    """
    Return the synchronous MdrpProducer stored on ``app.state.producer``.

    Producing to Kafka is wrapped in a thread-pool executor inside async
    route handlers to avoid blocking the event loop.
    """
    return request.app.state.producer


# ---------------------------------------------------------------------------
# Storage client dependency
# ---------------------------------------------------------------------------


def get_storage_client(request: Request) -> BronzeStorageClient:
    """Return the BronzeStorageClient stored on ``app.state.storage``."""
    return request.app.state.storage


# ---------------------------------------------------------------------------
# Convenience type aliases for route signatures
# ---------------------------------------------------------------------------

RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]
ProducerDep = Annotated[MdrpProducer, Depends(get_producer)]
StorageDep = Annotated[BronzeStorageClient, Depends(get_storage_client)]
SettingsDep = Annotated[OpsApiSettings, Depends(get_settings)]
