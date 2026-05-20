"""
Redis-backed event deduplicator for the validation-service.

Uses Redis SETNX (SET if Not eXists) with an expiry TTL to track event IDs
we have already processed. The key space is:

    dedup:event:{event_id}

The value is the Unix timestamp at which the event was first seen; this aids
forensic queries but is not used for any logic.

SETNX is atomic — there is no race between checking and setting, making this
safe for multiple validation-service replicas running in parallel.
"""

from __future__ import annotations

from datetime import datetime, timezone

import redis

from mdrp_common.logging import get_logger

logger = get_logger(__name__)

_KEY_PREFIX = "dedup:event:"


class Deduplicator:
    """
    Redis-backed deduplicator.

    Parameters
    ----------
    redis_client:
        A connected redis.Redis instance. Caller is responsible for lifecycle.
    ttl_seconds:
        How long an event_id is remembered. Events with the same ID arriving
        after this window are treated as new (which is the correct behaviour
        for genuine re-deliveries after a long outage).
    """

    def __init__(self, redis_client: redis.Redis, ttl_seconds: int = 3600) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds

    def is_duplicate(self, event_id: str) -> bool:
        """
        Return True if this event_id has been seen before (duplicate).

        Uses SET NX EX atomically so that concurrent replicas cannot both
        accept the same event_id.

        Parameters
        ----------
        event_id:
            Unique identifier from the RawMarketEvent.

        Returns
        -------
        bool
            True  → already seen, caller should discard this event.
            False → first time seen, event has been recorded.
        """
        key = f"{_KEY_PREFIX}{event_id}"
        first_seen_epoch = str(datetime.now(timezone.utc).timestamp())

        # SET key value NX EX ttl — returns True if key was newly set, None if key existed
        was_set: bool | None = self._redis.set(
            key,
            first_seen_epoch,
            nx=True,
            ex=self._ttl,
        )

        if was_set:
            # Key did not exist; we just created it → not a duplicate
            logger.debug(
                "dedup_event_recorded",
                event_id=event_id,
                ttl_seconds=self._ttl,
            )
            return False

        # Key already existed → duplicate
        logger.debug("dedup_duplicate_detected", event_id=event_id)
        return True

    def ping(self) -> bool:
        """Return True if Redis is reachable. Used by health checks."""
        try:
            return self._redis.ping()
        except redis.RedisError:
            return False
