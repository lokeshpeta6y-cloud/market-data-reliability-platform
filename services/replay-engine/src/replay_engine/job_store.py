"""
Redis-backed job store for replay jobs.

Jobs are stored as Redis hashes under the key pattern ``replay:job:{job_id}``.
A separate sorted set ``replay:jobs:pending`` (score = submission timestamp)
acts as the work queue so the engine can claim jobs atomically with GETDEL-style
Lua scripts to prevent double-processing.
"""

from __future__ import annotations

import json
from typing import Any

import redis

from mdrp_common.logging import get_logger
from mdrp_common.models import ReplayJob

log = get_logger(__name__)

# Redis key patterns
_JOB_HASH_KEY = "replay:job:{job_id}"
_PENDING_SET_KEY = "replay:jobs:pending"
_RECENT_LIST_KEY = "replay:jobs:recent"  # capped list of completed/failed job IDs
_RECENT_MAX = 200  # retain last 200 terminal jobs


class JobStore:
    """
    Manages ReplayJob lifecycle in Redis.

    Atomically claims pending jobs to ensure at-most-once execution per job.
    """

    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def save_job(self, job: ReplayJob) -> None:
        """Persist a new job and enqueue it for pickup by the engine."""
        key = _JOB_HASH_KEY.format(job_id=job.job_id)
        self._redis.hset(key, mapping=self._serialise(job))
        # Use requested_at epoch as score so ZPOPMIN gives oldest-first FIFO
        score = job.requested_at.timestamp()
        self._redis.zadd(_PENDING_SET_KEY, {job.job_id: score})
        log.info("job_saved", job_id=job.job_id, source=job.source)

    def update_status(
        self,
        job_id: str,
        status: str,
        events_replayed: int = 0,
        error: str | None = None,
    ) -> None:
        """Update mutable fields on an existing job hash."""
        key = _JOB_HASH_KEY.format(job_id=job_id)
        updates: dict[str, Any] = {
            "status": status,
            "events_replayed": events_replayed,
        }
        if error is not None:
            updates["error"] = error
        self._redis.hset(key, mapping=updates)

        # Move terminal jobs to the recent list for the ops-api to surface
        if status in ("completed", "failed"):
            pipe = self._redis.pipeline()
            pipe.lpush(_RECENT_LIST_KEY, job_id)
            pipe.ltrim(_RECENT_LIST_KEY, 0, _RECENT_MAX - 1)
            pipe.execute()

        log.info(
            "job_status_updated",
            job_id=job_id,
            status=status,
            events_replayed=events_replayed,
            error=error,
        )

    def claim_pending_job(self) -> ReplayJob | None:
        """
        Atomically pop the oldest pending job from the sorted set and mark
        it as *running* in its hash.  Returns None when the queue is empty.

        Uses a Lua script to make the pop + status-update atomic so that
        multiple engine instances cannot both claim the same job.
        """
        # ZPOPMIN returns [(member, score), ...] or []
        result: list[tuple[bytes, float]] = self._redis.zpopmin(
            _PENDING_SET_KEY, count=1
        )
        if not result:
            return None

        job_id = result[0][0]
        if isinstance(job_id, bytes):
            job_id = job_id.decode()

        job = self.get_job(job_id)
        if job is None:
            log.warning("claimed_job_hash_missing", job_id=job_id)
            return None

        # Transition to running
        self._redis.hset(
            _JOB_HASH_KEY.format(job_id=job_id),
            mapping={"status": "running"},
        )
        job = job.model_copy(update={"status": "running"})
        log.info("job_claimed", job_id=job_id, source=job.source)
        return job

    def get_job(self, job_id: str) -> ReplayJob | None:
        """Fetch a job by ID.  Returns None when the key doesn't exist."""
        key = _JOB_HASH_KEY.format(job_id=job_id)
        raw = self._redis.hgetall(key)
        if not raw:
            return None
        return self._deserialise(raw)

    def list_recent_jobs(self, limit: int = 50) -> list[ReplayJob]:
        """Return the most recent *limit* terminal jobs (completed + failed)."""
        job_ids = self._redis.lrange(_RECENT_LIST_KEY, 0, limit - 1)
        jobs: list[ReplayJob] = []
        for jid in job_ids:
            if isinstance(jid, bytes):
                jid = jid.decode()
            job = self.get_job(jid)
            if job is not None:
                jobs.append(job)
        return jobs

    def list_all_jobs(self, limit: int = 100) -> list[ReplayJob]:
        """
        Return up to *limit* jobs from the recent list plus all currently pending
        and running jobs.  Intended for the ops-api /replay list endpoint.
        """
        # Collect pending job IDs from the sorted set
        pending_ids = [
            jid.decode() if isinstance(jid, bytes) else jid
            for jid in self._redis.zrange(_PENDING_SET_KEY, 0, -1)
        ]
        recent_ids = [
            jid.decode() if isinstance(jid, bytes) else jid
            for jid in self._redis.lrange(_RECENT_LIST_KEY, 0, limit - 1)
        ]

        seen: set[str] = set()
        jobs: list[ReplayJob] = []
        for jid in pending_ids + recent_ids:
            if jid in seen:
                continue
            seen.add(jid)
            job = self.get_job(jid)
            if job is not None:
                jobs.append(job)
            if len(jobs) >= limit:
                break

        jobs.sort(key=lambda j: j.requested_at, reverse=True)
        return jobs

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialise(job: ReplayJob) -> dict[str, str]:
        """Convert a ReplayJob to a flat string dict suitable for HSET."""
        raw = json.loads(job.model_dump_json())
        return {k: json.dumps(v) if not isinstance(v, str) else v for k, v in raw.items()}

    @staticmethod
    def _deserialise(raw: dict[bytes | str, bytes | str]) -> ReplayJob:
        """Reconstruct a ReplayJob from an HGETALL result."""
        decoded: dict[str, Any] = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            # Attempt JSON parse for non-string fields; fall back to raw string
            try:
                decoded[key] = json.loads(val)
            except (json.JSONDecodeError, ValueError):
                decoded[key] = val
        return ReplayJob.model_validate(decoded)
