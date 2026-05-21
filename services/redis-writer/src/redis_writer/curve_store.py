"""
Redis persistence layer for the Market Data Reliability Platform.

Key schema
----------
curve:latest:{instrument}       Redis Hash   — field = tenor, value = TenorPrice JSON
curve:history:{instrument}      Redis Sorted Set — member = CurveEvent JSON,
                                                   score  = event_timestamp Unix-ms
curve:snapshot:{instrument}     Redis String — ForwardCurveSnapshot JSON
provider:health:{provider}      Redis Hash   — last_event_at (ISO), events_per_minute

The Sorted Set history is trimmed to the *most recent* ``curve_history_max_entries``
members after every write (ZREMRANGEBYRANK removes the oldest entries).

Provider health tracks a sliding events-per-minute counter via two Redis keys:
  provider:health:{provider}:minute:{epoch_minute}  (TTL 120 s)
The sum of these two keys gives the rolling one-minute event count.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

import redis

from mdrp_common.logging import get_logger
from mdrp_common.models import CurveEvent, ForwardCurveSnapshot, TenorPrice

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

_PFX_LATEST = "curve:latest:"
_PFX_HISTORY = "curve:history:"
_PFX_SNAPSHOT = "curve:snapshot:"
_PFX_HEALTH = "provider:health:"
_PFX_HEALTH_MINUTE = "provider:health:{}:minute:{}"


# ---------------------------------------------------------------------------
# CurveStore
# ---------------------------------------------------------------------------


class CurveStore:
    """
    All Redis I/O for the redis-writer service.

    Parameters
    ----------
    redis_client:
        A connected ``redis.Redis`` instance.  Caller owns lifecycle.
    curve_history_max_entries:
        Maximum number of entries retained in the history sorted set per
        instrument.  Oldest (lowest score) entries are pruned on every write.
    expected_tenors:
        Mapping of canonical instrument → expected number of unique tenors used
        to compute snapshot completeness.
    snapshot_completeness_threshold:
        Minimum completeness fraction [0, 1] required before a
        ForwardCurveSnapshot is written to Redis.
    staleness_threshold_seconds:
        Seconds since ``last_event_at`` after which an instrument is considered
        stale.
    """

    def __init__(
        self,
        redis_client: redis.Redis[Any],
        curve_history_max_entries: int = 1000,
        expected_tenors: dict[str, int] | None = None,
        snapshot_completeness_threshold: float = 0.80,
        staleness_threshold_seconds: int = 600,
    ) -> None:
        self._redis = redis_client
        self._history_max = curve_history_max_entries
        self._expected_tenors: dict[str, int] = expected_tenors or {}
        self._snapshot_threshold = snapshot_completeness_threshold
        self._staleness_threshold = staleness_threshold_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_tenor(self, event: CurveEvent) -> None:
        """
        Persist *event* to Redis and conditionally assemble a snapshot.

        Operations performed (all atomic within a pipeline):
        1. HSET  curve:latest:{instrument}  tenor  <TenorPrice JSON>
        2. ZADD  curve:history:{instrument}  score=event_timestamp_ms  member=event JSON
        3. ZREMRANGEBYRANK  (trim to last N entries)
        4. HSET  provider:health:{provider}  (last_event_at, events_per_minute)

        After the pipeline, attempt snapshot assembly if completeness >= threshold.
        """
        instrument = event.instrument
        provider = event.provider
        event_ts_ms = int(event.event_timestamp.timestamp() * 1000)

        tenor_price = TenorPrice(
            tenor=event.tenor,
            price=event.price,
            quality_score=event.quality_score,
            last_updated=event.event_timestamp,
        )
        tenor_price_json = tenor_price.model_dump_json()

        # Serialise curve event for history (exclude large nested data if present)
        history_member = event.model_dump_json()

        now_iso = datetime.now(UTC).isoformat()
        epm = self._events_per_minute(provider)

        pipe = self._redis.pipeline(transaction=True)

        # 1. Latest tenor data
        pipe.hset(_PFX_LATEST + instrument, event.tenor, tenor_price_json)

        # 2. History sorted set
        pipe.zadd(_PFX_HISTORY + instrument, {history_member: event_ts_ms})

        # 3. Trim to last N entries (keep highest scores = most recent timestamps)
        # ZREMRANGEBYRANK removes from lowest score up to offset before the Nth-from-end
        pipe.zremrangebyrank(
            _PFX_HISTORY + instrument, 0, -(self._history_max + 1)
        )

        # 4. Provider health
        pipe.hset(
            _PFX_HEALTH + provider,
            mapping={
                "last_event_at": now_iso,
                "events_per_minute": str(epm + 1),
            },
        )

        pipe.execute()

        # Increment rolling minute counter (outside pipeline so TTL applies cleanly)
        self._increment_minute_counter(provider)

        # Conditionally assemble snapshot
        self._maybe_assemble_snapshot(event)

    def get_snapshot(self, instrument: str) -> ForwardCurveSnapshot | None:
        """Return the stored ForwardCurveSnapshot for *instrument*, or None."""
        raw = self._redis.get(_PFX_SNAPSHOT + instrument)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            return ForwardCurveSnapshot.model_validate(data)
        except Exception as exc:
            log.warning(
                "snapshot_deserialisation_failed",
                instrument=instrument,
                error=str(exc),
            )
            return None

    def get_stale_instruments(self) -> list[str]:
        """
        Return a list of instrument names whose last event is older than
        ``staleness_threshold_seconds``.

        Scans all ``curve:latest:*`` keys and reads ``last_event_at`` from the
        corresponding provider health hash.  Instruments with no health record
        are skipped.
        """
        stale: list[str] = []
        now_ts = time.time()

        # Discover all instruments from existing latest keys
        cursor = 0
        instruments: list[str] = []
        while True:
            cursor, keys = self._redis.scan(
                cursor=cursor, match=f"{_PFX_LATEST}*", count=200
            )
            for key in keys:
                # key is bytes or str depending on decode_responses setting
                key_str = key.decode() if isinstance(key, bytes) else key
                instruments.append(key_str[len(_PFX_LATEST):])
            if cursor == 0:
                break

        for instrument in instruments:
            last_event_at = self._last_event_at_for_instrument(instrument)
            if last_event_at is None:
                continue
            staleness_s = now_ts - last_event_at
            if staleness_s > self._staleness_threshold:
                stale.append(instrument)

        return stale

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _maybe_assemble_snapshot(self, event: CurveEvent) -> None:
        """
        Check if we have enough tenors for *event.instrument* to assemble a
        ForwardCurveSnapshot; if so, write it to Redis.
        """
        instrument = event.instrument
        expected = self._expected_tenors.get(instrument)
        if not expected:
            # No expectation configured — skip snapshot assembly
            return

        # Read current tenor data from the hash
        raw_tenors: dict[bytes | str, bytes | str] = self._redis.hgetall(
            _PFX_LATEST + instrument
        )
        if not raw_tenors:
            return

        tenors: dict[str, TenorPrice] = {}
        for tenor_key, tenor_val in raw_tenors.items():
            key_str = tenor_key.decode() if isinstance(tenor_key, bytes) else tenor_key
            val_str = tenor_val.decode() if isinstance(tenor_val, bytes) else tenor_val
            try:
                tenors[key_str] = TenorPrice.model_validate(json.loads(val_str))
            except Exception as exc:
                log.warning(
                    "tenor_deserialisation_failed",
                    instrument=instrument,
                    tenor=key_str,
                    error=str(exc),
                )
                continue

        completeness = len(tenors) / expected
        if completeness < self._snapshot_threshold:
            log.debug(
                "snapshot_skipped_insufficient_tenors",
                instrument=instrument,
                present=len(tenors),
                expected=expected,
                completeness=round(completeness, 4),
            )
            return

        snapshot = ForwardCurveSnapshot(
            curve_name=event.curve_name,
            instrument=instrument,
            as_of=event.event_timestamp,
            tenors=tenors,
            completeness=min(1.0, completeness),
            is_authoritative=True,
            version=event.version,
            provider=event.provider,
        )

        self._redis.set(
            _PFX_SNAPSHOT + instrument,
            snapshot.model_dump_json(),
        )

        log.info(
            "snapshot_assembled",
            instrument=instrument,
            completeness=round(completeness, 4),
            tenor_count=len(tenors),
            version=event.version,
        )

    def _last_event_at_for_instrument(self, instrument: str) -> float | None:
        """
        Return the Unix timestamp of the most recent event for *instrument* by
        reading the max score from its history sorted set.  Returns None when
        there are no history entries.
        """
        results = self._redis.zrevrangebyscore(
            _PFX_HISTORY + instrument,
            max="+inf",
            min="-inf",
            start=0,
            num=1,
            withscores=True,
        )
        if not results:
            return None
        # score is Unix-ms
        _member, score = results[0]
        return float(score) / 1000.0

    def _events_per_minute(self, provider: str) -> int:
        """Read the stored events_per_minute counter for *provider* from the health hash."""
        raw = self._redis.hget(_PFX_HEALTH + provider, "events_per_minute")
        if raw is None:
            return 0
        try:
            return int(raw.decode() if isinstance(raw, bytes) else raw)
        except (ValueError, AttributeError):
            return 0

    def _increment_minute_counter(self, provider: str) -> None:
        """
        Increment a per-minute event counter for *provider*.

        Uses a key with a 120-second TTL so the counter automatically expires.
        The rolling events_per_minute stored in the health hash is updated
        separately in the pipeline; this counter is for future precision use.
        """
        epoch_minute = int(time.time() // 60)
        key = _PFX_HEALTH_MINUTE.format(provider, epoch_minute)
        pipe = self._redis.pipeline(transaction=False)
        pipe.incr(key)
        pipe.expire(key, 120)
        pipe.execute()
