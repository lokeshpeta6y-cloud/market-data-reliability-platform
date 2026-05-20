"""
SnapshotAssembler — groups CurveEvents into ForwardCurveSnapshot objects.

Windowing strategy
------------------
Events are bucketed into fixed-width tumbling windows of ``snapshot_window_minutes``
minutes, keyed by ``(curve_name, window_start)``.  The window start is calculated
by truncating ``event_timestamp`` to the nearest window boundary:

    window_start = floor(event_timestamp / window_width) * window_width

A window is considered *ready* (eligible for flushing) once its end time is at
least ``2 * snapshot_window_minutes`` in the past.  This gives a full window width
of grace time for late-arriving events before the snapshot is finalised.

Completeness and authority
--------------------------
- ``completeness`` = tenors_received / expected_tenors.
- ``expected_tenors`` defaults to the first snapshot size seen for that
  ``curve_name`` (i.e. we learn the expected shape from the data).  If
  ``expected_tenors_per_curve > 0`` is configured it overrides the learned value.
- ``is_authoritative = completeness >= 0.95 AND min(quality_score) >= 0.70``
  (both thresholds are configurable in settings).

Thread safety
-------------
All public methods acquire ``self._lock`` so the assembler can safely be shared
between the consume thread and a polling/flush thread.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone

from mdrp_common.logging import get_logger
from mdrp_common.models import CurveEvent, ForwardCurveSnapshot, TenorPrice

logger = get_logger(__name__)

# Authoritative completeness floor (overridden by settings when constructed)
_DEFAULT_AUTH_COMPLETENESS = 0.95


class _WindowBuffer:
    """Accumulates CurveEvents for a single (curve_name, window_start) window."""

    __slots__ = (
        "curve_name",
        "instrument",
        "provider",
        "window_start",
        "version",
        "tenors",
    )

    def __init__(
        self,
        curve_name: str,
        instrument: str,
        provider: str,
        window_start: datetime,
        version: int,
    ) -> None:
        self.curve_name = curve_name
        self.instrument = instrument
        self.provider = provider
        self.window_start = window_start
        self.version = version
        # Latest price per tenor (last-write-wins within the window)
        self.tenors: dict[str, TenorPrice] = {}

    def add(self, event: CurveEvent) -> None:
        """Add or overwrite the tenor entry with the latest event."""
        self.tenors[event.tenor] = TenorPrice(
            tenor=event.tenor,
            price=event.price,
            quality_score=event.quality_score,
            last_updated=event.event_timestamp,
        )
        # Track the highest version seen
        if event.version > self.version:
            self.version = event.version

    def build_snapshot(
        self,
        expected_tenors: int,
        min_completeness: float,
        min_quality_score: float,
        auth_completeness_threshold: float,
    ) -> ForwardCurveSnapshot:
        """Assemble a ForwardCurveSnapshot from the buffered tenor data."""
        tenors_received = len(self.tenors)
        # Guard against zero expected_tenors (should not happen, but be safe)
        effective_expected = expected_tenors if expected_tenors > 0 else tenors_received
        completeness = tenors_received / effective_expected if effective_expected > 0 else 0.0
        completeness = min(completeness, 1.0)  # cap at 1.0 in case of overcounting

        quality_scores = [tp.quality_score for tp in self.tenors.values()]
        min_qs = min(quality_scores) if quality_scores else 0.0

        is_authoritative = (
            completeness >= auth_completeness_threshold and min_qs >= min_quality_score
        )

        return ForwardCurveSnapshot(
            curve_name=self.curve_name,
            instrument=self.instrument,
            as_of=self.window_start,
            tenors=dict(self.tenors),
            completeness=completeness,
            is_authoritative=is_authoritative,
            version=self.version,
            provider=self.provider,
        )


class SnapshotAssembler:
    """
    Buffers CurveEvents by ``(curve_name, time_window)`` and returns ready
    ForwardCurveSnapshot objects when windows expire.

    Parameters
    ----------
    snapshot_window_minutes:
        Width of each tumbling window in minutes.
    min_completeness:
        Minimum completeness for a snapshot written to Gold.
    min_quality_score:
        Minimum quality score floor for an authoritative snapshot.
    expected_tenors_per_curve:
        If > 0, use this as the denominator for completeness rather than
        the learned first-window size.
    auth_completeness_threshold:
        Completeness threshold above which ``is_authoritative`` is True
        (must also pass ``min_quality_score``).  Defaults to 0.95.
    """

    def __init__(
        self,
        snapshot_window_minutes: int = 5,
        min_completeness: float = 0.80,
        min_quality_score: float = 0.70,
        expected_tenors_per_curve: int = 0,
        auth_completeness_threshold: float = _DEFAULT_AUTH_COMPLETENESS,
    ) -> None:
        self._window_minutes = snapshot_window_minutes
        self._window_seconds = snapshot_window_minutes * 60
        self._min_completeness = min_completeness
        self._min_quality_score = min_quality_score
        self._expected_tenors_override = expected_tenors_per_curve
        self._auth_threshold = auth_completeness_threshold

        # {(curve_name, window_start): _WindowBuffer}
        self._windows: dict[tuple[str, datetime], _WindowBuffer] = {}

        # Learned expected tenor count per curve name
        self._learned_expected: dict[str, int] = defaultdict(int)

        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, event: CurveEvent) -> None:
        """
        Add a CurveEvent to its corresponding time window buffer.

        The window start is derived by truncating ``event_timestamp`` to the
        nearest ``snapshot_window_minutes`` boundary (UTC).
        """
        window_start = self._window_start_for(event.event_timestamp)
        key = (event.curve_name, window_start)

        with self._lock:
            if key not in self._windows:
                self._windows[key] = _WindowBuffer(
                    curve_name=event.curve_name,
                    instrument=event.instrument,
                    provider=event.provider,
                    window_start=window_start,
                    version=event.version,
                )
            self._windows[key].add(event)

    def get_ready_snapshots(self) -> list[ForwardCurveSnapshot]:
        """
        Return all snapshots whose windows ended more than one window-width ago,
        removing them from the internal state.

        Only snapshots with ``completeness >= min_completeness`` are returned
        (non-authoritative partial curves are dropped with a warning log).
        """
        now = datetime.now(timezone.utc)
        cutoff_seconds = 2 * self._window_seconds
        ready: list[ForwardCurveSnapshot] = []
        expired_keys: list[tuple[str, datetime]] = []

        with self._lock:
            for key, buf in self._windows.items():
                window_end = buf.window_start.timestamp() + self._window_seconds
                age_seconds = now.timestamp() - window_end
                if age_seconds >= cutoff_seconds:
                    expired_keys.append(key)

            for key in expired_keys:
                buf = self._windows.pop(key)
                expected = self._get_expected_tenors(buf.curve_name, len(buf.tenors))
                snapshot = buf.build_snapshot(
                    expected_tenors=expected,
                    min_completeness=self._min_completeness,
                    min_quality_score=self._min_quality_score,
                    auth_completeness_threshold=self._auth_threshold,
                )

                if snapshot.completeness < self._min_completeness:
                    logger.warning(
                        "snapshot_below_min_completeness_discarded",
                        curve_name=snapshot.curve_name,
                        as_of=snapshot.as_of.isoformat(),
                        completeness=round(snapshot.completeness, 4),
                        tenors_received=len(buf.tenors),
                        expected_tenors=expected,
                        min_completeness=self._min_completeness,
                    )
                    continue

                logger.info(
                    "snapshot_ready",
                    curve_name=snapshot.curve_name,
                    as_of=snapshot.as_of.isoformat(),
                    completeness=round(snapshot.completeness, 4),
                    is_authoritative=snapshot.is_authoritative,
                    tenors=len(buf.tenors),
                )
                ready.append(snapshot)

        return ready

    def pending_window_count(self) -> int:
        """Return the number of open (not yet expired) windows."""
        with self._lock:
            return len(self._windows)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _window_start_for(self, ts: datetime) -> datetime:
        """Truncate *ts* to the current window boundary (UTC)."""
        epoch_seconds = int(ts.timestamp())
        window_epoch = (epoch_seconds // self._window_seconds) * self._window_seconds
        return datetime.fromtimestamp(window_epoch, tz=timezone.utc)

    def _get_expected_tenors(self, curve_name: str, tenors_seen: int) -> int:
        """
        Return the expected tenor count for *curve_name*.

        If ``expected_tenors_override`` was configured, always use that.
        Otherwise, use the maximum tenors seen for this curve so far.
        """
        if self._expected_tenors_override > 0:
            return self._expected_tenors_override

        # Learn from the data: the expected count grows monotonically
        if tenors_seen > self._learned_expected[curve_name]:
            self._learned_expected[curve_name] = tenors_seen

        return self._learned_expected[curve_name]
