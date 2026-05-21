"""
Fault injector for the provider emulator.

FaultInjector takes a clean list of RawMarketEvents and returns a potentially
enlarged (duplicates) or reduced (partial curve) list with realistic faults
applied.  Every injected fault is recorded in the event's `injected_faults`
field so that downstream validation can measure detection rates.

Design decisions
----------------
* DELAYED and OUT_OF_ORDER events are managed through an internal hold queue.
  Callers must call `drain_ready()` on every publish cycle to collect events
  whose hold time has expired.  This simulates realistic network buffering.
* Faults are applied probabilistically but independently — an event can receive
  multiple faults (e.g. DELAYED + DUPLICATE).
* All randomness uses the standard `random` module seeded from the OS; for
  reproducible tests, callers can `random.seed(n)` before calling inject().
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from mdrp_common.logging import get_logger
from mdrp_common.metrics import FAULTS_INJECTED_TOTAL
from mdrp_common.models import FaultType, RawMarketEvent

logger = get_logger(__name__)

# Field aliases used by the SCHEMA_DRIFT fault.
# Maps the original payload key to the drifted key name.
_SCHEMA_DRIFT_MAP: dict[str, str] = {
    "price": "px",
    "tenor": "delivery_tenor",
    "bid": "bid_px",
    "ask": "ask_px",
    "volume": "vol",
    "currency": "ccy",
    "unit": "uom",
    "curve_name": "curve",
    "open_interest": "oi",
    "vwap": "vwap_price",
}

# Fields that, when nulled, make an event structurally malformed.
_REQUIRED_FIELDS: list[str] = ["price", "tenor", "curve_name", "currency", "unit"]


# ---------------------------------------------------------------------------
# Hold-queue entry
# ---------------------------------------------------------------------------


@dataclass
class _HeldEvent:
    """An event being held in the delay / out-of-order queue."""

    event: RawMarketEvent
    release_at: float  # monotonic time in seconds


# ---------------------------------------------------------------------------
# FaultInjector
# ---------------------------------------------------------------------------


class FaultInjector:
    """
    Applies configurable fault types to a stream of clean RawMarketEvents.

    Parameters
    ----------
    fault_rate_duplicate : float
        Probability that any single event is duplicated.
    fault_rate_malformed : float
        Probability that any single event has a field corrupted or nulled.
    fault_rate_delayed : float
        Probability that any single event is held for a random delay.
    fault_rate_out_of_order : float
        Probability that any single event is held briefly and released
        out of sequence relative to adjacent events.
    fault_rate_schema_drift : float
        Probability that a payload field is renamed to a drifted name.
    fault_rate_stale : float
        Probability that an event's event_timestamp is backdated by 2-24 hours.
    fault_rate_partial_curve : float
        Probability that, for a batch of events, some tenors are randomly dropped.
    delay_min_seconds : float
        Minimum hold time in seconds for DELAYED events.
    delay_max_seconds : float
        Maximum hold time in seconds for DELAYED events.
    out_of_order_max_hold_seconds : float
        Maximum hold time for OUT_OF_ORDER events (kept short to maintain
        approximate ordering).
    delay_queue_max_size : int
        Maximum number of events that can be held in the delay queue at once.
        When the queue is full, new candidates are published immediately.
    """

    def __init__(
        self,
        fault_rate_duplicate: float = 0.02,
        fault_rate_malformed: float = 0.01,
        fault_rate_delayed: float = 0.05,
        fault_rate_out_of_order: float = 0.03,
        fault_rate_schema_drift: float = 0.005,
        fault_rate_stale: float = 0.01,
        fault_rate_partial_curve: float = 0.02,
        delay_min_seconds: float = 2.0,
        delay_max_seconds: float = 30.0,
        out_of_order_max_hold_seconds: float = 5.0,
        delay_queue_max_size: int = 500,
    ) -> None:
        self._rates = {
            FaultType.DUPLICATE: fault_rate_duplicate,
            FaultType.MALFORMED: fault_rate_malformed,
            FaultType.DELAYED: fault_rate_delayed,
            FaultType.OUT_OF_ORDER: fault_rate_out_of_order,
            FaultType.SCHEMA_DRIFT: fault_rate_schema_drift,
            FaultType.STALE: fault_rate_stale,
            FaultType.PARTIAL_CURVE: fault_rate_partial_curve,
        }
        self._delay_min = delay_min_seconds
        self._delay_max = delay_max_seconds
        self._ooo_max_hold = out_of_order_max_hold_seconds
        self._queue_max = delay_queue_max_size

        # Hold queues: list of _HeldEvent
        self._delay_queue: list[_HeldEvent] = []
        self._ooo_queue: list[_HeldEvent] = []

        logger.info(
            "fault_injector_initialised",
            rates={k.value: v for k, v in self._rates.items()},
        )

    # ------------------------------------------------------------------ #
    # Public interface
    # ------------------------------------------------------------------ #

    def inject(self, events: list[RawMarketEvent]) -> list[RawMarketEvent]:
        """
        Apply fault injection to a batch of clean events.

        The returned list may be:
        - Shorter than the input (partial curve drop, events moved to hold queue)
        - Longer than the input (duplicates)
        - The same length (no structural changes, but individual events mutated)

        Events routed to the delay or out-of-order queue are NOT in the returned
        list; they will appear in a later call to `drain_ready()`.
        """
        if not events:
            return []

        # PARTIAL_CURVE is applied at the batch level: if the batch represents
        # a single instrument's forward curve, randomly drop some tenors.
        if self._roll(FaultType.PARTIAL_CURVE) and len(events) > 2:
            events = self._apply_partial_curve(events)

        output: list[RawMarketEvent] = []

        for event in events:
            processed = self._apply_event_faults(event)
            output.extend(processed)

        return output

    def drain_ready(self) -> list[RawMarketEvent]:
        """
        Return all held events whose release time has passed.

        Call this on every publish cycle to flush the delay/OOO queues.
        """
        now = time.monotonic()
        ready: list[RawMarketEvent] = []

        still_held: list[_HeldEvent] = []
        for held in self._delay_queue:
            if held.release_at <= now:
                ready.append(held.event)
            else:
                still_held.append(held)
        self._delay_queue = still_held

        # Out-of-order: release in shuffled order to simulate re-ordering
        ooo_ready: list[RawMarketEvent] = []
        still_held_ooo: list[_HeldEvent] = []
        for held in self._ooo_queue:
            if held.release_at <= now:
                ooo_ready.append(held.event)
            else:
                still_held_ooo.append(held)
        self._ooo_queue = still_held_ooo

        if len(ooo_ready) > 1:
            random.shuffle(ooo_ready)
        ready.extend(ooo_ready)

        if ready:
            logger.debug(
                "held_events_released",
                count=len(ready),
                delay_queue_remaining=len(self._delay_queue),
                ooo_queue_remaining=len(self._ooo_queue),
            )
        return ready

    @property
    def delay_queue_depth(self) -> int:
        return len(self._delay_queue)

    @property
    def ooo_queue_depth(self) -> int:
        return len(self._ooo_queue)

    # ------------------------------------------------------------------ #
    # Per-event fault application
    # ------------------------------------------------------------------ #

    def _apply_event_faults(self, event: RawMarketEvent) -> list[RawMarketEvent]:
        """
        Apply all relevant per-event faults.  Returns 0 events (held in queue),
        1 event (normal or mutated), or 2 events (duplicate).
        """
        # Work on a deep copy so we don't mutate the caller's object.
        event = _deep_copy_event(event)

        # ---- STALE ----
        if self._roll(FaultType.STALE):
            event = self._apply_stale(event)

        # ---- MALFORMED ----
        if self._roll(FaultType.MALFORMED):
            event = self._apply_malformed(event)

        # ---- SCHEMA_DRIFT ----
        if self._roll(FaultType.SCHEMA_DRIFT):
            event = self._apply_schema_drift(event)

        # ---- DELAYED ----
        # Delayed events are moved to the hold queue and not returned now.
        if self._roll(FaultType.DELAYED) and len(self._delay_queue) < self._queue_max:
            event.injected_faults.append(FaultType.DELAYED)
            FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.DELAYED.value).inc()
            hold_secs = random.uniform(self._delay_min, self._delay_max)
            self._delay_queue.append(
                _HeldEvent(event=event, release_at=time.monotonic() + hold_secs)
            )
            logger.debug(
                "event_delayed",
                event_id=event.event_id,
                hold_seconds=round(hold_secs, 2),
            )
            # Return empty — this event will surface via drain_ready()
            return []

        # ---- OUT_OF_ORDER ----
        if (
            self._roll(FaultType.OUT_OF_ORDER)
            and len(self._ooo_queue) < self._queue_max
        ):
            event.injected_faults.append(FaultType.OUT_OF_ORDER)
            FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.OUT_OF_ORDER.value).inc()
            hold_secs = random.uniform(0.5, self._ooo_max_hold)
            self._ooo_queue.append(
                _HeldEvent(event=event, release_at=time.monotonic() + hold_secs)
            )
            logger.debug(
                "event_held_for_out_of_order",
                event_id=event.event_id,
                hold_seconds=round(hold_secs, 2),
            )
            return []

        # ---- DUPLICATE ----
        if self._roll(FaultType.DUPLICATE):
            return self._apply_duplicate(event)

        return [event]

    # ------------------------------------------------------------------ #
    # Individual fault implementations
    # ------------------------------------------------------------------ #

    def _apply_duplicate(self, event: RawMarketEvent) -> list[RawMarketEvent]:
        """Return the original event plus a copy with the same event_id."""
        dup = _deep_copy_event(event)
        # The duplicate shares the same event_id — that is the whole point.
        # The validation service must detect and deduplicate it.
        event.injected_faults.append(FaultType.DUPLICATE)
        dup.injected_faults.append(FaultType.DUPLICATE)
        FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.DUPLICATE.value).inc()
        logger.debug("fault_duplicate_injected", event_id=event.event_id)
        return [event, dup]

    def _apply_malformed(self, event: RawMarketEvent) -> RawMarketEvent:
        """
        Randomly corrupt the event payload.

        Strategies (chosen at random):
        1. Null out a required field
        2. Replace a numeric field with a string that cannot be parsed
        3. Replace the entire payload with an empty dict
        4. Set price to a negative value
        """
        payload = dict(event.payload)
        strategy = random.choice(["null_field", "wrong_type", "empty_payload", "negative_price"])

        if strategy == "null_field" and _REQUIRED_FIELDS:
            target = random.choice(_REQUIRED_FIELDS)
            payload[target] = None

        elif strategy == "wrong_type":
            target = random.choice(_REQUIRED_FIELDS)
            payload[target] = "~~CORRUPTED~~"

        elif strategy == "empty_payload":
            payload = {}

        elif strategy == "negative_price":
            payload["price"] = -abs(payload.get("price", 1.0))  # type: ignore[arg-type]

        event = event.model_copy(
            update={"payload": payload, "injected_faults": list(event.injected_faults)}
        )
        event.injected_faults.append(FaultType.MALFORMED)
        FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.MALFORMED.value).inc()
        logger.debug(
            "fault_malformed_injected",
            event_id=event.event_id,
            strategy=strategy,
        )
        return event

    def _apply_schema_drift(self, event: RawMarketEvent) -> RawMarketEvent:
        """
        Rename one or more payload fields to their drifted alternatives.

        Picks a random subset of the drift map that actually overlaps with the
        event's payload keys.
        """
        payload = dict(event.payload)
        driftable = [k for k in _SCHEMA_DRIFT_MAP if k in payload]
        if not driftable:
            return event

        # Drift 1-3 fields
        count = random.randint(1, min(3, len(driftable)))
        to_drift = random.sample(driftable, count)

        for original_key in to_drift:
            new_key = _SCHEMA_DRIFT_MAP[original_key]
            payload[new_key] = payload.pop(original_key)

        event = event.model_copy(
            update={"payload": payload, "injected_faults": list(event.injected_faults)}
        )
        event.injected_faults.append(FaultType.SCHEMA_DRIFT)
        FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.SCHEMA_DRIFT.value).inc()
        logger.debug(
            "fault_schema_drift_injected",
            event_id=event.event_id,
            drifted_fields=to_drift,
        )
        return event

    def _apply_stale(self, event: RawMarketEvent) -> RawMarketEvent:
        """Backdate the event_timestamp by 2-24 hours."""
        stale_hours = random.uniform(2.0, 24.0)
        stale_seconds = stale_hours * 3600
        new_ts = datetime.fromtimestamp(
            event.event_timestamp.timestamp() - stale_seconds,
            tz=UTC,
        )
        event = event.model_copy(
            update={
                "event_timestamp": new_ts,
                "injected_faults": list(event.injected_faults),
            }
        )
        event.injected_faults.append(FaultType.STALE)
        FAULTS_INJECTED_TOTAL.labels(fault_type=FaultType.STALE.value).inc()
        logger.debug(
            "fault_stale_injected",
            event_id=event.event_id,
            stale_hours=round(stale_hours, 2),
        )
        return event

    def _apply_partial_curve(self, events: list[RawMarketEvent]) -> list[RawMarketEvent]:
        """
        Drop a random fraction (20-50%) of the events in the batch to simulate
        a partial curve delivery.

        The dropped events are discarded (not held in a queue) — they represent
        missing tenors, not delayed ones.
        """
        drop_fraction = random.uniform(0.20, 0.50)
        drop_count = max(1, int(len(events) * drop_fraction))
        indices_to_drop = set(random.sample(range(len(events)), drop_count))

        kept: list[RawMarketEvent] = []
        for i, event in enumerate(events):
            if i in indices_to_drop:
                FAULTS_INJECTED_TOTAL.labels(
                    fault_type=FaultType.PARTIAL_CURVE.value
                ).inc()
                logger.debug(
                    "fault_partial_curve_tenor_dropped",
                    event_id=event.event_id,
                    instrument=event.instrument,
                )
            else:
                # Mark the kept events so downstream knows the curve was partial
                mutated = event.model_copy(
                    update={"injected_faults": list(event.injected_faults)}
                )
                mutated.injected_faults.append(FaultType.PARTIAL_CURVE)
                kept.append(mutated)

        logger.debug(
            "fault_partial_curve_applied",
            original_count=len(events),
            kept_count=len(kept),
            dropped_count=drop_count,
        )
        return kept

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _roll(self, fault_type: FaultType) -> bool:
        """Return True with probability equal to the configured rate."""
        return random.random() < self._rates[fault_type]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _deep_copy_event(event: RawMarketEvent) -> RawMarketEvent:
    """
    Return a deep copy of a RawMarketEvent.

    We use model_copy(deep=True) from Pydantic v2 rather than copy.deepcopy
    to stay within the model's validation boundary.
    """
    return event.model_copy(deep=True)
