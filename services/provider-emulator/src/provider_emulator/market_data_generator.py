"""
Synthetic market data generator.

Produces realistic forward curve snapshots for EEX/CME-style energy instruments.
Each instrument maintains its own price state and evolves via a geometric random
walk, keeping prices anchored near market-realistic levels.

One call to MarketDataGenerator.generate_curve_batch() returns a list of
RawMarketEvents — one per (instrument, tenor) combination — representing a single
snapshot of the full forward curve for that instrument.

Thread-safety: the generator is NOT thread-safe. Use it from a single thread or
protect it externally.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Final

from mdrp_common.logging import get_logger
from mdrp_common.models import RawMarketEvent

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Instrument definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TenorSpec:
    """A single tenor within a forward curve."""

    label: str  # e.g. "M+1", "Q+2", "Dec-25"
    offset_months: int  # approximate months to delivery start


@dataclass(frozen=True)
class InstrumentSpec:
    """
    All static metadata for one simulated instrument.

    mid_price     – starting mid-market price (realistic value)
    vol           – annualised daily vol fraction used for the random walk
    currency      – ISO 4217 code
    unit          – price unit string
    provider      – exchange / venue name
    curve_name    – canonical curve name used in CurveEvent
    tenors        – ordered list of tenors for this curve
    """

    instrument: str
    mid_price: float
    vol: float
    currency: str
    unit: str
    provider: str
    curve_name: str
    tenors: list[TenorSpec]


def _monthly_tenors(count: int) -> list[TenorSpec]:
    return [TenorSpec(label=f"M+{i}", offset_months=i) for i in range(1, count + 1)]


def _quarterly_tenors(count: int) -> list[TenorSpec]:
    return [TenorSpec(label=f"Q+{i}", offset_months=i * 3) for i in range(1, count + 1)]


def _eua_dec_tenors() -> list[TenorSpec]:
    """December EUA contracts for the next 5 years."""
    current_year = datetime.now(timezone.utc).year
    return [
        TenorSpec(label=f"Dec-{current_year + i}", offset_months=i * 12)
        for i in range(1, 6)
    ]


# Monthly tenors M+1 … M+24 plus quarterly Q+1 … Q+8
_TTF_POWER_TENORS: list[TenorSpec] = _monthly_tenors(24) + _quarterly_tenors(8)

INSTRUMENT_SPECS: Final[dict[str, InstrumentSpec]] = {
    "TTF": InstrumentSpec(
        instrument="TTF",
        mid_price=30.0,   # EUR/MWh — typical 2024-range midpoint
        vol=0.35,
        currency="EUR",
        unit="MWh",
        provider="EEX",
        curve_name="TTF_GAS_FORWARD",
        tenors=_monthly_tenors(24),
    ),
    "NBP": InstrumentSpec(
        instrument="NBP",
        mid_price=28.5,   # GBP/MWh — slightly below TTF historically
        vol=0.38,
        currency="GBP",
        unit="MWh",
        provider="ICE",
        curve_name="NBP_GAS_FORWARD",
        tenors=_monthly_tenors(24),
    ),
    "TTF_POWER": InstrumentSpec(
        instrument="TTF_POWER",
        mid_price=95.0,   # EUR/MWh — German baseload power
        vol=0.30,
        currency="EUR",
        unit="MWh",
        provider="EEX",
        curve_name="DE_POWER_BASE_FORWARD",
        tenors=_TTF_POWER_TENORS,
    ),
    "BRENT": InstrumentSpec(
        instrument="BRENT",
        mid_price=80.0,   # USD/bbl
        vol=0.28,
        currency="USD",
        unit="bbl",
        provider="ICE",
        curve_name="BRENT_CRUDE_FORWARD",
        tenors=_monthly_tenors(24),
    ),
    "WTI": InstrumentSpec(
        instrument="WTI",
        mid_price=76.0,   # USD/bbl — WTI trades ~$3-4 below Brent
        vol=0.28,
        currency="USD",
        unit="bbl",
        provider="CME",
        curve_name="WTI_CRUDE_FORWARD",
        tenors=_monthly_tenors(24),
    ),
    "EU_ETS": InstrumentSpec(
        instrument="EU_ETS",
        mid_price=65.0,   # EUR/tCO2 — EUA price typical 2024 range
        vol=0.22,
        currency="EUR",
        unit="tCO2",
        provider="EEX",
        curve_name="EU_ETS_EUA_FORWARD",
        tenors=_eua_dec_tenors(),
    ),
}


# ---------------------------------------------------------------------------
# Price state — one per instrument, maintained across calls
# ---------------------------------------------------------------------------


@dataclass
class _PriceState:
    """
    Stores the current mid-price for every tenor of a single instrument.
    Prices evolve independently via geometric Brownian motion.
    """

    spec: InstrumentSpec
    # tenor label -> current price
    prices: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Initialise with the spec mid-price, adding a small term-structure
        # adjustment: later tenors carry a slight contango.
        for tenor in self.spec.tenors:
            contango_factor = 1.0 + (tenor.offset_months / 1200.0)  # ~0.1% per month
            self.prices[tenor.label] = self.spec.mid_price * contango_factor

    def step(self) -> None:
        """
        Advance prices by one time step using a geometric random walk.

        Daily vol is annualised_vol / sqrt(252).  We use that as the per-step
        sigma regardless of the actual wall-clock interval — the generator is
        called on a configurable interval, but the vol is calibrated to produce
        plausible intra-day moves for a ~5-second publish cycle.

        To simulate ~5-second moves we scale vol down further:
          sigma_step = daily_vol * sqrt(5 / 86400)
        """
        daily_vol = self.spec.vol / math.sqrt(252)
        # 5-second fraction of a trading day (assuming 8h = 28800s trading day)
        step_fraction = math.sqrt(5.0 / 28800.0)
        sigma = daily_vol * step_fraction

        for tenor_label in self.prices:
            # Mean-reversion pull towards the spec mid — prevents runaway drift.
            # Ornstein-Uhlenbeck flavour: pull = kappa * (mu - S)
            kappa = 0.001
            mu = self.spec.mid_price
            current = self.prices[tenor_label]
            drift = kappa * (mu - current)
            shock = random.gauss(0.0, sigma)
            # Geometric: S_{t+1} = S_t * exp((drift/S_t) + shock)
            log_return = (drift / max(current, 0.01)) + shock
            self.prices[tenor_label] = current * math.exp(log_return)
            # Hard floor: price cannot go below 0.5% of the initial mid
            self.prices[tenor_label] = max(
                self.prices[tenor_label], self.spec.mid_price * 0.005
            )


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class MarketDataGenerator:
    """
    Generates synthetic forward curve data for one or more instruments.

    Usage::

        gen = MarketDataGenerator(instruments=["TTF", "BRENT"])
        events = gen.generate_curve_batch("TTF")   # list[RawMarketEvent]
    """

    def __init__(
        self,
        instruments: list[str],
        provider_name: str = "provider-emulator",
    ) -> None:
        unknown = set(instruments) - set(INSTRUMENT_SPECS)
        if unknown:
            raise ValueError(f"Unknown instruments: {unknown!r}")

        self._provider_name = provider_name
        self._states: dict[str, _PriceState] = {
            inst: _PriceState(spec=INSTRUMENT_SPECS[inst]) for inst in instruments
        }
        logger.info(
            "market_data_generator_initialised",
            instruments=instruments,
            provider=provider_name,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    @property
    def instruments(self) -> list[str]:
        return list(self._states.keys())

    def generate_curve_batch(self, instrument: str) -> list[RawMarketEvent]:
        """
        Step prices and return one RawMarketEvent per tenor.

        All events in the batch share the same event_timestamp so that the
        downstream validation service can detect a coherent snapshot.
        """
        if instrument not in self._states:
            raise ValueError(f"Instrument {instrument!r} not configured")

        state = self._states[instrument]
        state.step()

        now = datetime.now(timezone.utc)
        spec = state.spec
        events: list[RawMarketEvent] = []

        for tenor in spec.tenors:
            price = state.prices[tenor.label]
            bid = price * (1.0 - random.uniform(0.0001, 0.0005))
            ask = price * (1.0 + random.uniform(0.0001, 0.0005))
            volume = random.randint(10, 5000)

            payload: dict[str, object] = {
                "price": round(price, 4),
                "bid": round(bid, 4),
                "ask": round(ask, 4),
                "volume": volume,
                "tenor": tenor.label,
                "offset_months": tenor.offset_months,
                "curve_name": spec.curve_name,
                "currency": spec.currency,
                "unit": spec.unit,
                "open": round(price * random.uniform(0.995, 1.005), 4),
                "high": round(price * random.uniform(1.001, 1.010), 4),
                "low": round(price * random.uniform(0.990, 0.999), 4),
                "close": round(price, 4),
                "vwap": round(price * random.uniform(0.9995, 1.0005), 4),
                "open_interest": random.randint(100, 50000),
                "source": "synthetic",
                "schema_version": "1.0",
            }

            event = RawMarketEvent(
                provider=self._provider_name,
                instrument=instrument,
                event_timestamp=now,
                payload=payload,
            )
            events.append(event)

        logger.debug(
            "curve_batch_generated",
            instrument=instrument,
            tenor_count=len(events),
            provider=self._provider_name,
        )
        return events

    def generate_all_batches(self) -> list[RawMarketEvent]:
        """
        Generate one full forward curve batch for every configured instrument.
        Returns the concatenated list of all events.
        """
        all_events: list[RawMarketEvent] = []
        for instrument in self._states:
            all_events.extend(self.generate_curve_batch(instrument))
        logger.info(
            "all_batches_generated",
            instrument_count=len(self._states),
            total_events=len(all_events),
        )
        return all_events
