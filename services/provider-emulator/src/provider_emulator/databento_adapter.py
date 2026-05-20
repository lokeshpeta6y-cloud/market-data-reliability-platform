"""
Databento historical data adapter.

Wraps the ``databento`` Python SDK to pull OHLCV-1d records from the
GLBX.MDP3 dataset (CME Globex) or DBEQ.BASIC (equities) and converts them
into RawMarketEvent objects compatible with the rest of the platform.

Activation
----------
This adapter is only instantiated when the ``DATABENTO_API_KEY`` environment
variable is set.  If the ``databento`` package is not installed, the adapter
raises ``DatabentoAdapterError`` at construction time, allowing the caller to
fall back to the synthetic generator gracefully.

Instrument mapping
------------------
Databento uses its own symbology.  The mapping from our instrument codes to
Databento symbols is defined in ``_INSTRUMENT_TO_DATABENTO``.  Each entry maps
to a (dataset, symbol_root) pair.  Front-month roll is handled by fetching all
active contracts and selecting the N nearest expiries.

Threading
---------
The adapter is NOT thread-safe.  Wrap in a lock if calling from multiple threads.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from mdrp_common.logging import get_logger
from mdrp_common.models import RawMarketEvent

if TYPE_CHECKING:
    # Import only for type checking to avoid hard dependency
    import databento as db

logger = get_logger(__name__)


class DatabentoAdapterError(Exception):
    """Raised when the adapter cannot be initialised or a fetch fails."""


# ---------------------------------------------------------------------------
# Instrument → Databento symbology map
# ---------------------------------------------------------------------------

# Maps our internal instrument codes to (dataset, continuous_symbol) pairs.
# We use continuous front-month symbols where available for simplicity.
_INSTRUMENT_TO_DATABENTO: dict[str, dict[str, str]] = {
    "BRENT": {
        "dataset": "GLBX.MDP3",
        "symbol": "BRN",          # ICE Brent on CME Globex (alias)
        "currency": "USD",
        "unit": "bbl",
        "curve_name": "BRENT_CRUDE_FORWARD",
    },
    "WTI": {
        "dataset": "GLBX.MDP3",
        "symbol": "CL",           # NYMEX WTI Crude
        "currency": "USD",
        "unit": "bbl",
        "curve_name": "WTI_CRUDE_FORWARD",
    },
    # Gas / power markets are not available on GLBX.MDP3; we leave them as
    # synthetic only.  Adding them here is a placeholder for when an EEX feed
    # becomes available via Databento.
}


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class DatabentoAdapter:
    """
    Pulls OHLCV historical data from Databento and converts to RawMarketEvents.

    Parameters
    ----------
    api_key : str
        Databento API key.  Passed directly to ``databento.Historical``.
    dataset : str
        Default Databento dataset.  Can be overridden per instrument.
    lookback_days : int
        How many calendar days of history to pull on each refresh.
    provider_name : str
        Value stamped in the ``provider`` field of every RawMarketEvent.
    instruments : list[str]
        Which instruments to pull from Databento.  Instruments not in
        ``_INSTRUMENT_TO_DATABENTO`` are silently skipped.
    """

    def __init__(
        self,
        api_key: str,
        dataset: str = "GLBX.MDP3",
        lookback_days: int = 5,
        provider_name: str = "databento",
        instruments: list[str] | None = None,
    ) -> None:
        self._api_key = api_key
        self._default_dataset = dataset
        self._lookback_days = lookback_days
        self._provider_name = provider_name
        self._instruments = instruments or list(_INSTRUMENT_TO_DATABENTO.keys())

        # Attempt to import the SDK — fail loudly if not installed.
        try:
            import databento  # noqa: F401 — side-effect import to validate presence
            self._db_module = databento
        except ImportError as exc:
            raise DatabentoAdapterError(
                "The 'databento' package is not installed. "
                "Install it with: pip install databento"
            ) from exc

        # Instantiate the historical client (validates the API key format).
        try:
            self._client: Any = self._db_module.Historical(api_key=api_key)
        except Exception as exc:
            raise DatabentoAdapterError(
                f"Failed to initialise Databento historical client: {exc}"
            ) from exc

        logger.info(
            "databento_adapter_initialised",
            dataset=dataset,
            lookback_days=lookback_days,
            instruments=self._instruments,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def fetch_events(self) -> list[RawMarketEvent]:
        """
        Pull recent OHLCV data for all configured instruments and return
        them as RawMarketEvents.

        Returns an empty list if no supported instruments are configured or
        if all fetches fail (errors are logged but not re-raised so the
        caller can fall back to synthetic data).
        """
        events: list[RawMarketEvent] = []
        end_date = date.today()
        start_date = end_date - timedelta(days=self._lookback_days)

        for instrument in self._instruments:
            spec = _INSTRUMENT_TO_DATABENTO.get(instrument)
            if spec is None:
                logger.debug(
                    "databento_instrument_not_mapped",
                    instrument=instrument,
                )
                continue

            try:
                batch = self._fetch_instrument(
                    instrument=instrument,
                    spec=spec,
                    start_date=start_date,
                    end_date=end_date,
                )
                events.extend(batch)
                logger.info(
                    "databento_instrument_fetched",
                    instrument=instrument,
                    event_count=len(batch),
                )
            except DatabentoAdapterError as exc:
                logger.warning(
                    "databento_instrument_fetch_failed",
                    instrument=instrument,
                    error=str(exc),
                )
                # Continue with remaining instruments

        logger.info(
            "databento_fetch_complete",
            total_events=len(events),
        )
        return events

    # ------------------------------------------------------------------ #
    # Internal fetch helpers
    # ------------------------------------------------------------------ #

    def _fetch_instrument(
        self,
        instrument: str,
        spec: dict[str, str],
        start_date: date,
        end_date: date,
    ) -> list[RawMarketEvent]:
        """
        Fetch OHLCV-1d records for a single instrument from Databento.

        Returns a list of RawMarketEvents — one per bar (trading day × contract).
        """
        dataset: str = spec.get("dataset", self._default_dataset)
        symbol: str = spec["symbol"]

        try:
            # timeseries.get_range returns a DBNStore; iterate to get records.
            store = self._client.timeseries.get_range(
                dataset=dataset,
                symbols=[symbol],
                schema="ohlcv-1d",
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                stype_in="parent",  # roll-adjusted continuous contract
            )
        except Exception as exc:
            raise DatabentoAdapterError(
                f"Databento timeseries.get_range failed for {instrument}: {exc}"
            ) from exc

        events: list[RawMarketEvent] = []
        try:
            for record in store:
                event = self._record_to_event(record, instrument, spec)
                if event is not None:
                    events.append(event)
        except Exception as exc:
            raise DatabentoAdapterError(
                f"Failed to iterate Databento records for {instrument}: {exc}"
            ) from exc

        return events

    def _record_to_event(
        self,
        record: Any,
        instrument: str,
        spec: dict[str, str],
    ) -> RawMarketEvent | None:
        """
        Convert a single Databento OHLCV record to a RawMarketEvent.

        Returns None if the record does not contain sufficient price data.
        """
        try:
            # Databento OHLCV records use integer fixed-point prices (1e-9 scale).
            price_scale = 1e-9

            open_px = getattr(record, "open", None)
            high_px = getattr(record, "high", None)
            low_px = getattr(record, "low", None)
            close_px = getattr(record, "close", None)
            volume = getattr(record, "volume", None)

            if close_px is None or close_px == 0:
                return None

            open_f = float(open_px) * price_scale if open_px is not None else None
            high_f = float(high_px) * price_scale if high_px is not None else None
            low_f = float(low_px) * price_scale if low_px is not None else None
            close_f = float(close_px) * price_scale
            volume_i = int(volume) if volume is not None else None

            # ts_event is nanoseconds since epoch in Databento records
            ts_event_ns = getattr(record, "ts_event", None)
            if ts_event_ns is not None:
                event_ts = datetime.fromtimestamp(
                    float(ts_event_ns) / 1e9, tz=timezone.utc
                )
            else:
                event_ts = datetime.now(timezone.utc)

            # Symbol from the record (may be the specific contract code)
            raw_symbol = getattr(record, "symbol", spec["symbol"])
            # Use symbol as the tenor label for front-month data
            tenor_label = str(raw_symbol) if raw_symbol else "M+1"

            payload: dict[str, Any] = {
                "price": round(close_f, 4),
                "open": round(open_f, 4) if open_f is not None else None,
                "high": round(high_f, 4) if high_f is not None else None,
                "low": round(low_f, 4) if low_f is not None else None,
                "close": round(close_f, 4),
                "volume": volume_i,
                "tenor": tenor_label,
                "curve_name": spec.get("curve_name", instrument),
                "currency": spec.get("currency", "USD"),
                "unit": spec.get("unit", ""),
                "dataset": spec.get("dataset", self._default_dataset),
                "raw_symbol": str(raw_symbol),
                "source": "databento",
                "schema_version": "1.0",
            }

            return RawMarketEvent(
                provider=self._provider_name,
                instrument=instrument,
                event_timestamp=event_ts,
                payload=payload,
            )

        except (AttributeError, TypeError, ValueError) as exc:
            logger.warning(
                "databento_record_conversion_failed",
                instrument=instrument,
                error=str(exc),
            )
            return None
