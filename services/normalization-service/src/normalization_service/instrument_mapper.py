"""
Instrument normalisation for the Market Data Reliability Platform.

Maps provider-specific instrument symbols to canonical instrument names,
and assigns default currency and unit for each canonical instrument.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Canonical instrument → (currency, unit)
# ---------------------------------------------------------------------------

_INSTRUMENT_DEFAULTS: dict[str, tuple[str, str]] = {
    "TTF": ("EUR", "MWh"),
    "NBP": ("EUR", "MWh"),
    "TTF_POWER": ("EUR", "MWh"),
    "BRENT": ("USD", "bbl"),
    "WTI": ("USD", "bbl"),
    "EU_ETS": ("EUR", "tonne"),
}

# ---------------------------------------------------------------------------
# Raw symbol → canonical instrument
# ---------------------------------------------------------------------------

_SYMBOL_MAP: dict[str, str] = {
    # Natural gas – TTF
    "TTF": "TTF",
    "TTF GAS": "TTF",
    "TTF_GAS": "TTF",
    # Natural gas – NBP
    "NBP": "NBP",
    "NBP GAS": "NBP",
    # Power – TTF Power (European electricity forward curve)
    "TTF_POWER": "TTF_POWER",
    "TTFPOWER": "TTF_POWER",
    "EEX_POWER": "TTF_POWER",
    # Oil – Brent
    "BRENT": "BRENT",
    "BRN": "BRENT",
    "CO": "BRENT",
    # Oil – WTI
    "WTI": "WTI",
    "CL": "WTI",
    # Carbon – EU ETS
    "EUA": "EU_ETS",
    "EU_ETS": "EU_ETS",
    "CO2": "EU_ETS",
}


# ---------------------------------------------------------------------------
# InstrumentMapper
# ---------------------------------------------------------------------------


class InstrumentMapper:
    """
    Stateless instrument normaliser.

    ``normalise`` maps a raw provider symbol to:
      - the canonical instrument name
      - the default currency for that instrument
      - the default unit for that instrument

    Raises ``ValueError`` for unknown symbols.
    """

    def normalise(self, raw_symbol: str) -> tuple[str, str, str]:
        """
        Return ``(canonical_instrument, currency, unit)`` for *raw_symbol*.

        Matching is case-insensitive and leading/trailing whitespace is stripped.

        Parameters
        ----------
        raw_symbol:
            Provider-supplied instrument symbol or name.

        Returns
        -------
        tuple[str, str, str]
            ``(canonical_instrument, currency, unit)``

        Raises
        ------
        ValueError
            When *raw_symbol* cannot be mapped to a known instrument.
        """
        normalised_key = raw_symbol.strip().upper()
        canonical = _SYMBOL_MAP.get(normalised_key)
        if canonical is None:
            raise ValueError(
                f"Unknown instrument symbol: {raw_symbol!r}. "
                "Cannot map to a canonical instrument."
            )
        currency, unit = _INSTRUMENT_DEFAULTS[canonical]
        return canonical, currency, unit

    def is_known(self, raw_symbol: str) -> bool:
        """Return True if *raw_symbol* maps to a known instrument."""
        return raw_symbol.strip().upper() in _SYMBOL_MAP
