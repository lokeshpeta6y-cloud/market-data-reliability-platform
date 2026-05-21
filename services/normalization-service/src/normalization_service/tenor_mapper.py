"""
Tenor string normalisation for the Market Data Reliability Platform.

Converts provider-specific tenor representations into the canonical format
used throughout the platform, and infers the corresponding DeliveryPeriod.

Canonical formats
-----------------
Monthly   : YYYY-MM          e.g. "2024-03"
Quarterly : YYYY-Qn          e.g. "2024-Q1"
Seasonal  : YYYY-SUM / YYYY-WIN   e.g. "2024-SUM"
Annual    : YYYY-CAL         e.g. "2024-CAL"
Spot      : "spot"
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from mdrp_common.models import DeliveryPeriod


# ---------------------------------------------------------------------------
# Month name / abbreviation look-up tables
# ---------------------------------------------------------------------------

_MONTH_ABBR_TO_NUM: dict[str, str] = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}

_FULL_MONTH_TO_NUM: dict[str, str] = {
    "january": "01",
    "february": "02",
    "march": "03",
    "april": "04",
    "may": "05",
    "june": "06",
    "july": "07",
    "august": "08",
    "september": "09",
    "october": "10",
    "november": "11",
    "december": "12",
}

# Two-digit year suffix → four-digit year (20xx assumption)
def _expand_2y(yy: str) -> str:
    """Convert a two-digit year string to four digits (20xx)."""
    return f"20{yy}"


# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

# ISO monthly: "2024-03" or "2024-3"
_RE_ISO_MONTHLY = re.compile(r"^(\d{4})-(\d{1,2})$")

# Three-letter month + 2-digit year: "MAR24", "mar24"
_RE_ABBR_MONTHLY = re.compile(r"^([A-Za-z]{3})(\d{2})$")

# "Mar-24", "Mar-2024"
_RE_ABBR_DASH_YEAR = re.compile(r"^([A-Za-z]{3})-(\d{2}|\d{4})$")

# "March 2024" / "MARCH 2024"
_RE_FULL_MONTH_YEAR = re.compile(r"^([A-Za-z]+)\s+(\d{4})$")

# Quarterly: "Q1-2024", "Q1 2024", "2024Q1", "2024-Q1"
_RE_QUARTERLY_Q_FIRST = re.compile(r"^Q([1-4])[\s\-](\d{4})$", re.IGNORECASE)
_RE_QUARTERLY_Y_FIRST = re.compile(r"^(\d{4})[\-]?Q([1-4])$", re.IGNORECASE)

# Seasonal summer
_RE_SEASONAL_SUMMER = re.compile(
    r"^(?:Summer\s+(\d{4})|SUM(\d{2}))$", re.IGNORECASE
)

# Seasonal winter
_RE_SEASONAL_WINTER = re.compile(
    r"^(?:Winter\s+(\d{4})|WIN(\d{2}))$", re.IGNORECASE
)

# Annual: "Cal-2024", "CAL24", bare "2024"
_RE_ANNUAL_CAL_DASH = re.compile(r"^Cal-(\d{4})$", re.IGNORECASE)
_RE_ANNUAL_CAL_ABBR = re.compile(r"^CAL(\d{2})$", re.IGNORECASE)
_RE_ANNUAL_BARE = re.compile(r"^(\d{4})$")

# Spot: "spot", "d+1", "D+1"
_RE_SPOT = re.compile(r"^(spot|d\+1)$", re.IGNORECASE)

# Relative monthly: "M+1", "M+24" — months forward from today
_RE_RELATIVE_MONTHLY = re.compile(r"^M\+(\d+)$", re.IGNORECASE)

# Relative quarterly: "Q+1", "Q+8" — quarters forward from today
_RE_RELATIVE_QUARTERLY = re.compile(r"^Q\+(\d+)$", re.IGNORECASE)


def _months_forward(n: int) -> str:
    """Return YYYY-MM for the date exactly *n* months from today."""
    now = datetime.now(UTC)
    total_months = now.month - 1 + n
    year = now.year + total_months // 12
    month = total_months % 12 + 1
    return f"{year}-{month:02d}"


def _quarters_forward(n: int) -> str:
    """Return YYYY-QQ for the calendar quarter *n* quarters from today."""
    now = datetime.now(UTC)
    total_quarters = (now.month - 1) // 3 + n
    year = now.year + total_quarters // 4
    quarter = total_quarters % 4 + 1
    return f"{year}-Q{quarter}"


# ---------------------------------------------------------------------------
# TenorMapper
# ---------------------------------------------------------------------------


class TenorMapper:
    """
    Stateless tenor normaliser.

    ``normalise`` parses a raw tenor string and returns the canonical tenor
    string together with the corresponding ``DeliveryPeriod``.

    Raises ``ValueError`` for inputs that do not match any known pattern.
    """

    def normalise(self, raw_tenor: str) -> tuple[str, DeliveryPeriod]:
        """
        Parse *raw_tenor* into ``(canonical_tenor, DeliveryPeriod)``.

        Parameters
        ----------
        raw_tenor:
            Provider-supplied tenor string. Leading/trailing whitespace is
            stripped; matching is case-insensitive where appropriate.

        Returns
        -------
        tuple[str, DeliveryPeriod]
            ``(canonical_tenor_string, delivery_period)``

        Raises
        ------
        ValueError
            When *raw_tenor* does not match any recognised pattern.
        """
        tenor = raw_tenor.strip()

        # --- Relative monthly: "M+N" → absolute YYYY-MM ---
        m = _RE_RELATIVE_MONTHLY.match(tenor)
        if m:
            return _months_forward(int(m.group(1))), DeliveryPeriod.MONTHLY

        # --- Relative quarterly: "Q+N" → absolute YYYY-QQ ---
        m = _RE_RELATIVE_QUARTERLY.match(tenor)
        if m:
            return _quarters_forward(int(m.group(1))), DeliveryPeriod.QUARTERLY

        # --- Spot ---
        if _RE_SPOT.match(tenor):
            return "spot", DeliveryPeriod.SPOT

        # --- ISO monthly: "2024-03" ---
        m = _RE_ISO_MONTHLY.match(tenor)
        if m:
            year, month = m.group(1), m.group(2).zfill(2)
            _validate_month(month)
            return f"{year}-{month}", DeliveryPeriod.MONTHLY

        # --- "MAR24" → "2024-03" ---
        m = _RE_ABBR_MONTHLY.match(tenor)
        if m:
            abbr, yy = m.group(1).lower(), m.group(2)
            if abbr in _MONTH_ABBR_TO_NUM:
                return (
                    f"{_expand_2y(yy)}-{_MONTH_ABBR_TO_NUM[abbr]}",
                    DeliveryPeriod.MONTHLY,
                )

        # --- "Mar-24" / "Mar-2024" ---
        m = _RE_ABBR_DASH_YEAR.match(tenor)
        if m:
            abbr = m.group(1).lower()
            year_raw = m.group(2)
            if abbr in _MONTH_ABBR_TO_NUM:
                year = year_raw if len(year_raw) == 4 else _expand_2y(year_raw)
                return (
                    f"{year}-{_MONTH_ABBR_TO_NUM[abbr]}",
                    DeliveryPeriod.MONTHLY,
                )

        # --- "March 2024" ---
        m = _RE_FULL_MONTH_YEAR.match(tenor)
        if m:
            month_name = m.group(1).lower()
            year = m.group(2)
            if month_name in _FULL_MONTH_TO_NUM:
                return (
                    f"{year}-{_FULL_MONTH_TO_NUM[month_name]}",
                    DeliveryPeriod.MONTHLY,
                )

        # --- Quarterly: "Q1-2024", "Q1 2024" ---
        m = _RE_QUARTERLY_Q_FIRST.match(tenor)
        if m:
            q, year = m.group(1), m.group(2)
            return f"{year}-Q{q}", DeliveryPeriod.QUARTERLY

        # --- Quarterly: "2024Q1", "2024-Q1" ---
        m = _RE_QUARTERLY_Y_FIRST.match(tenor)
        if m:
            year, q = m.group(1), m.group(2)
            return f"{year}-Q{q}", DeliveryPeriod.QUARTERLY

        # --- Seasonal summer ---
        m = _RE_SEASONAL_SUMMER.match(tenor)
        if m:
            year = m.group(1) or _expand_2y(m.group(2))
            return f"{year}-SUM", DeliveryPeriod.SEASONAL

        # --- Seasonal winter ---
        m = _RE_SEASONAL_WINTER.match(tenor)
        if m:
            year = m.group(1) or _expand_2y(m.group(2))
            return f"{year}-WIN", DeliveryPeriod.SEASONAL

        # --- Annual: "Cal-2024" ---
        m = _RE_ANNUAL_CAL_DASH.match(tenor)
        if m:
            return f"{m.group(1)}-CAL", DeliveryPeriod.ANNUAL

        # --- Annual: "CAL24" ---
        m = _RE_ANNUAL_CAL_ABBR.match(tenor)
        if m:
            return f"{_expand_2y(m.group(1))}-CAL", DeliveryPeriod.ANNUAL

        # --- Annual: bare "2024" ---
        m = _RE_ANNUAL_BARE.match(tenor)
        if m:
            return f"{m.group(1)}-CAL", DeliveryPeriod.ANNUAL

        raise ValueError(
            f"Unrecognised tenor string: {raw_tenor!r}. "
            "Cannot map to a canonical format."
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_month(month: str) -> None:
    """Raise ValueError if *month* (zero-padded) is not 01–12."""
    if not (1 <= int(month) <= 12):
        raise ValueError(f"Invalid month number: {month!r}")
