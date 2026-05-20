"""
Unit tests for the TenorMapper in normalization-service.

Verifies canonical mapping and DeliveryPeriod inference for all supported
input formats.  No external dependencies.
"""

from __future__ import annotations

import pytest

from mdrp_common.models import DeliveryPeriod
from normalization_service.tenor_mapper import TenorMapper


@pytest.fixture
def mapper() -> TenorMapper:
    return TenorMapper()


# ---------------------------------------------------------------------------
# Monthly
# ---------------------------------------------------------------------------


class TestMonthlyTenors:
    def test_iso_format(self, mapper):
        canon, period = mapper.normalise("2024-03")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_iso_format_single_digit_month(self, mapper):
        canon, period = mapper.normalise("2024-3")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_abbr_uppercase_mar24(self, mapper):
        canon, period = mapper.normalise("MAR24")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_abbr_lowercase_mar24(self, mapper):
        canon, period = mapper.normalise("mar24")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_abbr_dash_short_year(self, mapper):
        canon, period = mapper.normalise("Mar-24")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_abbr_dash_full_year(self, mapper):
        canon, period = mapper.normalise("Mar-2024")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_full_name_title_case(self, mapper):
        canon, period = mapper.normalise("March 2024")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_full_name_uppercase(self, mapper):
        canon, period = mapper.normalise("MARCH 2024")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_full_name_lowercase(self, mapper):
        canon, period = mapper.normalise("march 2024")
        assert canon == "2024-03"
        assert period == DeliveryPeriod.MONTHLY

    def test_january(self, mapper):
        canon, period = mapper.normalise("JAN24")
        assert canon == "2024-01"
        assert period == DeliveryPeriod.MONTHLY

    def test_december(self, mapper):
        canon, period = mapper.normalise("DEC24")
        assert canon == "2024-12"
        assert period == DeliveryPeriod.MONTHLY

    def test_february_full_name(self, mapper):
        canon, period = mapper.normalise("February 2025")
        assert canon == "2025-02"
        assert period == DeliveryPeriod.MONTHLY

    @pytest.mark.parametrize("raw,expected", [
        ("2024-03", "2024-03"),
        ("MAR24", "2024-03"),
        ("Mar-24", "2024-03"),
        ("March 2024", "2024-03"),
    ])
    def test_all_march_2024_forms_equivalent(self, mapper, raw, expected):
        canon, period = mapper.normalise(raw)
        assert canon == expected
        assert period == DeliveryPeriod.MONTHLY


# ---------------------------------------------------------------------------
# Quarterly
# ---------------------------------------------------------------------------


class TestQuarterlyTenors:
    def test_q_first_dash(self, mapper):
        canon, period = mapper.normalise("Q1-2024")
        assert canon == "2024-Q1"
        assert period == DeliveryPeriod.QUARTERLY

    def test_q_first_space(self, mapper):
        canon, period = mapper.normalise("Q1 2024")
        assert canon == "2024-Q1"
        assert period == DeliveryPeriod.QUARTERLY

    def test_year_first_dash(self, mapper):
        canon, period = mapper.normalise("2024-Q1")
        assert canon == "2024-Q1"
        assert period == DeliveryPeriod.QUARTERLY

    def test_year_first_no_dash(self, mapper):
        canon, period = mapper.normalise("2024Q1")
        assert canon == "2024-Q1"
        assert period == DeliveryPeriod.QUARTERLY

    def test_q2(self, mapper):
        canon, period = mapper.normalise("Q2-2024")
        assert canon == "2024-Q2"
        assert period == DeliveryPeriod.QUARTERLY

    def test_q3(self, mapper):
        canon, period = mapper.normalise("Q3-2024")
        assert canon == "2024-Q3"
        assert period == DeliveryPeriod.QUARTERLY

    def test_q4(self, mapper):
        canon, period = mapper.normalise("Q4-2024")
        assert canon == "2024-Q4"
        assert period == DeliveryPeriod.QUARTERLY

    def test_lowercase_q(self, mapper):
        canon, period = mapper.normalise("q1-2024")
        assert canon == "2024-Q1"
        assert period == DeliveryPeriod.QUARTERLY

    @pytest.mark.parametrize("raw,expected", [
        ("Q1-2024", "2024-Q1"),
        ("Q1 2024", "2024-Q1"),
        ("2024-Q1", "2024-Q1"),
        ("2024Q1", "2024-Q1"),
    ])
    def test_all_q1_2024_forms_equivalent(self, mapper, raw, expected):
        canon, period = mapper.normalise(raw)
        assert canon == expected
        assert period == DeliveryPeriod.QUARTERLY


# ---------------------------------------------------------------------------
# Seasonal
# ---------------------------------------------------------------------------


class TestSeasonalTenors:
    def test_summer_full_title_case(self, mapper):
        canon, period = mapper.normalise("Summer 2024")
        assert canon == "2024-SUM"
        assert period == DeliveryPeriod.SEASONAL

    def test_summer_full_uppercase(self, mapper):
        canon, period = mapper.normalise("SUMMER 2024")
        assert canon == "2024-SUM"
        assert period == DeliveryPeriod.SEASONAL

    def test_summer_abbr_two_digit_year(self, mapper):
        canon, period = mapper.normalise("SUM24")
        assert canon == "2024-SUM"
        assert period == DeliveryPeriod.SEASONAL

    def test_summer_abbr_lowercase(self, mapper):
        canon, period = mapper.normalise("sum24")
        assert canon == "2024-SUM"
        assert period == DeliveryPeriod.SEASONAL

    def test_winter_full_title_case(self, mapper):
        canon, period = mapper.normalise("Winter 2024")
        assert canon == "2024-WIN"
        assert period == DeliveryPeriod.SEASONAL

    def test_winter_full_uppercase(self, mapper):
        canon, period = mapper.normalise("WINTER 2024")
        assert canon == "2024-WIN"
        assert period == DeliveryPeriod.SEASONAL

    def test_winter_abbr_two_digit_year(self, mapper):
        canon, period = mapper.normalise("WIN24")
        assert canon == "2024-WIN"
        assert period == DeliveryPeriod.SEASONAL

    def test_winter_abbr_lowercase(self, mapper):
        canon, period = mapper.normalise("win24")
        assert canon == "2024-WIN"
        assert period == DeliveryPeriod.SEASONAL

    def test_summer_2025(self, mapper):
        canon, period = mapper.normalise("Summer 2025")
        assert canon == "2025-SUM"
        assert period == DeliveryPeriod.SEASONAL


# ---------------------------------------------------------------------------
# Annual
# ---------------------------------------------------------------------------


class TestAnnualTenors:
    def test_cal_dash_full_year(self, mapper):
        canon, period = mapper.normalise("Cal-2024")
        assert canon == "2024-CAL"
        assert period == DeliveryPeriod.ANNUAL

    def test_cal_dash_uppercase(self, mapper):
        canon, period = mapper.normalise("CAL-2024")
        assert canon == "2024-CAL"
        assert period == DeliveryPeriod.ANNUAL

    def test_cal_abbr_two_digit_year(self, mapper):
        canon, period = mapper.normalise("CAL24")
        assert canon == "2024-CAL"
        assert period == DeliveryPeriod.ANNUAL

    def test_cal_abbr_lowercase(self, mapper):
        canon, period = mapper.normalise("cal24")
        assert canon == "2024-CAL"
        assert period == DeliveryPeriod.ANNUAL

    def test_bare_four_digit_year(self, mapper):
        canon, period = mapper.normalise("2024")
        assert canon == "2024-CAL"
        assert period == DeliveryPeriod.ANNUAL

    def test_bare_year_2025(self, mapper):
        canon, period = mapper.normalise("2025")
        assert canon == "2025-CAL"
        assert period == DeliveryPeriod.ANNUAL

    @pytest.mark.parametrize("raw,expected", [
        ("Cal-2024", "2024-CAL"),
        ("CAL24", "2024-CAL"),
        ("2024", "2024-CAL"),
    ])
    def test_all_cal_2024_forms_equivalent(self, mapper, raw, expected):
        canon, period = mapper.normalise(raw)
        assert canon == expected
        assert period == DeliveryPeriod.ANNUAL


# ---------------------------------------------------------------------------
# Spot
# ---------------------------------------------------------------------------


class TestSpotTenors:
    def test_spot_lowercase(self, mapper):
        canon, period = mapper.normalise("spot")
        assert canon == "spot"
        assert period == DeliveryPeriod.SPOT

    def test_spot_uppercase(self, mapper):
        canon, period = mapper.normalise("SPOT")
        assert canon == "spot"
        assert period == DeliveryPeriod.SPOT

    def test_d_plus_one(self, mapper):
        canon, period = mapper.normalise("D+1")
        assert canon == "spot"
        assert period == DeliveryPeriod.SPOT

    def test_d_plus_one_lowercase(self, mapper):
        canon, period = mapper.normalise("d+1")
        assert canon == "spot"
        assert period == DeliveryPeriod.SPOT


# ---------------------------------------------------------------------------
# Whitespace handling
# ---------------------------------------------------------------------------


class TestWhitespaceHandling:
    def test_leading_whitespace_stripped(self, mapper):
        canon, period = mapper.normalise("  2024-03")
        assert canon == "2024-03"

    def test_trailing_whitespace_stripped(self, mapper):
        canon, period = mapper.normalise("2024-03  ")
        assert canon == "2024-03"

    def test_both_sides_stripped(self, mapper):
        canon, period = mapper.normalise("  MAR24  ")
        assert canon == "2024-03"


# ---------------------------------------------------------------------------
# Invalid inputs
# ---------------------------------------------------------------------------


class TestInvalidTenors:
    @pytest.mark.parametrize("invalid", [
        "INVALID",
        "2024-13",       # invalid month 13
        "Q5-2024",       # no quarter 5
        "XYZ24",         # unrecognised month abbreviation
        "",              # empty string
        "not-a-tenor",
        "2024-00",       # month 0 invalid
        "ABC",
        "99",            # two-digit year alone
        "2024-Q5",       # no quarter 5
    ])
    def test_invalid_raises_value_error(self, mapper, invalid):
        with pytest.raises(ValueError):
            mapper.normalise(invalid)
