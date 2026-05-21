"""Unit tests for normalization_service.tenor_mapper."""

import pytest

from normalization_service.tenor_mapper import TenorMapper
from mdrp_common.models import DeliveryPeriod


@pytest.fixture()
def mapper() -> TenorMapper:
    return TenorMapper()


class TestMonthly:
    def test_iso_monthly(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("2024-03")
        assert tenor == "2024-03"
        assert period is DeliveryPeriod.MONTHLY

    def test_iso_monthly_single_digit(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("2024-3")
        assert tenor == "2024-03"
        assert period is DeliveryPeriod.MONTHLY

    def test_abbr_monthly_upper(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("MAR24")
        assert tenor == "2024-03"
        assert period is DeliveryPeriod.MONTHLY

    def test_abbr_monthly_lower(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("dec24")
        assert tenor == "2024-12"
        assert period is DeliveryPeriod.MONTHLY

    def test_abbr_dash_2y(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Mar-24")
        assert tenor == "2024-03"
        assert period is DeliveryPeriod.MONTHLY

    def test_abbr_dash_4y(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Jun-2025")
        assert tenor == "2025-06"
        assert period is DeliveryPeriod.MONTHLY

    def test_full_month_year(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("March 2024")
        assert tenor == "2024-03"
        assert period is DeliveryPeriod.MONTHLY

    def test_full_month_year_upper(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("DECEMBER 2025")
        assert tenor == "2025-12"
        assert period is DeliveryPeriod.MONTHLY

    def test_relative_m_plus_1(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("M+1")
        assert period is DeliveryPeriod.MONTHLY
        assert len(tenor) == 7
        assert tenor[4] == "-"

    def test_relative_m_plus_12(self, mapper: TenorMapper) -> None:
        _, period = mapper.normalise("M+12")
        assert period is DeliveryPeriod.MONTHLY

    def test_whitespace_stripped(self, mapper: TenorMapper) -> None:
        tenor, _ = mapper.normalise("  2024-06  ")
        assert tenor == "2024-06"


class TestQuarterly:
    def test_q_first_dash(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Q1-2024")
        assert tenor == "2024-Q1"
        assert period is DeliveryPeriod.QUARTERLY

    def test_q_first_space(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Q3 2025")
        assert tenor == "2025-Q3"
        assert period is DeliveryPeriod.QUARTERLY

    def test_year_first_dash(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("2024-Q2")
        assert tenor == "2024-Q2"
        assert period is DeliveryPeriod.QUARTERLY

    def test_year_first_no_dash(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("2024Q4")
        assert tenor == "2024-Q4"
        assert period is DeliveryPeriod.QUARTERLY

    def test_relative_q_plus_1(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Q+1")
        assert period is DeliveryPeriod.QUARTERLY
        assert "-Q" in tenor


class TestSeasonal:
    def test_summer_full(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Summer 2024")
        assert tenor == "2024-SUM"
        assert period is DeliveryPeriod.SEASONAL

    def test_summer_abbr(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("SUM24")
        assert tenor == "2024-SUM"
        assert period is DeliveryPeriod.SEASONAL

    def test_winter_full(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Winter 2025")
        assert tenor == "2025-WIN"
        assert period is DeliveryPeriod.SEASONAL

    def test_winter_abbr(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("WIN25")
        assert tenor == "2025-WIN"
        assert period is DeliveryPeriod.SEASONAL


class TestAnnual:
    def test_cal_dash(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("Cal-2024")
        assert tenor == "2024-CAL"
        assert period is DeliveryPeriod.ANNUAL

    def test_cal_abbr(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("CAL24")
        assert tenor == "2024-CAL"
        assert period is DeliveryPeriod.ANNUAL

    def test_bare_year(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("2024")
        assert tenor == "2024-CAL"
        assert period is DeliveryPeriod.ANNUAL


class TestSpot:
    def test_spot_lower(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("spot")
        assert tenor == "spot"
        assert period is DeliveryPeriod.SPOT

    def test_d_plus_1(self, mapper: TenorMapper) -> None:
        tenor, period = mapper.normalise("D+1")
        assert tenor == "spot"
        assert period is DeliveryPeriod.SPOT


class TestErrors:
    def test_unknown_raises(self, mapper: TenorMapper) -> None:
        with pytest.raises(ValueError, match="Unrecognised tenor"):
            mapper.normalise("GARBAGE")

    def test_empty_raises(self, mapper: TenorMapper) -> None:
        with pytest.raises(ValueError):
            mapper.normalise("")

    def test_invalid_month_raises(self, mapper: TenorMapper) -> None:
        with pytest.raises(ValueError):
            mapper.normalise("2024-13")
