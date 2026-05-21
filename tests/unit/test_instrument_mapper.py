"""Unit tests for normalization_service.instrument_mapper."""

import pytest

from normalization_service.instrument_mapper import InstrumentMapper


@pytest.fixture()
def mapper() -> InstrumentMapper:
    return InstrumentMapper()


class TestKnownInstruments:
    def test_ttf(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("TTF")
        assert instrument == "TTF"
        assert currency == "EUR"
        assert unit == "MWh"

    def test_ttf_gas_space(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("TTF GAS")
        assert instrument == "TTF"

    def test_ttf_gas_underscore(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("TTF_GAS")
        assert instrument == "TTF"

    def test_nbp(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("NBP")
        assert instrument == "NBP"
        assert currency == "EUR"
        assert unit == "MWh"

    def test_ttf_power(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("TTF_POWER")
        assert instrument == "TTF_POWER"
        assert currency == "EUR"
        assert unit == "MWh"

    def test_brent_canonical(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("BRENT")
        assert instrument == "BRENT"
        assert currency == "USD"
        assert unit == "bbl"

    def test_brent_alias_brn(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("BRN")
        assert instrument == "BRENT"

    def test_wti(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("WTI")
        assert instrument == "WTI"
        assert currency == "USD"
        assert unit == "bbl"

    def test_wti_alias_cl(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("CL")
        assert instrument == "WTI"

    def test_eu_ets_canonical(self, mapper: InstrumentMapper) -> None:
        instrument, currency, unit = mapper.normalise("EU_ETS")
        assert instrument == "EU_ETS"
        assert currency == "EUR"
        assert unit == "tonne"

    def test_eua_alias(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("EUA")
        assert instrument == "EU_ETS"

    def test_case_insensitive(self, mapper: InstrumentMapper) -> None:
        instrument, _, _ = mapper.normalise("wti")
        assert instrument == "WTI"


class TestIsKnown:
    def test_known_returns_true(self, mapper: InstrumentMapper) -> None:
        assert mapper.is_known("TTF") is True

    def test_unknown_returns_false(self, mapper: InstrumentMapper) -> None:
        assert mapper.is_known("GARBAGE") is False


class TestErrors:
    def test_unknown_raises(self, mapper: InstrumentMapper) -> None:
        with pytest.raises(ValueError, match="Unknown instrument"):
            mapper.normalise("UNKNOWN_INST")

    def test_empty_raises(self, mapper: InstrumentMapper) -> None:
        with pytest.raises(ValueError):
            mapper.normalise("")
