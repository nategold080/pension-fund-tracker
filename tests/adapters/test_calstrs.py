"""Tests for the CalSTRS adapter using cached PDF data."""

import pytest
from pathlib import Path

from src.adapters.calstrs import CalSTRSAdapter


CACHE_FILE = Path("data/cache/calstrs/pe_performance_table.pdf")


@pytest.fixture
def adapter():
    return CalSTRSAdapter(use_cache=True)


@pytest.fixture
def records(adapter):
    return adapter.extract()


@pytest.mark.skipif(
    not CACHE_FILE.exists(),
    reason="CalSTRS cache file not available"
)
class TestCalSTRSAdapter:
    """Test suite for CalSTRS PE data extraction."""

    def test_extract_returns_records(self, records):
        assert len(records) > 400
        assert len(records) < 600

    def test_record_has_required_fields(self, records):
        required_fields = [
            "fund_name_raw", "vintage_year", "commitment_mm",
            "source_url", "extraction_method", "extraction_confidence",
            "as_of_date",
        ]
        for field in required_fields:
            assert field in records[0], f"Missing field: {field}"

    def test_all_records_have_fund_name(self, records):
        for r in records:
            assert r["fund_name_raw"], "Empty fund name in record"

    def test_most_records_have_commitment(self, records):
        with_commitment = [r for r in records if r["commitment_mm"] is not None]
        assert len(with_commitment) / len(records) > 0.99

    def test_all_records_have_vintage_year(self, records):
        for r in records:
            assert r["vintage_year"] is not None, \
                f"Missing vintage for {r['fund_name_raw']}"
            assert 1990 <= r["vintage_year"] <= 2030, \
                f"Invalid vintage {r['vintage_year']} for {r['fund_name_raw']}"

    def test_extraction_method_is_deterministic_pdf(self, records):
        for r in records:
            assert r["extraction_method"] == "deterministic_pdf"
            assert r["extraction_confidence"] == 0.95

    def test_as_of_date_is_set(self, records):
        dates = {r["as_of_date"] for r in records}
        assert len(dates) == 1
        assert dates.pop() is not None

    def test_commitment_amounts_reasonable(self, records):
        for r in records:
            c = r["commitment_mm"]
            if c is not None:
                assert 0 <= c <= 5000, \
                    f"Unreasonable commitment ${c}M for {r['fund_name_raw']}"

    def test_irr_values_reasonable(self, records):
        irr_count = 0
        for r in records:
            if r["net_irr"] is not None:
                irr_count += 1
                assert -1.0 <= r["net_irr"] <= 1.50, \
                    f"Unreasonable IRR {r['net_irr']} for {r['fund_name_raw']}"
        # CalSTRS reports IRR for most funds
        assert irr_count > len(records) * 0.90

    def test_net_multiple_reasonable(self, records):
        for r in records:
            if r["net_multiple"] is not None:
                assert 0 <= r["net_multiple"] <= 15.0, \
                    f"Unreasonable multiple {r['net_multiple']} for {r['fund_name_raw']}"

    def test_known_fund_present(self, records):
        fund_names = [r["fund_name_raw"] for r in records]
        blackstone = [n for n in fund_names if "Blackstone" in n]
        assert len(blackstone) > 0, "Expected Blackstone funds in CalSTRS data"

    def test_source_url_set(self, records):
        for r in records:
            assert r["source_url"].startswith("https://")

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "calstrs"
        assert info["name"] == "CalSTRS"
        assert info["state"] == "CA"
