"""Tests for the Oregon PERS adapter using cached PDF data."""

import pytest
from pathlib import Path

from src.adapters.oregon import OregonAdapter


CACHE_FILE = Path("data/cache/oregon/pe_portfolio_q3_2025.pdf")


@pytest.fixture
def adapter():
    return OregonAdapter(use_cache=True)


@pytest.fixture
def records(adapter):
    return adapter.extract()


@pytest.mark.skipif(
    not CACHE_FILE.exists(),
    reason="Oregon cache file not available"
)
class TestOregonAdapter:
    """Test suite for Oregon PERS PE data extraction."""

    def test_extract_returns_records(self, records):
        assert len(records) > 350
        assert len(records) < 550

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

    def test_all_records_have_commitment(self, records):
        for r in records:
            assert r["commitment_mm"] is not None, \
                f"Missing commitment for {r['fund_name_raw']}"
            assert r["commitment_mm"] > 0, \
                f"Non-positive commitment for {r['fund_name_raw']}"

    def test_all_records_have_vintage_year(self, records):
        for r in records:
            assert r["vintage_year"] is not None, \
                f"Missing vintage for {r['fund_name_raw']}"
            assert 1980 <= r["vintage_year"] <= 2030, \
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
                assert 0.1 <= c <= 5000, \
                    f"Unreasonable commitment ${c}M for {r['fund_name_raw']}"

    def test_irr_values_reasonable(self, records):
        irr_count = 0
        for r in records:
            if r["net_irr"] is not None:
                irr_count += 1
                assert -1.0 <= r["net_irr"] <= 1.50, \
                    f"Unreasonable IRR {r['net_irr']} for {r['fund_name_raw']}"
        assert irr_count > len(records) * 0.80

    def test_net_multiple_reasonable(self, records):
        for r in records:
            if r["net_multiple"] is not None:
                assert 0 <= r["net_multiple"] <= 50.0, \
                    f"Unreasonable multiple {r['net_multiple']} for {r['fund_name_raw']}"

    def test_known_fund_present(self, records):
        fund_names = [r["fund_name_raw"] for r in records]
        carlyle = [n for n in fund_names if "Carlyle" in n]
        assert len(carlyle) > 0, "Expected Carlyle funds in Oregon data"

    def test_source_url_set(self, records):
        for r in records:
            assert r["source_url"].startswith("https://")

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "oregon"
        assert info["name"] == "Oregon PERS"
        assert info["state"] == "OR"
