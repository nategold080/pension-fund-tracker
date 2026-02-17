"""Tests for the NY Common Retirement Fund adapter using cached PDF data."""

import pytest
from pathlib import Path

from src.adapters.ny_common import NYCommonAdapter


CACHE_FILE = Path("data/cache/ny_common/asset_listing_2025.pdf")


@pytest.fixture
def adapter():
    return NYCommonAdapter(use_cache=True)


@pytest.fixture
def records(adapter):
    return adapter.extract()


@pytest.mark.skipif(
    not CACHE_FILE.exists(),
    reason="NY Common cache file not available"
)
class TestNYCommonAdapter:
    """Test suite for NY Common PE data extraction."""

    def test_extract_returns_records(self, records):
        assert len(records) > 300
        assert len(records) < 500

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
        # At least 99% of records should have commitments
        assert len(with_commitment) / len(records) > 0.99
        for r in with_commitment:
            assert r["commitment_mm"] > 0, \
                f"Non-positive commitment for {r['fund_name_raw']}"

    def test_all_records_have_vintage_year(self, records):
        for r in records:
            assert r["vintage_year"] is not None, \
                f"Missing vintage for {r['fund_name_raw']}"
            assert 1985 <= r["vintage_year"] <= 2030, \
                f"Invalid vintage {r['vintage_year']} for {r['fund_name_raw']}"

    def test_extraction_method_is_deterministic_pdf(self, records):
        for r in records:
            assert r["extraction_method"] == "deterministic_pdf"
            assert r["extraction_confidence"] == 0.95

    def test_as_of_date_is_march_2025(self, records):
        dates = {r["as_of_date"] for r in records}
        assert len(dates) == 1
        assert dates.pop() == "2025-03-31"

    def test_commitment_amounts_reasonable(self, records):
        for r in records:
            c = r["commitment_mm"]
            if c is not None:
                assert 0.1 <= c <= 5000, \
                    f"Unreasonable commitment ${c}M for {r['fund_name_raw']}"

    def test_net_multiple_reasonable(self, records):
        for r in records:
            if r["net_multiple"] is not None:
                assert 0 <= r["net_multiple"] <= 50, \
                    f"Unreasonable multiple {r['net_multiple']} for {r['fund_name_raw']}"

    def test_total_commitment_matches_pdf(self, records):
        """Total committed should be ~$70B (per 2025 PDF total line)."""
        total = sum(r["commitment_mm"] for r in records if r["commitment_mm"])
        assert 60_000 < total < 80_000, f"Total commitment ${total:,.0f}M unexpected"

    def test_known_fund_present(self, records):
        """Verify well-known funds are in the data."""
        fund_names = [r["fund_name_raw"] for r in records]
        # Check for Warburg Pincus funds (appear on last PE page)
        warburg = [n for n in fund_names if "Warburg" in n]
        assert len(warburg) > 0, "Expected Warburg Pincus funds in NY Common data"
        # Check for Hellman & Friedman
        hf = [n for n in fund_names if "Hellman" in n]
        assert len(hf) > 0, "Expected Hellman & Friedman funds in NY Common data"

    def test_source_url_set(self, records):
        for r in records:
            assert r["source_url"].startswith("https://")

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "ny_common"
        assert info["name"] == "New York State Common Retirement Fund"
        assert info["state"] == "NY"
