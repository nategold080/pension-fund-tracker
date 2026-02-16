"""Tests for the CalPERS adapter using cached HTML data."""

import pytest
from pathlib import Path

from src.adapters.calpers import CalPERSAdapter


CACHE_FILE = Path("data/cache/calpers/pep_fund_performance_print.html")


@pytest.fixture
def adapter():
    """Create CalPERS adapter using cached data."""
    return CalPERSAdapter(use_cache=True)


@pytest.fixture
def records(adapter):
    """Extract records from cached data."""
    return adapter.extract()


@pytest.mark.skipif(
    not CACHE_FILE.exists(),
    reason="CalPERS cache file not available"
)
class TestCalPERSAdapter:
    """Test suite for CalPERS PE data extraction."""

    def test_extract_returns_records(self, records):
        assert len(records) > 400
        assert len(records) < 600  # Reasonable upper bound

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
            assert r["fund_name_raw"], f"Empty fund name in record"

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
            assert 1990 <= r["vintage_year"] <= 2030, \
                f"Invalid vintage {r['vintage_year']} for {r['fund_name_raw']}"

    def test_extraction_method_is_deterministic(self, records):
        for r in records:
            assert r["extraction_method"] == "deterministic_html"
            assert r["extraction_confidence"] == 1.0

    def test_as_of_date_is_consistent(self, records):
        dates = {r["as_of_date"] for r in records}
        assert len(dates) == 1, f"Multiple as-of dates found: {dates}"
        assert dates.pop() is not None

    def test_commitment_amounts_reasonable(self, records):
        """Commitment sizes should be between $1M and $5B."""
        for r in records:
            c = r["commitment_mm"]
            assert 0.1 <= c <= 5000, \
                f"Unreasonable commitment ${c}M for {r['fund_name_raw']}"

    def test_irr_values_reasonable(self, records):
        """IRR values should be between -50% and +100%."""
        for r in records:
            if r["net_irr"] is not None:
                assert -0.5 <= r["net_irr"] <= 1.0, \
                    f"Unreasonable IRR {r['net_irr']} for {r['fund_name_raw']}"

    def test_net_multiple_reasonable(self, records):
        """Net multiples should be between 0 and 10x."""
        for r in records:
            if r["net_multiple"] is not None:
                assert 0 <= r["net_multiple"] <= 10, \
                    f"Unreasonable multiple {r['net_multiple']} for {r['fund_name_raw']}"

    def test_known_fund_present(self, records):
        """Verify a well-known fund is in the data."""
        fund_names = [r["fund_name_raw"] for r in records]
        # Look for any Blackstone fund
        blackstone = [n for n in fund_names if "Blackstone" in n]
        assert len(blackstone) > 0, "Expected Blackstone funds in CalPERS data"

    def test_source_url_set(self, records):
        for r in records:
            assert r["source_url"].startswith("https://")

    def test_source_hash(self, adapter):
        """Test source hash computation."""
        raw = adapter.fetch_source()
        h = adapter.get_source_hash(raw)
        assert len(h) == 64  # SHA256 hex digest
        # Same data should produce same hash
        assert adapter.get_source_hash(raw) == h

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "calpers"
        assert info["name"] == "CalPERS"
        assert info["state"] == "CA"
