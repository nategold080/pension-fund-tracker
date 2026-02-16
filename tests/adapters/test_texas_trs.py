"""Tests for the Texas TRS adapter using cached ACFR data."""

import pytest
from pathlib import Path

from src.adapters.texas_trs import TexasTRSAdapter


CACHE_FILE = Path("data/cache/texas_trs/acfr_2024.pdf")


@pytest.fixture
def adapter():
    return TexasTRSAdapter(use_cache=True)


@pytest.fixture
def records(adapter):
    return adapter.extract()


@pytest.mark.skipif(
    not CACHE_FILE.exists(),
    reason="Texas TRS cache file not available"
)
class TestTexasTRSAdapter:
    """Test suite for Texas TRS PE data extraction (ACFR summary mode)."""

    def test_extract_returns_summary_record(self, records):
        # ACFR only provides summary-level data (1 record)
        assert len(records) >= 1

    def test_summary_record_has_reasonable_values(self, records):
        r = records[0]
        # TRS PE portfolio is ~$33B NAV + $12B unfunded = ~$45B total
        assert r["commitment_mm"] is not None
        assert r["commitment_mm"] > 30_000  # At least $30B
        assert r["commitment_mm"] < 60_000  # Less than $60B
        assert r["remaining_value_mm"] is not None
        assert r["remaining_value_mm"] > 25_000

    def test_summary_has_low_confidence(self, records):
        """Summary data should have lower confidence than fund-level data."""
        r = records[0]
        assert r["extraction_confidence"] < 0.85

    def test_extraction_method(self, records):
        for r in records:
            assert r["extraction_method"] == "deterministic_pdf"

    def test_source_document_indicates_summary(self, records):
        r = records[0]
        assert "Summary" in r["source_document"] or "ACFR" in r["source_document"]

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "texas_trs"
        assert info["name"] == "Texas Teacher Retirement System"
        assert info["state"] == "TX"
