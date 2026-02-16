"""Tests for the Florida SBA adapter."""

import pytest
from pathlib import Path

from src.adapters.florida_sba import FloridaSBAAdapter


@pytest.fixture
def adapter():
    return FloridaSBAAdapter(use_cache=True)


class TestFloridaSBAAdapter:
    """Test suite for Florida SBA adapter."""

    def test_pension_fund_info(self, adapter):
        info = adapter.get_pension_fund_info()
        assert info["id"] == "florida_sba"
        assert info["name"] == "Florida State Board of Administration"
        assert info["state"] == "FL"

    def test_fetch_raises_without_cache(self, adapter):
        """Without a cached PDF, fetch should raise FileNotFoundError."""
        # Remove any stale cache files
        cache_dir = Path("data/cache/florida_sba")
        has_valid_pdf = False
        if cache_dir.exists():
            for f in cache_dir.glob("*.pdf"):
                data = f.read_bytes()
                if len(data) > 1000 and data[:5] == b"%PDF-":
                    has_valid_pdf = True
                    break

        if not has_valid_pdf:
            with pytest.raises(FileNotFoundError, match="sbafla.com"):
                adapter.fetch_source()

    def test_adapter_attributes(self, adapter):
        assert adapter.pension_fund_id == "florida_sba"
        assert adapter.state == "FL"
        assert "sbafla.com" in adapter.source_url
