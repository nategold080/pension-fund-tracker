"""Tests for the pipeline module."""

import pytest
from pathlib import Path

from src.database import Database
from src.pipeline import Pipeline
from src.adapters.base import PensionFundAdapter


class DummyAdapter(PensionFundAdapter):
    """A minimal adapter for testing the pipeline without real data."""

    pension_fund_id = "dummy"
    pension_fund_name = "Dummy Fund"
    state = "XX"
    full_name = "Dummy Test Pension Fund"
    total_aum_mm = 1000.0
    data_source_type = "test"
    disclosure_quality = "excellent"
    source_url = "https://example.com"

    def __init__(self, records=None, should_fail=False, **kwargs):
        self._records = records or []
        self._should_fail = should_fail

    def fetch_source(self):
        if self._should_fail:
            raise ConnectionError("Simulated fetch failure")
        return b"test data"

    def parse(self, raw_data) -> list[dict]:
        return self._records


class FailingFetchAdapter(DummyAdapter):
    """Adapter that fails on fetch (e.g., missing cache file)."""

    pension_fund_id = "failing"
    pension_fund_name = "Failing Fund"

    def fetch_source(self):
        raise FileNotFoundError("Cache file not found")


@pytest.fixture
def db(tmp_path):
    db = Database(tmp_path / "test_pipeline.db")
    db.migrate()
    yield db
    db.close()


@pytest.fixture
def sample_records():
    return [
        {
            "fund_name_raw": "Alpha Fund I, L.P.",
            "general_partner": "Alpha Capital",
            "vintage_year": 2020,
            "asset_class": "Private Equity",
            "sub_strategy": "Buyout",
            "commitment_mm": 100.0,
            "capital_called_mm": 80.0,
            "capital_distributed_mm": 40.0,
            "remaining_value_mm": 90.0,
            "net_irr": 0.15,
            "net_multiple": 1.6,
            "dpi": None,
            "as_of_date": "2025-06-30",
            "source_url": "https://example.com",
            "source_document": "Test Report",
            "extraction_method": "deterministic_test",
            "extraction_confidence": 0.95,
        },
        {
            "fund_name_raw": "Beta Ventures III",
            "general_partner": "Beta",
            "vintage_year": 2019,
            "asset_class": "Private Equity",
            "sub_strategy": "Venture Capital",
            "commitment_mm": 50.0,
            "capital_called_mm": 45.0,
            "capital_distributed_mm": 20.0,
            "remaining_value_mm": 60.0,
            "net_irr": 0.25,
            "net_multiple": 1.8,
            "dpi": None,
            "as_of_date": "2025-06-30",
            "source_url": "https://example.com",
            "source_document": "Test Report",
            "extraction_method": "deterministic_test",
            "extraction_confidence": 0.95,
        },
    ]


class TestPipeline:
    def test_pipeline_runs_adapter_successfully(self, db, sample_records):
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        results = pipeline.run([adapter])

        assert "dummy" in results
        assert results["dummy"]["status"] == "completed"
        assert results["dummy"]["records_extracted"] == 2

    def test_pipeline_inserts_records_into_db(self, db, sample_records):
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        pipeline.run([adapter])

        commitments = db.get_commitments(pension_fund_id="dummy")
        assert len(commitments) == 2

    def test_pipeline_handles_adapter_failure_gracefully(self, db):
        good_adapter = DummyAdapter(records=[{
            "fund_name_raw": "Good Fund", "general_partner": None,
            "vintage_year": 2020, "asset_class": "Private Equity",
            "sub_strategy": None, "commitment_mm": 100.0,
            "capital_called_mm": None, "capital_distributed_mm": None,
            "remaining_value_mm": None, "net_irr": None, "net_multiple": None,
            "dpi": None, "as_of_date": "2025-06-30",
            "source_url": "https://example.com", "source_document": "Test",
            "extraction_method": "test", "extraction_confidence": 0.95,
        }])
        failing_adapter = FailingFetchAdapter()

        pipeline = Pipeline(db)
        results = pipeline.run([failing_adapter, good_adapter])

        assert results["failing"]["status"] == "error"
        assert "FileNotFoundError" in results["failing"]["error"]
        assert results["dummy"]["status"] == "completed"
        assert results["dummy"]["records_extracted"] == 1

    def test_pipeline_creates_extraction_run(self, db, sample_records):
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        pipeline.run([adapter])

        runs = db.get_extraction_runs(pension_fund_id="dummy")
        assert len(runs) >= 1
        assert runs[0]["status"] == "completed"
        assert runs[0]["records_extracted"] == 2

    def test_entity_resolution_creates_funds(self, db, sample_records):
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        pipeline.run([adapter])

        funds = db.list_funds()
        assert len(funds) >= 2
        fund_names = [f["fund_name"] for f in funds]
        assert any("Alpha" in n for n in fund_names)
        assert any("Beta" in n for n in fund_names)

    def test_pipeline_registers_pension_fund(self, db, sample_records):
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        pipeline.run([adapter])

        pf = db.get_pension_fund("dummy")
        assert pf is not None
        assert pf["name"] == "Dummy Fund"

    def test_pipeline_with_empty_adapter(self, db):
        adapter = DummyAdapter(records=[])
        pipeline = Pipeline(db)
        results = pipeline.run([adapter])

        assert results["dummy"]["status"] == "completed"
        assert results["dummy"]["records_extracted"] == 0

    def test_pipeline_extracts_consulting_data(self, db, sample_records):
        """Test that consulting data extraction is called during pipeline run."""
        adapter = DummyAdapter(records=sample_records)
        pipeline = Pipeline(db)
        results = pipeline.run([adapter])

        # DummyAdapter returns empty consulting data by default
        assert results["dummy"]["status"] == "completed"
        assert results["dummy"].get("consulting_extracted", 0) == 0

    def test_pipeline_consulting_data_with_seeded_firms(self, db, sample_records):
        """Test consulting extraction when firms are seeded in the DB."""
        # Seed a consulting firm
        db.upsert_consulting_firm(
            id="test_firm",
            name="Test Consulting LLC",
            name_normalized="test consulting",
            firm_type="general_consultant",
        )
        # Seed the pension fund so the engagement FK is satisfied
        db.upsert_pension_fund(id="dummy", name="Dummy Fund", state="XX")

        # Create adapter that returns consulting data
        class ConsultingAdapter(DummyAdapter):
            def extract_consulting_data(self):
                return [{
                    "consulting_firm_name": "Test Consulting LLC",
                    "role": "general_investment_consultant",
                    "mandate_scope": "all_asset_classes",
                    "is_current": True,
                    "source_url": "https://example.com",
                    "extraction_method": "test",
                    "extraction_confidence": 0.95,
                }]

        adapter = ConsultingAdapter(records=sample_records)
        pipeline = Pipeline(db)
        results = pipeline.run([adapter])

        assert results["dummy"]["consulting_extracted"] == 1
        engagements = db.get_consulting_engagements_joined(pension_fund_id="dummy")
        assert len(engagements) == 1
        assert engagements[0]["consulting_firm_name"] == "Test Consulting LLC"
