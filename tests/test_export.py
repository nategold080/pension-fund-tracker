"""Tests for the export module."""

import csv
import pytest
from pathlib import Path

from src.database import Database
from src.export import Exporter


@pytest.fixture
def db(tmp_path):
    """Create an in-memory database with test data."""
    db = Database(tmp_path / "test.db")
    db.migrate()

    # Create pension funds
    db.upsert_pension_fund(id="pf1", name="Test Fund A", state="CA")
    db.upsert_pension_fund(id="pf2", name="Test Fund B", state="WA")

    # Create funds
    db.upsert_fund(id="f1", fund_name="Alpha Fund I", fund_name_raw="Alpha Fund I, L.P.",
                   general_partner="Alpha", vintage_year=2020, asset_class="Private Equity",
                   sub_strategy="Buyout")
    db.upsert_fund(id="f2", fund_name="Beta Ventures III", fund_name_raw="Beta Ventures III",
                   general_partner="Beta", vintage_year=2021, asset_class="Private Equity",
                   sub_strategy="Venture Capital")

    # Create commitments (f1 appears in both pension funds for cross-linking)
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=100.0,
        vintage_year=2020, net_irr=0.15, net_multiple=1.5, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf2", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_pdf", commitment_mm=50.0,
        vintage_year=2020, net_irr=0.14, net_multiple=1.48, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f2", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=25.0,
        vintage_year=2021, net_irr=0.30, net_multiple=2.1, as_of_date="2025-06-30",
    )

    yield db
    db.close()


@pytest.fixture
def exporter(db, tmp_path):
    return Exporter(db, export_dir=tmp_path / "exports")


class TestExporter:
    def test_export_commitments_csv(self, exporter):
        path = exporter.export_commitments_csv()
        assert path is not None
        assert path.exists()

        with open(path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3
        assert "pension_fund_name" in reader.fieldnames
        assert "fund_name" in reader.fieldnames
        assert "commitment_mm" in reader.fieldnames

    def test_export_commitments_csv_has_expected_columns(self, exporter):
        path = exporter.export_commitments_csv()
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        expected_cols = [
            "pension_fund_name", "fund_name", "vintage_year",
            "commitment_mm", "net_irr", "net_multiple",
            "source_url", "extraction_method",
        ]
        for col in expected_cols:
            assert col in reader.fieldnames, f"Missing column: {col}"

    def test_export_summary_csv(self, exporter):
        path = exporter.export_summary_csv()
        assert path is not None
        assert path.exists()

        with open(path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2  # Two funds
        assert "fund_name" in reader.fieldnames
        assert "pension_fund_count" in reader.fieldnames

    def test_export_quality_report(self, exporter):
        path = exporter.export_quality_report()
        assert path is not None
        assert path.exists()

        content = path.read_text()
        assert "Data Quality Report" in content

    def test_export_all(self, exporter):
        results = exporter.export_all()
        assert "commitments" in results
        assert results["commitments"] is not None
        assert results["commitments"].exists()
