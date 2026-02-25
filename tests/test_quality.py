"""Tests for the quality module."""

import pytest
from pathlib import Path

from src.database import Database
from src.quality import QualityChecker


@pytest.fixture
def db(tmp_path):
    """Create an in-memory database with test data."""
    db = Database(tmp_path / "test.db")
    db.migrate()

    # Create a pension fund
    db.upsert_pension_fund(id="test_pf", name="Test Fund", state="CA")

    # Create test funds
    db.upsert_fund(id="f1", fund_name="Good Fund I", fund_name_raw="Good Fund I",
                   vintage_year=2020, asset_class="Private Equity")
    db.upsert_fund(id="f2", fund_name="Bad Multiple Fund", fund_name_raw="Bad Multiple Fund",
                   vintage_year=2021, asset_class="Private Equity")
    db.upsert_fund(id="f3", fund_name="Bad IRR Fund", fund_name_raw="Bad IRR Fund",
                   vintage_year=2019, asset_class="Private Equity")
    db.upsert_fund(id="f4", fund_name="Huge Commitment Fund", fund_name_raw="Huge Commitment Fund",
                   vintage_year=2022, asset_class="Private Equity")
    db.upsert_fund(id="f5", fund_name="Negative Multiple Fund", fund_name_raw="Negative Multiple Fund",
                   vintage_year=2023, asset_class="Private Equity")

    # Good record
    db.upsert_commitment(
        pension_fund_id="test_pf", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=100.0,
        vintage_year=2020, net_irr=0.15, net_multiple=1.5, as_of_date="2025-06-30",
    )
    # Bad multiple (>15x)
    db.upsert_commitment(
        pension_fund_id="test_pf", fund_id="f2", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=50.0,
        vintage_year=2021, net_multiple=20.0, as_of_date="2025-06-30",
    )
    # Bad IRR (>150%)
    db.upsert_commitment(
        pension_fund_id="test_pf", fund_id="f3", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=75.0,
        vintage_year=2019, net_irr=2.0, as_of_date="2025-06-30",
    )
    # Huge commitment (>$5B)
    db.upsert_commitment(
        pension_fund_id="test_pf", fund_id="f4", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=6000.0,
        vintage_year=2022, as_of_date="2025-06-30",
    )
    # Negative multiple
    db.upsert_commitment(
        pension_fund_id="test_pf", fund_id="f5", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=30.0,
        vintage_year=2023, net_multiple=-0.3, as_of_date="2025-06-30",
    )

    yield db
    db.close()


class TestQualityChecker:
    def test_flags_high_multiple(self, db):
        checker = QualityChecker(db)
        summary = checker.run_all_checks()
        flags = [f for f in db.get_review_queue(resolved=False)
                 if "20.00x" in f["flag_detail"]]
        assert len(flags) > 0, "Should flag multiple of 20x"

    def test_flags_high_irr(self, db):
        checker = QualityChecker(db)
        checker.run_all_checks()
        flags = [f for f in db.get_review_queue(resolved=False)
                 if "Net IRR" in f["flag_detail"] and "200" in f["flag_detail"]]
        assert len(flags) > 0, "Should flag IRR of 200%"

    def test_flags_huge_commitment(self, db):
        checker = QualityChecker(db)
        checker.run_all_checks()
        flags = [f for f in db.get_review_queue(resolved=False)
                 if "6000" in f["flag_detail"]]
        assert len(flags) > 0, "Should flag commitment of $6000M"

    def test_flags_negative_multiple(self, db):
        checker = QualityChecker(db)
        checker.run_all_checks()
        flags = [f for f in db.get_review_queue(resolved=False)
                 if "Negative net multiple" in f["flag_detail"]]
        assert len(flags) > 0, "Should flag negative multiple"

    def test_good_record_not_flagged(self, db):
        checker = QualityChecker(db)
        checker.run_all_checks()
        flags = [f for f in db.get_review_queue(resolved=False)
                 if "Good Fund I" in f["flag_detail"]]
        assert len(flags) == 0, "Good record should not be flagged"

    def test_generate_report(self, db):
        checker = QualityChecker(db)
        report = checker.generate_report()
        assert "Data Quality Report" in report
        assert "Total commitment records" in report
        assert "Quality flags created" in report

    def test_completeness_computation(self, db):
        checker = QualityChecker(db)
        summary = checker.run_all_checks()
        assert "completeness" in summary
        assert summary["completeness"]["commitment_mm"] == 100.0

    def test_run_all_checks_returns_summary(self, db):
        checker = QualityChecker(db)
        summary = checker.run_all_checks()
        assert "total_records" in summary
        assert summary["total_records"] == 5
        assert "flags_created" in summary
        assert summary["flags_created"] > 0
