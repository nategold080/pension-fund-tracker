"""Tests for the analysis module."""

import csv
import pytest
from pathlib import Path

from src.database import Database
from src import analysis


@pytest.fixture
def db(tmp_path):
    """Create a database with test data for analysis queries."""
    db = Database(tmp_path / "test_analysis.db")
    db.migrate()

    # Create pension funds
    db.upsert_pension_fund(id="pf1", name="PensionA", state="CA",
                           full_name="Pension Fund A", total_aum_mm=100000.0)
    db.upsert_pension_fund(id="pf2", name="PensionB", state="WA",
                           full_name="Pension Fund B", total_aum_mm=80000.0)
    db.upsert_pension_fund(id="pf3", name="PensionC", state="OR",
                           full_name="Pension Fund C", total_aum_mm=60000.0)

    # Create funds — mix of vintage years, strategies, and fund numbers
    db.upsert_fund(id="f1", fund_name="KKR North America XII",
                   fund_name_raw="KKR North America Fund XII, L.P.",
                   general_partner="KKR", vintage_year=2017,
                   asset_class="Private Equity", sub_strategy="Buyout")
    db.upsert_fund(id="f2", fund_name="Sequoia Capital Fund I",
                   fund_name_raw="Sequoia Capital Fund I, L.P.",
                   general_partner="Sequoia", vintage_year=2021,
                   asset_class="Private Equity", sub_strategy="Venture Capital")
    db.upsert_fund(id="f3", fund_name="Acme Growth Partners II",
                   fund_name_raw="Acme Growth Partners II",
                   general_partner="Acme", vintage_year=2022,
                   asset_class="Private Equity", sub_strategy="Growth Equity")
    db.upsert_fund(id="f4", fund_name="Warburg Pincus XIV",
                   fund_name_raw="Warburg Pincus Private Equity XIV, L.P.",
                   general_partner="Warburg Pincus", vintage_year=2019,
                   asset_class="Private Equity", sub_strategy="Buyout")

    # f1 (KKR) committed by all 3 pension funds — cross-linked
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=200.0,
        vintage_year=2017, net_irr=0.18, net_multiple=1.8,
        capital_called_mm=180.0, capital_distributed_mm=100.0,
        remaining_value_mm=224.0, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf2", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_pdf", commitment_mm=150.0,
        vintage_year=2017, net_irr=0.17, net_multiple=1.75,
        capital_called_mm=140.0, capital_distributed_mm=80.0,
        remaining_value_mm=165.0, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf3", fund_id="f1", source_url="https://test.com",
        extraction_method="deterministic_pdf", commitment_mm=100.0,
        vintage_year=2017, net_irr=0.16, net_multiple=1.7,
        capital_called_mm=95.0, capital_distributed_mm=60.0,
        remaining_value_mm=101.5, as_of_date="2025-06-30",
    )

    # f2 (Sequoia Fund I, vintage 2021) — emerging manager, 2 pensions
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f2", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=50.0,
        vintage_year=2021, net_irr=0.35, net_multiple=2.5,
        capital_called_mm=40.0, capital_distributed_mm=10.0,
        remaining_value_mm=90.0, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf2", fund_id="f2", source_url="https://test.com",
        extraction_method="deterministic_pdf", commitment_mm=30.0,
        vintage_year=2021, net_irr=0.33, net_multiple=2.4,
        capital_called_mm=25.0, capital_distributed_mm=5.0,
        remaining_value_mm=55.0, as_of_date="2025-06-30",
    )

    # f3 (Acme Growth Partners II, vintage 2022) — emerging manager, 1 pension
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f3", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=25.0,
        vintage_year=2022, net_irr=None, net_multiple=None,
        as_of_date="2025-06-30",
    )

    # f4 (Warburg, vintage 2019) — 2 pensions, for PE performance query
    db.upsert_commitment(
        pension_fund_id="pf1", fund_id="f4", source_url="https://test.com",
        extraction_method="deterministic_html", commitment_mm=175.0,
        vintage_year=2019, net_irr=0.22, net_multiple=1.9,
        capital_called_mm=160.0, capital_distributed_mm=90.0,
        remaining_value_mm=214.0, as_of_date="2025-06-30",
    )
    db.upsert_commitment(
        pension_fund_id="pf3", fund_id="f4", source_url="https://test.com",
        extraction_method="deterministic_pdf", commitment_mm=80.0,
        vintage_year=2019, net_irr=0.21, net_multiple=1.85,
        capital_called_mm=75.0, capital_distributed_mm=40.0,
        remaining_value_mm=98.75, as_of_date="2025-06-30",
    )

    yield db
    db.close()


@pytest.fixture
def empty_db(tmp_path):
    """Create an empty database (no data)."""
    db = Database(tmp_path / "test_analysis_empty.db")
    db.migrate()
    yield db
    db.close()


@pytest.fixture(autouse=True)
def use_tmp_demo_dir(tmp_path, monkeypatch):
    """Redirect DEMO_DIR to tmp_path so tests don't write to real data/."""
    demo_dir = tmp_path / "demo_exports"
    demo_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(analysis, "DEMO_DIR", demo_dir)


class TestAnalysisOutputFiles:
    """Test that analysis generates expected output files."""

    def test_run_all_generates_all_files(self, db, tmp_path):
        results = analysis.run_all(db)
        assert len(results) == 7

        for name, path in results.items():
            assert path.exists(), f"Output file missing for {name}: {path}"

    def test_csv_files_are_valid(self, db, tmp_path):
        results = analysis.run_all(db)
        csv_files = [p for n, p in results.items() if str(p).endswith(".csv")]
        assert len(csv_files) >= 5

        for path in csv_files:
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                assert len(header) >= 2, f"CSV {path.name} has too few columns"

    def test_dataset_readme_generated(self, db, tmp_path):
        results = analysis.run_all(db)
        readme_path = results["dataset_readme"]
        assert readme_path.exists()

        content = readme_path.read_text(encoding="utf-8")
        assert "Alternative Investment Commitment Dataset" in content
        assert "PensionA" in content
        assert "PensionB" in content


class TestAnalysisEmptyDatabase:
    """Test that analysis handles empty database gracefully."""

    def test_run_all_with_empty_db(self, empty_db, tmp_path):
        results = analysis.run_all(empty_db)
        # Should complete without error and produce files
        assert len(results) == 7
        for name, path in results.items():
            assert path.exists(), f"Output missing for {name} with empty DB"

    def test_csv_files_have_headers_only(self, empty_db, tmp_path):
        results = analysis.run_all(empty_db)
        # CSVs should have headers even with no data
        for name, path in results.items():
            if str(path).endswith(".csv"):
                with open(path, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    rows = list(reader)
                assert len(header) >= 2, f"{path.name} header too short"
                assert len(rows) == 0, f"{path.name} should have no data rows"


class TestAnalysisSummaryStats:
    """Test that summary statistics are reasonable."""

    def test_gp_penetration_includes_cross_linked_gps(self, db, tmp_path):
        path = analysis.generate_gp_penetration(db)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # KKR appears in 3 pension systems → should be in output
        gp_names = [r["General Partner"] for r in rows]
        assert any("KKR" in name for name in gp_names), \
            f"Expected KKR in GP penetration, got: {gp_names}"

    def test_pe_performance_includes_2015_2020_vintages(self, db, tmp_path):
        path = analysis.generate_buyout_performance(db)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # f1 (2017) and f4 (2019) should appear — they have IRR data
        assert len(rows) >= 2
        vintages = {r["Vintage Year"] for r in rows}
        assert "2017" in vintages or "2019" in vintages

    def test_emerging_manager_commitments(self, db, tmp_path):
        path = analysis.generate_emerging_manager_commitments(db)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Sequoia Capital Fund I (vintage 2021) should qualify as emerging
        fund_names = [r["Fund Name"] for r in rows]
        assert any("Sequoia" in n for n in fund_names), \
            f"Expected Sequoia Fund I in emerging managers, got: {fund_names}"

    def test_vc_commitments_filters_to_vc(self, db, tmp_path):
        path = analysis.generate_vc_commitments_by_pension(db)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Only Sequoia (sub_strategy=Venture Capital) should appear
        for row in rows:
            assert "Sequoia" in row["Fund Name"], \
                f"Non-VC fund in VC export: {row['Fund Name']}"

    def test_commitment_trends_groups_by_vintage(self, db, tmp_path):
        path = analysis.generate_commitment_trends(db)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) >= 1
        # Should have vintage years as a column
        assert "Vintage Year" in reader.fieldnames
