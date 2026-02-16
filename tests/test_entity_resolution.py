"""Tests for entity resolution module."""

import pytest
import tempfile
from pathlib import Path

from src.database import Database, generate_id
from src.entity_resolution import FundRegistry


@pytest.fixture
def db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    database = Database(db_path)
    database.migrate()
    yield database
    database.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def registry(db):
    """Create a FundRegistry with the test database."""
    return FundRegistry(db)


@pytest.fixture
def seeded_registry(db):
    """Create a registry with some known funds pre-loaded."""
    # Create pension fund record (needed for foreign key on fund_aliases)
    db.upsert_pension_fund(id="calpers", name="CalPERS", state="CA")

    # Seed with known funds
    db.upsert_fund(
        id="fund-bcp7",
        fund_name="Blackstone Capital Partners VII",
        fund_name_raw="Blackstone Capital Partners VII, L.P.",
        general_partner="Blackstone Group",
        general_partner_normalized="Blackstone Group",
        vintage_year=2015,
        asset_class="Private Equity",
    )
    db.upsert_fund(
        id="fund-kkr12",
        fund_name="KKR Americas XII Fund",
        fund_name_raw="KKR Americas XII Fund, L.P.",
        general_partner="KKR",
        general_partner_normalized="KKR",
        vintage_year=2017,
        asset_class="Private Equity",
    )
    db.upsert_fund(
        id="fund-apollo9",
        fund_name="Apollo Investment Fund IX",
        fund_name_raw="Apollo Investment Fund IX, L.P.",
        general_partner="Apollo Global Management",
        general_partner_normalized="Apollo Global Management",
        vintage_year=2018,
        asset_class="Private Equity",
    )
    # Add an alias
    db.add_fund_alias("fund-bcp7", "BCP VII", source_pension_fund_id="calpers")

    return FundRegistry(db)


class TestFundRegistryBasic:
    """Basic functionality tests."""

    def test_empty_registry_creates_new(self, registry):
        fund_id, match_type = registry.resolve("Some New Fund")
        assert match_type == "new"
        assert fund_id is not None

    def test_resolve_twice_same_name_returns_same_id(self, registry):
        id1, type1 = registry.resolve("Test Fund Alpha, L.P.")
        id2, type2 = registry.resolve("Test Fund Alpha, L.P.")
        # Second time should be exact match (normalized names match)
        assert id1 == id2

    def test_stats(self, registry):
        registry.resolve("Fund A")
        registry.resolve("Fund B")
        stats = registry.get_stats()
        assert stats["total_funds"] == 2


class TestExactMatch:
    """Tests for exact name matching."""

    def test_exact_canonical_match(self, seeded_registry):
        fund_id, match_type = seeded_registry.resolve(
            "Blackstone Capital Partners VII"
        )
        assert match_type == "exact"
        assert fund_id == "fund-bcp7"

    def test_exact_match_ignores_lp_suffix(self, seeded_registry):
        """LP suffix is stripped during normalization, so should still match."""
        fund_id, match_type = seeded_registry.resolve(
            "Blackstone Capital Partners VII, L.P."
        )
        assert match_type == "exact"
        assert fund_id == "fund-bcp7"


class TestAliasMatch:
    """Tests for alias-based matching."""

    def test_alias_match(self, seeded_registry):
        fund_id, match_type = seeded_registry.resolve("BCP VII")
        assert match_type == "alias"
        assert fund_id == "fund-bcp7"


class TestFuzzyMatch:
    """Tests for fuzzy matching with secondary signals."""

    def test_fuzzy_match_with_vintage(self, seeded_registry):
        """Slightly different name + same vintage year should fuzzy match."""
        fund_id, match_type = seeded_registry.resolve(
            "KKR Americas Fund XII",
            general_partner="KKR",
            vintage_year=2017,
        )
        assert match_type == "fuzzy"
        assert fund_id == "fund-kkr12"

    def test_fuzzy_match_with_gp_and_vintage(self, seeded_registry):
        """GP match + vintage match should help fuzzy matching."""
        # Use a name that's similar but not identical after normalization
        fund_id, match_type = seeded_registry.resolve(
            "Apollo Investment Fund No. IX",
            general_partner="Apollo Global Management",
            vintage_year=2018,
        )
        assert match_type == "fuzzy"
        assert fund_id == "fund-apollo9"

    def test_no_fuzzy_match_without_secondary(self, seeded_registry):
        """Name similarity alone shouldn't be enough for fuzzy match."""
        fund_id, match_type = seeded_registry.resolve(
            "Blackstone Capital Partners VIII"  # Different fund number
        )
        # Without GP or vintage, this should be a new fund
        assert match_type == "new"
        assert fund_id != "fund-bcp7"

    def test_different_funds_not_matched(self, seeded_registry):
        """Two genuinely different funds with somewhat similar names should NOT match."""
        id1, type1 = seeded_registry.resolve(
            "Warburg Pincus Private Equity XII, L.P.",
            vintage_year=2019,
        )
        id2, type2 = seeded_registry.resolve(
            "Warburg Pincus Private Equity XI, L.P.",
            vintage_year=2017,
        )
        assert id1 != id2, "Different funds with different vintages should not match"

    def test_fuzzy_match_creates_alias(self, seeded_registry, db):
        """Fuzzy match should create an alias for future exact matching."""
        # First resolve creates fuzzy match
        fund_id, match_type = seeded_registry.resolve(
            "KKR Americas Fund XII",
            general_partner="KKR",
            vintage_year=2017,
        )
        assert match_type == "fuzzy"

        # Second resolve should be alias match
        fund_id2, match_type2 = seeded_registry.resolve("KKR Americas Fund XII")
        assert match_type2 == "alias"
        assert fund_id2 == fund_id


class TestNewFundCreation:
    """Tests for new fund creation."""

    def test_new_fund_stored_in_db(self, registry, db):
        fund_id, match_type = registry.resolve(
            "Brand New Fund VII, L.P.",
            general_partner="New GP LLC",
            vintage_year=2023,
        )
        assert match_type == "new"

        # Verify it's in the database
        fund = db.get_fund(fund_id)
        assert fund is not None
        assert fund["fund_name_raw"] == "Brand New Fund VII, L.P."
        assert fund["general_partner"] == "New GP LLC"
        assert fund["vintage_year"] == 2023
