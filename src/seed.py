"""Seed data loader for consulting firms and engagements.

Loads curated consulting firm data from YAML seed files into the database.
All operations are idempotent — running twice produces no duplicates.
"""

import logging
from pathlib import Path

import yaml

from src.database import Database
from src.utils.normalization import normalize_consulting_firm_name

logger = logging.getLogger(__name__)

DEFAULT_SEED_PATH = Path("data/seed/consulting_firms.yaml")


def load_consulting_seed(db: Database, seed_path: Path | None = None) -> dict:
    """Load consulting firms and engagements from YAML seed file.

    Args:
        db: Database instance (must already be migrated).
        seed_path: Path to YAML seed file. Defaults to data/seed/consulting_firms.yaml.

    Returns:
        Dict with counts: firms_loaded, aliases_loaded, engagements_loaded.
    """
    seed_path = seed_path or DEFAULT_SEED_PATH
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")

    with open(seed_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    firms_loaded = 0
    aliases_loaded = 0
    engagements_loaded = 0

    # Ensure referenced pension funds exist (minimal stubs)
    for pf in data.get("pension_funds", []):
        db.upsert_pension_fund(
            id=pf["id"],
            name=pf["name"],
            full_name=pf.get("full_name"),
            state=pf.get("state"),
        )
        logger.debug(f"Ensured pension fund stub: {pf['name']}")

    # Load consulting firms
    for firm in data.get("consulting_firms", []):
        firm_id = firm["id"]
        name = firm["name"]
        name_normalized = normalize_consulting_firm_name(name)

        db.upsert_consulting_firm(
            id=firm_id,
            name=name,
            name_normalized=name_normalized,
            firm_type=firm.get("firm_type"),
            headquarters=firm.get("headquarters"),
            website_url=firm.get("website_url"),
            notes=firm.get("notes"),
        )
        firms_loaded += 1
        logger.debug(f"Loaded consulting firm: {name}")

        # Load aliases
        for alias in firm.get("aliases", []):
            db.add_consulting_firm_alias(firm_id, alias)
            aliases_loaded += 1

    # Load engagements
    for eng in data.get("engagements", []):
        db.upsert_consulting_engagement(
            consulting_firm_id=eng["consulting_firm_id"],
            pension_fund_id=eng["pension_fund_id"],
            role=eng["role"],
            mandate_scope=eng.get("mandate_scope"),
            start_date=eng.get("start_date"),
            end_date=eng.get("end_date"),
            is_current=eng.get("is_current"),
            annual_fee_usd=eng.get("annual_fee_usd"),
            fee_basis=eng.get("fee_basis"),
            contract_term_years=eng.get("contract_term_years"),
            source_url=eng.get("source_url"),
            source_document=eng.get("source_document"),
            source_page=eng.get("source_page"),
            extraction_method=eng.get("extraction_method"),
            extraction_confidence=eng.get("extraction_confidence"),
        )
        engagements_loaded += 1
        logger.debug(
            f"Loaded engagement: {eng['consulting_firm_id']} -> {eng['pension_fund_id']}"
        )

    logger.info(
        f"Seed complete: {firms_loaded} firms, {aliases_loaded} aliases, "
        f"{engagements_loaded} engagements"
    )

    return {
        "firms_loaded": firms_loaded,
        "aliases_loaded": aliases_loaded,
        "engagements_loaded": engagements_loaded,
    }
