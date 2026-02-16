"""Entity resolution for matching fund names across pension systems.

Uses a registry-based approach:
1. Exact match on canonical name
2. Exact match on known alias
3. Fuzzy match with secondary confirmation (GP, vintage year)
4. Create new fund if no match found

All resolution decisions are logged for auditability.
"""

import logging
from typing import Optional

from rapidfuzz import fuzz

from src.database import Database, generate_id
from src.utils.normalization import (
    classify_fund_strategy,
    extract_fund_number,
    normalize_fund_name,
    normalize_gp_name,
)

logger = logging.getLogger(__name__)


class FundRegistry:
    """Registry for resolving fund names across pension systems.

    Maintains a master list of known funds and aliases. When a new name
    is encountered, attempts matching in order:
    1. Exact match on canonical name
    2. Exact match on known alias
    3. Fuzzy match (requires secondary confirmation)
    4. Create new fund entry
    """

    def __init__(self, db: Database):
        self.db = db
        self._load_registry()

    def _load_registry(self):
        """Load all funds and aliases from the database into memory."""
        self._funds = {}  # id -> fund dict
        self._name_to_id = {}  # normalized canonical name -> fund id
        self._alias_to_id = {}  # alias text -> fund id

        for fund in self.db.list_funds():
            self._funds[fund["id"]] = fund
            normalized = normalize_fund_name(fund["fund_name"]).lower()
            self._name_to_id[normalized] = fund["id"]

        for alias in self.db.get_fund_aliases():
            self._alias_to_id[alias["alias"].lower()] = alias["fund_id"]

    def resolve(
        self,
        fund_name_raw: str,
        general_partner: Optional[str] = None,
        vintage_year: Optional[int] = None,
        source_pension_fund_id: Optional[str] = None,
    ) -> tuple[str, str]:
        """Resolve a fund name to a fund_id and match type.

        Args:
            fund_name_raw: The fund name exactly as it appears in the source.
            general_partner: GP name if available.
            vintage_year: Vintage year if available.
            source_pension_fund_id: Which pension fund this came from.

        Returns:
            Tuple of (fund_id, match_type) where match_type is one of:
            'exact', 'alias', 'fuzzy', 'new'
        """
        normalized = normalize_fund_name(fund_name_raw).lower()

        # 1. Exact match on canonical name
        if normalized in self._name_to_id:
            fund_id = self._name_to_id[normalized]
            logger.debug(f"Exact match: '{fund_name_raw}' -> {fund_id}")
            return fund_id, "exact"

        # 2. Exact match on alias
        raw_lower = fund_name_raw.strip().lower()
        if raw_lower in self._alias_to_id:
            fund_id = self._alias_to_id[raw_lower]
            logger.debug(f"Alias match: '{fund_name_raw}' -> {fund_id}")
            return fund_id, "alias"

        if normalized in self._alias_to_id:
            fund_id = self._alias_to_id[normalized]
            logger.debug(f"Alias match (normalized): '{fund_name_raw}' -> {fund_id}")
            return fund_id, "alias"

        # 3. Fuzzy match — requires at least TWO of: name sim > 0.85, GP match, vintage match
        best_match = self._fuzzy_match(
            fund_name_raw, normalized, general_partner, vintage_year
        )
        if best_match:
            fund_id, score = best_match
            # Add as alias for future lookups
            self.db.add_fund_alias(fund_id, fund_name_raw, source_pension_fund_id)
            self._alias_to_id[raw_lower] = fund_id
            self._alias_to_id[normalized] = fund_id
            logger.info(
                f"Fuzzy match: '{fund_name_raw}' -> {fund_id} "
                f"(score={score:.3f})"
            )
            return fund_id, "fuzzy"

        # 4. No match — create new fund
        fund_id = self._create_new_fund(
            fund_name_raw, general_partner, vintage_year, source_pension_fund_id
        )
        logger.info(f"New fund: '{fund_name_raw}' -> {fund_id}")
        return fund_id, "new"

    @staticmethod
    def _distinctive_tokens(normalized_name: str) -> set[str]:
        """Extract distinctive tokens from a fund name, stripping generic terms.

        Generic words like 'fund', 'capital', 'partners', etc. appear in nearly
        every fund name. What distinguishes funds is the GP/brand name.
        Returns the set of non-generic tokens for overlap comparison.
        """
        _GENERIC = {
            'fund', 'capital', 'partners', 'partner', 'investment', 'investments',
            'equity', 'ventures', 'venture', 'credit', 'group', 'management',
            'global', 'international', 'opportunities', 'special', 'situations',
            'growth', 'buyout', 'real', 'estate', 'infrastructure', 'the',
            'of', 'and', 'new', 'north', 'south', 'east', 'west',
            'i', 'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x',
            'xi', 'xii', 'xiii', 'xiv', 'xv', 'xvi', 'xvii', 'xviii',
            'a', 'b', 'c', 'd', 'e', 'lp', 'llc', 'ltd', 'inc', 'co',
            'no', 'no.', 'series', 'coinvestment', 'co-investment',
            'scsp', 'scsp', 'te', 'us', 'u.s.', 'europe', 'asia',
            'america', 'americas', 'latin', 'pacific',
        }
        tokens = set(normalized_name.lower().split())
        return tokens - _GENERIC

    def _fuzzy_match(
        self,
        fund_name_raw: str,
        normalized: str,
        general_partner: Optional[str],
        vintage_year: Optional[int],
    ) -> Optional[tuple[str, float]]:
        """Attempt fuzzy matching against all known funds.

        Requires at least TWO of:
        - Name similarity > 85%
        - GP name match
        - Vintage year match

        Additional safeguards:
        - Fund numbers (Roman numerals) must match exactly if both names have one
        - Both token_sort_ratio and standard ratio are checked to avoid false
          positives from token reordering of generic words

        Returns:
            Tuple of (fund_id, similarity_score) or None.
        """
        gp_normalized = normalize_gp_name(general_partner).lower() if general_partner else None
        input_fund_num = extract_fund_number(fund_name_raw)
        input_distinctive = self._distinctive_tokens(normalized)

        best_id = None
        best_score = 0.0

        for fund_id, fund in self._funds.items():
            signals = 0
            name_score = 0.0

            # Hard reject: if both names have a fund number and they differ
            canonical_fund_num = extract_fund_number(fund["fund_name"])
            if input_fund_num and canonical_fund_num:
                if input_fund_num != canonical_fund_num:
                    continue

            # Signal 1: Name similarity
            canonical_normalized = normalize_fund_name(fund["fund_name"]).lower()
            # Use token_sort_ratio for handling word reordering
            token_score = fuzz.token_sort_ratio(normalized, canonical_normalized) / 100.0
            # Also check standard ratio — if it's very low while token_sort is high,
            # this indicates the names share common words (Capital, Partners, Fund)
            # but are structurally different (different GP names)
            standard_score = fuzz.ratio(normalized, canonical_normalized) / 100.0
            if standard_score < 0.65:
                continue  # names are too structurally different regardless of token overlap

            # Check distinctive token overlap — reject if GP names are clearly different
            canonical_distinctive = self._distinctive_tokens(canonical_normalized)
            if input_distinctive and canonical_distinctive:
                overlap = input_distinctive & canonical_distinctive
                union = input_distinctive | canonical_distinctive
                if union and len(overlap) / len(union) < 0.3:
                    continue  # distinctive parts of names don't overlap enough

            # Check for strategy-distinguishing keywords — if one name has a
            # strategy word the other doesn't, they're likely different vehicles
            _STRATEGY_WORDS = {'credit', 'asia', 'europe', 'latin', 'real', 'infrastructure'}
            input_strategies = set(normalized.split()) & _STRATEGY_WORDS
            canon_strategies = set(canonical_normalized.split()) & _STRATEGY_WORDS
            if input_strategies != canon_strategies:
                continue  # different strategy/geography = different vehicle

            name_score = token_score
            if name_score > 0.85:
                signals += 1

            # Signal 2: GP match
            if gp_normalized and fund.get("general_partner_normalized"):
                gp_score = fuzz.ratio(
                    gp_normalized,
                    fund["general_partner_normalized"].lower()
                ) / 100.0
                if gp_score > 0.85:
                    signals += 1

            # Signal 3: Vintage year match
            if vintage_year and fund.get("vintage_year"):
                if vintage_year == fund["vintage_year"]:
                    signals += 1

            # Need at least 2 signals, and name score must be > 0.75 at minimum
            if signals >= 2 and name_score > 0.75 and name_score > best_score:
                best_score = name_score
                best_id = fund_id

        if best_id:
            return best_id, best_score
        return None

    def _create_new_fund(
        self,
        fund_name_raw: str,
        general_partner: Optional[str],
        vintage_year: Optional[int],
        source_pension_fund_id: Optional[str],
    ) -> str:
        """Create a new fund entry in the database and registry."""
        fund_id = generate_id()
        canonical_name = normalize_fund_name(fund_name_raw)
        gp_normalized = normalize_gp_name(general_partner) if general_partner else None

        asset_class, sub_strategy = classify_fund_strategy(fund_name_raw)

        self.db.upsert_fund(
            id=fund_id,
            fund_name=canonical_name if canonical_name else fund_name_raw,
            fund_name_raw=fund_name_raw,
            general_partner=general_partner,
            general_partner_normalized=gp_normalized,
            vintage_year=vintage_year,
            asset_class=asset_class,
            sub_strategy=sub_strategy,
        )

        # Update in-memory registry
        self._funds[fund_id] = {
            "id": fund_id,
            "fund_name": canonical_name if canonical_name else fund_name_raw,
            "fund_name_raw": fund_name_raw,
            "general_partner": general_partner,
            "general_partner_normalized": gp_normalized,
            "vintage_year": vintage_year,
            "asset_class": asset_class,
            "sub_strategy": sub_strategy,
        }
        normalized_lower = (canonical_name if canonical_name else fund_name_raw).lower()
        self._name_to_id[normalized_lower] = fund_id

        return fund_id

    def get_stats(self) -> dict:
        """Return statistics about the registry."""
        return {
            "total_funds": len(self._funds),
            "total_aliases": len(self._alias_to_id),
        }
