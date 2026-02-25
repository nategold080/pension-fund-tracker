"""Base adapter class for pension fund data extraction.

Every pension fund gets its own adapter that implements this interface.
Adding a new fund means writing a new adapter, not modifying core pipeline code.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class PensionFundAdapter(ABC):
    """Abstract base class for pension fund data adapters.

    Each adapter knows how to:
    - Find its fund's data source
    - Download/fetch the raw data
    - Parse the raw data into standardized commitment records
    """

    # Subclasses must set these
    pension_fund_id: str = ""
    pension_fund_name: str = ""
    state: str = ""
    source_url: str = ""

    # Optional metadata — subclasses should override
    full_name: str = ""
    total_aum_mm: Optional[float] = None
    data_source_type: str = ""        # "html", "pdf", "csv", "api"
    disclosure_quality: str = ""       # "excellent", "good", "limited", "none"

    @abstractmethod
    def fetch_source(self) -> bytes | str:
        """Download the source data.

        Returns:
            Raw content (bytes for binary files, str for HTML/CSV).
        """

    @abstractmethod
    def parse(self, raw_data) -> list[dict]:
        """Parse raw data into a list of commitment dicts with standardized field names.

        Each dict must contain these keys (values may be None where data is unavailable):
            - fund_name_raw: str (required) — the fund name exactly as it appears in the source
            - general_partner: str | None — GP name as it appears in the source
            - vintage_year: int | None
            - asset_class: str | None — e.g., "Private Equity", "Real Estate"
            - sub_strategy: str | None — e.g., "Buyout", "Venture Capital"
            - commitment_mm: float | None — commitment amount in millions
            - capital_called_mm: float | None — capital called/contributed in millions
            - capital_distributed_mm: float | None — distributions in millions
            - remaining_value_mm: float | None — remaining/market value in millions
            - net_irr: float | None — as decimal (0.15 for 15%)
            - net_multiple: float | None — TVPI/net multiple
            - dpi: float | None — distributions to paid-in
            - as_of_date: str | None — ISO date (YYYY-MM-DD)
            - source_url: str (required) — URL of the data source
            - source_document: str | None — name of the document/page
            - extraction_method: str (required) — e.g., "deterministic_html", "deterministic_csv"
            - extraction_confidence: float (required) — 1.0 for deterministic, lower for LLM

        Returns:
            List of commitment dicts.
        """

    def extract(self) -> list[dict]:
        """Full extraction pipeline: fetch source data, then parse it.

        Returns:
            List of parsed commitment dicts.
        """
        logger.info(f"Starting extraction for {self.pension_fund_name}")
        raw_data = self.fetch_source()
        logger.info(f"Fetched source data for {self.pension_fund_name} "
                     f"({len(raw_data) if raw_data else 0} bytes/chars)")
        records = self.parse(raw_data)
        logger.info(f"Parsed {len(records)} records from {self.pension_fund_name}")
        return records

    def get_source_hash(self, raw_data) -> str:
        """Compute SHA256 hash of source data for change detection.

        Args:
            raw_data: The raw content (bytes or str).

        Returns:
            Hex digest of the SHA256 hash.
        """
        if isinstance(raw_data, str):
            data = raw_data.encode("utf-8")
        else:
            data = raw_data
        return hashlib.sha256(data).hexdigest()

    def extract_consulting_data(self) -> list[dict]:
        """Extract consulting firm engagement data from this pension fund's sources.

        Override in subclasses that have consulting data in their source documents.
        Each dict should contain:
            - consulting_firm_name: str (required) — firm name as it appears in the source
            - role: str (required) — e.g., "general_investment_consultant"
            - mandate_scope: str | None
            - start_date: str | None — ISO date
            - end_date: str | None — ISO date
            - is_current: bool | None
            - annual_fee_usd: float | None
            - fee_basis: str | None
            - contract_term_years: float | None
            - source_url: str (required)
            - source_document: str | None
            - source_page: int | None
            - extraction_method: str (required)
            - extraction_confidence: float (required)

        Returns:
            List of consulting engagement dicts. Empty list by default.
        """
        return []

    def get_pension_fund_info(self) -> dict:
        """Return pension fund metadata for database registration."""
        return {
            "id": self.pension_fund_id,
            "name": self.pension_fund_name,
            "full_name": self.full_name or None,
            "state": self.state,
            "total_aum_mm": self.total_aum_mm,
            "website_url": self.source_url,
            "data_source_type": self.data_source_type or None,
            "disclosure_quality": self.disclosure_quality or None,
        }
