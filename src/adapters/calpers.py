"""CalPERS Private Equity Holdings adapter.

Data source: CalPERS publishes quarterly-updated PE fund performance data
as an HTML table on their website. The printer-friendly version provides
a clean, parseable table.

Columns available:
- Fund (name)
- Vintage Year
- Capital Committed (raw dollars)
- Cash In (capital called/contributed, raw dollars)
- Cash Out (distributions, raw dollars)
- Cash Out & Remaining Value (total value, raw dollars)
- Net IRR (percentage)
- Investment Multiple (mostly empty; footnote markers for N/M funds)

As-of date is stated on the page (typically lagged 2 quarters from GPs).
"""

import logging
import re
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

from src.adapters.base import PensionFundAdapter
from src.utils.normalization import (
    extract_as_of_date_from_text,
    parse_dollar_amount,
    parse_percentage,
    parse_vintage_year,
)

logger = logging.getLogger(__name__)

CALPERS_URL = (
    "https://www.calpers.ca.gov/investments/about-investment-office/"
    "investment-organization/pep-fund-performance-print"
)
CACHE_DIR = Path("data/cache/calpers")


class CalPERSAdapter(PensionFundAdapter):
    """Adapter for CalPERS Private Equity Program fund performance data."""

    pension_fund_id = "calpers"
    pension_fund_name = "CalPERS"
    state = "CA"
    full_name = "California Public Employees' Retirement System"
    total_aum_mm = 503000.0
    data_source_type = "html"
    disclosure_quality = "excellent"
    source_url = CALPERS_URL

    def __init__(self, use_cache: bool = False):
        """Initialize the adapter.

        Args:
            use_cache: If True, read from cached HTML instead of fetching live.
        """
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "pep_fund_performance_print.html"

    def fetch_source(self) -> str:
        """Fetch the CalPERS PE holdings page HTML.

        Returns:
            HTML content as string.
        """
        if self.use_cache and self._cache_path.exists():
            logger.info(f"Loading CalPERS data from cache: {self._cache_path}")
            return self._cache_path.read_text(encoding="utf-8")

        logger.info(f"Fetching CalPERS PE data from {CALPERS_URL}")
        resp = requests.get(CALPERS_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Cache for future use
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(html, encoding="utf-8")
        logger.info(f"Cached CalPERS data to {self._cache_path}")

        return html

    def _extract_as_of_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract the as-of date from the page content.

        CalPERS states the reporting date on the page, e.g.,
        "as of March 31, 2025".
        """
        result = extract_as_of_date_from_text(soup.get_text())
        if result is None:
            logger.warning("Could not extract as-of date from CalPERS page")
        return result

    def parse(self, raw_data: str) -> list[dict]:
        """Parse CalPERS HTML table into commitment records.

        Args:
            raw_data: HTML content of the PE holdings page.

        Returns:
            List of commitment dicts with standardized field names.
        """
        soup = BeautifulSoup(raw_data, "lxml")
        as_of_date = self._extract_as_of_date(soup)

        table = soup.find("table")
        if not table:
            logger.error("No table found in CalPERS HTML")
            return []

        rows = table.find_all("tr")
        if len(rows) < 3:
            logger.error(f"CalPERS table has only {len(rows)} rows, expected 400+")
            return []

        # Skip header rows (first two rows are headers)
        data_rows = rows[2:]
        records = []

        for row in data_rows:
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            # Extract raw text from each cell
            fund_name = cells[0].get_text(strip=True)
            vintage_raw = cells[1].get_text(strip=True)
            committed_raw = cells[2].get_text(strip=True)
            cash_in_raw = cells[3].get_text(strip=True)
            cash_out_raw = cells[4].get_text(strip=True)
            total_value_raw = cells[5].get_text(strip=True)
            irr_raw = cells[6].get_text(strip=True)

            # Clean the IRR — remove footnote markers like "N/M1"
            irr_clean = re.sub(r'(\d)\s*$', r'\1', irr_raw)
            if irr_clean.startswith("N/M"):
                irr_clean = None

            # Skip empty rows
            if not fund_name:
                continue

            # Parse values — CalPERS amounts are in raw dollars, not millions
            commitment_mm = parse_dollar_amount(committed_raw)
            capital_called_mm = parse_dollar_amount(cash_in_raw)
            capital_distributed_mm = parse_dollar_amount(cash_out_raw)

            # "Cash Out & Remaining Value" = distributions + remaining NAV
            # Remaining value = total_value - cash_out
            total_value_mm = parse_dollar_amount(total_value_raw)

            remaining_value_mm = None
            if total_value_mm is not None and capital_distributed_mm is not None:
                remaining_value_mm = total_value_mm - capital_distributed_mm

            # Compute net multiple = total_value / cash_in
            net_multiple = None
            if total_value_mm is not None and capital_called_mm is not None and capital_called_mm > 0:
                net_multiple = round(total_value_mm / capital_called_mm, 4)

            net_irr = parse_percentage(irr_clean) if irr_clean else None

            record = {
                "fund_name_raw": fund_name,
                "general_partner": None,  # CalPERS doesn't list GP separately
                "vintage_year": parse_vintage_year(vintage_raw),
                "asset_class": "Private Equity",
                "sub_strategy": None,  # Not provided in this table
                "commitment_mm": commitment_mm,
                "capital_called_mm": capital_called_mm,
                "capital_distributed_mm": capital_distributed_mm,
                "remaining_value_mm": remaining_value_mm,
                "net_irr": net_irr,
                "net_multiple": net_multiple,
                "dpi": None,  # Not directly provided
                "as_of_date": as_of_date,
                "source_url": CALPERS_URL,
                "source_document": "CalPERS PEP Fund Performance Review",
                "extraction_method": "deterministic_html",
                "extraction_confidence": 1.0,
            }
            records.append(record)

        logger.info(f"Parsed {len(records)} CalPERS PE commitment records")
        return records
