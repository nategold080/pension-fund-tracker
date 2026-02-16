"""New York State Common Retirement Fund Private Equity adapter.

Data source: NY State Comptroller publishes an annual Asset Listing PDF.
URL: https://www.osc.ny.gov/files/retirement/resources/pdf/asset-listing-2024.pdf

The Private Equity Investments section (pages 170-175 in the 2024 edition)
contains: Security Description (fund name), Date Committed, Committed,
Contributed, Cumulative Distributions, Fair Value, Total Value.
All amounts in raw dollars.

Parsing strategy: Word-level extraction with column assignment by x-position.
"""

import io
import logging
import re
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

from src.adapters.base import PensionFundAdapter
from src.utils.normalization import parse_dollar_amount

logger = logging.getLogger(__name__)

NY_COMMON_PDF_URL = (
    "https://www.osc.ny.gov/files/retirement/resources/pdf/asset-listing-2024.pdf"
)
NY_COMMON_PAGE_URL = (
    "https://www.osc.ny.gov/common-retirement-fund/resources/"
    "financial-reporting-and-asset-allocation"
)
CACHE_DIR = Path("data/cache/ny_common")

# Column x-position boundaries (from word-level inspection of the PDF):
#   Fund Name:     x < 245
#   Date:          245 <= x < 280
#   Committed:     280 <= x < 340
#   Contributed:   340 <= x < 400
#   Distributions: 400 <= x < 458
#   Fair Value:    458 <= x < 515
#   Total Value:   x >= 515
NY_COL_BOUNDARIES = [245, 280, 340, 400, 458, 515]


class NYCommonAdapter(PensionFundAdapter):
    """Adapter for NY State Common Retirement Fund Private Equity data."""

    pension_fund_id = "ny_common"
    pension_fund_name = "New York State Common Retirement Fund"
    state = "NY"
    full_name = "New York State Common Retirement Fund"
    total_aum_mm = 268000.0
    data_source_type = "pdf"
    disclosure_quality = "good"
    source_url = NY_COMMON_PDF_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "asset_listing_2024.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the NY Common asset listing PDF."""
        if self.use_cache and self._cache_path.exists():
            logger.info(f"Loading NY Common data from cache: {self._cache_path}")
            return self._cache_path.read_bytes()

        logger.info(f"Fetching NY Common data from {NY_COMMON_PDF_URL}")
        resp = requests.get(
            NY_COMMON_PDF_URL,
            timeout=120,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
        )
        resp.raise_for_status()
        data = resp.content

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_bytes(data)
        logger.info(f"Cached NY Common data to {self._cache_path}")

        return data

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse NY Common asset listing PDF for Private Equity Investments."""
        records = []

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            in_pe_section = False

            for page in pdf.pages:
                text = page.extract_text() or ""

                # Detect PE section start/end
                if "PRIVATE EQUITY INVESTMENTS" in text:
                    in_pe_section = True
                if in_pe_section and "FUND OF FUNDS" in text.upper():
                    # Stop at Fund of Funds section
                    break
                if not in_pe_section:
                    continue

                page_records = self._parse_page_by_words(page)
                records.extend(page_records)

        logger.info(f"Parsed {len(records)} NY Common PE commitment records")
        return records

    def _parse_page_by_words(self, page) -> list[dict]:
        """Parse a single PDF page by extracting words and grouping by row."""
        words = page.extract_words()
        if not words:
            return []

        # Group words by vertical position (row)
        row_tolerance = 3
        rows_dict: dict[float, list] = {}
        for w in words:
            top = round(w["top"] / row_tolerance) * row_tolerance
            if top not in rows_dict:
                rows_dict[top] = []
            rows_dict[top].append(w)

        records = []
        for _top, row_words in sorted(rows_dict.items()):
            row_words.sort(key=lambda w: w["x0"])

            # Assign words to 7 columns
            columns: list[list[str]] = [[] for _ in range(7)]
            for w in row_words:
                x = w["x0"]
                col_idx = 0
                for i, boundary in enumerate(NY_COL_BOUNDARIES):
                    if x >= boundary:
                        col_idx = i + 1
                columns[col_idx].append(w["text"])

            col_texts = [" ".join(wl).strip() for wl in columns]

            fund_name = col_texts[0]
            date_text = col_texts[1]
            committed_text = col_texts[2]
            contributed_text = col_texts[3]
            distributed_text = col_texts[4]
            fair_value_text = col_texts[5]
            total_value_text = col_texts[6]

            # Skip non-data rows: must have a date in MM/DD/YY format
            if not date_text or not re.match(r"^\d{2}/\d{2}/\d{2}$", date_text):
                continue

            if not fund_name:
                continue

            # Skip total/summary rows
            if any(
                x in fund_name.lower()
                for x in ["total", "subtotal", "summary", "private equity investments"]
            ):
                continue

            # Parse the commitment date to extract vintage year
            vintage_year = self._extract_vintage_year(date_text)

            # Parse dollar amounts (raw dollars -> millions)
            committed_text = self._rejoin_number(committed_text)
            contributed_text = self._rejoin_number(contributed_text)
            distributed_text = self._rejoin_number(distributed_text)
            fair_value_text = self._rejoin_number(fair_value_text)
            total_value_text = self._rejoin_number(total_value_text)

            commitment_mm = parse_dollar_amount(committed_text)
            capital_called_mm = parse_dollar_amount(contributed_text)
            capital_distributed_mm = parse_dollar_amount(distributed_text)
            remaining_value_mm = parse_dollar_amount(fair_value_text)

            # Compute net multiple from contributed and total value
            net_multiple = None
            if capital_called_mm and capital_called_mm > 0:
                total_value = parse_dollar_amount(total_value_text)
                if total_value and total_value > 0:
                    net_multiple = round(total_value / capital_called_mm, 4)

            records.append(
                {
                    "fund_name_raw": fund_name,
                    "general_partner": None,
                    "vintage_year": vintage_year,
                    "asset_class": "Private Equity",
                    "sub_strategy": None,
                    "commitment_mm": commitment_mm,
                    "capital_called_mm": capital_called_mm,
                    "capital_distributed_mm": capital_distributed_mm,
                    "remaining_value_mm": remaining_value_mm,
                    "net_irr": None,  # Not provided in this source
                    "net_multiple": net_multiple,
                    "dpi": None,
                    "as_of_date": "2024-03-31",
                    "source_url": NY_COMMON_PDF_URL,
                    "source_document": "NY Common Retirement Fund Asset Listing 2024",
                    "extraction_method": "deterministic_pdf",
                    "extraction_confidence": 0.95,
                }
            )

        return records

    @staticmethod
    def _extract_vintage_year(date_text: str) -> Optional[int]:
        """Extract vintage year from MM/DD/YY date format.

        Converts 2-digit year: 00-30 -> 2000-2030, 31-99 -> 1931-1999.
        """
        match = re.match(r"(\d{2})/(\d{2})/(\d{2})", date_text)
        if not match:
            return None
        yy = int(match.group(3))
        year = 2000 + yy if yy <= 30 else 1900 + yy
        return year

    @staticmethod
    def _rejoin_number(text: str) -> str:
        """Rejoin number parts split by spaces: '9 1,515,215' -> '91,515,215'."""
        if not text:
            return text
        return re.sub(r"(\d)\s+(\d)", r"\1\2", text)
