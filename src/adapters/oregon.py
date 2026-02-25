"""Oregon PERS (OPERF / Oregon State Treasury) Private Equity adapter.

Data source: Oregon Treasury publishes quarterly OPERF PE portfolio PDFs.
URL: https://www.oregon.gov/treasury/invested-for-oregon/pages/performance-holdings.aspx

Columns: Vintage Year, Partnership (fund name), Capital Commitment,
Total Capital Contributed, Total Capital Distributed, Fair Market Value,
Total Value Multiple, IRR.
All amounts in millions ($ prefix).

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
from src.utils.normalization import (
    parse_dollar_amount, parse_percentage, parse_multiple,
    extract_as_of_date_from_text,
)

logger = logging.getLogger(__name__)

OREGON_PDF_URL = (
    "https://www.oregon.gov/treasury/invested-for-oregon/Documents/"
    "Invested-for-OR-Performance-and-Holdings/2025/"
    "OPERF_Private_Equity_Portfolio_-_Quarter_3_2025.pdf"
)
OREGON_PAGE_URL = "https://www.oregon.gov/treasury/invested-for-oregon/pages/performance-holdings.aspx"
CACHE_DIR = Path("data/cache/oregon")

# Column x-position boundaries (from inspection)
# Vintage Year: 0-70, Fund Name: 70-245, Commitment: 245-305,
# Contributed: 305-365, Distributed: 365-420, Market Value: 420-465,
# Multiple: 465-510, IRR: 510+
OREGON_COL_BOUNDARIES = [70, 245, 305, 365, 420, 465, 510]


class OregonAdapter(PensionFundAdapter):
    """Adapter for Oregon PERS Private Equity Portfolio data."""

    pension_fund_id = "oregon"
    pension_fund_name = "Oregon PERS"
    state = "OR"
    full_name = "Oregon Public Employees Retirement System"
    total_aum_mm = 100000.0
    data_source_type = "pdf"
    disclosure_quality = "excellent"
    source_url = OREGON_PDF_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "pe_portfolio_q3_2025.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the Oregon PERS PE portfolio PDF."""
        if self.use_cache and self._cache_path.exists():
            logger.info(f"Loading Oregon data from cache: {self._cache_path}")
            return self._cache_path.read_bytes()

        logger.info(f"Fetching Oregon PE data from {OREGON_PDF_URL}")
        resp = requests.get(OREGON_PDF_URL, timeout=30)
        resp.raise_for_status()
        data = resp.content

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_bytes(data)
        logger.info(f"Cached Oregon data to {self._cache_path}")

        return data

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse Oregon PERS PE portfolio PDF."""
        records = []
        as_of_date = None

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            # Get as-of date
            first_text = pdf.pages[0].extract_text() or ""
            as_of_date = extract_as_of_date_from_text(first_text)

            for page in pdf.pages:
                page_records = self._parse_page_by_words(page, as_of_date)
                records.extend(page_records)

        logger.info(f"Parsed {len(records)} Oregon PERS PE commitment records")
        return records

    def _parse_page_by_words(self, page, as_of_date: Optional[str]) -> list[dict]:
        """Parse a page using word positions."""
        words = page.extract_words()
        if not words:
            return []

        # Group words by row
        row_tolerance = 3
        rows_dict = {}
        for w in words:
            top = round(w["top"] / row_tolerance) * row_tolerance
            if top not in rows_dict:
                rows_dict[top] = []
            rows_dict[top].append(w)

        records = []
        for top, row_words in sorted(rows_dict.items()):
            row_words.sort(key=lambda w: w["x0"])

            # Assign words to columns (8 columns)
            columns = [[] for _ in range(8)]
            for w in row_words:
                x = w["x0"]
                col_idx = 0
                for i, boundary in enumerate(OREGON_COL_BOUNDARIES):
                    if x >= boundary:
                        col_idx = i + 1
                columns[col_idx].append(w["text"])

            col_texts = [" ".join(wl).strip() for wl in columns]

            vintage_text = col_texts[0]
            fund_name = col_texts[1]
            commitment_text = col_texts[2]
            contributed_text = col_texts[3]
            distributed_text = col_texts[4]
            market_value_text = col_texts[5]
            multiple_text = col_texts[6]
            irr_text = col_texts[7]

            # Must have a vintage year (4-digit year) to be a data row
            if not vintage_text or not re.match(r'^(19|20)\d{2}$', vintage_text):
                continue

            if not fund_name:
                continue

            # Skip total/summary rows
            if any(x in fund_name.lower() for x in ["total", "subtotal", "summary"]):
                continue

            vintage_year = int(vintage_text)

            # Oregon amounts are in millions with $ prefix
            commitment_mm = self._parse_oregon_amount(commitment_text)
            capital_called_mm = self._parse_oregon_amount(contributed_text)
            capital_distributed_mm = self._parse_oregon_amount(distributed_text)
            remaining_value_mm = self._parse_oregon_amount(market_value_text)

            net_multiple = parse_multiple(multiple_text)

            # Parse IRR
            net_irr = None
            if irr_text and irr_text.lower() not in ("n.m.", "n.m", "n/m", ""):
                net_irr = parse_percentage(irr_text)

            records.append({
                "fund_name_raw": fund_name,
                "general_partner": None,
                "vintage_year": vintage_year,
                "asset_class": "Private Equity",
                "sub_strategy": None,
                "commitment_mm": commitment_mm,
                "capital_called_mm": capital_called_mm,
                "capital_distributed_mm": capital_distributed_mm,
                "remaining_value_mm": remaining_value_mm,
                "net_irr": net_irr,
                "net_multiple": net_multiple,
                "dpi": None,
                "as_of_date": as_of_date,
                "source_url": OREGON_PDF_URL,
                "source_document": "OPERF Private Equity Portfolio Q3 2025",
                "extraction_method": "deterministic_pdf",
                "extraction_confidence": 0.95,
            })

        return records

    @staticmethod
    def _parse_oregon_amount(text: str) -> Optional[float]:
        """Parse Oregon-format dollar amounts (already in millions with $ prefix).

        Examples: "$50.0", "$157.2", "$0.0", "-"
        """
        if not text or text.strip() in ("-", "—", ""):
            return None
        # Remove $ and commas, parse as float (already in millions)
        cleaned = text.replace("$", "").replace(",", "").strip()
        if not cleaned or cleaned in ("-", "—"):
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
