"""CalSTRS Private Equity Portfolio Performance adapter.

Data source: CalSTRS publishes a PDF with PE fund performance data.
URL: https://www.calstrs.com/private-equity-portfolio-performance-table

Columns: Description (fund name), VY (vintage year), Capital Committed,
Capital Contributed, Capital Distributed, Market Value, IRR
All amounts in raw dollars. IRR as percentage number.

Parsing strategy: Use pdfplumber word-level extraction to group words by
row (y-coordinate) and assign to columns by x-coordinate ranges, since
the text extraction joins fields without proper spacing.
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
    parse_dollar_amount, parse_percentage, rejoin_split_number,
    extract_as_of_date_from_text,
)

logger = logging.getLogger(__name__)

CALSTRS_URL = "https://www.calstrs.com/private-equity-portfolio-performance-table"
CACHE_DIR = Path("data/cache/calstrs")


class CalSTRSAdapter(PensionFundAdapter):
    """Adapter for CalSTRS Private Equity Portfolio Performance data."""

    pension_fund_id = "calstrs"
    pension_fund_name = "CalSTRS"
    state = "CA"
    full_name = "California State Teachers' Retirement System"
    total_aum_mm = 338000.0
    data_source_type = "pdf"
    disclosure_quality = "excellent"
    source_url = CALSTRS_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "pe_performance_table.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the CalSTRS PE performance PDF."""
        if self.use_cache and self._cache_path.exists():
            logger.info(f"Loading CalSTRS data from cache: {self._cache_path}")
            return self._cache_path.read_bytes()

        logger.info(f"Fetching CalSTRS PE data from {CALSTRS_URL}")
        resp = requests.get(CALSTRS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.content

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_bytes(data)
        logger.info(f"Cached CalSTRS data to {self._cache_path}")

        return data

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse CalSTRS PDF into commitment records using word-level extraction.

        Uses word x-coordinates to determine column assignments, avoiding
        issues with text extraction concatenating adjacent fields.
        """
        records = []
        as_of_date = None

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            # Extract as-of date from first page text
            first_text = pdf.pages[0].extract_text() or ""
            as_of_date = extract_as_of_date_from_text(first_text)

            for page in pdf.pages:
                page_records = self._parse_page_by_words(page, as_of_date)
                records.extend(page_records)

        logger.info(f"Parsed {len(records)} CalSTRS PE commitment records")
        return records

    def _parse_page_by_words(self, page, as_of_date: Optional[str]) -> list[dict]:
        """Parse a single PDF page by extracting words and grouping by row."""
        words = page.extract_words()
        if not words:
            return []

        # Group words by their vertical position (row)
        row_tolerance = 3  # words within 3 units are on the same row
        rows_dict = {}
        for w in words:
            top = round(w["top"] / row_tolerance) * row_tolerance
            if top not in rows_dict:
                rows_dict[top] = []
            rows_dict[top].append(w)

        # Sort rows by vertical position
        sorted_rows = sorted(rows_dict.items())

        # Determine column boundaries from header row or data patterns
        # CalSTRS columns (approximate x-positions from inspection):
        # Fund name: 0 - 240
        # Vintage Year: 240 - 290
        # Capital Committed: 290 - 360
        # Capital Contributed: 360 - 430
        # Capital Distributed: 430 - 500
        # Market Value: 500 - 560
        # IRR: 560+
        col_boundaries = [240, 290, 360, 430, 500, 560]

        records = []
        for top, row_words in sorted_rows:
            # Sort words in row by x position
            row_words.sort(key=lambda w: w["x0"])

            # Assign words to columns
            columns = [[] for _ in range(7)]  # 7 columns
            for w in row_words:
                x = w["x0"]
                col_idx = 0
                for i, boundary in enumerate(col_boundaries):
                    if x >= boundary:
                        col_idx = i + 1
                columns[col_idx].append(w["text"])

            # Join words within each column
            col_texts = [" ".join(words_list).strip() for words_list in columns]

            fund_name = col_texts[0]
            vy_text = col_texts[1]
            committed_text = col_texts[2]
            contributed_text = col_texts[3]
            distributed_text = col_texts[4]
            market_value_text = col_texts[5]
            irr_text = col_texts[6]

            # Skip non-data rows
            if not fund_name or not vy_text:
                continue
            if not re.match(r'^(19|20)\d{2}$', vy_text):
                continue

            # Reconstruct dollar amounts from words that may have been split
            committed_text = rejoin_split_number(committed_text)
            contributed_text = rejoin_split_number(contributed_text)
            distributed_text = rejoin_split_number(distributed_text)
            market_value_text = rejoin_split_number(market_value_text)

            vintage_year = int(vy_text)
            commitment_mm = parse_dollar_amount(committed_text)
            capital_called_mm = parse_dollar_amount(contributed_text)
            capital_distributed_mm = parse_dollar_amount(distributed_text)
            remaining_value_mm = parse_dollar_amount(market_value_text)

            # Parse IRR — CalSTRS PDF always lists IRR as a percentage number
            # without a % sign (e.g., "23.51" = 23.51%, "0.84" = 0.84%).
            # We always divide by 100 rather than using parse_percentage's
            # heuristic, which misinterprets values between -1 and 1.
            net_irr = None
            if irr_text and irr_text not in ("N/M", "NM", "N/M*", "*"):
                irr_clean = irr_text.replace("*", "").replace(",", "").strip()
                negative = False
                if irr_clean.startswith("(") and irr_clean.endswith(")"):
                    negative = True
                    irr_clean = irr_clean[1:-1].strip()
                try:
                    net_irr = float(irr_clean) / 100.0
                    if negative:
                        net_irr = -net_irr
                except ValueError:
                    pass

            # Compute net multiple
            net_multiple = None
            if capital_called_mm and capital_called_mm > 0:
                total_value = (capital_distributed_mm or 0) + (remaining_value_mm or 0)
                if total_value > 0:
                    net_multiple = round(total_value / capital_called_mm, 4)

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
                "source_url": CALSTRS_URL,
                "source_document": "CalSTRS Private Equity Portfolio Performance",
                "extraction_method": "deterministic_pdf",
                "extraction_confidence": 0.95,
            })

        return records

