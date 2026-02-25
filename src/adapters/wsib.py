"""Washington State Investment Board (WSIB) Private Equity adapter.

Data source: WSIB publishes quarterly PE IRR reports as PDFs.
URL: https://www.sib.wa.gov/reports.html

Columns: Investment Name, Initial Date, Commitment, Capital Paid-In,
Unfunded, Current Market Value, Distributions, Total Value, Multiple,
Gain/Loss Since Inception, Net IRR.
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
from src.utils.normalization import (
    parse_dollar_amount, parse_percentage, parse_multiple,
    extract_as_of_date_from_text,
)

logger = logging.getLogger(__name__)

WSIB_REPORT_URL = "https://www.sib.wa.gov/docs/reports/quarterly/ir063025.pdf"
CACHE_DIR = Path("data/cache/wsib")

# Column x-position boundaries (from inspection of word positions)
# Fund name: 0-265, Date: 265-310, Commitment: 310-355, Paid-In: 355-395,
# Unfunded: 395-445, Market Value: 445-485, Distributions: 485-525,
# Total Value: 525-558, Multiple: 558-595, Gain/Loss: 595-620, IRR: 620+
WSIB_COL_BOUNDARIES = [265, 310, 355, 395, 445, 485, 525, 558, 595, 620]


class WSIBAdapter(PensionFundAdapter):
    """Adapter for WSIB Private Equity IRR report."""

    pension_fund_id = "wsib"
    pension_fund_name = "WSIB"
    state = "WA"
    full_name = "Washington State Investment Board"
    total_aum_mm = 190000.0
    data_source_type = "pdf"
    disclosure_quality = "excellent"
    source_url = WSIB_REPORT_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "pe_irr_063025.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the WSIB PE IRR report PDF."""
        if self.use_cache and self._cache_path.exists():
            logger.info(f"Loading WSIB data from cache: {self._cache_path}")
            return self._cache_path.read_bytes()

        logger.info(f"Fetching WSIB PE data from {WSIB_REPORT_URL}")
        resp = requests.get(WSIB_REPORT_URL, timeout=30)
        resp.raise_for_status()
        data = resp.content

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_bytes(data)
        logger.info(f"Cached WSIB data to {self._cache_path}")

        return data

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse WSIB PE IRR report PDF using word-level extraction."""
        records = []
        as_of_date = None

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            # Gather all text to find as-of date
            all_text = ""
            for page in pdf.pages:
                text = page.extract_text() or ""
                all_text += text + "\n"
            as_of_date = extract_as_of_date_from_text(all_text)

            for page in pdf.pages:
                page_records = self._parse_page_by_words(page, as_of_date)
                records.extend(page_records)

        logger.info(f"Parsed {len(records)} WSIB PE commitment records")
        return records

    def _parse_page_by_words(self, page, as_of_date: Optional[str]) -> list[dict]:
        """Parse a page using word positions to assign columns."""
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

            # Assign words to columns (11 columns)
            columns = [[] for _ in range(11)]
            for w in row_words:
                x = w["x0"]
                col_idx = 0
                for i, boundary in enumerate(WSIB_COL_BOUNDARIES):
                    if x >= boundary:
                        col_idx = i + 1
                columns[col_idx].append(w["text"])

            col_texts = [" ".join(wl).strip() for wl in columns]

            fund_name = col_texts[0]
            date_text = col_texts[1]
            commitment_text = col_texts[2]
            paid_in_text = col_texts[3]
            unfunded_text = col_texts[4]
            market_value_text = col_texts[5]
            distributions_text = col_texts[6]
            total_value_text = col_texts[7]
            multiple_text = col_texts[8]
            gain_loss_text = col_texts[9]
            irr_text = col_texts[10]

            # Skip non-data rows: must have a date (M/D/YYYY) or N/A
            if not date_text or not re.match(r'\d{1,2}/\d{1,2}/\d{4}|N/A', date_text):
                continue

            # Skip category headers and subtotals
            if not fund_name or any(x in fund_name.lower() for x in
                                     ["subtotal", "total", "strategy", "summary"]):
                continue

            # Extract vintage year from date
            vintage_year = None
            year_match = re.search(r'/(\d{4})$', date_text)
            if year_match:
                vintage_year = int(year_match.group(1))

            # Parse financial data
            commitment_mm = parse_dollar_amount(commitment_text)
            capital_called_mm = parse_dollar_amount(paid_in_text)
            remaining_value_mm = parse_dollar_amount(market_value_text)
            capital_distributed_mm = parse_dollar_amount(distributions_text)
            net_multiple = parse_multiple(multiple_text)
            if net_multiple is not None and net_multiple < 0:
                logger.warning(
                    f"Negative multiple {net_multiple}x for '{fund_name}' — "
                    f"likely parsing artifact, using absolute value"
                )
                net_multiple = abs(net_multiple)

            # Parse IRR
            net_irr = None
            if irr_text and irr_text not in ("N/A",):
                irr_clean = irr_text.replace("%", "").strip()
                if irr_clean:
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
                "source_url": WSIB_REPORT_URL,
                "source_document": "WSIB Private Equity IRR Report",
                "extraction_method": "deterministic_pdf",
                "extraction_confidence": 0.90,
            })

        return records
