"""Florida State Board of Administration (SBA) Private Equity adapter.

Data source: Florida SBA publishes quarterly PE Performance Reports.
URL: https://www.sbafla.com/reporting/alternative-asset-status-performance-report/

Report format: Fund Name (alphabetical), Vintage Year, Commitment,
Paid-In Capital, Distributions, Market Value, TVPI, Net IRR.
Amounts in millions or raw dollars depending on report version.

NOTE: The sbafla.com website returns 403 for all programmatic access.
The user must manually download the PE performance report PDF and place
it in data/cache/florida_sba/ for this adapter to work.

Manual download steps:
  1. Visit https://www.sbafla.com/reporting/alternative-asset-status-performance-report/
  2. Download the latest "Private Equity Performance Report" PDF
  3. Save as data/cache/florida_sba/pe_performance_latest.pdf

Parsing strategy: Word-level pdfplumber extraction with column assignment by
x-position, following the same pattern as the other PDF-based adapters.
"""

import io
import logging
import re
from pathlib import Path
from typing import Optional

import pdfplumber

from src.adapters.base import PensionFundAdapter
from src.utils.normalization import (
    parse_percentage, parse_multiple,
    rejoin_split_number, extract_as_of_date_from_text,
)

logger = logging.getLogger(__name__)

FL_SBA_SOURCE_URL = (
    "https://www.sbafla.com/reporting/alternative-asset-status-performance-report/"
)
CACHE_DIR = Path("data/cache/florida_sba")


class FloridaSBAAdapter(PensionFundAdapter):
    """Adapter for Florida SBA Private Equity Performance data.

    Requires manually cached PDF from SBA website (403-protected).
    """

    pension_fund_id = "florida_sba"
    pension_fund_name = "Florida State Board of Administration"
    state = "FL"
    full_name = "Florida State Board of Administration"
    total_aum_mm = 260000.0
    data_source_type = "pdf"
    disclosure_quality = "none"
    source_url = FL_SBA_SOURCE_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = CACHE_DIR / "pe_performance_latest.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the Florida SBA PE performance PDF from cache.

        Raises FileNotFoundError if no cached PDF is available, since
        the SBA website blocks programmatic downloads.
        """
        # Check for the preferred cache file
        if self._cache_path.exists():
            logger.info(f"Loading FL SBA data from cache: {self._cache_path}")
            return self._cache_path.read_bytes()

        # Check for any PE performance PDF in the cache dir
        if CACHE_DIR.exists():
            pe_files = sorted(CACHE_DIR.glob("pe_performance*.pdf"), reverse=True)
            for f in pe_files:
                data = f.read_bytes()
                if len(data) > 1000 and data[:5] == b"%PDF-":
                    logger.info(f"Loading FL SBA data from cache: {f}")
                    return data

            # Check for any valid PDF in the cache
            all_pdfs = sorted(CACHE_DIR.glob("*.pdf"), reverse=True)
            for f in all_pdfs:
                data = f.read_bytes()
                if len(data) > 1000 and data[:5] == b"%PDF-":
                    logger.info(f"Loading FL SBA data from cache: {f}")
                    return data

        raise FileNotFoundError(
            "No Florida SBA PE performance PDF found in cache. "
            "The sbafla.com website blocks programmatic downloads (403). "
            "Please manually download the PE performance report from "
            f"{FL_SBA_SOURCE_URL} and save it as {self._cache_path}"
        )

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse Florida SBA PE performance PDF."""
        records = []
        as_of_date = None

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            # Extract as-of date from first page
            first_text = pdf.pages[0].extract_text() or ""
            as_of_date = extract_as_of_date_from_text(first_text)

            # Detect column boundaries from the first data page
            col_boundaries = None

            for page in pdf.pages:
                text = page.extract_text() or ""

                # Skip non-data pages
                if not self._is_pe_data_page(text):
                    continue

                words = page.extract_words()
                if not words:
                    continue

                # Auto-detect column boundaries from header row on first data page
                if col_boundaries is None:
                    col_boundaries = self._detect_column_boundaries(words)
                    if col_boundaries is None:
                        continue

                page_records = self._parse_page_by_words(
                    words, col_boundaries, as_of_date
                )
                records.extend(page_records)

        logger.info(f"Parsed {len(records)} FL SBA PE commitment records")
        return records

    def _is_pe_data_page(self, text: str) -> bool:
        """Check if a page contains PE fund data."""
        text_lower = text.lower()
        # Must have fund-like content
        has_funds = "l.p." in text_lower or ", lp" in text_lower or "partners" in text_lower
        # Must have numeric data (dollar amounts or percentages)
        has_numbers = bool(re.search(r"\d+[.,]\d+", text))
        return has_funds and has_numbers

    def _detect_column_boundaries(self, words: list[dict]) -> Optional[list[float]]:
        """Auto-detect column boundaries from header words.

        Florida SBA PE reports typically have columns:
        Fund Name | Vintage | Commitment | Paid-In | Distributed | Value | TVPI | IRR

        Returns list of x-position boundaries, or None if can't detect.
        """
        header_keywords = {
            "vintage": "vintage",
            "year": "vintage",
            "commitment": "commitment",
            "committed": "commitment",
            "paid": "paid_in",
            "paid-in": "paid_in",
            "contributed": "paid_in",
            "called": "paid_in",
            "distribution": "distributed",
            "distributed": "distributed",
            "value": "value",
            "valuation": "value",
            "market": "value",
            "nav": "value",
            "tvpi": "tvpi",
            "multiple": "tvpi",
            "irr": "irr",
        }

        # Find header words
        header_positions: dict[str, float] = {}
        for w in words:
            text_lower = w["text"].lower().strip()
            if text_lower in header_keywords:
                col_name = header_keywords[text_lower]
                if col_name not in header_positions:
                    header_positions[col_name] = w["x0"]

        # Need at least vintage + commitment + one more to form columns
        if "vintage" not in header_positions or "commitment" not in header_positions:
            return None

        # Build boundaries list sorted by x-position
        cols_sorted = sorted(header_positions.items(), key=lambda x: x[1])

        # Build boundaries: midpoints between consecutive column headers
        boundaries = []
        for i in range(len(cols_sorted)):
            if i == 0:
                # First boundary: midpoint before first numeric column
                boundaries.append(cols_sorted[0][1] - 10)
            else:
                mid = (cols_sorted[i - 1][1] + cols_sorted[i][1]) / 2
                boundaries.append(mid)

        self._col_names = [name for name, _ in cols_sorted]
        logger.info(
            f"Detected FL SBA columns: {self._col_names} "
            f"at boundaries: {[f'{b:.0f}' for b in boundaries]}"
        )

        return boundaries

    def _parse_page_by_words(
        self,
        words: list[dict],
        col_boundaries: list[float],
        as_of_date: Optional[str],
    ) -> list[dict]:
        """Parse a page using word positions and detected column boundaries."""
        # Group words by row
        row_tolerance = 3
        rows_dict: dict[float, list] = {}
        for w in words:
            top = round(w["top"] / row_tolerance) * row_tolerance
            if top not in rows_dict:
                rows_dict[top] = []
            rows_dict[top].append(w)

        num_cols = len(col_boundaries) + 1
        col_names = getattr(self, "_col_names", [])

        records = []
        for _top, row_words in sorted(rows_dict.items()):
            row_words.sort(key=lambda w: w["x0"])

            # Assign words to columns
            columns: list[list[str]] = [[] for _ in range(num_cols)]
            for w in row_words:
                x = w["x0"]
                col_idx = 0
                for i, boundary in enumerate(col_boundaries):
                    if x >= boundary:
                        col_idx = i + 1
                columns[col_idx].append(w["text"])

            col_texts = [" ".join(wl).strip() for wl in columns]

            # The first column (before first boundary) is the fund name
            fund_name = col_texts[0]

            if not fund_name or len(fund_name) < 3:
                continue

            # Skip header/total/summary rows
            if any(
                kw in fund_name.lower()
                for kw in [
                    "total", "subtotal", "summary", "fund name", "benchmark",
                    "reporting", "currency", "private equity", "cash flow",
                    "page", "as of",
                ]
            ):
                continue

            # Map column texts to fields based on detected column order
            field_values: dict[str, str] = {}
            for i, name in enumerate(col_names):
                if i + 1 < len(col_texts):
                    field_values[name] = rejoin_split_number(col_texts[i + 1])

            # Extract vintage year
            vintage_text = field_values.get("vintage", "")
            vintage_year = None
            if vintage_text:
                vy_match = re.search(r"(19|20)\d{2}", vintage_text)
                if vy_match:
                    vintage_year = int(vy_match.group(0))

            # Must have a vintage year to be a data row
            if vintage_year is None:
                continue

            # Parse financial fields
            commitment_mm = self._parse_fl_amount(field_values.get("commitment", ""))
            capital_called_mm = self._parse_fl_amount(field_values.get("paid_in", ""))
            capital_distributed_mm = self._parse_fl_amount(
                field_values.get("distributed", "")
            )
            remaining_value_mm = self._parse_fl_amount(field_values.get("value", ""))

            net_multiple = parse_multiple(field_values.get("tvpi", ""))
            net_irr = parse_percentage(field_values.get("irr", ""))

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
                    "net_irr": net_irr,
                    "net_multiple": net_multiple,
                    "dpi": None,
                    "as_of_date": as_of_date,
                    "source_url": FL_SBA_SOURCE_URL,
                    "source_document": "FL SBA PE Performance Report",
                    "extraction_method": "deterministic_pdf",
                    "extraction_confidence": 0.90,
                }
            )

        return records


    @staticmethod
    def _parse_fl_amount(text: str) -> Optional[float]:
        """Parse Florida SBA dollar amounts.

        SBA reports may use raw dollars or millions. Detect by magnitude:
        - Values > 100,000 are likely raw dollars -> convert to millions
        - Values < 100,000 are likely already in millions
        """
        if not text or text.strip() in ("-", "—", "", "N/A"):
            return None

        cleaned = text.replace("$", "").replace(",", "").replace(" ", "").strip()

        # Handle parentheses (negative)
        negative = False
        if cleaned.startswith("(") and cleaned.endswith(")"):
            negative = True
            cleaned = cleaned[1:-1]

        if not cleaned:
            return None

        try:
            value = float(cleaned)
            if negative:
                value = -value
            # Heuristic: if > 100,000, assume raw dollars
            if abs(value) > 100_000:
                return value / 1_000_000
            return value
        except ValueError:
            return None
