"""Texas Teacher Retirement System (TRS) Private Equity adapter.

Data source: TRS publishes PE fund-level data in Investment Management Committee
(IMC) board books, available at:
  https://www.trs.texas.gov/investments

The IMC book contains a Private Markets section with fund name, vintage year,
commitment, called, distributed, remaining value, IRR, and TVPI for each PE fund.

NOTE: The TRS website is behind Incapsula bot protection, which blocks
programmatic PDF downloads. The user must manually download the IMC board book
PDF and place it in data/cache/texas_trs/ for this adapter to work.

Alternatively, the ACFR (Annual Comprehensive Financial Report) is downloadable
but only contains summary-level PE data (total NAV, unfunded commitments) and
an external manager name list without dollar amounts.

Manual download steps:
  1. Visit https://www.trs.texas.gov/investments
  2. Navigate to Investment Management Division > Reports
  3. Download the latest IMC Board Book PDF
  4. Save as data/cache/texas_trs/imc_book_latest.pdf

Parsing strategy: Word-level pdfplumber extraction with column assignment by
x-position, following the same pattern as the other PDF-based adapters.
"""

import io
import logging
import re
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

from src.adapters.base import PensionFundAdapter
from src.utils.normalization import parse_dollar_amount, parse_percentage, parse_multiple

logger = logging.getLogger(__name__)

TRS_SOURCE_URL = "https://www.trs.texas.gov/investments/teams/private-markets"
TRS_ACFR_URL = (
    "https://www.trs.texas.gov/sites/default/files/migrated/trs-acfr-2024.pdf"
)
CACHE_DIR = Path("data/cache/texas_trs")

# Texas TRS ACFR external manager names (pages 129-130)
# These are the PE/alternative managers as listed in the 2024 ACFR.
# Used for entity resolution seeding when fund-level data isn't available.
TRS_EXTERNAL_PE_MANAGERS = [
    "Abacus Capital Group", "Actis", "Advent International", "Aeolus Capital Management",
    "AGR Ag Infrastructure", "AIMCo", "Altas Partners", "American Industrial Partners",
    "Angelo Gordon", "Apollo Global Management", "Ares Management",
    "CBRE Global Investors", "CCMP Capital Advisors", "Cerberus Capital Management",
    "Certares Management", "CIM Group", "Clearlake Capital Group",
    "General Atlantic", "General Catalyst", "Goldman Sachs", "Gores Group",
    "Graham Partners", "Great Hill Equity Partners", "Greenbelt Capital Partners",
    "Hellman & Friedman", "HPS Investment Partners", "Hycroft Advisors",
    "KKR", "Leonard Green & Partners", "Lexington Partners", "Lone Star",
    "Oaktree Capital Management", "Owl Rock Capital", "Permira",
    "Providence Equity Partners", "Silver Lake", "Thoma Bravo",
    "TPG Capital", "Vista Equity Partners", "Warburg Pincus",
]


class TexasTRSAdapter(PensionFundAdapter):
    """Adapter for Texas TRS Private Equity data.

    Requires manually cached PDF from TRS website (Incapsula-protected).
    Falls back to ACFR summary data if no IMC board book is available.
    """

    pension_fund_id = "texas_trs"
    pension_fund_name = "Texas Teacher Retirement System"
    state = "TX"
    full_name = "Teacher Retirement System of Texas"
    total_aum_mm = 200000.0
    data_source_type = "pdf"
    disclosure_quality = "limited"
    source_url = TRS_SOURCE_URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._imc_cache_path = CACHE_DIR / "imc_book_latest.pdf"
        self._acfr_cache_path = CACHE_DIR / "acfr_2024.pdf"

    def fetch_source(self) -> bytes:
        """Fetch the TRS data source.

        Prefers the IMC board book PDF (requires manual download).
        Falls back to downloading the ACFR.
        """
        # Prefer manually-cached IMC board book
        if self._imc_cache_path.exists():
            logger.info(f"Loading TRS IMC board book from cache: {self._imc_cache_path}")
            return self._imc_cache_path.read_bytes()

        # Check for any IMC book PDF in cache dir
        if CACHE_DIR.exists():
            imc_files = sorted(CACHE_DIR.glob("imc_book_*.pdf"), reverse=True)
            for f in imc_files:
                if f.stat().st_size > 10000:
                    logger.info(f"Loading TRS IMC board book from cache: {f}")
                    return f.read_bytes()

        # Fall back to ACFR (downloadable but only has summary data)
        if self._acfr_cache_path.exists():
            logger.info(f"Loading TRS ACFR from cache: {self._acfr_cache_path}")
            return self._acfr_cache_path.read_bytes()

        logger.info(f"Downloading TRS ACFR from {TRS_ACFR_URL}")
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })
        # Visit main page first for session cookies
        session.get("https://www.trs.texas.gov/", timeout=30)
        resp = session.get(TRS_ACFR_URL, timeout=120)
        resp.raise_for_status()
        data = resp.content

        if data[:5] != b"%PDF-":
            raise ValueError(
                "TRS ACFR download did not return a valid PDF. "
                "The site may be blocking programmatic access. "
                "Please manually download the IMC board book and save to "
                f"{self._imc_cache_path}"
            )

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._acfr_cache_path.write_bytes(data)
        logger.info(f"Cached TRS ACFR to {self._acfr_cache_path}")
        return data

    def parse(self, raw_data: bytes) -> list[dict]:
        """Parse TRS data.

        If the source is an IMC board book, extracts fund-level PE data.
        If the source is the ACFR, extracts only summary-level data.
        """
        records = []

        with pdfplumber.open(io.BytesIO(raw_data)) as pdf:
            # Detect which document we have
            first_text = (pdf.pages[0].extract_text() or "").lower()

            if "investment management" in first_text and len(pdf.pages) < 150:
                # IMC board book - look for PE fund-level tables
                records = self._parse_imc_book(pdf)
            else:
                # ACFR - extract summary data only
                records = self._parse_acfr_summary(pdf)

        logger.info(f"Parsed {len(records)} Texas TRS records")
        return records

    def _parse_imc_book(self, pdf) -> list[dict]:
        """Parse PE fund-level data from IMC board book."""
        records = []

        for page in pdf.pages:
            text = page.extract_text() or ""
            text_lower = text.lower()

            # Look for pages with PE fund-level tabular data
            if not ("private" in text_lower and ("equity" in text_lower or "markets" in text_lower)):
                continue

            # Check if this page has tabular fund data (vintage years + dollar amounts)
            if not re.search(r"20[012]\d.*[$]?\d+[\.,]\d+", text):
                continue

            words = page.extract_words()
            if not words:
                continue

            page_records = self._parse_pe_fund_table(words, text)
            records.extend(page_records)

        return records

    def _parse_pe_fund_table(self, words: list[dict], full_text: str) -> list[dict]:
        """Parse a PE fund table from word positions.

        TRS board books typically have columns:
        Fund Name, Vintage, Commitment ($M), Called ($M), Distributed ($M),
        Remaining Value ($M), Net IRR, TVPI
        """
        # Group words by row
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
            row_text = " ".join(w["text"] for w in row_words)

            # Look for rows with a vintage year (20XX or 19XX)
            vintage_match = re.search(r"\b(19|20)\d{2}\b", row_text)
            if not vintage_match:
                continue

            vintage_year = int(vintage_match.group(0))
            if vintage_year < 1990 or vintage_year > 2026:
                continue

            # Extract fund name (text before the vintage year)
            vy_pos = row_text.find(vintage_match.group(0))
            fund_name = row_text[:vy_pos].strip().rstrip(",")

            if not fund_name or len(fund_name) < 3:
                continue

            # Skip summary/total rows
            if any(kw in fund_name.lower() for kw in ["total", "subtotal", "summary", "benchmark"]):
                continue

            # Extract dollar amounts after the vintage year
            after_vy = row_text[vy_pos + 4:]
            amounts = re.findall(r"[\$]?([\d,]+\.?\d*)", after_vy)

            commitment_mm = None
            capital_called_mm = None
            capital_distributed_mm = None
            remaining_value_mm = None
            net_irr = None
            net_multiple = None

            if len(amounts) >= 1:
                commitment_mm = self._parse_amount_mm(amounts[0])
            if len(amounts) >= 2:
                capital_called_mm = self._parse_amount_mm(amounts[1])
            if len(amounts) >= 3:
                capital_distributed_mm = self._parse_amount_mm(amounts[2])
            if len(amounts) >= 4:
                remaining_value_mm = self._parse_amount_mm(amounts[3])
            if len(amounts) >= 5:
                net_irr = parse_percentage(amounts[4])
            if len(amounts) >= 6:
                net_multiple = parse_multiple(amounts[5])

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
                "as_of_date": None,
                "source_url": TRS_SOURCE_URL,
                "source_document": "TRS IMC Board Book",
                "extraction_method": "deterministic_pdf",
                "extraction_confidence": 0.90,
            })

        return records

    def _parse_acfr_summary(self, pdf) -> list[dict]:
        """Extract summary-level PE data from the ACFR.

        The ACFR only has total PE NAV and unfunded commitments, plus an
        external manager name list. This creates a single summary record
        to indicate data was found but is not fund-level.
        """
        records = []

        for page in pdf.pages:
            text = page.extract_text() or ""

            # Look for the NAV and Unfunded Commitments table
            if "Net Asset Value" not in text or "Unfunded Capital" not in text:
                continue

            # Extract PE totals
            pe_match = re.search(
                r"Private\s+Equity\s+[$]?\s*([\d,]+)\s+[$]?\s*([\d,]+)",
                text,
            )
            if pe_match:
                nav_raw = pe_match.group(1).replace(",", "")
                unfunded_raw = pe_match.group(2).replace(",", "")

                try:
                    nav_mm = float(nav_raw) / 1_000_000
                    unfunded_mm = float(unfunded_raw) / 1_000_000
                except ValueError:
                    continue

                records.append({
                    "fund_name_raw": "Texas TRS Private Equity Portfolio (Summary)",
                    "general_partner": None,
                    "vintage_year": None,
                    "asset_class": "Private Equity",
                    "sub_strategy": None,
                    "commitment_mm": nav_mm + unfunded_mm,
                    "capital_called_mm": nav_mm,
                    "capital_distributed_mm": None,
                    "remaining_value_mm": nav_mm,
                    "net_irr": None,
                    "net_multiple": None,
                    "dpi": None,
                    "as_of_date": "2024-08-31",
                    "source_url": TRS_ACFR_URL,
                    "source_document": "TRS ACFR 2024 - Summary Only",
                    "extraction_method": "deterministic_pdf",
                    "extraction_confidence": 0.70,
                })
                logger.warning(
                    "Texas TRS: Only summary-level PE data available from ACFR. "
                    f"PE NAV: ${nav_mm:,.0f}M, Unfunded: ${unfunded_mm:,.0f}M. "
                    "For fund-level data, manually download the IMC board book "
                    f"and save to {self._imc_cache_path}"
                )
                break

        return records

    @staticmethod
    def _parse_amount_mm(text: str) -> Optional[float]:
        """Parse a dollar amount, assuming values in millions."""
        if not text:
            return None
        cleaned = text.replace(",", "").replace("$", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
