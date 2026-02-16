"""Normalization utilities for parsing financial data from various string formats.

All functions handle None, empty strings, "N/A", "n/a", "-", "—" gracefully
by returning None instead of raising exceptions.
"""

import re
from datetime import date, datetime
from typing import Optional

from dateutil import parser as dateutil_parser


# Values that should be treated as "no data"
_EMPTY_VALUES = {None, "", "N/A", "n/a", "N/a", "NA", "na", "-", "—", "–", "n.a.", "n.a", "--", "---", "None", "none"}


def _is_empty(value) -> bool:
    """Check if a value represents missing data."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() in _EMPTY_VALUES
    return False


def parse_dollar_amount(value, context_in_millions: bool = False) -> Optional[float]:
    """Parse a dollar amount string and return value in millions.

    Handles formats like:
    - "$1.2B" or "$1.2 billion" → 1200.0
    - "$500M" or "$500 million" → 500.0
    - "$45,000,000" → 45.0 (converts to millions)
    - "$45,000" → 0.045 (converts to millions)
    - "1,200.5" with context_in_millions=True → 1200.5
    - "$1.2K" or "$1,200" → 0.0012 (converts to millions)
    - "(500)" or "($500M)" → -500.0 (parentheses = negative)

    Args:
        value: String or numeric dollar amount.
        context_in_millions: If True, treat bare numbers as already in millions.

    Returns:
        Float value in millions, or None if unparseable.
    """
    if _is_empty(value):
        return None

    if isinstance(value, (int, float)):
        return float(value) if context_in_millions else float(value) / 1_000_000

    s = str(value).strip()
    if s in _EMPTY_VALUES:
        return None

    # Check for negative (parentheses notation)
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    # Remove dollar sign and whitespace
    s = s.replace("$", "").replace(" ", "").strip()

    if not s:
        return None

    # Check for negative sign
    if s.startswith("-"):
        negative = not negative
        s = s[1:]

    # Detect scale suffix
    s_upper = s.upper()
    scale = 1.0  # multiplier to get to millions

    if s_upper.endswith("B") or s_upper.endswith("BILLION"):
        s = re.sub(r'(?i)(billion|b)$', '', s)
        scale = 1000.0  # billions to millions
    elif s_upper.endswith("M") or s_upper.endswith("MILLION") or s_upper.endswith("MM"):
        s = re.sub(r'(?i)(million|mm|m)$', '', s)
        scale = 1.0  # already in millions
    elif s_upper.endswith("K") or s_upper.endswith("THOUSAND"):
        s = re.sub(r'(?i)(thousand|k)$', '', s)
        scale = 0.001  # thousands to millions
    elif context_in_millions:
        scale = 1.0
    else:
        # No suffix and not in millions context — assume raw dollars
        scale = 1.0 / 1_000_000

    # Remove commas
    s = s.replace(",", "")

    if not s:
        return None

    try:
        result = float(s) * scale
        return -result if negative else result
    except ValueError:
        return None


def parse_percentage(value) -> Optional[float]:
    """Parse a percentage string and return as a decimal fraction.

    Handles formats like:
    - "15.3%" → 0.153
    - "-2.1%" → -0.021
    - "0.153" (already decimal) → 0.153
    - "15.3" (ambiguous — treated as percentage if > 1 or < -1) → 0.153

    Returns:
        Float as decimal (0.15 for 15%), or None if unparseable.
    """
    if _is_empty(value):
        return None

    if isinstance(value, (int, float)):
        # If the absolute value is > 1, assume it's a percentage needing division
        if abs(value) > 1:
            return float(value) / 100.0
        return float(value)

    s = str(value).strip()
    if s in _EMPTY_VALUES:
        return None

    # Check for parentheses (negative)
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    has_percent_sign = "%" in s
    s = s.replace("%", "").replace(",", "").strip()

    if not s:
        return None

    try:
        num = float(s)
        if negative:
            num = -num
        # If it had a percent sign, divide by 100
        if has_percent_sign:
            return num / 100.0
        # Heuristic: if no percent sign, values > 1 or < -1 are likely percentages
        # (e.g., "15.3" meaning 15.3%)
        # Values between -1 and 1 are likely already decimals
        if abs(num) > 1:
            return num / 100.0
        return num
    except ValueError:
        return None


def parse_date(value) -> Optional[str]:
    """Parse various date formats and return as ISO format string (YYYY-MM-DD).

    Handles formats like:
    - "12/31/2023", "2023-12-31", "December 31, 2023"
    - "Q4 2023" → "2023-12-31" (end of quarter)
    - "FY 2023" or "FY2023" → "2023-06-30" (typical fiscal year end)
    - "June 30, 2023", "6/30/2023"
    - "2023" (year only) → "2023-12-31"

    Returns:
        ISO date string (YYYY-MM-DD) or None if unparseable.
    """
    if _is_empty(value):
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    s = str(value).strip()
    if s in _EMPTY_VALUES:
        return None

    # Handle quarter notation
    quarter_match = re.match(r'Q([1-4])\s*(\d{4})', s, re.IGNORECASE)
    if quarter_match:
        q, year = int(quarter_match.group(1)), int(quarter_match.group(2))
        quarter_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
        return f"{year}-{quarter_ends[q]}"

    # Handle fiscal year notation
    fy_match = re.match(r'FY\s*(\d{4})', s, re.IGNORECASE)
    if fy_match:
        year = int(fy_match.group(1))
        return f"{year}-06-30"

    # Handle year only
    year_match = re.match(r'^(\d{4})$', s)
    if year_match:
        return f"{s}-12-31"

    # Use dateutil for everything else
    try:
        parsed = dateutil_parser.parse(s)
        return parsed.date().isoformat()
    except (ValueError, TypeError):
        return None


def parse_multiple(value) -> Optional[float]:
    """Parse a fund multiple/MOIC from string.

    Handles formats like:
    - "1.5x" or "1.50X" → 1.5
    - "1.50" → 1.5
    - "2.1x" → 2.1

    Returns:
        Float value of the multiple, or None if unparseable.
    """
    if _is_empty(value):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if s in _EMPTY_VALUES:
        return None

    # Remove 'x' or 'X' suffix
    s = re.sub(r'[xX]$', '', s).strip()
    s = s.replace(",", "")

    if not s:
        return None

    try:
        return float(s)
    except ValueError:
        return None


def parse_vintage_year(value) -> Optional[int]:
    """Parse a vintage year from various formats.

    Handles: 2023, "2023", "FY 2023", "Vintage 2023"

    Returns:
        Integer year or None if invalid/unparseable.
    """
    if _is_empty(value):
        return None

    if isinstance(value, int):
        if 1980 <= value <= 2030:
            return value
        return None

    if isinstance(value, float):
        v = int(value)
        if 1980 <= v <= 2030:
            return v
        return None

    s = str(value).strip()
    if s in _EMPTY_VALUES:
        return None

    # Extract 4-digit year
    match = re.search(r'(\d{4})', s)
    if match:
        year = int(match.group(1))
        if 1980 <= year <= 2030:
            return year

    return None


def normalize_fund_name(name: str) -> str:
    """Normalize a fund name for comparison purposes.

    - Strip whitespace and lowercase
    - Remove common suffixes like "L.P.", "LP", "LLC", "Ltd"
    - Normalize common abbreviations
    - Collapse multiple spaces

    Returns the normalized name (does NOT replace the raw name — that's kept for provenance).
    """
    if not name:
        return ""

    s = name.strip()

    # Remove common legal suffixes
    s = re.sub(r',?\s*L\.?P\.?$', '', s, flags=re.IGNORECASE)
    s = re.sub(r',?\s*LLC$', '', s, flags=re.IGNORECASE)
    s = re.sub(r',?\s*Ltd\.?$', '', s, flags=re.IGNORECASE)
    s = re.sub(r',?\s*Inc\.?$', '', s, flags=re.IGNORECASE)
    s = re.sub(r',?\s*Co\.?$', '', s, flags=re.IGNORECASE)

    # Normalize Roman numerals spacing (e.g., "Fund VII" stays, but ensure consistency)
    # Normalize "Fund" abbreviations
    s = re.sub(r'\bFd\b', 'Fund', s, flags=re.IGNORECASE)
    s = re.sub(r'\bPrtrs\b', 'Partners', s, flags=re.IGNORECASE)
    s = re.sub(r'\bPtnrs\b', 'Partners', s, flags=re.IGNORECASE)
    s = re.sub(r'\bCap\b', 'Capital', s, flags=re.IGNORECASE)
    s = re.sub(r'\bMgmt\b', 'Management', s, flags=re.IGNORECASE)
    s = re.sub(r'\bIntl\b', 'International', s, flags=re.IGNORECASE)
    s = re.sub(r'\bInv\b', 'Investment', s, flags=re.IGNORECASE)

    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def extract_fund_number(name: str) -> Optional[str]:
    """Extract the primary Roman numeral fund number from a fund name.

    Returns the Roman numeral (e.g., 'VII', 'XIV') or None if not found.
    Used to prevent fuzzy matching of different fund series (e.g., Fund V vs Fund VI).

    Strategy: find the LARGEST Roman numeral token (by value) in the name,
    which is almost always the primary fund series number. Small numerals
    like I or II at the end after a larger one are sub-fund designators.
    """
    if not name:
        return None

    _ROMAN_VALUES = {
        'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
        'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
        'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15,
        'XVI': 16, 'XVII': 17, 'XVIII': 18, 'XIX': 19, 'XX': 20,
        'XXI': 21, 'XXII': 22, 'XXIII': 23, 'XXIV': 24, 'XXV': 25,
    }

    # Split into tokens
    tokens = re.split(r'[\s,\-\'\"()]+', name.strip())

    best_roman = None
    best_value = 0

    for i, token in enumerate(tokens):
        upper = token.upper().rstrip('.')
        if upper not in _ROMAN_VALUES:
            continue

        value = _ROMAN_VALUES[upper]

        # Skip standalone 'I' unless preceded by a fund-context word
        if upper == 'I' and i > 0:
            prev = tokens[i - 1].lower().rstrip('.,')
            fund_words = {'fund', 'partners', 'capital', 'equity', 'ventures',
                          'opportunities', 'growth', 'credit', 'europe', 'asia',
                          'evergreen', 'springblue'}
            if prev not in fund_words:
                continue
        elif upper == 'I' and i == 0:
            continue  # 'I' at start of name is not a fund number

        # Skip 'V' if it could be part of a word
        if upper == 'V' and i > 0:
            prev = tokens[i - 1].lower()
            if prev.endswith('v'):
                continue  # part of a split word

        # Take the largest Roman numeral found (the primary fund number)
        if value > best_value:
            best_value = value
            best_roman = upper

    return best_roman


def classify_fund_strategy(fund_name: str) -> tuple[str, Optional[str]]:
    """Classify a fund's asset class and sub_strategy based on its name.

    Uses keyword matching against common PE industry naming conventions.

    Args:
        fund_name: The fund name (raw or normalized).

    Returns:
        Tuple of (asset_class, sub_strategy). Sub_strategy may be None
        if no specific strategy keyword is found.
    """
    if not fund_name:
        return "Private Equity", None

    name_lower = fund_name.lower()

    # Check sub-strategies in priority order (most specific first)
    # Real Estate
    if any(kw in name_lower for kw in ('real estate', 'realty', 'property', 'reit')):
        return "Real Assets", "Real Estate"

    # Infrastructure
    if 'infrastructure' in name_lower or 'infra ' in name_lower:
        return "Real Assets", "Infrastructure"

    # Natural Resources / Energy
    if any(kw in name_lower for kw in ('natural resource', 'timber', 'mining', 'oil ', 'gas ')):
        return "Real Assets", "Natural Resources"
    if 'energy' in name_lower:
        return "Private Equity", "Energy"

    # Fund of Funds
    if 'fund of funds' in name_lower or 'pathway' in name_lower:
        return "Private Equity", "Fund of Funds"

    # Secondaries
    if any(kw in name_lower for kw in ('secondar', 'secondary')):
        return "Private Equity", "Secondaries"

    # Co-investments
    if any(kw in name_lower for kw in ('co-invest', 'coinvest', 'co invest')):
        return "Private Equity", "Co-Investment"

    # Credit / Debt
    if any(kw in name_lower for kw in ('credit', 'debt', 'loan', 'lending', 'mezzanine', 'mezz')):
        return "Private Credit", "Credit"

    # Distressed / Special Situations
    if any(kw in name_lower for kw in ('distress', 'special situation', 'turnaround', 'recovery', 'rescue')):
        return "Private Equity", "Distressed/Special Situations"

    # Venture Capital
    if any(kw in name_lower for kw in ('venture', 'seed', 'early stage', 'early-stage')):
        return "Private Equity", "Venture Capital"

    # Growth Equity
    if 'growth' in name_lower:
        return "Private Equity", "Growth Equity"

    # Buyout (explicit keyword)
    if 'buyout' in name_lower:
        return "Private Equity", "Buyout"

    # Opportunities funds (often distressed or multi-strategy)
    if 'opportunit' in name_lower:
        return "Private Equity", "Opportunistic"

    # No specific keyword found
    return "Private Equity", None


def normalize_gp_name(name: str) -> str:
    """Normalize a General Partner name for matching."""
    if not name:
        return ""
    return normalize_fund_name(name)
