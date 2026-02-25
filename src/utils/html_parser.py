"""HTML parsing utilities for pension fund data extraction.

Provides helper functions for extracting data from HTML tables
using BeautifulSoup with the lxml parser.
"""

import logging

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def parse_html_table(html: str, table_index: int = 0) -> list[list[str]]:
    """Extract a table from HTML as a list of rows.

    Args:
        html: HTML content string.
        table_index: Which table to extract (0-indexed).

    Returns:
        List of rows, where each row is a list of cell text values.
    """
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")

    if table_index >= len(tables):
        logger.warning(f"Table index {table_index} not found, only {len(tables)} tables")
        return []

    table = tables[table_index]
    rows = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cells:
            rows.append(cells)

    return rows
