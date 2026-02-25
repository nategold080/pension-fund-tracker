"""Generate methodology_overview.pdf using reportlab."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)

OUTPUT = "/Users/nathangoldberg/Desktop/pension fund project/data/exports/demo/methodology_overview.pdf"

# Colors
DARK = HexColor("#1a1a2e")
ACCENT = HexColor("#16213e")
MEDIUM = HexColor("#0f3460")
LIGHT_BG = HexColor("#f0f0f5")
BORDER = HexColor("#cccccc")
MUTED = HexColor("#555555")

styles = getSampleStyleSheet()

# Custom styles
title_style = ParagraphStyle(
    "DocTitle", parent=styles["Title"],
    fontSize=20, leading=24, textColor=DARK,
    spaceAfter=2, alignment=TA_LEFT, fontName="Helvetica-Bold"
)
subtitle_style = ParagraphStyle(
    "DocSubtitle", parent=styles["Normal"],
    fontSize=11, leading=14, textColor=MUTED,
    spaceAfter=16, fontName="Helvetica"
)
h1_style = ParagraphStyle(
    "H1", parent=styles["Heading1"],
    fontSize=14, leading=18, textColor=DARK,
    spaceBefore=18, spaceAfter=8, fontName="Helvetica-Bold"
)
h2_style = ParagraphStyle(
    "H2", parent=styles["Heading2"],
    fontSize=11, leading=14, textColor=ACCENT,
    spaceBefore=12, spaceAfter=4, fontName="Helvetica-Bold"
)
body_style = ParagraphStyle(
    "Body", parent=styles["Normal"],
    fontSize=9.5, leading=13, textColor=black,
    spaceAfter=6, fontName="Helvetica"
)
body_bold_style = ParagraphStyle(
    "BodyBold", parent=body_style,
    fontName="Helvetica-Bold"
)
small_style = ParagraphStyle(
    "Small", parent=body_style,
    fontSize=8.5, leading=11, textColor=MUTED
)
bullet_style = ParagraphStyle(
    "Bullet", parent=body_style,
    leftIndent=18, bulletIndent=6, spaceBefore=1, spaceAfter=1
)
table_header_style = ParagraphStyle(
    "TH", parent=styles["Normal"],
    fontSize=8.5, leading=10, textColor=white,
    fontName="Helvetica-Bold"
)
table_cell_style = ParagraphStyle(
    "TD", parent=styles["Normal"],
    fontSize=8.5, leading=11, textColor=black,
    fontName="Helvetica"
)
table_cell_bold = ParagraphStyle(
    "TDBold", parent=table_cell_style,
    fontName="Helvetica-Bold"
)


def P(text, style=body_style):
    return Paragraph(text, style)


def make_source_table(name, rows):
    """Build a compact key-value table for a data source."""
    data = [[P(name, h2_style), ""]]
    for label, value in rows:
        data.append([P(label, table_cell_bold), P(value, table_cell_style)])

    col_widths = [1.35 * inch, 5.15 * inch]
    t = Table(data, colWidths=col_widths, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("SPAN", (0, 0), (1, 0)),
        ("BACKGROUND", (0, 0), (1, 0), LIGHT_BG),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 0), (1, 0), 0.5, BORDER),
        ("LINEBELOW", (0, -1), (1, -1), 0.5, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def make_field_table():
    header = [P("Field", table_header_style), P("Coverage", table_header_style)]
    rows = [
        ("Commitment amount", "99.9%"),
        ("Vintage year", "99.7%"),
        ("Capital called", "98.4%"),
        ("Net multiple", "98.0%"),
        ("Capital distributed", "94.8%"),
        ("Net IRR", "52.6% (4 of 5 sources disclose; NY Common does not)"),
        ("Remaining value", "84.8%"),
    ]
    data = [header] + [[P(r[0], table_cell_style), P(r[1], table_cell_style)] for r in rows]
    col_widths = [2.0 * inch, 4.5 * inch]
    t = Table(data, colWidths=col_widths, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, 0), white),
        ("BACKGROUND", (0, 1), (-1, -1), white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [white, LIGHT_BG]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t


def build():
    doc = SimpleDocTemplate(
        OUTPUT, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.65 * inch, bottomMargin=0.6 * inch
    )

    story = []

    # Title block
    story.append(P("Data Sourcing &amp; Extraction Methodology", title_style))
    story.append(P("Alternative Investment Commitment Data \u2014 U.S. Public Pension Funds", subtitle_style))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER, spaceAfter=10))

    # Overview
    story.append(P("Overview", h1_style))
    story.append(P(
        "This dataset captures private equity and alternative investment commitments disclosed by "
        "five major U.S. public pension systems, representing over $1.5 trillion in combined AUM. "
        "The pipeline extracts structured data directly from official government disclosures \u2014 "
        "no web scraping of third-party aggregators, no LLM-generated values, no manual data entry."
    ))
    story.append(P(
        "<b>2,125 commitment records</b> across <b>1,610 unique funds</b>, "
        "with <b>345 funds independently verified across 2+ pension systems.</b>"
    ))
    story.append(Spacer(1, 4))

    # --- Data Sources ---
    story.append(P("Data Sources", h1_style))

    sources = [
        ("CalPERS \u2014 California Public Employees\u2019 Retirement System", [
            ("Source", "calpers.ca.gov \u2014 PEP Fund Performance (printer-friendly page)"),
            ("Format", "HTML table, parsed deterministically with BeautifulSoup"),
            ("As-of date", "March 31, 2025"),
            ("Records", "429 commitments"),
            ("Fields", "Fund name, vintage year, commitment, capital called, distributed, remaining value, net IRR, net multiple"),
            ("Confidence", "1.0 \u2014 structured HTML, zero parsing ambiguity"),
        ]),
        ("CalSTRS \u2014 California State Teachers\u2019 Retirement System", [
            ("Source", "calstrs.com \u2014 Private Equity Portfolio Performance Table"),
            ("Format", "PDF, parsed with pdfplumber word-level extraction"),
            ("As-of date", "June 30, 2025"),
            ("Records", "473 commitments"),
            ("Fields", "Fund name, vintage year, commitment, contributed, distributed, market value, net IRR, net multiple"),
            ("Confidence", "0.95 \u2014 structured PDF table with consistent column layout"),
        ]),
        ("WSIB \u2014 Washington State Investment Board", [
            ("Source", "sib.wa.gov \u2014 Quarterly Private Equity IRR Report (Q2 2025)"),
            ("Format", "PDF, parsed with pdfplumber word-level extraction"),
            ("As-of date", "June 30, 2025"),
            ("Records", "462 commitments"),
            ("Fields", "Fund name, commitment date, commitment, paid-in, unfunded, distributions, market value, total value, net multiple, net IRR"),
            ("Confidence", "0.90 \u2014 structured PDF; minor header artifacts, data rows parse cleanly"),
        ]),
        ("Oregon PERS \u2014 Oregon Public Employees Retirement System", [
            ("Source", "oregon.gov/treasury \u2014 OPERF Private Equity Portfolio (Q3 2025)"),
            ("Format", "PDF, parsed with pdfplumber word-level extraction"),
            ("As-of date", "September 30, 2025"),
            ("Records", "402 commitments"),
            ("Fields", "Fund name, vintage year, commitment, contributed, distributed, fair market value, total value multiple, net IRR"),
            ("Confidence", "0.95 \u2014 structured PDF table with dollar-denominated columns"),
        ]),
        ("NY Common \u2014 New York State Common Retirement Fund", [
            ("Source", "osc.ny.gov \u2014 Annual Asset Listing 2024 (pages 170\u2013175)"),
            ("Format", "PDF, parsed with pdfplumber word-level extraction"),
            ("As-of date", "March 31, 2024"),
            ("Records", "359 commitments"),
            ("Fields", "Fund name, date committed, commitment, contributed, distributions, fair value, total value, net multiple"),
            ("Confidence", "0.95 \u2014 structured PDF table; IRR not disclosed by this source"),
        ]),
    ]

    for name, rows in sources:
        story.append(KeepTogether([make_source_table(name, rows), Spacer(1, 6)]))

    # --- Extraction Approach ---
    story.append(KeepTogether([
        P("Extraction Approach", h1_style),
        P("Deterministic Parsing \u2014 No LLM Dependency", h2_style),
        P(
            "Every record in this dataset was extracted using deterministic, rule-based parsing. "
            "No large language model was used to read, interpret, or generate any data values."
        ),
    ]))
    story.append(P(
        "<b>HTML sources</b> (CalPERS): Parsed with BeautifulSoup. Each table row maps directly "
        "to a commitment record. Field mapping is explicit and verifiable.", bullet_style
    ))
    story.append(P(
        "<b>PDF sources</b> (CalSTRS, WSIB, Oregon, NY Common): Parsed with pdfplumber\u2019s "
        "word-level extraction. Words are grouped into rows by y-coordinate proximity, then "
        "assigned to columns by x-position against source-specific column boundaries calibrated "
        "to each document\u2019s layout.", bullet_style
    ))
    story.append(Spacer(1, 2))
    story.append(P(
        "Every record carries its source URL, document name, extraction method, and a confidence "
        "score. The pipeline is fully reproducible \u2014 running it twice on the same source "
        "documents yields identical results."
    ))
    story.append(Spacer(1, 4))

    story.append(P("Field Completeness", h2_style))
    story.append(make_field_table())
    story.append(Spacer(1, 4))

    # --- Entity Resolution ---
    story.append(KeepTogether([
        P("Entity Resolution", h1_style),
        P(
            "When the same private equity fund appears in multiple pension portfolios \u2014 often "
            "under slightly different names \u2014 the pipeline links those records to a single "
            "canonical fund entity."
        ),
        P(
            "<b>Method:</b> Multi-signal fuzzy matching using the rapidfuzz library. "
            "A match requires <b>at least two</b> of the following signals to agree:"
        ),
        P(
            "<b>1. Name similarity</b> \u2014 Token-sort ratio above 85% between normalized fund names",
            bullet_style
        ),
        P(
            "<b>2. General partner match</b> \u2014 GP name similarity above 85%",
            bullet_style
        ),
        P(
            "<b>3. Vintage year match</b> \u2014 Exact year agreement",
            bullet_style
        ),
    ]))
    story.append(Spacer(1, 2))
    story.append(P(
        "Additional safeguards prevent false positives: fund sequence numbers "
        "(e.g., \u201cFund III\u201d vs. \u201cFund IV\u201d) must match exactly, "
        "strategy keywords (credit, Asia, Europe, infrastructure) must agree, "
        "and a minimum distinctiveness threshold filters out matches on generic terms."
    ))
    story.append(P(
        "<b>Result:</b> 345 funds are independently cross-linked across two or more pension systems. "
        "All matching decisions are logged with the raw input name, resolved fund ID, match type, "
        "and similarity score for full auditability."
    ))

    # --- Cross-System Validation ---
    story.append(KeepTogether([
        P("Cross-System Validation", h1_style),
        P(
            "Where the same fund appears across multiple pension portfolios, reported performance "
            "metrics serve as an independent consistency check."
        ),
        P(
            "<b>Net IRR agreement:</b> For funds reported by two or more systems with overlapping "
            "reporting periods, 95% of paired IRR values agree within 2 percentage points \u2014 "
            "consistent with expected differences in reporting date and fee structures.",
            bullet_style
        ),
        P(
            "<b>Vintage year agreement:</b> 100% exact match across all cross-linked funds.",
            bullet_style
        ),
        P(
            "<b>Commitment reasonableness:</b> All values validated against expected ranges "
            "($1M\u2013$5B per commitment, multiples 0.5x\u20134.0x, IRRs -20% to +50%). "
            "Outliers flagged for manual review.",
            bullet_style
        ),
    ]))

    # --- Provenance ---
    story.append(KeepTogether([
        P("Provenance &amp; Reproducibility", h1_style),
        P("Every commitment record in the database carries:"),
        P("Source URL pointing to the original government disclosure", bullet_style),
        P("Document name and page number (for PDFs)", bullet_style),
        P("Extraction method (deterministic_html or deterministic_pdf)", bullet_style),
        P("Extraction confidence score (0.90\u20131.0)", bullet_style),
        P("Extraction timestamp and as-of reporting date", bullet_style),
        Spacer(1, 4),
        P(
            "The pipeline is idempotent \u2014 re-running against the same source documents produces "
            "identical output with no duplicate records. Each run is logged with records extracted, "
            "updated, and flagged."
        ),
    ]))

    # Footer
    story.append(Spacer(1, 12))
    story.append(HRFlowable(width="100%", thickness=0.5, color=BORDER, spaceAfter=6))
    story.append(P(
        "Pipeline built in Python. Data stored in SQLite with full relational schema. "
        "Exports available as CSV or direct database query.",
        small_style
    ))

    doc.build(story)
    print(f"PDF written to {OUTPUT}")


if __name__ == "__main__":
    build()
