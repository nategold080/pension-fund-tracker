"""Generate revised one-page Methodology PDF for Dakota Marketplace."""

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

OUTPUT_PATH = "Pension_Fund_Commitment_Data_Methodology.pdf"

# ── Colors ────────────────────────────────────────────────────────────────

NAVY = colors.HexColor("#1B2A4A")
ACCENT = colors.HexColor("#4472C4")
DARK_TEXT = colors.HexColor("#2D2D2D")
MED_TEXT = colors.HexColor("#555555")
LIGHT_BG = colors.HexColor("#F2F4F7")
RULE_COLOR = colors.HexColor("#D9DCE3")

# ── Styles ────────────────────────────────────────────────────────────────

style_title = ParagraphStyle(
    "Title", fontName="Helvetica-Bold", fontSize=16, textColor=NAVY,
    leading=19, spaceAfter=1,
)
style_contact = ParagraphStyle(
    "Contact", fontName="Helvetica", fontSize=8.5, textColor=MED_TEXT,
    leading=11, spaceAfter=0,
)
style_section = ParagraphStyle(
    "Section", fontName="Helvetica-Bold", fontSize=10, textColor=NAVY,
    leading=13, spaceBefore=8, spaceAfter=3,
)
style_body = ParagraphStyle(
    "Body", fontName="Helvetica", fontSize=8.5, textColor=DARK_TEXT,
    leading=12, spaceAfter=1,
)
style_stat_label = ParagraphStyle(
    "StatLabel", fontName="Helvetica", fontSize=8, textColor=MED_TEXT,
    leading=10,
)
style_stat_value = ParagraphStyle(
    "StatValue", fontName="Helvetica-Bold", fontSize=8, textColor=DARK_TEXT,
    leading=10,
)
style_use_case = ParagraphStyle(
    "UseCase", fontName="Helvetica", fontSize=8.5, textColor=DARK_TEXT,
    leading=12, spaceBefore=1, spaceAfter=1, leftIndent=12,
)
style_nd_note = ParagraphStyle(
    "NDNote", fontName="Helvetica-Oblique", fontSize=7.5, textColor=MED_TEXT,
    leading=10, spaceBefore=3, spaceAfter=0,
)
style_footer = ParagraphStyle(
    "Footer", fontName="Helvetica-Oblique", fontSize=7, textColor=MED_TEXT,
    leading=9, alignment=1,
)


def build_pdf():
    doc = SimpleDocTemplate(
        OUTPUT_PATH, pagesize=letter,
        topMargin=0.45 * inch, bottomMargin=0.35 * inch,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
    )

    story = []
    W = doc.width

    # ── Header ────────────────────────────────────────────────────────────
    story.append(Paragraph("Pension Fund Commitment Intelligence", style_title))
    story.append(Paragraph(
        "Nathan Goldberg&nbsp;&nbsp;|&nbsp;&nbsp;"
        "nathanmauricegoldberg@gmail.com&nbsp;&nbsp;|&nbsp;&nbsp;"
        "February 19, 2026",
        style_contact,
    ))

    rule = Table([[""]], colWidths=[W], rowHeights=[1])
    rule.setStyle(TableStyle([("LINEBELOW", (0, 0), (-1, -1), 1.5, ACCENT)]))
    story.append(Spacer(1, 4))
    story.append(rule)
    story.append(Spacer(1, 6))

    # ── What This Is ──────────────────────────────────────────────────────
    story.append(Paragraph("What This Is", style_section))
    story.append(Paragraph(
        "Structured commitment-level data extracted from public pension fund "
        "board documents — quarterly investment reports, portfolio performance "
        "reviews, and statutory disclosure filings. Each record represents a "
        "single LP commitment to a specific GP fund with standardized fields "
        "for cross-allocator analysis.",
        style_body,
    ))
    story.append(Spacer(1, 3))

    # ── Sample Coverage ───────────────────────────────────────────────────
    story.append(Paragraph("Sample Coverage", style_section))

    stats = [
        ["Pension Systems", "5 (CalPERS, CalSTRS, NY Common, Oregon, WSIB)"],
        ["Records", "117 verified commitment records"],
        ["Unique GPs / Funds", "23 GPs across 76 funds"],
        ["Vintage Range", "2015 – 2025"],
        ["Asset Classes", "Private Equity, Private Credit, Real Assets"],
    ]
    stat_data = [[Paragraph(l, style_stat_label), Paragraph(v, style_stat_value)]
                 for l, v in stats]
    stat_table = Table(stat_data, colWidths=[1.15 * inch, W - 1.15 * inch])
    stat_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 1.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
        ("LEFTPADDING", (0, 0), (0, -1), 0),
        ("LINEBELOW", (0, 0), (-1, -2), 0.5, RULE_COLOR),
    ]))
    story.append(stat_table)

    story.append(Paragraph(
        "Fields marked N/D were not published in the source document. Disclosure "
        "granularity varies by pension system; some systems report full cash flow "
        "and performance data while others publish commitment-level records only.",
        style_nd_note,
    ))
    story.append(Spacer(1, 3))

    # ── Extraction & Structuring ──────────────────────────────────────────
    story.append(Paragraph("Extraction &amp; Structuring", style_section))
    story.append(Paragraph(
        "An automated pipeline processes public pension documents — HTML tables "
        "for web-published data, word-level PDF extraction for document filings. "
        "All parsing is deterministic and rule-based, not LLM-generated. GP and "
        "fund names are entity-resolved across systems using a canonical registry "
        "with alias mapping, so the same fund committed to by CalPERS and Oregon "
        "links to a single record. Each commitment is flagged as a new "
        "relationship or re-up based on historical GP commitment patterns.",
        style_body,
    ))
    story.append(Spacer(1, 3))

    # ── Cross-Allocator Intelligence ──────────────────────────────────────
    story.append(Paragraph("Cross-Allocator Intelligence", style_section))
    story.append(Paragraph(
        "<b>8 GPs appear across all 5 pension systems:</b> Blackstone, KKR, "
        "Hellman &amp; Friedman, Francisco Partners, Thoma Bravo, TA Associates, "
        "TPG, Centerbridge. 13 GPs total appear across 3 or more systems.",
        style_body,
    ))
    story.append(Paragraph(
        "This cross-linkage enables identification of which managers are winning "
        "allocations across multiple large LPs and reveals convergent deployment "
        "patterns across the largest U.S. public pension programs.",
        style_body,
    ))
    story.append(Spacer(1, 3))

    # ── Use Cases for Fundraising Teams ───────────────────────────────────
    story.append(Paragraph("Use Cases for Fundraising Teams", style_section))
    story.append(Paragraph(
        "Identify which pensions are actively deploying to a specific strategy "
        "or asset class.",
        style_use_case,
    ))
    story.append(Paragraph(
        "Track re-up patterns to gauge LP satisfaction with existing GP "
        "relationships.",
        style_use_case,
    ))
    story.append(Paragraph(
        "See which investment consultants (Meketa, Callan, Wilshire) are driving "
        "allocations and to whom.",
        style_use_case,
    ))
    story.append(Spacer(1, 3))

    # ── Coverage & Scalability ────────────────────────────────────────────
    story.append(Paragraph("Coverage &amp; Scalability", style_section))
    story.append(Paragraph(
        "The 5 systems shown here are a proof of concept — the pipeline is "
        "modular, with each pension system as a self-contained adapter. "
        "Disclosure formats across U.S. public pensions follow common patterns, "
        "so each new integration is faster than the last. Adding a system like "
        "Texas TRS, Florida SBA, or Virginia Retirement System is a matter of "
        "days, not weeks. Next targets include those three plus Pennsylvania "
        "PSERS and Ohio STRS, with the infrastructure already in place to go "
        "well beyond that.",
        style_body,
    ))

    story.append(Spacer(1, 10))

    # ── Footer ────────────────────────────────────────────────────────────
    story.append(Paragraph(
        "CONFIDENTIAL — Prepared for Dakota Marketplace evaluation purposes only.",
        style_footer,
    ))

    doc.build(story)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
