"""Generate 'Board Minutes Intelligence: Methodology & Sample Extraction' PDF."""

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    KeepTogether,
)

OUTPUT_PATH = "Board_Minutes_Intelligence_Methodology.pdf"

# ── Colors ────────────────────────────────────────────────────────────────

NAVY = colors.HexColor("#1B2A4A")
ACCENT = colors.HexColor("#4472C4")
DARK_TEXT = colors.HexColor("#2D2D2D")
MED_TEXT = colors.HexColor("#555555")
LIGHT_BG = colors.HexColor("#F2F4F7")
LIGHT_ACCENT = colors.HexColor("#E8EEF7")
RULE_COLOR = colors.HexColor("#D9DCE3")
GREEN = colors.HexColor("#2E7D32")
AMBER = colors.HexColor("#E65100")

# ── Styles ────────────────────────────────────────────────────────────────

style_title = ParagraphStyle(
    "Title", fontName="Helvetica-Bold", fontSize=17, textColor=NAVY,
    leading=21, spaceAfter=1,
)
style_subtitle = ParagraphStyle(
    "Subtitle", fontName="Helvetica", fontSize=10, textColor=MED_TEXT,
    leading=13, spaceAfter=0,
)
style_contact = ParagraphStyle(
    "Contact", fontName="Helvetica", fontSize=8.5, textColor=MED_TEXT,
    leading=11, spaceAfter=0,
)
style_h1 = ParagraphStyle(
    "H1", fontName="Helvetica-Bold", fontSize=12, textColor=NAVY,
    leading=15, spaceBefore=14, spaceAfter=5,
)
style_h2 = ParagraphStyle(
    "H2", fontName="Helvetica-Bold", fontSize=10, textColor=NAVY,
    leading=13, spaceBefore=10, spaceAfter=3,
)
style_h3 = ParagraphStyle(
    "H3", fontName="Helvetica-Bold", fontSize=9, textColor=ACCENT,
    leading=12, spaceBefore=6, spaceAfter=2,
)
style_body = ParagraphStyle(
    "Body", fontName="Helvetica", fontSize=8.5, textColor=DARK_TEXT,
    leading=12, spaceAfter=2, alignment=TA_JUSTIFY,
)
style_body_tight = ParagraphStyle(
    "BodyTight", fontName="Helvetica", fontSize=8.5, textColor=DARK_TEXT,
    leading=11, spaceAfter=1, alignment=TA_JUSTIFY,
)
style_bullet = ParagraphStyle(
    "Bullet", fontName="Helvetica", fontSize=8.5, textColor=DARK_TEXT,
    leading=11, spaceAfter=1, leftIndent=16, bulletIndent=6,
    bulletFontName="Helvetica", bulletFontSize=8.5,
)
style_method = ParagraphStyle(
    "Method", fontName="Helvetica-Oblique", fontSize=7.5, textColor=MED_TEXT,
    leading=10, spaceBefore=1, spaceAfter=4, leftIndent=16,
)
style_callout = ParagraphStyle(
    "Callout", fontName="Helvetica-Oblique", fontSize=8, textColor=ACCENT,
    leading=11, spaceBefore=2, spaceAfter=2, leftIndent=8,
)
style_table_header = ParagraphStyle(
    "TH", fontName="Helvetica-Bold", fontSize=8, textColor=colors.white,
    leading=10, alignment=TA_CENTER,
)
style_table_cell = ParagraphStyle(
    "TD", fontName="Helvetica", fontSize=8, textColor=DARK_TEXT,
    leading=10, alignment=TA_LEFT,
)
style_table_cell_center = ParagraphStyle(
    "TDC", fontName="Helvetica", fontSize=8, textColor=DARK_TEXT,
    leading=10, alignment=TA_CENTER,
)
style_footer = ParagraphStyle(
    "Footer", fontName="Helvetica-Oblique", fontSize=7, textColor=MED_TEXT,
    leading=9, alignment=TA_CENTER,
)
style_stat_label = ParagraphStyle(
    "StatLabel", fontName="Helvetica", fontSize=8, textColor=MED_TEXT,
    leading=10,
)
style_stat_value = ParagraphStyle(
    "StatValue", fontName="Helvetica-Bold", fontSize=8, textColor=DARK_TEXT,
    leading=10,
)
style_event_label = ParagraphStyle(
    "EventLabel", fontName="Helvetica-Bold", fontSize=8, textColor=NAVY,
    leading=10,
)
style_event_detail = ParagraphStyle(
    "EventDetail", fontName="Helvetica", fontSize=8, textColor=DARK_TEXT,
    leading=10,
)


def _rule(width, weight=1.5, color=ACCENT):
    """Create a horizontal rule."""
    r = Table([[""]], colWidths=[width], rowHeights=[1])
    r.setStyle(TableStyle([("LINEBELOW", (0, 0), (-1, -1), weight, color)]))
    return r


def _thin_rule(width):
    return _rule(width, weight=0.5, color=RULE_COLOR)


def _method_note(text):
    """Methodology note (italic, gray, indented)."""
    return Paragraph(f"<i>Extraction method: {text}</i>", style_method)


def _event_row(label, detail):
    """Single event as a mini-table row."""
    return [
        Paragraph(label, style_event_label),
        Paragraph(detail, style_event_detail),
    ]


def build_pdf():
    doc = SimpleDocTemplate(
        OUTPUT_PATH, pagesize=letter,
        topMargin=0.5 * inch, bottomMargin=0.4 * inch,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
    )

    story = []
    W = doc.width

    # ── Title Block ───────────────────────────────────────────────────────

    story.append(Paragraph("Board Minutes Intelligence", style_title))
    story.append(Paragraph(
        "Methodology &amp; Sample Extraction",
        style_subtitle,
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Nathan Goldberg&nbsp;&nbsp;|&nbsp;&nbsp;"
        "nathanmauricegoldberg@gmail.com&nbsp;&nbsp;|&nbsp;&nbsp;"
        "February 2026",
        style_contact,
    ))
    story.append(Spacer(1, 4))
    story.append(_rule(W))
    story.append(Spacer(1, 8))

    # ── Overview ──────────────────────────────────────────────────────────

    story.append(Paragraph("Overview", style_h1))
    story.append(Paragraph(
        "Public pension fund board meetings generate hundreds of pages of "
        "material each quarter (formal minutes, staff presentations, consultant "
        "reports, and policy recommendations). Buried inside are "
        "forward-looking allocation signals that are extremely valuable "
        "to fundraising teams: new asset class targets, manager hire/fire "
        "decisions, commitment pacing plans, and strategic direction changes. "
        "Today this intelligence is extracted manually, if at all.",
        style_body,
    ))
    story.append(Paragraph(
        "Our NLP pipeline systematically extracts structured events from "
        "these board packets using deterministic, rule-based pattern "
        "matching, not large language models. Regex-based extractors "
        "identify investment commitments, vote outcomes, personnel changes, "
        "and policy approvals, while spaCy named entity recognition tags "
        "organizations, dollar amounts, and key individuals. Every "
        "extraction carries provenance metadata: source document, page "
        "number, section header, extraction pattern, and confidence score.",
        style_body,
    ))
    story.append(Paragraph(
        "The result is a structured feed of pension fund board actions, "
        "refreshed after each public meeting, that turns hundreds of "
        "pages of PDF into a dozen actionable signals per meeting. The "
        "following pages walk through three real board meetings to "
        "demonstrate the extraction pipeline and the intelligence it produces.",
        style_body,
    ))

    story.append(Spacer(1, 6))
    story.append(_thin_rule(W))

    # ── Signal Categories Table ───────────────────────────────────────────

    story.append(Paragraph("What We Extract", style_h2))

    cat_data = [
        [Paragraph("Signal Category", style_table_header),
         Paragraph("Description", style_table_header),
         Paragraph("Example", style_table_header)],
        [Paragraph("Investment Commitments", style_table_cell),
         Paragraph("Named fund, dollar amount, GP, vote outcome", style_table_cell),
         Paragraph("$400M to TowerBrook Structured Opps IV", style_table_cell)],
        [Paragraph("Manager Selections", style_table_cell),
         Paragraph("New manager hired for a specific mandate", style_table_cell),
         Paragraph("PineStone Asset Mgmt for global equity", style_table_cell)],
        [Paragraph("Personnel Changes", style_table_cell),
         Paragraph("Officer elections, departures, vacancies", style_table_cell),
         Paragraph("Makowski elected Board Chair", style_table_cell)],
        [Paragraph("Policy / SAA Changes", style_table_cell),
         Paragraph("Asset allocation target shifts, new asset classes", style_table_cell),
         Paragraph("New 3% private credit allocation", style_table_cell)],
        [Paragraph("Pacing &amp; Deployment Plans", style_table_cell),
         Paragraph("Annual commitment targets by asset class", style_table_cell),
         Paragraph("$2.5B PE commitment target for 2026", style_table_cell)],
        [Paragraph("Dissent &amp; Sentiment", style_table_cell),
         Paragraph("Board member objections, read-into-record statements", style_table_cell),
         Paragraph("Treasurer dissent on private markets overweight", style_table_cell)],
    ]
    cat_table = Table(cat_data, colWidths=[1.4 * inch, 2.5 * inch, W - 3.9 * inch])
    cat_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(cat_table)

    story.append(Spacer(1, 6))

    # ── PAGE BREAK — Meeting 1 ────────────────────────────────────────────
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # MEETING 1: WSIB September 2025
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph("Sample Extraction 1", style_h1))

    # Meeting info box
    info_data = [
        [Paragraph("<b>Pension System</b>", style_stat_label),
         Paragraph("Washington State Investment Board (WSIB)", style_stat_value),
         Paragraph("<b>Meeting Date</b>", style_stat_label),
         Paragraph("September 18, 2025", style_stat_value)],
        [Paragraph("<b>Document</b>", style_stat_label),
         Paragraph("Board Meeting Minutes (126 pages)", style_stat_value),
         Paragraph("<b>Events Extracted</b>", style_stat_label),
         Paragraph("14 structured events", style_stat_value)],
    ]
    info_table = Table(info_data, colWidths=[1.1 * inch, 2.3 * inch, 1.1 * inch, W - 4.5 * inch])
    info_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("BOX", (0, 0), (-1, -1), 0.5, ACCENT),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 6))

    # -- Personnel Changes --
    story.append(Paragraph("Personnel Changes", style_h3))

    p_data = [
        [Paragraph("<b>Event</b>", style_table_header),
         Paragraph("<b>Person</b>", style_table_header),
         Paragraph("<b>New Role</b>", style_table_header),
         Paragraph("<b>Vote</b>", style_table_header)],
        [Paragraph("Officer Election", style_table_cell),
         Paragraph("Rick Makowski", style_table_cell),
         Paragraph("Board Chair", style_table_cell_center),
         Paragraph("By acclamation", style_table_cell_center)],
        [Paragraph("Officer Election", style_table_cell),
         Paragraph("Jill Ketelsen", style_table_cell),
         Paragraph("Vice Chair", style_table_cell_center),
         Paragraph("By acclamation", style_table_cell_center)],
        [Paragraph("Committee Appointment", style_table_cell),
         Paragraph("Rick Makowski", style_table_cell),
         Paragraph("Private Markets Committee", style_table_cell_center),
         Paragraph("Carried unanimously", style_table_cell_center)],
        [Paragraph("Committee Removal", style_table_cell),
         Paragraph("Jill Ketelsen", style_table_cell),
         Paragraph("Private Markets Committee", style_table_cell_center),
         Paragraph("Carried unanimously", style_table_cell_center)],
    ]
    p_table = Table(p_data, colWidths=[1.4 * inch, 1.3 * inch, 2.0 * inch, W - 4.7 * inch])
    p_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(p_table)
    story.append(_method_note(
        "Regex patterns match 'elected/declared as [Role]' and "
        "'appoint/removed from [Committee]' structures in meeting narratives. "
        "Vote outcomes extracted by searching 800-char context window after each motion."
    ))

    # -- Investment Commitments --
    story.append(Paragraph("Investment Commitments", style_h3))

    i_data = [
        [Paragraph("<b>Fund</b>", style_table_header),
         Paragraph("<b>Amount</b>", style_table_header),
         Paragraph("<b>Strategy</b>", style_table_header),
         Paragraph("<b>GP Relationship</b>", style_table_header),
         Paragraph("<b>Vote</b>", style_table_header)],
        [Paragraph("Menlo Ventures XVII, L.P.", style_table_cell),
         Paragraph("$175M", style_table_cell_center),
         Paragraph("Early-stage VC", style_table_cell),
         Paragraph("Since 2000; 6 prior funds; $665M total", style_table_cell),
         Paragraph("Carried unanimously", style_table_cell_center)],
        [Paragraph("Menlo Inflection Fund IV", style_table_cell),
         Paragraph("$225M", style_table_cell_center),
         Paragraph("Late-stage / growth", style_table_cell),
         Paragraph("(Same GP)", style_table_cell),
         Paragraph("Carried unanimously", style_table_cell_center)],
    ]
    i_table = Table(i_data, colWidths=[1.6 * inch, 0.7 * inch, 1.2 * inch, 2.0 * inch, W - 5.5 * inch])
    i_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(i_table)
    story.append(_method_note(
        "Pattern: 'Board invest up to $[amount] [million/billion] in [Fund Name]'. "
        "GP relationship history extracted from nearby 'Since YYYY... committed... "
        "to NN prior funds' patterns. Fund descriptions captured from '[Fund] is a "
        "[description] fund' structures."
    ))

    # -- Manager Selection --
    story.append(Paragraph("Manager Selection", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>PineStone Asset Management</b> selected "
        "as active global equity strategy manager (carried unanimously)",
        style_bullet,
    ))
    story.append(_method_note(
        "Pattern: 'Board select [Manager] as [role/mandate description]'. "
        "Captures both the manager entity and the mandate type."
    ))

    # -- Forward-Looking Signals --
    story.append(Paragraph("Forward-Looking Allocation Signals", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Strategic Asset Allocation Study initiated.</b> "
        "Meketa presented 'From Modeling to Policy,' a full SAA review "
        "comparing current portfolio (25% PE, 18% RE, 0% private credit) against "
        "modeled alternatives with higher private market allocations (28% PE, "
        "21% RE, 3% private credit).",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Private credit education session.</b> "
        "Board received educational briefing on 'Private Credit Offers Many "
        "Benefits,' covering diversification, cash flow predictability, and "
        "downside protection. This precedes the November vote to add private "
        "credit as a new asset class.",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Decision factor voting exercise.</b> "
        "Board ranked return, risk, liquidity, complexity, and cost trade-offs "
        "in an advisory vote, providing quantitative signal on board risk "
        "appetite ahead of formal policy decisions.",
        style_bullet,
    ))
    story.append(_method_note(
        "Forward-looking signals identified via section detection (ALL CAPS "
        "headers, agenda numbering) combined with strategic keyword matching "
        "(asset allocation, private credit, policy recommendation). Tabular "
        "data from presentation slides extracted via PDF table parsing."
    ))

    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Fundraising signal: WSIB's September meeting telegraphed a coming "
        "private credit allocation months before the formal November vote. "
        "Teams tracking this pipeline would have had early notice to position.",
        style_callout,
    ))

    # ── PAGE BREAK — Meeting 2 ────────────────────────────────────────────
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # MEETING 2: WSIB November 2025
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph("Sample Extraction 2", style_h1))

    info_data2 = [
        [Paragraph("<b>Pension System</b>", style_stat_label),
         Paragraph("Washington State Investment Board (WSIB)", style_stat_value),
         Paragraph("<b>Meeting Date</b>", style_stat_label),
         Paragraph("November 20, 2025", style_stat_value)],
        [Paragraph("<b>Document</b>", style_stat_label),
         Paragraph("Board Meeting Minutes (124 pages)", style_stat_value),
         Paragraph("<b>Events Extracted</b>", style_stat_label),
         Paragraph("12 structured events", style_stat_value)],
    ]
    info_table2 = Table(info_data2, colWidths=[1.1 * inch, 2.3 * inch, 1.1 * inch, W - 4.5 * inch])
    info_table2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("BOX", (0, 0), (-1, -1), 0.5, ACCENT),
    ]))
    story.append(info_table2)
    story.append(Spacer(1, 6))

    # -- SAA Policy Change (the big signal) --
    story.append(Paragraph("Strategic Asset Allocation Change (Major Signal)", style_h3))
    story.append(Paragraph(
        "The Board approved a new Strategic Asset Allocation introducing "
        "<b>private credit at 3%</b>, a brand-new asset class for WSIB. "
        "The Staff Recommendation established the following target "
        "portfolio, effective 2026:",
        style_body_tight,
    ))

    saa_data = [
        [Paragraph("<b>Asset Class</b>", style_table_header),
         Paragraph("<b>Prior Target</b>", style_table_header),
         Paragraph("<b>New Target</b>", style_table_header),
         Paragraph("<b>Policy Range</b>", style_table_header),
         Paragraph("<b>Change</b>", style_table_header)],
        [Paragraph("Public Equity", style_table_cell),
         Paragraph("32%", style_table_cell_center),
         Paragraph("28%", style_table_cell_center),
         Paragraph("23 – 33%", style_table_cell_center),
         Paragraph("-4%", style_table_cell_center)],
        [Paragraph("Fixed Income", style_table_cell),
         Paragraph("20%", style_table_cell_center),
         Paragraph("19%", style_table_cell_center),
         Paragraph("14 – 24%", style_table_cell_center),
         Paragraph("-1%", style_table_cell_center)],
        [Paragraph("Private Equity", style_table_cell),
         Paragraph("25%", style_table_cell_center),
         Paragraph("23%", style_table_cell_center),
         Paragraph("18 – 28%", style_table_cell_center),
         Paragraph("-2%", style_table_cell_center)],
        [Paragraph("Private Credit", style_table_cell),
         Paragraph("-", style_table_cell_center),
         Paragraph("<b>3%</b>", style_table_cell_center),
         Paragraph("1 – 5%", style_table_cell_center),
         Paragraph("<b>+3% (new)</b>", style_table_cell_center)],
        [Paragraph("Real Estate", style_table_cell),
         Paragraph("18%", style_table_cell_center),
         Paragraph("18%", style_table_cell_center),
         Paragraph("13 – 23%", style_table_cell_center),
         Paragraph("-", style_table_cell_center)],
        [Paragraph("Tangible Assets", style_table_cell),
         Paragraph("5%", style_table_cell_center),
         Paragraph("9%", style_table_cell_center),
         Paragraph("4 – 14%", style_table_cell_center),
         Paragraph("+4%", style_table_cell_center)],
    ]
    saa_table = Table(saa_data, colWidths=[1.3 * inch, 1.0 * inch, 1.0 * inch, 1.2 * inch, W - 4.5 * inch])
    saa_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("BACKGROUND", (0, 4), (-1, 4), colors.HexColor("#E8F5E9")),  # Highlight new PC row
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(saa_table)
    story.append(_method_note(
        "SAA tables extracted from PDF slide content via section detection "
        "('COMMINGLED TRUST FUND STRATEGIC ASSET ALLOCATION' header). "
        "Before/after targets parsed from 'Staff Recommendation' and "
        "'Requested Action' presentation tables using pdfplumber table extraction."
    ))

    # -- Investment Commitment --
    story.append(Paragraph("Investment Commitment", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>TowerBrook Structured Opportunities Fund IV</b>, "
        "$400M commitment approved. Strategy: structured equity / turnaround "
        "fund. GP relationship since 2021, 1 prior fund.",
        style_bullet,
    ))
    story.append(_method_note(
        "Same commitment extraction pattern as Meeting 1. "
        "Amount, fund name, and GP history captured from motion text "
        "and surrounding narrative context."
    ))

    # -- Policy Approvals --
    story.append(Paragraph("Policy Approvals", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2026 Real Estate Annual Plan</b>, approved as proposed",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2026 Private Equity Annual Plan</b>, approved as proposed",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2026 Public Equity Annual Plan</b>, approved as proposed",
        style_bullet,
    ))
    story.append(_method_note(
        "Pattern: 'Board approve [policy description] as proposed/presented'. "
        "Annual plan approvals are significant, as they authorize staff to "
        "deploy capital in each asset class for the coming year."
    ))

    # -- Dissent --
    story.append(Paragraph("Dissenting Statement", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Treasurer Mike Pellicciotti</b> read a formal "
        "statement into the record opposing the SAA change, citing concerns "
        "about private markets overweight, liquidity risk, and fee levels. "
        "Sentiment analysis: negative (6 negative signals, 1 positive). "
        "Motion carried with Treasurer opposed.",
        style_bullet,
    ))
    story.append(_method_note(
        "Dissent identified via pattern: '[Title] [Name] read the following "
        "statement into the record'. Statement boundary detected by scanning "
        "for next section header or motion. Sentiment scored by counting "
        "pension-specific positive/negative keywords (risk, concern, "
        "overweight, costly vs. strong, prudent, diversified)."
    ))

    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Fundraising signal: The 3% private credit allocation across WSIB's "
        "$180B+ portfolio implies approximately $5B+ in new private credit "
        "deployment over the buildout period. This is actionable intelligence "
        "for any credit fund in market.",
        style_callout,
    ))

    # ── PAGE BREAK — Meeting 3 ────────────────────────────────────────────
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # MEETING 3: Oregon January 2026
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph("Sample Extraction 3", style_h1))

    info_data3 = [
        [Paragraph("<b>Pension System</b>", style_stat_label),
         Paragraph("Oregon Investment Council (OPERF)", style_stat_value),
         Paragraph("<b>Meeting Date</b>", style_stat_label),
         Paragraph("January 21, 2026", style_stat_value)],
        [Paragraph("<b>Document</b>", style_stat_label),
         Paragraph("OIC Meeting Materials (103 pages)", style_stat_value),
         Paragraph("<b>Events Extracted</b>", style_stat_label),
         Paragraph("11 structured events", style_stat_value)],
    ]
    info_table3 = Table(info_data3, colWidths=[1.1 * inch, 2.3 * inch, 1.1 * inch, W - 4.5 * inch])
    info_table3.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("BOX", (0, 0), (-1, -1), 0.5, ACCENT),
    ]))
    story.append(info_table3)
    story.append(Spacer(1, 6))

    # -- Commitment Approvals --
    story.append(Paragraph("December 2025 Commitment Approvals", style_h3))
    story.append(Paragraph(
        "The PE Annual Review presented commitments approved by staff "
        "in December 2025, including named GPs, exact dollar amounts, and "
        "fund vehicles:",
        style_body_tight,
    ))

    c_data = [
        [Paragraph("<b>GP</b>", style_table_header),
         Paragraph("<b>Fund</b>", style_table_header),
         Paragraph("<b>Amount</b>", style_table_header),
         Paragraph("<b>Strategy</b>", style_table_header)],
        [Paragraph("Advent International", style_table_cell),
         Paragraph("GPE XI", style_table_cell),
         Paragraph("$250M", style_table_cell_center),
         Paragraph("Global PE Buyout", style_table_cell)],
        [Paragraph("Advent International", style_table_cell),
         Paragraph("Advent Tech III", style_table_cell),
         Paragraph("$100M", style_table_cell_center),
         Paragraph("Tech Buyout", style_table_cell)],
        [Paragraph("TPG Capital", style_table_cell),
         Paragraph("TPG Partners X", style_table_cell),
         Paragraph("$213M", style_table_cell_center),
         Paragraph("Large-Cap Buyout", style_table_cell)],
        [Paragraph("TPG Capital", style_table_cell),
         Paragraph("TPG Healthcare III", style_table_cell),
         Paragraph("$38M", style_table_cell_center),
         Paragraph("Sector PE", style_table_cell)],
        [Paragraph("Francisco Partners", style_table_cell),
         Paragraph("FP Fund VIII", style_table_cell),
         Paragraph("$250M", style_table_cell_center),
         Paragraph("Tech PE", style_table_cell)],
        [Paragraph("Francisco Partners", style_table_cell),
         Paragraph("Agility IV", style_table_cell),
         Paragraph("$100M", style_table_cell_center),
         Paragraph("Tech Growth", style_table_cell)],
        [Paragraph("General Atlantic", style_table_cell),
         Paragraph("SMA", style_table_cell),
         Paragraph("$200M", style_table_cell_center),
         Paragraph("Growth Equity", style_table_cell)],
        [Paragraph("Pathway Capital", style_table_cell),
         Paragraph("PPEF III Co-Invest", style_table_cell),
         Paragraph("$500M", style_table_cell_center),
         Paragraph("Co-Investment Program", style_table_cell)],
    ]
    c_table = Table(c_data, colWidths=[1.5 * inch, 1.6 * inch, 0.8 * inch, W - 3.9 * inch])
    c_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(c_table)

    story.append(Paragraph(
        "<b>Total: $1.65B</b> across 6 GPs and 8 fund vehicles "
        "(includes $500M co-investment recycling commitment via Pathway).",
        style_body_tight,
    ))
    story.append(_method_note(
        "Oregon-specific pattern matches structured commitment listings "
        "in committee reports: '[Date] [Fund Name] $[Amount]M USD'. "
        "Also captured from tabular data in PE Annual Review slides "
        "using pdfplumber table extraction. Amount sanity-checked "
        "against $1M–$5B range."
    ))

    # -- Pacing Plan / Forward-Looking --
    story.append(Paragraph("2026 Pacing Plan &amp; Deployment Targets", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2026 commitment target:</b> Lower end of "
        "$2.5–3.5B range. Staff notes: 'commitment sizing and number of "
        "selections will be reflective of expected slower commitment pacing.'",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2025 deployment shortfall:</b> Committed only "
        "$1.7B against $2.5–3.5B target, a signal that deployment capacity "
        "rolled forward.",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Portfolio positioning priorities:</b> Primary "
        "funds ~80%, co-investments growing to ~20% target. Buyout 80–85%, "
        "Growth/VC 15–20%. Geography: 70% NA / 23% Europe / 7% Asia.",
        style_bullet,
    ))
    story.append(_method_note(
        "Pacing data extracted from 'PE Annual Review' section via keyword "
        "matching (pacing, target, commitment, deployment). Dollar amounts "
        "and percentage targets parsed from surrounding context. "
        "Year-over-year shortfall calculated by comparing stated target "
        "vs. actual deployment figures."
    ))

    # -- Personnel Signals --
    story.append(Paragraph("Personnel &amp; Organizational Signals", style_h3))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Senior Investment Officer for PE (VACANT).</b> "
        "Org chart shows this role unfilled, with search to be conducted in "
        "2026. Indicates potential organizational bottleneck or shift in PE "
        "strategy leadership.",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>Tad Fergusson</b>, Director of PE, joined "
        "July 2025. New PE leadership (under 1 year tenure) driving 2026 "
        "annual plan.",
        style_bullet,
    ))
    story.append(Paragraph(
        "<bullet>&bull;</bullet> <b>2026 initiative:</b> 'Increase lower-middle "
        "and middle-market exposure,' an explicit strategy shift signaling "
        "demand for mid-market GPs.",
        style_bullet,
    ))
    story.append(_method_note(
        "Personnel signals extracted from org chart text (VACANT keyword "
        "detection), named entity recognition (spaCy PERSON entities + "
        "role titles), and strategic initiative bullet parsing."
    ))

    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Fundraising signal: Oregon's explicit commitment table names the "
        "exact GPs winning allocations, the dollar amounts, and the "
        "strategy categories. The stated priority shift toward mid-market "
        "and increased co-investment is directly actionable for placement "
        "teams targeting Oregon OPERF.",
        style_callout,
    ))

    # ── PAGE BREAK — Coverage & Quality ────────────────────────────────────
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # CLOSING: Coverage, Cadence, Quality
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph("Coverage, Refresh Cadence &amp; Data Quality", style_h1))

    # -- Current Coverage --
    story.append(Paragraph("Current Coverage", style_h2))

    cov_data = [
        [Paragraph("<b>Pension System</b>", style_table_header),
         Paragraph("<b>AUM</b>", style_table_header),
         Paragraph("<b>Meeting Frequency</b>", style_table_header),
         Paragraph("<b>Document Type</b>", style_table_header),
         Paragraph("<b>Signal Richness</b>", style_table_header)],
        [Paragraph("WSIB (WA)", style_table_cell),
         Paragraph("$180B+", style_table_cell_center),
         Paragraph("Quarterly", style_table_cell_center),
         Paragraph("Minutes + Presentations", style_table_cell),
         Paragraph("Very High", style_table_cell_center)],
        [Paragraph("Oregon OPERF", style_table_cell),
         Paragraph("$100B+", style_table_cell_center),
         Paragraph("Monthly", style_table_cell_center),
         Paragraph("Minutes + Annual Reviews", style_table_cell),
         Paragraph("Very High", style_table_cell_center)],
        [Paragraph("CalPERS", style_table_cell),
         Paragraph("$500B+", style_table_cell_center),
         Paragraph("Monthly", style_table_cell_center),
         Paragraph("Minutes + Board Actions", style_table_cell),
         Paragraph("High", style_table_cell_center)],
        [Paragraph("CalSTRS", style_table_cell),
         Paragraph("$340B+", style_table_cell_center),
         Paragraph("Bi-monthly", style_table_cell_center),
         Paragraph("Minutes + Investment Reports", style_table_cell),
         Paragraph("High", style_table_cell_center)],
        [Paragraph("NY Common", style_table_cell),
         Paragraph("$268B+", style_table_cell_center),
         Paragraph("Quarterly", style_table_cell_center),
         Paragraph("Transaction Reports", style_table_cell),
         Paragraph("Medium", style_table_cell_center)],
    ]
    cov_table = Table(cov_data, colWidths=[1.1 * inch, 0.7 * inch, 1.1 * inch, 1.7 * inch, W - 4.6 * inch])
    cov_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(cov_table)
    story.append(Spacer(1, 4))

    story.append(Paragraph(
        "The five pension systems above represent over <b>$1.4 trillion</b> "
        "in combined AUM. Board meeting documents are publicly available "
        "on each system's website and published within days of each meeting.",
        style_body,
    ))

    # -- Refresh Cadence --
    story.append(Paragraph("Refresh Cadence", style_h2))
    story.append(Paragraph(
        "Board meeting minutes and materials are published on a predictable "
        "schedule. Our pipeline monitors source URLs and processes new "
        "documents within 24 hours of publication:",
        style_body,
    ))

    cadence_data = [
        [Paragraph("<b>Activity</b>", style_table_header),
         Paragraph("<b>Frequency</b>", style_table_header),
         Paragraph("<b>Detail</b>", style_table_header)],
        [Paragraph("Source monitoring", style_table_cell),
         Paragraph("Daily", style_table_cell_center),
         Paragraph("Automated checks for new board documents", style_table_cell)],
        [Paragraph("Document processing", style_table_cell),
         Paragraph("Within 24 hours", style_table_cell_center),
         Paragraph("NLP extraction on new PDFs as published", style_table_cell)],
        [Paragraph("Data delivery", style_table_cell),
         Paragraph("Per meeting cycle", style_table_cell_center),
         Paragraph("Monthly (Oregon), quarterly (WSIB), bi-monthly (CalSTRS)", style_table_cell)],
        [Paragraph("Coverage expansion", style_table_cell),
         Paragraph("Ongoing", style_table_cell_center),
         Paragraph("Each new system requires adapter development (days, not weeks)", style_table_cell)],
    ]
    cadence_table = Table(cadence_data, colWidths=[1.4 * inch, 1.2 * inch, W - 2.6 * inch])
    cadence_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(cadence_table)
    story.append(Spacer(1, 4))

    # -- Data Quality --
    story.append(Paragraph("Data Quality &amp; Methodology", style_h2))

    story.append(Paragraph(
        "<b>Extraction approach:</b> All extraction is deterministic and "
        "rule-based. We use regex pattern matching for structured events "
        "(commitments, motions, elections) and spaCy NER for entity "
        "tagging (organizations, dollar amounts, personnel). No LLM is "
        "used for primary extraction, ensuring reproducibility, "
        "auditability, and zero hallucination risk.",
        style_body,
    ))
    story.append(Paragraph(
        "<b>Confidence scoring:</b> Each extracted event carries a "
        "confidence score (0.0–1.0) based on pattern specificity and "
        "context validation. High-confidence events (>0.90) include "
        "named commitments with explicit dollar amounts and formal "
        "motions with recorded vote outcomes. Lower-confidence events "
        "are flagged for human review.",
        style_body,
    ))
    story.append(Paragraph(
        "<b>Provenance tracking:</b> Every data point includes its source "
        "document, page number, section header, and the extraction "
        "pattern that matched. This full audit trail means any extraction "
        "can be verified against the original source in seconds.",
        style_body,
    ))

    quality_stats = [
        [Paragraph("Metric", style_table_header),
         Paragraph("Value", style_table_header)],
        [Paragraph("Avg. events per meeting", style_table_cell),
         Paragraph("12–15 structured events", style_table_cell)],
        [Paragraph("Commitment extraction accuracy", style_table_cell),
         Paragraph("100% precision on named, dollar-valued commitments (validated sample, n=37)", style_table_cell)],
        [Paragraph("False positive rate", style_table_cell),
         Paragraph("<2% (proxy voting / agenda text filtered)", style_table_cell)],
        [Paragraph("Forward-looking signals per meeting", style_table_cell),
         Paragraph("3–8 actionable allocation signals", style_table_cell)],
        [Paragraph("Processing time per document", style_table_cell),
         Paragraph("<30 seconds (100+ page PDF)", style_table_cell)],
    ]
    q_table = Table(quality_stats, colWidths=[2.2 * inch, W - 2.2 * inch])
    q_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.5, RULE_COLOR),
    ]))
    story.append(Spacer(1, 2))
    story.append(q_table)

    # -- Scalability --
    story.append(Spacer(1, 6))
    story.append(Paragraph("Scalability", style_h2))
    story.append(Paragraph(
        "The pipeline is modular: each pension system has a self-contained "
        "adapter, and board minutes parsing is a shared NLP layer that works "
        "across document formats. Adding a new pension system's board "
        "minutes requires only pattern calibration for that system's "
        "document conventions. High-priority expansion targets include "
        "Texas TRS, Florida SBA, Virginia RS, Pennsylvania PSERS, and "
        "Ohio STRS, all of which publish board materials in formats "
        "our existing pipeline can process.",
        style_body,
    ))
    story.append(Paragraph(
        "Combined with our existing commitment-level data product (5 "
        "pension systems, 117 verified commitment records, entity-resolved "
        "across allocators), board minutes intelligence provides the "
        "forward-looking layer that transforms historical allocation data "
        "into a predictive fundraising tool.",
        style_body,
    ))

    # ── Footer ────────────────────────────────────────────────────────────
    story.append(Spacer(1, 16))
    story.append(_rule(W, weight=0.5, color=RULE_COLOR))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "CONFIDENTIAL: Prepared for Dakota Marketplace evaluation purposes only.",
        style_footer,
    ))

    doc.build(story)
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    build_pdf()
