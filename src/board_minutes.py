"""NLP extraction pipeline for pension fund board meeting minutes.

Extracts structured events from board meeting PDFs using deterministic
rule-based NLP — no LLM required. Handles:
  - Investment decisions (commitments, manager selections)
  - Personnel changes (elections, appointments, departures)
  - Motions and vote outcomes
  - Strategic / policy decisions
  - Performance metrics
  - Attendance rosters
  - Sentiment signals from dissenting statements and public comment
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path

import pdfplumber
import spacy

logger = logging.getLogger(__name__)

# ── Load spaCy model ─────────────────────────────────────────────────────

try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.warning("spaCy model not found — run: python -m spacy download en_core_web_sm")
    nlp = None


# ── Data classes ─────────────────────────────────────────────────────────

class EventType(str, Enum):
    INVESTMENT_COMMITMENT = "investment_commitment"
    MANAGER_SELECTION = "manager_selection"
    PERSONNEL_CHANGE = "personnel_change"
    COMMITTEE_ASSIGNMENT = "committee_assignment"
    POLICY_APPROVAL = "policy_approval"
    STRATEGIC_DECISION = "strategic_decision"
    PERFORMANCE_REPORT = "performance_report"
    DISSENT = "dissent"
    PUBLIC_COMMENT_SUMMARY = "public_comment_summary"
    MOTION = "motion"


@dataclass
class BoardEvent:
    """A single structured event extracted from board minutes."""
    event_type: EventType
    summary: str
    details: dict = field(default_factory=dict)
    section: str = ""
    page: int = 0
    confidence: float = 1.0
    raw_text: str = ""


@dataclass
class Attendee:
    name: str
    title: str = ""
    organization: str = ""
    role: str = ""  # "member", "staff", "presenter", "consultant", "public"


@dataclass
class MeetingMinutes:
    """Complete parsed output of a single board meeting."""
    pension_fund: str
    meeting_date: str
    document_path: str
    attendees: list[Attendee] = field(default_factory=list)
    events: list[BoardEvent] = field(default_factory=list)
    sections: list[dict] = field(default_factory=list)
    raw_text: str = ""


# ── PDF text extraction ──────────────────────────────────────────────────

def extract_text_from_pdf(path: str | Path) -> list[dict]:
    """Extract text from PDF, returning list of {page, text} dicts."""
    pages = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            pages.append({"page": i + 1, "text": text})
    return pages


def normalize_text(text: str) -> str:
    """Clean up PDF-extracted text: fix broken lines, normalize whitespace."""
    # Join lines that break mid-sentence (lowercase continuation after newline)
    text = re.sub(r"(\w)\n([a-z])", r"\1 \2", text)
    # Join lines that break mid-name (e.g., "Menlo\nVentures")
    text = re.sub(r"([A-Z][a-z]+)\n([A-Z][a-z]+)", r"\1 \2", text)
    # Normalize multiple spaces
    text = re.sub(r"  +", " ", text)
    return text


# ── Section detection ────────────────────────────────────────────────────

# WSIB-style section headers (ALL CAPS or bold-formatted keywords)
SECTION_PATTERNS = [
    # All-caps headers like "CALL TO ORDER", "PRIVATE MARKETS COMMITTEE REPORT"
    re.compile(r"^([A-Z][A-Z &/\-–()]{5,})$", re.MULTILINE),
    # Numbered agenda items: "1. Call to Order (9:30a)"
    re.compile(r"^\d+\.\s+([A-Z][^(\n]{5,})(?:\s*\([\d:]+)", re.MULTILINE),
    # Bold-style headers from Oregon: "Review & Approval of Minutes"
    re.compile(r"^((?:OPERF|CSF|CTF)[:\s].+)$", re.MULTILINE),
]

KNOWN_SECTIONS = [
    "CALL TO ORDER", "ADOPTION OF MINUTES", "PUBLIC COMMENT",
    "OFFICER ELECTIONS", "CEO REPORT", "AUDIT COMMITTEE REPORT",
    "PUBLIC MARKETS COMMITTEE REPORT", "PRIVATE MARKETS COMMITTEE REPORT",
    "ADMINISTRATIVE COMMITTEE REPORT", "EDUCATION SESSION",
    "QUARTERLY PERFORMANCE UPDATE", "EXECUTIVE SESSION",
    "REAL ESTATE ANNUAL PLAN", "PUBLIC EQUITY ANNUAL PLAN",
    "PRIVATE EQUITY ANNUAL PLAN", "CEM BENCHMARKING REPORT",
    "ANNUAL OFFICE OF THE STATE ACTUARY", "PEER ANALYSIS",
    "COMMINGLED TRUST FUND STRATEGIC ASSET ALLOCATION",
    "LABOR AND INDUSTRIES PORTFOLIO REVIEW",
    "PROXY VOTING", "ETHICS REVIEW", "GOVERNANCE",
    "COMMITTEE ASSIGNMENTS", "OTHER ITEMS", "ADJOURN",
    "COMMITTEE REPORTS", "WRITTEN PUBLIC COMMENT",
]


def detect_sections(full_text: str) -> list[dict]:
    """Split document into sections based on headers.

    Returns list of {title, start_pos, end_pos, text} dicts.
    """
    header_positions = []

    for pattern in SECTION_PATTERNS:
        for match in pattern.finditer(full_text):
            title = match.group(1).strip()
            # Filter out short matches, page footers, "APPROVED", etc.
            if len(title) < 8 or title in ("APPROVED",):
                continue
            header_positions.append((match.start(), title))

    # Also look for known section keywords preceded by newlines
    for section_name in KNOWN_SECTIONS:
        for match in re.finditer(
            rf"\n({re.escape(section_name)}[^\n]*)", full_text, re.IGNORECASE
        ):
            header_positions.append((match.start(), match.group(1).strip()))

    # Deduplicate and sort by position
    seen = set()
    unique = []
    for pos, title in sorted(header_positions):
        norm = title.upper()[:40]
        if norm not in seen:
            seen.add(norm)
            unique.append((pos, title))

    # Build sections
    sections = []
    for i, (pos, title) in enumerate(unique):
        end = unique[i + 1][0] if i + 1 < len(unique) else len(full_text)
        sections.append({
            "title": title,
            "start_pos": pos,
            "end_pos": end,
            "text": full_text[pos:end],
        })

    return sections


# ── Attendance extraction ────────────────────────────────────────────────

TITLE_PATTERNS = re.compile(
    r",?\s*(Chief (?:Executive|Investment) Officer|"
    r"(?:Assistant )?Senior Investment Officer|"
    r"Investment Officer|Portfolio Manager|"
    r"(?:Asset )?Stewardship Officer|"
    r"(?:Assistant )?Corporate Governance Officer|"
    r"(?:Legal, )?Risk,? and Compliance Director|"
    r"Executive Assistant|Confidential Secretary|"
    r"(?:Incoming |Outgoing )?Chair|Vice Chair|"
    r"(?:State )?Treasurer|Senator|Representative|"
    r"Director of (?:Private Markets|Capital Markets)|"
    r"Senior Actuary|Managing (?:Director|Principal)|"
    r"Vice President|Partner|Portfolio (?:Senior )?Analyst)"
    r"(?:\s*[-–]\s*[A-Za-z &]+)?",
    re.IGNORECASE,
)

ORG_PATTERNS = re.compile(
    r",?\s*(?:Attorney General's Office|"
    r"(?:Meketa|Callan|CEM|Glass Lewis|Albourne|Aksia|Aon|Pathway|"
    r"Oaktree|PineStone|TowerBrook|Menlo)[A-Za-z .]*|"
    r"Office of (?:the )?State (?:Actuary|Treasurer)|"
    r"Department of (?:Justice|Retirement Systems))",
    re.IGNORECASE,
)


def extract_attendance(text: str) -> list[Attendee]:
    """Extract attendance roster from meeting minutes header."""
    attendees = []

    # Find "Members Present:" block
    members_match = re.search(
        r"Members Present:\s*(.*?)(?=Members Absent:|Also Present:|Staff Present:|$)",
        text, re.DOTALL | re.IGNORECASE,
    )
    if members_match:
        block = members_match.group(1)
        for line in block.strip().split("\n"):
            name = line.strip().rstrip(",")
            if not name or len(name) < 3:
                continue
            title = ""
            title_match = TITLE_PATTERNS.search(name)
            if title_match:
                title = title_match.group(0).strip().lstrip(",").strip()
                name = name[:title_match.start()].strip().rstrip(",")
            attendees.append(Attendee(name=name, title=title, role="member"))

    # Find "Also Present:" / "Staff Present:" block
    staff_match = re.search(
        r"(?:Also|Staff) Present:\s*(.*?)(?=(?:Members Absent|Consultants Present|"
        r"PERS Present|Legal Counsel|Staff Participating|CALL TO ORDER|$))",
        text, re.DOTALL | re.IGNORECASE,
    )
    if staff_match:
        block = staff_match.group(1)
        for line in block.strip().split("\n"):
            name = line.strip().rstrip(",")
            if not name or len(name) < 3:
                continue
            title = ""
            org = ""
            title_match = TITLE_PATTERNS.search(name)
            if title_match:
                title = title_match.group(0).strip().lstrip(",").strip()
                name = name[:title_match.start()].strip().rstrip(",")
            org_match = ORG_PATTERNS.search(name)
            if org_match:
                org = org_match.group(0).strip().lstrip(",").strip()
                name = name[:org_match.start()].strip().rstrip(",")
            attendees.append(Attendee(
                name=name, title=title, organization=org, role="staff",
            ))

    # Find "Consultants Present:" block (Oregon style)
    cons_match = re.search(
        r"Consultants Present:\s*(.*?)(?=PERS Present|Legal Counsel|$)",
        text, re.DOTALL | re.IGNORECASE,
    )
    if cons_match:
        block = cons_match.group(1)
        for name in re.split(r",\s*", block.strip()):
            name = name.strip()
            if name and len(name) > 2:
                attendees.append(Attendee(name=name, role="consultant"))

    return attendees


# ── Investment commitment extraction ─────────────────────────────────────

# Matches: "Board invest up to $175 million, plus fees and expenses, in Menlo Ventures XVII, L.P."
INVESTMENT_MOTION_RE = re.compile(
    r"(?:Board|Committee)\s+(?:invest|commit)\s+up\s+to\s+"
    r"\$?([\d,.]+)\s*(million|billion)"
    r"[^,]*?(?:,\s*(?:plus fees and expenses,?\s*))?in\s+"
    r"([A-Z][A-Za-z0-9 &\-'.]+(?:\s+(?:[IVXLCDM]+|[0-9]+))?"
    r"(?:,?\s*L\.?P\.?)?)",
    re.IGNORECASE,
)

# Matches: "Board select PineStone Asset Management as an active global equity strategy manager"
MANAGER_SELECT_RE = re.compile(
    r"Board\s+select\s+([A-Z][A-Za-z0-9 &\-'.]+?)\s+as\s+(?:an?\s+)?"
    r"([a-z][a-z ]+(?:strategy|manager|consultant|advisor))",
    re.IGNORECASE,
)

# Fund description pattern: "[Fund], L.P. is a [description] fund with a target size of $X"
FUND_DESC_RE = re.compile(
    r"([A-Z][A-Za-z0-9 &\-'.]+(?:\s+[IVXLCDM]+)?(?:,?\s*L\.?P\.?)?)\s+"
    r"is\s+(?:a|an)\s+(.{10,200}?)\s+fund\s+"
    r"(?:with\s+a\s+target\s+size\s+of\s+\$?([\d,.]+)\s*(million|billion))?",
    re.IGNORECASE,
)

# Commitment history: "Since YYYY the WSIB has committed over $X.X billion to NN prior [GP] funds"
HISTORY_RE = re.compile(
    r"(?:Since|since)\s+(\d{4})\s+(?:the\s+)?(\w+)\s+has\s+committed\s+"
    r"(?:over\s+)?\$?([\d,.]+)\s*(million|billion)\s+to\s+(\d+)\s+prior",
    re.IGNORECASE,
)


def extract_investment_decisions(text: str, section: str = "") -> list[BoardEvent]:
    """Extract investment commitment events from text."""
    events = []

    for match in INVESTMENT_MOTION_RE.finditer(text):
        amount_str = match.group(1).replace(",", "")
        unit = match.group(2).lower()
        amount = float(amount_str)
        if unit in ("billion", "b"):
            amount *= 1000
        fund_name = match.group(3).strip().rstrip(",. ")

        # Look for fund description nearby
        desc = ""
        fund_desc_match = FUND_DESC_RE.search(text[max(0, match.end() - 50):match.end() + 500])
        if fund_desc_match:
            desc = fund_desc_match.group(2).strip()

        # Look for commitment history
        history = {}
        hist_match = HISTORY_RE.search(text[max(0, match.start() - 200):match.end() + 800])
        if hist_match:
            history = {
                "relationship_since": int(hist_match.group(1)),
                "total_committed_mm": float(hist_match.group(3).replace(",", "")) * (
                    1000 if hist_match.group(4).lower() in ("billion", "b") else 1
                ),
                "prior_fund_count": int(hist_match.group(5)),
            }

        events.append(BoardEvent(
            event_type=EventType.INVESTMENT_COMMITMENT,
            summary=f"Approved investment of up to ${amount:.0f}M in {fund_name}",
            details={
                "fund_name": fund_name,
                "amount_mm": amount,
                "description": desc,
                "history": history,
            },
            section=section,
            raw_text=match.group(0),
        ))

    for match in MANAGER_SELECT_RE.finditer(text):
        manager_name = match.group(1).strip()
        role = match.group(2).strip()

        events.append(BoardEvent(
            event_type=EventType.MANAGER_SELECTION,
            summary=f"Selected {manager_name} as {role}",
            details={"manager_name": manager_name, "role": role},
            section=section,
            raw_text=match.group(0),
        ))

    return events


# ── Motion / vote extraction ─────────────────────────────────────────────

# Motion pattern: bold text in minutes "X moved that the Board..."
MOTION_RE = re.compile(
    r"(\w[\w .]+?)\s+moved\s+that\s+the\s+Board\s+"
    r"((?:invest|select|approve|adopt|form|appoint|go into)[^.]*\.)",
    re.IGNORECASE,
)

# Vote outcomes
VOTE_UNANIMOUS_RE = re.compile(
    r"(?:The\s+)?motion\s+carried\s+unanimously",
    re.IGNORECASE,
)
VOTE_WITH_DISSENT_RE = re.compile(
    r"(?:The\s+)?motion\s+carried\s+with\s+(\w[\w .]+?)\s+opposed",
    re.IGNORECASE,
)
VOTE_ACCLAMATION_RE = re.compile(
    r"(\w[\w .]+?)\s+was\s+declared\s+\w+\s+(?:Chair|Vice Chair)\s+by\s+acclamation",
    re.IGNORECASE,
)

# "seconded" extraction
SECONDED_RE = re.compile(
    r"(\w[\w .]+?)\s+seconded\s+(?:the\s+)?motion",
    re.IGNORECASE,
)


def extract_motions(text: str, section: str = "") -> list[BoardEvent]:
    """Extract formal motions and their vote outcomes."""
    events = []

    for match in MOTION_RE.finditer(text):
        mover = match.group(1).strip()
        action = match.group(2).strip()

        # Look for seconded and vote outcome nearby (search further for outcome)
        context = text[match.start():min(match.end() + 800, len(text))]

        seconder = ""
        sec_match = SECONDED_RE.search(context)
        if sec_match:
            seconder = sec_match.group(1).strip()

        outcome = "unknown"
        dissenter = ""
        if VOTE_UNANIMOUS_RE.search(context):
            outcome = "carried_unanimously"
        else:
            dissent_match = VOTE_WITH_DISSENT_RE.search(context)
            if dissent_match:
                outcome = "carried_with_dissent"
                dissenter = dissent_match.group(1).strip()
            elif re.search(r"motion\s+carried", context, re.IGNORECASE):
                outcome = "carried"

        events.append(BoardEvent(
            event_type=EventType.MOTION,
            summary=f"Motion by {mover}: {action[:100]}",
            details={
                "mover": mover,
                "seconder": seconder,
                "action": action,
                "outcome": outcome,
                "dissenter": dissenter,
            },
            section=section,
            raw_text=context[:500],
        ))

    return events


# ── Personnel change extraction ──────────────────────────────────────────

ELECTION_RE = re.compile(
    r"(\w[\w .]+?)\s+(?:was\s+)?(?:nominated|declared|elected|appointed)\s+"
    r"(?:as\s+)?(?:Board\s+)?(Chair|Vice Chair|Committee Chair|"
    r"(?:Chief (?:Executive|Investment) Officer))",
    re.IGNORECASE,
)

APPOINTMENT_RE = re.compile(
    r"(?:Board\s+)?appoint\s+(\w[\w .]+?)\s+to\s+(?:the\s+)?"
    r"([A-Z][A-Za-z ]+Committee)",
    re.IGNORECASE,
)

REMOVAL_RE = re.compile(
    r"(\w[\w .]+?)\s+(?:be\s+)?removed\s+from\s+(?:the\s+)?"
    r"([A-Z][A-Za-z ]+Committee)",
    re.IGNORECASE,
)

DEPARTURE_RE = re.compile(
    r"(\w[\w .]+?)\s+(?:left\s+the\s+meeting|"
    r"(?:announced\s+(?:his|her|their)\s+)?(?:retirement|resignation|departure))",
    re.IGNORECASE,
)

CHAIR_TRANSITION_RE = re.compile(
    r"(\w[\w .]+?),?\s+(?:Outgoing|Incoming)\s+Chair",
    re.IGNORECASE,
)


def extract_personnel_changes(text: str, section: str = "") -> list[BoardEvent]:
    """Extract personnel-related events."""
    events = []

    for match in ELECTION_RE.finditer(text):
        person = match.group(1).strip()
        role = match.group(2).strip()
        events.append(BoardEvent(
            event_type=EventType.PERSONNEL_CHANGE,
            summary=f"{person} elected/appointed as {role}",
            details={"person": person, "new_role": role, "action": "elected"},
            section=section,
            raw_text=match.group(0),
        ))

    for match in APPOINTMENT_RE.finditer(text):
        person = match.group(1).strip()
        committee = match.group(2).strip()
        events.append(BoardEvent(
            event_type=EventType.COMMITTEE_ASSIGNMENT,
            summary=f"{person} appointed to {committee}",
            details={"person": person, "committee": committee, "action": "appointed"},
            section=section,
            raw_text=match.group(0),
        ))

    for match in REMOVAL_RE.finditer(text):
        person = match.group(1).strip()
        committee = match.group(2).strip()
        events.append(BoardEvent(
            event_type=EventType.COMMITTEE_ASSIGNMENT,
            summary=f"{person} removed from {committee}",
            details={"person": person, "committee": committee, "action": "removed"},
            section=section,
            raw_text=match.group(0),
        ))

    for match in CHAIR_TRANSITION_RE.finditer(text):
        person = match.group(1).strip()
        direction = "outgoing" if "Outgoing" in match.group(0) else "incoming"
        events.append(BoardEvent(
            event_type=EventType.PERSONNEL_CHANGE,
            summary=f"{person} — {direction} Chair",
            details={"person": person, "action": direction, "role": "Chair"},
            section=section,
            raw_text=match.group(0),
        ))

    return events


# ── Performance metrics extraction ───────────────────────────────────────

RETURN_RE = re.compile(
    r"(?:returned?|performance\s+of)\s+([\-]?\d+\.?\d*)\s*percent"
    r"(?:\s+for\s+the\s+(.+?)(?:\.|,))?",
    re.IGNORECASE,
)

AUM_RE = re.compile(
    r"(?:assets\s+under\s+management|AUM)\s+(?:increased|decreased|"
    r"of|reached|totaled?)\s+(?:to\s+)?\$?([\d,.]+)\s*(million|billion|M|B)",
    re.IGNORECASE,
)

BENCHMARK_RE = re.compile(
    r"(?:trailing|outperform|underperform)\w*\s+(?:the\s+)?(?:policy\s+)?"
    r"benchmark\s+(?:of\s+)?([\d.]+)%?",
    re.IGNORECASE,
)


def extract_performance_metrics(text: str, section: str = "") -> list[BoardEvent]:
    """Extract performance reporting metrics."""
    events = []

    for match in RETURN_RE.finditer(text):
        pct = float(match.group(1))
        period = match.group(2).strip() if match.group(2) else ""
        events.append(BoardEvent(
            event_type=EventType.PERFORMANCE_REPORT,
            summary=f"Return of {pct}%{' for ' + period if period else ''}",
            details={"return_pct": pct, "period": period},
            section=section,
            raw_text=match.group(0),
        ))

    for match in AUM_RE.finditer(text):
        amount = float(match.group(1).replace(",", ""))
        unit = match.group(2).lower()
        if unit in ("billion", "b"):
            amount *= 1000
        events.append(BoardEvent(
            event_type=EventType.PERFORMANCE_REPORT,
            summary=f"AUM: ${amount:,.0f}M",
            details={"aum_mm": amount},
            section=section,
            raw_text=match.group(0),
        ))

    return events


# ── Strategic / policy decision extraction ───────────────────────────────

ALLOCATION_CHANGE_RE = re.compile(
    r"(?:allocation|target)\s+(?:of\s+)?"
    r"(\d+)\s*percent\s+(?:to|for)\s+([a-z][a-z ]+)",
    re.IGNORECASE,
)

POLICY_APPROVAL_RE = re.compile(
    r"Board\s+approve\s+(?:the\s+)?(.{10,200}?)"
    r"(?:\s+as\s+(?:proposed|presented)|\.\s)",
    re.IGNORECASE,
)

STRATEGIC_KEYWORDS = re.compile(
    r"\b(strategic plan|annual plan|asset allocation|policy recommendation|"
    r"investment beliefs?|risk tolerance|funded status|peer comparison|"
    r"private credit|diversification|liquidity|rebalancing)\b",
    re.IGNORECASE,
)


def extract_strategic_decisions(text: str, section: str = "") -> list[BoardEvent]:
    """Extract strategic and policy decisions."""
    events = []

    for match in POLICY_APPROVAL_RE.finditer(text):
        policy = match.group(1).strip()
        events.append(BoardEvent(
            event_type=EventType.POLICY_APPROVAL,
            summary=f"Approved: {policy}",
            details={"policy": policy},
            section=section,
            raw_text=match.group(0),
        ))

    return events


# ── Dissent / sentiment extraction ───────────────────────────────────────

DISSENT_SIGNAL_RE = re.compile(
    r"((?:Treasurer|Chair|Senator|Representative|Mr\.|Ms\.|Dr\.)\s+\w[\w .]+?)\s+"
    r"(?:read\s+the\s+following\s+statement\s+into\s+the\s+record|"
    r"expressed\s+(?:concern|opposition|dissent|disagreement)|"
    r"will\s+be\s+voting\s+no|"
    r"voted?\s+(?:no|against))",
    re.IGNORECASE,
)

# Patterns to skip — proxy voting sections produce false positives
DISSENT_SKIP_SECTIONS = re.compile(
    r"proxy\s+voting|shareholder\s+proposal|directors?\s+opposed|"
    r"compensation\s+ratchet|ESG|plummeting\s+industry",
    re.IGNORECASE,
)

# Sentiment word lists (pension-specific)
NEGATIVE_SIGNALS = [
    "risk", "concern", "underperform", "loss", "volatile", "misguided",
    "dangerous", "shortfall", "overweight", "illiquid", "outlier", "caution",
    "unprecedented", "painful", "threaten", "costly", "shock",
]
POSITIVE_SIGNALS = [
    "strong", "outperform", "growth", "opportunity", "prudent", "diversif",
    "resilient", "well-funded", "high-quality", "consistent", "top performer",
    "cost-effective", "value added",
]


def extract_dissent_and_sentiment(text: str, section: str = "") -> list[BoardEvent]:
    """Extract dissenting statements and significant sentiment signals."""
    events = []

    # Skip proxy voting / shareholder proposal sections entirely
    if DISSENT_SKIP_SECTIONS.search(section):
        return events

    for match in DISSENT_SIGNAL_RE.finditer(text):
        person = match.group(1).strip()

        # Skip if this looks like proxy voting content
        context_around = text[max(0, match.start() - 200):match.end() + 200]
        if DISSENT_SKIP_SECTIONS.search(context_around):
            continue

        # Grab the full statement (look for the dissenter's text block)
        statement_start = match.end()
        # Find next section header or motion as boundary
        end_match = re.search(
            r"\n(?:The motion|Chair \w+ (?:moved|expressed)|"
            r"\[The Board|\n[A-Z][A-Z ]{5,}\n)",
            text[statement_start:statement_start + 5000],
        )
        statement_end = statement_start + (end_match.start() if end_match else 2000)
        statement = text[statement_start:statement_end].strip()

        # Score sentiment
        neg_count = sum(1 for w in NEGATIVE_SIGNALS if w.lower() in statement.lower())
        pos_count = sum(1 for w in POSITIVE_SIGNALS if w.lower() in statement.lower())
        sentiment = "negative" if neg_count > pos_count else (
            "positive" if pos_count > neg_count else "neutral"
        )

        events.append(BoardEvent(
            event_type=EventType.DISSENT,
            summary=f"Dissenting statement by {person}",
            details={
                "person": person,
                "sentiment": sentiment,
                "negative_signals": neg_count,
                "positive_signals": pos_count,
                "statement_preview": statement[:500],
            },
            section=section,
            raw_text=statement[:1000],
        ))

    return events


# ── Public comment extraction ────────────────────────────────────────────

PUBLIC_COMMENT_RE = re.compile(
    r"(?:public\s+comment(?:er)?s?\s+(?:urged|asked|called|expressed|raised|"
    r"signed|referenced|advocated|highlighted))"
    r"(.{20,500}?)(?:\.\s+[A-Z]|\n\n)",
    re.IGNORECASE | re.DOTALL,
)


def extract_public_comments(text: str, section: str = "") -> list[BoardEvent]:
    """Extract public comment summaries."""
    events = []

    for match in PUBLIC_COMMENT_RE.finditer(text):
        comment = match.group(0).strip()
        events.append(BoardEvent(
            event_type=EventType.PUBLIC_COMMENT_SUMMARY,
            summary=f"Public comment: {comment[:150]}...",
            details={"full_text": comment},
            section=section,
            raw_text=comment,
        ))

    return events


# ── Oregon-style commitment reports (from meeting minutes tables) ────────

# Oregon committee reports list commitments like:
#   November 18th   Willamette Investment Partners, L.P.   $250M USD
OREGON_COMMITMENT_RE = re.compile(
    r"(?:November|December|January|February|March|April|May|June|"
    r"July|August|September|October)\s+\d{1,2}(?:st|nd|rd|th)?\s+"
    r"([A-Z][A-Za-z0-9 &\-'.]+(?:\s+[IVXLCDM]+)?(?:,?\s*L\.?P\.?)?)\s+"
    r"\$([\d,.]+)\s*([MBK])\s*(?:USD)?",
)


def extract_oregon_commitments(text: str, section: str = "",
                               pension_fund: str = "") -> list[BoardEvent]:
    """Extract Oregon-style commitment listings from Committee Reports.

    Only applies to Oregon Investment Council documents.
    """
    # Only run this on Oregon documents
    if pension_fund and pension_fund != "Oregon":
        return []

    events = []

    for match in OREGON_COMMITMENT_RE.finditer(text):
        fund_name = match.group(1).strip()
        amount_str = match.group(2).replace(",", "")
        suffix = match.group(3).upper()

        amount = float(amount_str)
        if suffix == "B":
            amount *= 1000
        elif suffix == "K":
            amount /= 1000

        # Sanity check: skip if amount is unreasonable
        if amount < 1 or amount > 5000:
            continue

        events.append(BoardEvent(
            event_type=EventType.INVESTMENT_COMMITMENT,
            summary=f"Commitment: ${amount:.0f}M to {fund_name}",
            details={"fund_name": fund_name, "amount_mm": amount},
            section=section,
            raw_text=match.group(0),
        ))

    return events


# ── Entity extraction with spaCy ─────────────────────────────────────────

def extract_entities(text: str) -> dict[str, list[str]]:
    """Use spaCy NER to extract named entities from text."""
    if nlp is None:
        return {}

    doc = nlp(text[:100000])  # Limit to avoid memory issues

    entities = {
        "persons": [],
        "organizations": [],
        "money": [],
        "dates": [],
        "percentages": [],
    }

    for ent in doc.ents:
        if ent.label_ == "PERSON":
            entities["persons"].append(ent.text)
        elif ent.label_ == "ORG":
            entities["organizations"].append(ent.text)
        elif ent.label_ == "MONEY":
            entities["money"].append(ent.text)
        elif ent.label_ == "DATE":
            entities["dates"].append(ent.text)
        elif ent.label_ == "PERCENT":
            entities["percentages"].append(ent.text)

    # Deduplicate while preserving order
    for key in entities:
        seen = set()
        deduped = []
        for item in entities[key]:
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        entities[key] = deduped

    return entities


# ── Meeting date extraction ──────────────────────────────────────────────

MEETING_DATE_RE = re.compile(
    r"(?:Board\s+Meeting\s+Minutes|Meeting\s+Minutes|Board\s+Meeting)\s*\n\s*"
    r"(\w+\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)

MEETING_DATE_ALT_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+\d{1,2},?\s+\d{4}",
)


def extract_meeting_date(text: str) -> str:
    """Extract the meeting date from document header."""
    match = MEETING_DATE_RE.search(text[:2000])
    if match:
        return match.group(1).strip()
    match = MEETING_DATE_ALT_RE.search(text[:2000])
    if match:
        return match.group(0).strip()
    return ""


# ── Pension fund identification ──────────────────────────────────────────

PENSION_FUND_PATTERNS = {
    "WSIB": re.compile(r"Washington State Investment Board|WSIB|SIB", re.IGNORECASE),
    "Oregon": re.compile(r"Oregon Investment Council|OIC|OPERF|Oregon State Treasury", re.IGNORECASE),
    "CalPERS": re.compile(r"CalPERS|California Public Employees", re.IGNORECASE),
    "CalSTRS": re.compile(r"CalSTRS|California State Teachers", re.IGNORECASE),
    "NY Common": re.compile(r"New York State Common|NYS Common|Comptroller", re.IGNORECASE),
}


def identify_pension_fund(text: str) -> str:
    """Identify which pension fund this document belongs to."""
    header = text[:3000]
    for name, pattern in PENSION_FUND_PATTERNS.items():
        if pattern.search(header):
            return name
    return "Unknown"


# ── Main parsing pipeline ────────────────────────────────────────────────

def parse_meeting_minutes(pdf_path: str | Path) -> MeetingMinutes:
    """Parse a board meeting minutes PDF into structured data.

    This is the main entry point. It orchestrates all extraction functions
    and returns a complete MeetingMinutes object.
    """
    pdf_path = Path(pdf_path)
    logger.info(f"Parsing board minutes: {pdf_path.name}")

    # Extract raw text
    pages = extract_text_from_pdf(pdf_path)
    full_text = "\n\n".join(p["text"] for p in pages)
    full_text = normalize_text(full_text)

    # Identify basics
    pension_fund = identify_pension_fund(full_text)
    meeting_date = extract_meeting_date(full_text)

    logger.info(f"  Pension fund: {pension_fund}")
    logger.info(f"  Meeting date: {meeting_date}")
    logger.info(f"  Pages: {len(pages)}, Characters: {len(full_text):,}")

    # Extract attendance
    attendees = extract_attendance(full_text)
    logger.info(f"  Attendees found: {len(attendees)}")

    # Detect sections
    sections = detect_sections(full_text)
    logger.info(f"  Sections detected: {len(sections)}")

    # Extract events from each section
    all_events = []

    for sec in sections:
        sec_text = sec["text"]
        sec_title = sec["title"]

        all_events.extend(extract_investment_decisions(sec_text, sec_title))
        all_events.extend(extract_motions(sec_text, sec_title))
        all_events.extend(extract_personnel_changes(sec_text, sec_title))
        all_events.extend(extract_performance_metrics(sec_text, sec_title))
        all_events.extend(extract_strategic_decisions(sec_text, sec_title))
        all_events.extend(extract_dissent_and_sentiment(sec_text, sec_title))
        all_events.extend(extract_public_comments(sec_text, sec_title))
        all_events.extend(extract_oregon_commitments(
            sec_text, sec_title, pension_fund=pension_fund,
        ))

    # Also run full-text extraction for things that might span sections
    full_events = extract_investment_decisions(full_text, "full_document")
    for evt in full_events:
        if not any(
            e.details.get("fund_name") == evt.details.get("fund_name")
            for e in all_events if e.event_type == evt.event_type
        ):
            all_events.append(evt)

    logger.info(f"  Events extracted: {len(all_events)}")
    for evt_type in EventType:
        count = sum(1 for e in all_events if e.event_type == evt_type)
        if count > 0:
            logger.info(f"    {evt_type.value}: {count}")

    return MeetingMinutes(
        pension_fund=pension_fund,
        meeting_date=meeting_date,
        document_path=str(pdf_path),
        attendees=attendees,
        events=all_events,
        sections=[{"title": s["title"], "length": len(s["text"])} for s in sections],
        raw_text=full_text,
    )


# ── Human-readable report ────────────────────────────────────────────────

def format_report(minutes: MeetingMinutes) -> str:
    """Format parsed minutes into a human-readable report."""
    lines = []
    lines.append(f"{'=' * 70}")
    lines.append(f"BOARD MEETING INTELLIGENCE REPORT")
    lines.append(f"{'=' * 70}")
    lines.append(f"Pension Fund:  {minutes.pension_fund}")
    lines.append(f"Meeting Date:  {minutes.meeting_date}")
    lines.append(f"Source:        {Path(minutes.document_path).name}")
    lines.append(f"{'─' * 70}")

    # Attendance summary
    members = [a for a in minutes.attendees if a.role == "member"]
    staff = [a for a in minutes.attendees if a.role == "staff"]
    if members:
        lines.append(f"\nATTENDANCE ({len(members)} members, {len(staff)} staff)")
        lines.append(f"  Members: {', '.join(a.name for a in members[:10])}")
        if len(members) > 10:
            lines.append(f"           ...and {len(members) - 10} more")
        key_staff = [a for a in staff if a.title]
        if key_staff:
            lines.append(f"  Key Staff:")
            for a in key_staff[:8]:
                lines.append(f"    - {a.name}, {a.title}")

    # Sections overview
    if minutes.sections:
        lines.append(f"\nSECTIONS ({len(minutes.sections)})")
        for sec in minutes.sections:
            lines.append(f"  - {sec['title']} ({sec['length']:,} chars)")

    # Events by type
    event_groups = {}
    for evt in minutes.events:
        event_groups.setdefault(evt.event_type, []).append(evt)

    # Investment decisions
    investments = event_groups.get(EventType.INVESTMENT_COMMITMENT, [])
    if investments:
        lines.append(f"\nINVESTMENT COMMITMENTS ({len(investments)})")
        for evt in investments:
            d = evt.details
            lines.append(f"  * {evt.summary}")
            if d.get("description"):
                lines.append(f"    Strategy: {d['description'][:100]}")
            if d.get("history"):
                h = d["history"]
                lines.append(
                    f"    History: Relationship since {h.get('relationship_since')}, "
                    f"{h.get('prior_fund_count')} prior funds, "
                    f"${h.get('total_committed_mm', 0):,.0f}M total committed"
                )

    # Manager selections
    managers = event_groups.get(EventType.MANAGER_SELECTION, [])
    if managers:
        lines.append(f"\nMANAGER SELECTIONS ({len(managers)})")
        for evt in managers:
            lines.append(f"  * {evt.summary}")

    # Personnel changes
    personnel = event_groups.get(EventType.PERSONNEL_CHANGE, [])
    if personnel:
        lines.append(f"\nPERSONNEL CHANGES ({len(personnel)})")
        for evt in personnel:
            lines.append(f"  * {evt.summary}")

    # Committee assignments
    committees = event_groups.get(EventType.COMMITTEE_ASSIGNMENT, [])
    if committees:
        lines.append(f"\nCOMMITTEE ASSIGNMENTS ({len(committees)})")
        for evt in committees:
            lines.append(f"  * {evt.summary}")

    # Policy approvals
    policies = event_groups.get(EventType.POLICY_APPROVAL, [])
    if policies:
        lines.append(f"\nPOLICY APPROVALS ({len(policies)})")
        for evt in policies:
            lines.append(f"  * {evt.summary}")

    # Motions
    motions = event_groups.get(EventType.MOTION, [])
    if motions:
        lines.append(f"\nFORMAL MOTIONS ({len(motions)})")
        for evt in motions:
            d = evt.details
            outcome = d.get("outcome", "unknown").replace("_", " ").title()
            dissenter = f" (dissent: {d['dissenter']})" if d.get("dissenter") else ""
            lines.append(f"  * {evt.summary}")
            lines.append(f"    Outcome: {outcome}{dissenter}")

    # Performance reports
    perf = event_groups.get(EventType.PERFORMANCE_REPORT, [])
    if perf:
        lines.append(f"\nPERFORMANCE METRICS ({len(perf)})")
        for evt in perf:
            lines.append(f"  * {evt.summary}")

    # Dissent / sentiment
    dissent = event_groups.get(EventType.DISSENT, [])
    if dissent:
        lines.append(f"\nDISSENTING STATEMENTS ({len(dissent)})")
        for evt in dissent:
            d = evt.details
            lines.append(f"  * {evt.summary} [sentiment: {d.get('sentiment', '?')}]")
            preview = d.get("statement_preview", "")
            if preview:
                lines.append(f"    \"{preview[:200]}...\"")

    # Public comment
    pub = event_groups.get(EventType.PUBLIC_COMMENT_SUMMARY, [])
    if pub:
        lines.append(f"\nPUBLIC COMMENT THEMES ({len(pub)})")
        for evt in pub:
            lines.append(f"  * {evt.summary}")

    lines.append(f"\n{'=' * 70}")
    lines.append(f"Total events extracted: {len(minutes.events)}")
    lines.append(f"{'=' * 70}")

    return "\n".join(lines)


# ── CLI entry point ──────────────────────────────────────────────────────

def main():
    """Parse all board meeting minutes in data/cache/board_minutes/."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    base = Path("data/cache/board_minutes")
    if not base.exists():
        print(f"No board minutes directory found at {base}")
        sys.exit(1)

    pdfs = sorted(base.rglob("*.pdf"))
    if not pdfs:
        print("No PDF files found in board_minutes cache.")
        sys.exit(1)

    print(f"Found {len(pdfs)} board meeting documents.\n")

    all_minutes = []
    for pdf_path in pdfs:
        try:
            minutes = parse_meeting_minutes(pdf_path)
            all_minutes.append(minutes)
            report = format_report(minutes)
            print(report)
            print()
        except Exception as e:
            logger.error(f"Failed to parse {pdf_path.name}: {e}")
            import traceback
            traceback.print_exc()

    # Summary across all meetings
    if len(all_minutes) > 1:
        print(f"\n{'=' * 70}")
        print(f"CROSS-MEETING SUMMARY")
        print(f"{'=' * 70}")
        total_events = sum(len(m.events) for m in all_minutes)
        print(f"Meetings parsed: {len(all_minutes)}")
        print(f"Total events:    {total_events}")

        all_investments = []
        for m in all_minutes:
            for evt in m.events:
                if evt.event_type == EventType.INVESTMENT_COMMITMENT:
                    all_investments.append((m.pension_fund, m.meeting_date, evt))

        if all_investments:
            print(f"\nAll investment commitments across meetings:")
            total_mm = 0
            for pf, date, evt in all_investments:
                amt = evt.details.get("amount_mm", 0)
                total_mm += amt
                print(f"  {pf} ({date}): {evt.summary}")
            print(f"  Total: ${total_mm:,.0f}M")


if __name__ == "__main__":
    main()
