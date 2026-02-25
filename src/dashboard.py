"""Streamlit dashboard for the Pension Fund Alternative Investment Tracker.

Polished, client-facing dashboard for demos and outreach.

Run: streamlit run src/dashboard.py
"""

import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Constants ─────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pension_tracker.db"
BOARD_INTEL_PATH = Path(__file__).resolve().parent.parent / "data" / "board_intelligence.json"

BRAND_BLUE = "#1B2A4A"
ACCENT_BLUE = "#0984E3"
LIGHT_BG = "#F8FAFC"

PALETTE = [
    "#0984E3", "#6C5CE7", "#00B894", "#E17055", "#FDCB6E",
    "#74B9FF", "#A29BFE", "#55EFC4", "#FF7675", "#DFE6E9",
]

STRATEGY_ORDER = [
    "Buyout", "Growth Equity", "Venture Capital", "Opportunistic",
    "Co-Investment", "Credit", "Energy", "Fund of Funds",
    "Distressed/Special Situations", "Secondary", "Other",
]


# ── Data loading ──────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300)
def load_commitments():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT c.id, c.commitment_mm, c.vintage_year, c.net_irr, c.net_multiple,
               c.capital_called_mm, c.capital_distributed_mm, c.remaining_value_mm,
               c.dpi, c.as_of_date,
               f.fund_name, f.general_partner, f.asset_class, f.sub_strategy,
               p.name as pension_fund, p.state as pension_state
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        JOIN pension_funds p ON c.pension_fund_id = p.id
        ORDER BY f.fund_name
    """, conn)


@st.cache_data(ttl=300)
def load_fund_summary():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT f.fund_name, f.general_partner, f.vintage_year,
               f.asset_class, f.sub_strategy,
               COUNT(DISTINCT c.pension_fund_id) as pension_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple
        FROM funds f
        JOIN commitments c ON f.id = c.fund_id
        GROUP BY f.id
        ORDER BY total_commitment_mm DESC
    """, conn)


@st.cache_data(ttl=300)
def load_gp_summary():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT f.general_partner,
               COUNT(DISTINCT f.id) as fund_count,
               COUNT(DISTINCT c.pension_fund_id) as pension_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple,
               MIN(f.vintage_year) as earliest_vintage,
               MAX(f.vintage_year) as latest_vintage
        FROM funds f
        JOIN commitments c ON f.id = c.fund_id
        WHERE f.general_partner IS NOT NULL AND f.general_partner != ''
        GROUP BY f.general_partner_normalized
        ORDER BY total_commitment_mm DESC
    """, conn)


@st.cache_data(ttl=300)
def load_pension_summary():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT p.name as pension_fund, p.state,
               COUNT(DISTINCT c.fund_id) as fund_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple,
               MIN(c.as_of_date) as earliest_date,
               MAX(c.as_of_date) as latest_date
        FROM pension_funds p
        JOIN commitments c ON p.id = c.pension_fund_id
        GROUP BY p.id
        ORDER BY total_commitment_mm DESC
    """, conn)


@st.cache_data(ttl=300)
def load_capital_flows():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT p.name as pension_fund,
               SUM(c.capital_called_mm) as total_called,
               SUM(c.capital_distributed_mm) as total_distributed,
               SUM(c.remaining_value_mm) as total_remaining
        FROM commitments c
        JOIN pension_funds p ON c.pension_fund_id = p.id
        GROUP BY p.id
        ORDER BY total_called DESC
    """, conn)


@st.cache_data(ttl=300)
def load_vintage_performance():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT c.vintage_year,
               COUNT(*) as fund_count,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple,
               SUM(c.commitment_mm) as total_commitment_mm
        FROM commitments c
        WHERE c.vintage_year IS NOT NULL AND c.vintage_year >= 1995
        GROUP BY c.vintage_year
        HAVING COUNT(*) >= 5
        ORDER BY c.vintage_year
    """, conn)


@st.cache_data(ttl=300)
def load_widely_held_funds():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT f.fund_name, f.general_partner, f.vintage_year, f.sub_strategy,
               COUNT(DISTINCT c.pension_fund_id) as pension_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple
        FROM funds f
        JOIN commitments c ON f.id = c.fund_id
        GROUP BY f.id
        HAVING COUNT(DISTINCT c.pension_fund_id) >= 4
        ORDER BY pension_count DESC, total_commitment_mm DESC
    """, conn)


@st.cache_data(ttl=300)
def load_strategy_performance():
    conn = get_connection()
    return pd.read_sql_query("""
        SELECT f.sub_strategy,
               COUNT(*) as fund_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               AVG(c.net_irr) as avg_irr,
               AVG(c.net_multiple) as avg_multiple
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        WHERE f.sub_strategy IS NOT NULL AND f.sub_strategy != ''
        GROUP BY f.sub_strategy
        HAVING COUNT(*) >= 10
        ORDER BY total_commitment_mm DESC
    """, conn)


@st.cache_data(ttl=600)
def load_board_intelligence():
    if BOARD_INTEL_PATH.exists():
        with open(BOARD_INTEL_PATH) as f:
            return json.load(f)
    return None


# ── Helpers ───────────────────────────────────────────────────────────────

def fmt_dollars(val, unit="M"):
    if pd.isna(val):
        return ""
    if unit == "B":
        return f"${val / 1000:,.1f}B"
    return f"${val:,.1f}M"


def fmt_irr(val):
    if pd.isna(val):
        return "N/A"
    return f"{val:.1%}"


def fmt_multiple(val):
    if pd.isna(val):
        return ""
    return f"{val:.2f}x"


def section_header(text):
    st.markdown(
        f'<p class="section-header">{text}</p>',
        unsafe_allow_html=True,
    )


def plotly_dark_layout(fig, **kwargs):
    defaults = dict(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=40, b=40),
    )
    defaults.update(kwargs)
    fig.update_layout(**defaults)
    return fig


def _signal_card(title, detail):
    """Render a forward-looking signal as a styled card."""
    st.markdown(
        f"<div style='background: #162240; border-left: 3px solid {ACCENT_BLUE}; "
        f"padding: 12px 16px; margin-bottom: 8px; border-radius: 0 6px 6px 0;'>"
        f"<span style='font-family: Inter, sans-serif; font-weight: 600; "
        f"font-size: 0.9rem; color: #E2E8F0;'>{title}</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.85rem; "
        f"color: #94A3B8; line-height: 1.5;'>{detail}</span></div>",
        unsafe_allow_html=True,
    )


def _fundraising_callout(text):
    """Render an actionable fundraising signal callout."""
    st.markdown(
        f"<div style='background: #1a2744; border: 1px solid #0984E3; "
        f"border-radius: 8px; padding: 14px 18px; margin-top: 12px;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.85rem; "
        f"font-weight: 600; color: #0984E3;'>FUNDRAISING SIGNAL</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.88rem; "
        f"color: #E2E8F0; line-height: 1.6; font-style: italic;'>{text}</span></div>",
        unsafe_allow_html=True,
    )


# ── Board Intelligence Tab ────────────────────────────────────────────────

def _board_card(color, label, title, body):
    """Render a compact board intelligence card with a colored type label."""
    st.markdown(
        f"<div style='background: #1B2A4A; border-left: 3px solid {color}; "
        f"padding: 12px 16px; margin-bottom: 8px; border-radius: 0 6px 6px 0;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.7rem; "
        f"font-weight: 600; color: {color}; text-transform: uppercase; "
        f"letter-spacing: 0.06em;'>{label}</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-weight: 600; "
        f"font-size: 0.92rem; color: #E2E8F0;'>{title}</span>"
        f"{'<br><span style=\"font-family: Inter, sans-serif; font-size: 0.82rem; color: #94A3B8; line-height: 1.5;\">' + body + '</span>' if body else ''}"
        f"</div>",
        unsafe_allow_html=True,
    )


def render_board_intelligence():
    """Render the Board Intelligence tab showing meeting extraction data."""
    data = load_board_intelligence()
    if data is None:
        st.info("Board intelligence data not available.")
        return

    meetings = data["meetings"]
    stats = data["pipeline_stats"]
    coverage = data["coverage"]

    # Reorder: WSIB Nov first, then WSIB Sep, then Oregon
    display_order = [1, 0, 2]
    ordered_meetings = [meetings[i] for i in display_order]

    # ── Overview blurb ────────────────────────────────────────────────
    st.markdown(
        "<div style='background: #1B2A4A; border: 1px solid #334155; border-radius: 10px; "
        "padding: 20px 24px; margin-bottom: 1.2rem; line-height: 1.7;'>"
        "<span style='font-family: Inter, sans-serif; font-size: 0.95rem; color: #E2E8F0;'>"
        "Our hybrid extraction pipeline combines deterministic NLP with PageIndex tree RAG to surface "
        "structured, forward-looking allocation signals from public pension fund board meeting minutes — "
        "the kind of intelligence that is buried across hundreds of pages of narratives, presentation "
        "slides, and formal motions, and would otherwise require hours of manual review to surface. "
        "Below are live extractions from recent board meetings."
        "</span></div>",
        unsafe_allow_html=True,
    )

    # ── Meeting selector (prev/next navigation) ─────────────────────
    if "board_intel_idx" not in st.session_state:
        st.session_state.board_intel_idx = 0

    n_meetings = len(ordered_meetings)
    nav1, nav2, nav3 = st.columns([1, 4, 1])
    with nav1:
        if st.button("\u2190  Previous", use_container_width=True):
            st.session_state.board_intel_idx = (st.session_state.board_intel_idx - 1) % n_meetings
            st.rerun()
    with nav2:
        idx = st.session_state.board_intel_idx
        st.markdown(
            f"<p style='text-align: center; font-family: Inter, sans-serif; "
            f"font-size: 1rem; font-weight: 600; color: #E2E8F0; margin: 6px 0;'>"
            f"Example {idx + 1}</p>",
            unsafe_allow_html=True,
        )
    with nav3:
        if st.button("Next  \u2192", use_container_width=True):
            st.session_state.board_intel_idx = (st.session_state.board_intel_idx + 1) % n_meetings
            st.rerun()

    m = ordered_meetings[st.session_state.board_intel_idx]

    # ── Meeting banner with 3 stats ───────────────────────────────────
    commit_total = sum(c["amount_mm"] for c in m["investment_commitments"])
    insight = m.get("insight", "")
    st.markdown(
        f"<div style='background: linear-gradient(135deg, #162240, #1B2A4A); "
        f"border: 1px solid #334155; border-radius: 10px; "
        f"padding: 20px 28px; margin: 0.8rem 0 1.2rem 0;'>"
        f"<div style='display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;'>"
        f"<div>"
        f"<span style='font-family: Inter, sans-serif; font-size: 1.4rem; font-weight: 700; "
        f"color: #FFFFFF;'>{m['pension_system']}</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.95rem; "
        f"color: #94A3B8;'>{m['meeting_date']} &nbsp;&bull;&nbsp; "
        f"{m['document']} ({m['pages']} pages)</span>"
        f"</div>"
        f"<div style='display: flex; gap: 28px;'>"
        f"<div style='text-align: center;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 1.5rem; font-weight: 700; "
        f"color: {ACCENT_BLUE};'>{m['events_extracted']}</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.7rem; color: #94A3B8; "
        f"text-transform: uppercase; letter-spacing: 0.05em;'>Events</span></div>"
        f"<div style='text-align: center;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 1.5rem; font-weight: 700; "
        f"color: {ACCENT_BLUE};'>${commit_total:,.0f}M</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.7rem; color: #94A3B8; "
        f"text-transform: uppercase; letter-spacing: 0.05em;'>Commitments</span></div>"
        f"<div style='text-align: center;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 1.5rem; font-weight: 700; "
        f"color: {ACCENT_BLUE};'>{len(m.get('forward_looking_signals', []))}</span><br>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.7rem; color: #94A3B8; "
        f"text-transform: uppercase; letter-spacing: 0.05em;'>Signals</span></div>"
        f"</div></div>"
        f"<div style='margin-top: 10px; padding-top: 10px; border-top: 1px solid #334155;'>"
        f"<span style='font-family: Inter, sans-serif; font-size: 0.7rem; font-weight: 600; "
        f"color: #94A3B8; text-transform: uppercase; letter-spacing: 0.06em;'>Insight</span>"
        f"<br><span style='font-family: Inter, sans-serif; font-size: 0.95rem; "
        f"color: #E2E8F0;'>{insight}</span>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

    # ── Extracted Intelligence (single consolidated feed) ─────────────
    section_header("Extracted Intelligence")

    # Investment commitments
    for c in m.get("investment_commitments", []):
        rel = f" ({c['gp_relationship']})" if c.get("gp_relationship") else ""
        detail_lines = [
            f"{c['strategy']} &nbsp;&bull;&nbsp; {c['gp']}{rel}"
            + (f" &nbsp;&bull;&nbsp; {c['vote']}" if c.get("vote") else ""),
        ]
        if c.get("context"):
            detail_lines.append(f"<em>{c['context']}</em>")
        extra_parts = []
        if c.get("fund_target_mm"):
            extra_parts.append(f"Fund target: ${c['fund_target_mm']:,.0f}M")
        if c.get("sector_focus"):
            extra_parts.append(f"Focus: {c['sector_focus']}")
        if extra_parts:
            detail_lines.append(" &nbsp;&bull;&nbsp; ".join(extra_parts))
        _board_card(
            ACCENT_BLUE, "Commitment Approved",
            f"${c['amount_mm']:,.0f}M \u2192 {c['fund']}",
            "<br>".join(detail_lines),
        )

    # Dissent
    for d in m.get("dissent", []):
        _board_card(
            "#FF7675", "Dissent",
            d["person"],
            f"{d['detail']}<br>"
            f"<span style='font-weight: 600;'>Outcome:</span> {d['outcome']}",
        )

    # Policy approvals
    for pa in m.get("policy_approvals", []):
        _board_card("#00B894", "Policy Approved", pa["policy"], pa["status"])

    # Personnel changes
    for pc in m.get("personnel_changes", []):
        vote_str = f" &nbsp;&bull;&nbsp; {pc['vote']}" if pc.get("vote") else ""
        _board_card(
            "#6C5CE7", pc["event"],
            pc["person"],
            f"{pc['new_role']}{vote_str}",
        )

    # Manager selections
    for ms in m.get("manager_selections", []):
        _board_card(
            "#6C5CE7", "Manager Selection",
            ms["manager"],
            f"{ms['mandate']} &nbsp;&bull;&nbsp; {ms['vote']}",
        )

    # SAA change table (at bottom of feed)
    if m.get("strategic_allocation_change"):
        saa = m["strategic_allocation_change"]
        _board_card("#E17055", "Strategic Shift", saa["description"], "")
        saa_df = pd.DataFrame(saa["changes"])
        st.dataframe(
            saa_df,
            column_config={
                "asset_class": "Asset Class",
                "prior": "Prior Target",
                "new": "New Target",
                "range": "Policy Range",
                "change": "Change",
            },
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("")

    # ── Forward-looking signals ───────────────────────────────────────
    if m.get("forward_looking_signals"):
        section_header("Forward-Looking Signals")
        for sig in m["forward_looking_signals"]:
            _signal_card(sig["signal"], sig["detail"])

    # ── Fundraising signal callout ────────────────────────────────────
    if m.get("fundraising_signal"):
        st.markdown("")
        _fundraising_callout(m["fundraising_signal"])

    st.markdown("")

    # ── Coverage & Methodology (collapsed) ────────────────────────────
    with st.expander("Coverage & Pipeline Methodology"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Current Coverage**")
            cov_df = pd.DataFrame(coverage)
            st.dataframe(
                cov_df,
                column_config={
                    "system": "Pension System",
                    "aum": "AUM",
                    "frequency": "Meeting Frequency",
                    "doc_type": "Document Type",
                    "richness": "Signal Richness",
                },
                use_container_width=True,
                hide_index=True,
            )

        with col2:
            st.markdown("**Pipeline Performance**")
            stat_items = [
                ("Avg. events per meeting", stats["avg_events_per_meeting"]),
                ("Commitment extraction accuracy", stats["commitment_accuracy"]),
                ("False positive rate", stats["false_positive_rate"]),
                ("Forward-looking signals per meeting", stats["signals_per_meeting"]),
                ("Processing time per document", stats["processing_time"]),
            ]
            if stats.get("enrichment_method"):
                stat_items.append(("Enrichment method", stats["enrichment_method"]))
            for label, value in stat_items:
                st.markdown(
                    f"<div style='display: flex; justify-content: space-between; "
                    f"padding: 8px 0; border-bottom: 1px solid #334155;'>"
                    f"<span style='font-family: Inter, sans-serif; font-size: 0.9rem; "
                    f"color: #94A3B8;'>{label}</span>"
                    f"<span style='font-family: Inter, sans-serif; font-size: 0.9rem; "
                    f"font-weight: 600; color: #E2E8F0;'>{value}</span></div>",
                    unsafe_allow_html=True,
                )

        st.markdown("")
        st.markdown(
            "<div style='background: #1B2A4A; border: 1px solid #334155; border-radius: 10px; "
            "padding: 16px 20px; line-height: 1.7;'>"
            "<span style='font-family: Inter, sans-serif; font-size: 0.88rem; color: #E2E8F0;'>"
            "<strong>Extraction methodology:</strong> Two-stage hybrid pipeline. "
            "<strong>Stage 1 (deterministic):</strong> Regex pattern matching identifies structured events "
            "(commitments, motions, elections) and spaCy NER tags entities (organizations, dollar amounts, personnel). "
            "These facts are reproducible, auditable, and carry zero hallucination risk. "
            "<strong>Stage 2 (PageIndex tree RAG):</strong> Qualitative enrichment layer extracts investment theses, "
            "dissent rationale, forward-looking signals, and contextual detail from unstructured narrative sections. "
            "Stage 1 facts are never overridden by Stage 2. Every data point includes its source document, "
            "page number, section header, and the extraction pattern or retrieval path that produced it."
            "</span></div>",
            unsafe_allow_html=True,
        )


# ── Page layout ───────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Pension Fund Investment Tracker",
        page_icon="\U0001f4ca",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # ── Custom CSS ─────────────────────────────────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .block-container { padding-top: 1.5rem; max-width: 1200px; }

    .main-title {
        font-family: 'Inter', sans-serif;
        font-size: 2.2rem;
        font-weight: 700;
        color: #FFFFFF;
        margin-bottom: 0;
        line-height: 1.2;
    }
    .main-subtitle {
        font-family: 'Inter', sans-serif;
        font-size: 1.05rem;
        color: #94A3B8;
        margin-top: 2px;
        margin-bottom: 1.2rem;
    }

    /* KPI cards — dark theme */
    [data-testid="stMetric"] {
        background: #1B2A4A;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] {
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem !important;
        font-weight: 500;
        color: #94A3B8 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem !important;
        font-weight: 700;
        color: #E2E8F0 !important;
    }

    /* Section headers */
    .section-header {
        font-family: 'Inter', sans-serif;
        font-size: 1.25rem;
        font-weight: 600;
        color: #FFFFFF;
        margin-top: 0.8rem;
        margin-bottom: 0.4rem;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid #E2E8F0;
    }

    /* Table styling */
    .dataframe { font-family: 'Inter', sans-serif !important; }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header[data-testid="stHeader"] { background: transparent; }
    [data-testid="stToolbar"] { display: none; }
    [data-testid="stAppDeployButton"] { display: none; }
    ._profileContainer_gzau3_53 { display: none !important; }
    ._container_gzau3_1 { display: none !important; }
    [data-testid="stStatusWidget"] { display: none; }
    div[class*="profileContainer"] { display: none !important; }
    div[class*="hostContainer"] { display: none !important; }
    iframe[title="streamlit_badge"] { display: none !important; }
    #stStreamlitBadge { display: none !important; }

    /* Tabs — prominent navigation */
    .stTabs [data-baseweb="tab-list"] {
        gap: 6px;
        background: #1B2A4A;
        border-radius: 10px;
        padding: 6px 8px;
        border: 1px solid #334155;
    }
    .stTabs [data-baseweb="tab"] {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1.05rem;
        padding: 10px 28px;
        border-radius: 8px;
        color: #94A3B8;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: #0984E3;
        color: #FFFFFF;
    }
    .stTabs [data-baseweb="tab-highlight"] { display: none; }
    .stTabs [data-baseweb="tab-border"] { display: none; }

    div[data-testid="stDataFrame"] div[class*="glideDataEditor"] {
        border: 1px solid #E2E8F0;
        border-radius: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── Title ──────────────────────────────────────────────────────────
    st.markdown(
        '<p class="main-title">Pension Fund Alternative Investment Tracker</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="main-subtitle">'
        'Cross-referencing private equity, venture capital, and alternative investment commitments '
        'across major U.S. public pension systems'
        '</p>',
        unsafe_allow_html=True,
    )

    # ── Tabs ───────────────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["Portfolio Analytics", "Board Intelligence"])

    # ══════════════════════════════════════════════════════════════════
    # TAB 1 — Portfolio Analytics
    # ══════════════════════════════════════════════════════════════════
    with tab1:
        # ── Overview note ─────────────────────────────────────────────
        st.markdown(
            "<div style='background: #1B2A4A; border: 1px solid #334155; border-radius: 10px; "
            "padding: 20px 24px; margin-bottom: 1.2rem; line-height: 1.7;'>"
            "<span style='font-family: Inter, sans-serif; font-size: 0.95rem; color: #E2E8F0;'>"
            "This dashboard aggregates alternative investment commitment data from public disclosures "
            "of major U.S. pension systems, cross-referencing how the same private equity and venture "
            "capital funds are valued across different institutional investors. "
            "The current dataset covers <strong>5 pension systems</strong> and serves as a "
            "<strong>proof of concept</strong> for a comprehensive cross-pension analytics platform. "
            "Expansion to additional state and municipal pension funds is underway."
            "</span></div>",
            unsafe_allow_html=True,
        )

        # ── Load data ─────────────────────────────────────────────────
        if not DB_PATH.exists():
            st.error(f"Database not found at {DB_PATH}. Run the pipeline first.")
        else:
            df = load_commitments()
            fund_summary = load_fund_summary()
            gp_summary = load_gp_summary()
            pension_summary = load_pension_summary()

            if df.empty:
                st.warning("No data loaded.")
            else:
                total_funds = fund_summary.shape[0]
                total_pensions = df["pension_fund"].nunique()
                total_commitment_bn = df["commitment_mm"].sum() / 1000.0
                total_gps = gp_summary.shape[0]
                cross_pension_count = fund_summary[fund_summary["pension_count"] >= 2].shape[0]

                # ── KPI Row ───────────────────────────────────────────
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Total Commitments", f"${total_commitment_bn:,.0f}B")
                c2.metric("Funds Tracked", f"{total_funds:,}")
                c3.metric("Pension Systems", f"{total_pensions}")
                c4.metric("General Partners", f"{total_gps:,}")
                c5.metric("Cross-Pension Funds", f"{cross_pension_count:,}")

                st.markdown("")

                # ── Pension System Overview ────────────────────────────
                section_header("Pension System Overview")

                pension_display = pension_summary.copy()
                pension_display["total_commitment_mm"] = pension_display["total_commitment_mm"].apply(lambda x: fmt_dollars(x, "B"))
                pension_display["avg_irr"] = pension_display["avg_irr"].apply(fmt_irr)
                pension_display["avg_multiple"] = pension_display["avg_multiple"].apply(fmt_multiple)

                st.dataframe(
                    pension_display[["pension_fund", "state", "fund_count", "total_commitment_mm",
                                     "avg_irr", "avg_multiple", "latest_date"]],
                    column_config={
                        "pension_fund": "Pension System",
                        "state": "State",
                        "fund_count": "Funds",
                        "total_commitment_mm": "Total Commitments",
                        "avg_irr": "Avg Net IRR",
                        "avg_multiple": "Avg Net Multiple",
                        "latest_date": "Latest Report Date",
                    },
                    use_container_width=True,
                    hide_index=True,
                )

                st.markdown("")

                # ── Capital Flow Analysis ─────────────────────────────
                section_header("Capital Flow by Pension System")

                capital_flows = load_capital_flows()
                if not capital_flows.empty:
                    cf = capital_flows.copy()
                    for c in ["total_called", "total_distributed", "total_remaining"]:
                        cf[c] = cf[c] / 1000.0

                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        y=cf["pension_fund"], x=cf["total_called"],
                        name="Capital Called",
                        orientation="h",
                        marker_color="#0984E3",
                        hovertemplate="%{y}<br>Capital Called: $%{x:.0f}B<extra></extra>",
                    ))
                    fig.add_trace(go.Bar(
                        y=cf["pension_fund"], x=cf["total_distributed"],
                        name="Capital Distributed",
                        orientation="h",
                        marker_color="#2ECC71",
                        hovertemplate="%{y}<br>Distributed: $%{x:.0f}B<extra></extra>",
                    ))
                    fig.add_trace(go.Bar(
                        y=cf["pension_fund"], x=cf["total_remaining"],
                        name="Remaining Value (NAV)",
                        orientation="h",
                        marker_color="#E17055",
                        hovertemplate="%{y}<br>Remaining: $%{x:.0f}B<extra></extra>",
                    ))
                    plotly_dark_layout(fig, height=320, barmode="group",
                                      xaxis_title="Amount ($B)",
                                      legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                                  xanchor="center", x=0.5))
                    st.plotly_chart(fig, use_container_width=True)

                st.markdown("")

                # ── Charts Row 1: Vintage + Strategy ──────────────────
                col1, col2 = st.columns(2)

                with col1:
                    section_header("Commitment Volume by Vintage Year")

                    vintage_data = df.dropna(subset=["vintage_year"]).copy()
                    vintage_data["vintage_year"] = vintage_data["vintage_year"].astype(int)
                    vintage_agg = (
                        vintage_data.groupby("vintage_year")
                        .agg(total_mm=("commitment_mm", "sum"), fund_count=("fund_name", "nunique"))
                        .reset_index()
                    )
                    vintage_agg["total_bn"] = vintage_agg["total_mm"] / 1000.0

                    fig = px.bar(
                        vintage_agg, x="vintage_year", y="total_bn",
                        labels={"vintage_year": "Vintage Year", "total_bn": "Commitments ($B)"},
                        color_discrete_sequence=[ACCENT_BLUE],
                        hover_data={"fund_count": True, "total_bn": ":.1f"},
                    )
                    plotly_dark_layout(fig, height=380, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    section_header("Allocation by Strategy")

                    strategy_data = df.copy()
                    strategy_data["sub_strategy"] = strategy_data["sub_strategy"].fillna("Other")
                    strat_agg = (
                        strategy_data.groupby("sub_strategy")
                        .agg(total_mm=("commitment_mm", "sum"))
                        .reset_index()
                        .sort_values("total_mm", ascending=False)
                    )
                    strat_agg["total_bn"] = strat_agg["total_mm"] / 1000.0

                    fig = px.pie(
                        strat_agg.head(10), values="total_bn", names="sub_strategy",
                        color_discrete_sequence=PALETTE,
                        hole=0.45,
                    )
                    plotly_dark_layout(fig, height=380, showlegend=True,
                                      legend=dict(font=dict(size=11)))
                    fig.update_traces(textposition="inside", textinfo="percent+label",
                                     textfont_size=10)
                    st.plotly_chart(fig, use_container_width=True)

                # ── Charts Row 2: GP bar + IRR Distribution ───────────
                col1, col2 = st.columns(2)

                with col1:
                    section_header("Top 15 GPs by Total Commitment")

                    top_gps = gp_summary.head(15).copy()
                    top_gps["total_bn"] = top_gps["total_commitment_mm"] / 1000.0

                    fig = go.Figure(go.Bar(
                        y=top_gps["general_partner"],
                        x=top_gps["total_bn"],
                        orientation="h",
                        marker_color=ACCENT_BLUE,
                        text=top_gps["total_bn"].apply(lambda x: f"${x:.1f}B"),
                        textposition="inside",
                        insidetextanchor="end",
                        textfont=dict(color="white"),
                    ))
                    plotly_dark_layout(fig, height=450, showlegend=False,
                                      xaxis_title="Total Commitments ($B)",
                                      yaxis=dict(autorange="reversed"))
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    section_header("Net IRR Distribution by Pension System")

                    perf_data = df.dropna(subset=["net_irr"]).copy()
                    perf_data = perf_data[perf_data["net_irr"].between(-0.5, 1.0)]

                    if not perf_data.empty:
                        import numpy as np

                        pensions = sorted(perf_data["pension_fund"].unique())
                        fig = go.Figure()

                        for i, pension in enumerate(pensions):
                            subset = perf_data[perf_data["pension_fund"] == pension]
                            n = len(subset)
                            median_irr = subset["net_irr"].median()
                            q1 = subset["net_irr"].quantile(0.25)
                            q3 = subset["net_irr"].quantile(0.75)

                            # Jittered dots — each dot is one fund
                            jitter = np.random.default_rng(42 + i).uniform(-0.28, 0.28, size=n)
                            fig.add_trace(go.Scatter(
                                x=subset["net_irr"],
                                y=[i + j for j in jitter],
                                mode="markers",
                                marker=dict(size=4, color=PALETTE[i], opacity=0.35),
                                showlegend=False,
                                hovertemplate=f"<b>{pension}</b><br>Net IRR: %{{x:.1%}}<extra></extra>",
                            ))

                            # IQR line (25th–75th percentile)
                            fig.add_trace(go.Scatter(
                                x=[q1, q3], y=[i, i],
                                mode="lines",
                                line=dict(color="white", width=3),
                                showlegend=False, hoverinfo="skip",
                            ))

                            # Median diamond
                            fig.add_trace(go.Scatter(
                                x=[median_irr], y=[i],
                                mode="markers+text",
                                marker=dict(size=14, color=PALETTE[i], symbol="diamond",
                                            line=dict(color="white", width=2)),
                                text=[f"{median_irr:.1%}"],
                                textposition="top center",
                                textfont=dict(color="white", size=11),
                                showlegend=False,
                                hovertemplate=(
                                    f"<b>{pension}</b><br>Median: {median_irr:.1%}<br>"
                                    f"IQR: {q1:.1%} – {q3:.1%}<br>{n} funds<extra></extra>"
                                ),
                            ))

                        # 0% reference line
                        fig.add_vline(x=0, line_dash="dot", line_color="rgba(255,255,255,0.3)")

                        plotly_dark_layout(fig, height=400, showlegend=False,
                                          xaxis=dict(title="Net IRR", tickformat=".0%",
                                                     zeroline=False),
                                          yaxis=dict(
                                              tickvals=list(range(len(pensions))),
                                              ticktext=[f"{p}  ({len(perf_data[perf_data['pension_fund']==p])})"
                                                        for p in pensions],
                                              range=[-0.6, len(pensions) - 0.4],
                                          ))
                        st.plotly_chart(fig, use_container_width=True)
                        st.caption("Each dot = one fund  · ◆ median  · white line = interquartile range  · NY Common excluded")
                    else:
                        st.info("Insufficient performance data.")

                # ── Charts Row 3: Vintage Perf + Strategy IRR ─────────
                col1, col2 = st.columns(2)

                with col1:
                    section_header("Performance by Vintage Year")

                    vintage_perf = load_vintage_performance()
                    vp = vintage_perf.dropna(subset=["avg_irr"]).copy()

                    if not vp.empty:
                        vp["vintage_year"] = vp["vintage_year"].astype(int)

                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=vp["vintage_year"], y=vp["avg_irr"],
                            mode="lines+markers",
                            name="Avg Net IRR",
                            line=dict(color="#0984E3", width=2),
                            marker=dict(size=6),
                            yaxis="y",
                            hovertemplate="Vintage %{x}<br>Avg IRR: %{y:.1%}<extra></extra>",
                        ))
                        fig.add_trace(go.Scatter(
                            x=vp["vintage_year"], y=vp["avg_multiple"],
                            mode="lines+markers",
                            name="Avg Net Multiple",
                            line=dict(color="#00B894", width=2),
                            marker=dict(size=6),
                            yaxis="y2",
                            hovertemplate="Vintage %{x}<br>Avg Multiple: %{y:.2f}x<extra></extra>",
                        ))
                        plotly_dark_layout(fig, height=400,
                                          xaxis_title="Vintage Year",
                                          yaxis=dict(title="Avg Net IRR", tickformat=".0%", side="left"),
                                          yaxis2=dict(title="Avg Net Multiple", overlaying="y",
                                                      side="right", tickformat=".2f"),
                                          legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                                      xanchor="center", x=0.5))
                        fig.add_hline(y=0, line_dash="dash", line_color="#64748B", opacity=0.3)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Insufficient vintage performance data.")

                with col2:
                    section_header("Average IRR by Strategy")

                    strat_perf = load_strategy_performance()
                    if not strat_perf.empty:
                        sp = strat_perf.dropna(subset=["avg_irr"]).head(10).copy()
                        sp = sp.sort_values("avg_irr", ascending=True)

                        fig = go.Figure(go.Bar(
                            y=sp["sub_strategy"], x=sp["avg_irr"],
                            orientation="h",
                            marker_color=ACCENT_BLUE,
                            text=sp["avg_irr"].apply(lambda x: f"{x:.1%}" if pd.notna(x) else ""),
                            textposition="inside",
                            insidetextanchor="end",
                            textfont=dict(color="white"),
                        ))
                        plotly_dark_layout(fig, height=400, showlegend=False,
                                          xaxis_title="Average Net IRR",
                                          xaxis=dict(tickformat=".0%"))
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Insufficient strategy performance data.")

                st.markdown("")

                # ── Cross-Pension Comparison ──────────────────────────
                section_header("Cross-Pension Fund Comparison")
                st.markdown(
                    '<p class="main-subtitle">'
                    'Compare how the same fund is valued and reported across different pension systems — '
                    'the unique value of cross-referencing multiple LP disclosures'
                    '</p>',
                    unsafe_allow_html=True,
                )

                cross_funds = fund_summary[fund_summary["pension_count"] >= 2].copy()

                if cross_funds.empty:
                    st.info("No funds found in multiple pension systems.")
                else:
                    # Summary metrics
                    c1, c2, c3, c4 = st.columns(4)
                    n_in_3 = len(cross_funds[cross_funds["pension_count"] >= 3])
                    n_in_4 = len(cross_funds[cross_funds["pension_count"] >= 4])
                    n_in_5 = len(cross_funds[cross_funds["pension_count"] >= 5])
                    c1.metric("In 2+ Systems", f"{len(cross_funds)}")
                    c2.metric("In 3+ Systems", f"{n_in_3}")
                    c3.metric("In 4+ Systems", f"{n_in_4}")
                    c4.metric("In All 5 Systems", f"{n_in_5}")

                    st.markdown("")

                    # Fund selector
                    cross_fund_names = cross_funds["fund_name"].tolist()
                    selected_fund = st.selectbox(
                        "Select a fund to compare across pension systems",
                        options=cross_fund_names,
                        index=0,
                    )

                    if selected_fund:
                        fund_data = df[df["fund_name"] == selected_fund].copy()

                        # Deduplicate: take latest as_of_date per pension+vintage combo
                        deduped = fund_data.sort_values("as_of_date").groupby(
                            ["pension_fund", "vintage_year"], as_index=False
                        ).last()

                        # Build bar labels: add year only when a pension has multiple vintages
                        pension_vy_count = deduped.groupby("pension_fund")["vintage_year"].nunique()
                        deduped["bar_label"] = deduped.apply(
                            lambda r: (f"{r['pension_fund']} ({int(r['vintage_year'])})"
                                       if pd.notna(r["vintage_year"]) and pension_vy_count.get(r["pension_fund"], 1) > 1
                                       else r["pension_fund"]),
                            axis=1,
                        )

                        col1, col2 = st.columns([2, 3])

                        with col1:
                            fig = go.Figure(go.Bar(
                                y=deduped["bar_label"],
                                x=deduped["commitment_mm"],
                                orientation="h",
                                marker_color=ACCENT_BLUE,
                                text=deduped["commitment_mm"].apply(lambda x: f"${x:,.0f}M"),
                                textposition="inside",
                                insidetextanchor="end",
                                textfont=dict(color="white"),
                            ))
                            plotly_dark_layout(fig, height=max(180, len(deduped) * 50),
                                              showlegend=False,
                                              xaxis_title="Commitment ($M)",
                                              margin=dict(l=40, r=20, t=10, b=40))
                            st.plotly_chart(fig, use_container_width=True)

                        with col2:
                            display_data = fund_data.copy()
                            display_data["commitment_mm"] = display_data["commitment_mm"].apply(fmt_dollars)
                            display_data["net_irr"] = display_data["net_irr"].apply(fmt_irr)
                            display_data["net_multiple"] = display_data["net_multiple"].apply(fmt_multiple)
                            display_data["capital_called_mm"] = display_data["capital_called_mm"].apply(fmt_dollars)
                            display_data["capital_distributed_mm"] = display_data["capital_distributed_mm"].apply(fmt_dollars)
                            display_data["remaining_value_mm"] = display_data["remaining_value_mm"].apply(fmt_dollars)

                            st.dataframe(
                                display_data[["pension_fund", "commitment_mm", "net_irr", "net_multiple",
                                              "capital_called_mm", "capital_distributed_mm",
                                              "remaining_value_mm", "as_of_date"]],
                                column_config={
                                    "pension_fund": "Pension System",
                                    "commitment_mm": "Commitment",
                                    "net_irr": "Net IRR",
                                    "net_multiple": "Net Multiple",
                                    "capital_called_mm": "Called",
                                    "capital_distributed_mm": "Distributed",
                                    "remaining_value_mm": "Remaining",
                                    "as_of_date": "As Of Date",
                                },
                                use_container_width=True,
                                hide_index=True,
                            )

                st.markdown("")

                # ── Fund Search ───────────────────────────────────────
                section_header("Fund & GP Search")

                search_query = st.text_input(
                    "Search by fund name or general partner",
                    placeholder="e.g. KKR, Blackstone Capital Partners, Apollo, Sequoia...",
                )

                if search_query:
                    mask = (
                        df["fund_name"].str.contains(search_query, case=False, na=False)
                        | df["general_partner"].str.contains(search_query, case=False, na=False)
                    )
                    results = df[mask]

                    if results.empty:
                        st.info(f'No funds found matching "{search_query}".')
                    else:
                        st.write(f"Found **{results['fund_name'].nunique()}** funds, **{len(results)}** commitment records")

                        display_df = results[[
                            "pension_fund", "fund_name", "general_partner", "sub_strategy",
                            "vintage_year", "commitment_mm", "net_irr", "net_multiple", "as_of_date",
                        ]].copy()
                        display_df["commitment_mm"] = display_df["commitment_mm"].apply(fmt_dollars)
                        display_df["net_irr"] = display_df["net_irr"].apply(fmt_irr)
                        display_df["net_multiple"] = display_df["net_multiple"].apply(fmt_multiple)

                        st.dataframe(
                            display_df,
                            column_config={
                                "pension_fund": "Pension System",
                                "fund_name": "Fund Name",
                                "general_partner": "GP",
                                "sub_strategy": "Strategy",
                                "vintage_year": "Vintage",
                                "commitment_mm": "Commitment",
                                "net_irr": "Net IRR",
                                "net_multiple": "Net Multiple",
                                "as_of_date": "As Of Date",
                            },
                            use_container_width=True,
                            hide_index=True,
                        )

                st.markdown("")

                # ── Data Explorer ─────────────────────────────────────
                section_header("Data Explorer")

                table_view = st.selectbox(
                    "Select a view",
                    options=[
                        "Top 25 Funds by Total Commitment",
                        "Most Widely Held Funds (4+ Pension Systems)",
                        "GP Leaderboard — Top 25 by Commitment",
                        "Top Performing Funds (by Net IRR)",
                        "Top Performing GPs (by Avg Net IRR)",
                    ],
                )

                if table_view == "Top 25 Funds by Total Commitment":
                    top_funds = fund_summary.head(25).copy()
                    top_funds["total_commitment_mm"] = top_funds["total_commitment_mm"].apply(fmt_dollars)
                    top_funds["avg_irr"] = top_funds["avg_irr"].apply(fmt_irr)
                    top_funds["avg_multiple"] = top_funds["avg_multiple"].apply(fmt_multiple)
                    st.dataframe(
                        top_funds[["fund_name", "general_partner", "sub_strategy", "vintage_year",
                                    "pension_count", "total_commitment_mm", "avg_irr", "avg_multiple"]],
                        column_config={
                            "fund_name": "Fund Name",
                            "general_partner": "GP",
                            "sub_strategy": "Strategy",
                            "vintage_year": "Vintage",
                            "pension_count": "Pensions",
                            "total_commitment_mm": "Total Commitment",
                            "avg_irr": "Avg Net IRR",
                            "avg_multiple": "Avg Net Multiple",
                        },
                        use_container_width=True,
                        hide_index=True,
                    )

                elif table_view == "Most Widely Held Funds (4+ Pension Systems)":
                    widely_held = load_widely_held_funds()
                    if not widely_held.empty:
                        wh = widely_held.copy()
                        wh["total_commitment_mm"] = wh["total_commitment_mm"].apply(fmt_dollars)
                        wh["avg_irr"] = wh["avg_irr"].apply(fmt_irr)
                        wh["avg_multiple"] = wh["avg_multiple"].apply(fmt_multiple)
                        st.dataframe(
                            wh[["fund_name", "general_partner", "sub_strategy", "vintage_year",
                                "pension_count", "total_commitment_mm", "avg_irr", "avg_multiple"]],
                            column_config={
                                "fund_name": "Fund Name",
                                "general_partner": "GP",
                                "sub_strategy": "Strategy",
                                "vintage_year": "Vintage",
                                "pension_count": st.column_config.NumberColumn("Pension Systems", format="%d"),
                                "total_commitment_mm": "Total Commitment",
                                "avg_irr": "Avg Net IRR",
                                "avg_multiple": "Avg Net Multiple",
                            },
                            use_container_width=True,
                            hide_index=True,
                        )

                elif table_view == "GP Leaderboard — Top 25 by Commitment":
                    top_gps_table = gp_summary.head(25).copy()
                    top_gps_table["total_commitment_mm"] = top_gps_table["total_commitment_mm"].apply(fmt_dollars)
                    top_gps_table["avg_irr"] = top_gps_table["avg_irr"].apply(fmt_irr)
                    top_gps_table["avg_multiple"] = top_gps_table["avg_multiple"].apply(fmt_multiple)
                    st.dataframe(
                        top_gps_table[["general_partner", "fund_count", "pension_count",
                                        "total_commitment_mm", "avg_irr", "avg_multiple",
                                        "earliest_vintage", "latest_vintage"]],
                        column_config={
                            "general_partner": "General Partner",
                            "fund_count": "Funds",
                            "pension_count": "Pensions",
                            "total_commitment_mm": "Total Commitment",
                            "avg_irr": "Avg Net IRR",
                            "avg_multiple": "Avg Net Multiple",
                            "earliest_vintage": "Earliest Vintage",
                            "latest_vintage": "Latest Vintage",
                        },
                        use_container_width=True,
                        hide_index=True,
                    )

                elif table_view == "Top Performing Funds (by Net IRR)":
                    perf_funds = fund_summary.dropna(subset=["avg_irr"]).copy()
                    perf_funds = perf_funds.sort_values("avg_irr", ascending=False).head(25)
                    perf_funds["total_commitment_mm"] = perf_funds["total_commitment_mm"].apply(fmt_dollars)
                    perf_funds["avg_irr"] = perf_funds["avg_irr"].apply(fmt_irr)
                    perf_funds["avg_multiple"] = perf_funds["avg_multiple"].apply(fmt_multiple)
                    st.dataframe(
                        perf_funds[["fund_name", "general_partner", "sub_strategy", "vintage_year",
                                     "pension_count", "total_commitment_mm", "avg_irr", "avg_multiple"]],
                        column_config={
                            "fund_name": "Fund Name",
                            "general_partner": "GP",
                            "sub_strategy": "Strategy",
                            "vintage_year": "Vintage",
                            "pension_count": "Pensions",
                            "total_commitment_mm": "Total Commitment",
                            "avg_irr": "Avg Net IRR",
                            "avg_multiple": "Avg Net Multiple",
                        },
                        use_container_width=True,
                        hide_index=True,
                    )

                elif table_view == "Top Performing GPs (by Avg Net IRR)":
                    perf_gps = gp_summary.dropna(subset=["avg_irr"]).copy()
                    perf_gps = perf_gps[perf_gps["fund_count"] >= 2]
                    perf_gps = perf_gps.sort_values("avg_irr", ascending=False).head(25)
                    perf_gps["total_commitment_mm"] = perf_gps["total_commitment_mm"].apply(fmt_dollars)
                    perf_gps["avg_irr"] = perf_gps["avg_irr"].apply(fmt_irr)
                    perf_gps["avg_multiple"] = perf_gps["avg_multiple"].apply(fmt_multiple)
                    st.dataframe(
                        perf_gps[["general_partner", "fund_count", "pension_count",
                                   "total_commitment_mm", "avg_irr", "avg_multiple",
                                   "earliest_vintage", "latest_vintage"]],
                        column_config={
                            "general_partner": "General Partner",
                            "fund_count": "Funds",
                            "pension_count": "Pensions",
                            "total_commitment_mm": "Total Commitment",
                            "avg_irr": "Avg Net IRR",
                            "avg_multiple": "Avg Net Multiple",
                            "earliest_vintage": "Earliest Vintage",
                            "latest_vintage": "Latest Vintage",
                        },
                        use_container_width=True,
                        hide_index=True,
                    )

                st.markdown("")

                # ── About / Methodology ───────────────────────────────
                st.markdown("")
                with st.expander("About This Data / Methodology", expanded=False):
                    st.markdown("""
### How It Works

The Pension Fund Alternative Investment Tracker aggregates private equity and alternative
investment commitment data from public disclosures of major U.S. pension systems:

1. **Source** — Each pension system publishes portfolio holdings in different formats
   (HTML tables, PDFs, annual reports). Custom adapters parse each source deterministically.
2. **Extract** — Fund names, GP names, commitment amounts, performance metrics (IRR,
   net multiple, DPI), and vintage years are extracted with provenance tracking.
3. **Resolve** — Entity resolution links the same fund across pension systems.
   "KKR North America Fund XII" at CalPERS and "KKR NA XII" at WSIB are matched
   using fuzzy matching on fund name + GP + vintage year.
4. **Validate** — Quality checks flag outlier values, cross-pension inconsistencies,
   and low-completeness records for human review.

### Data Coverage

| Pension System | Commitments | Source Type | Reporting Date |
|---|---|---|---|
| CalPERS | 429 | HTML tables | Q1 2025 |
| CalSTRS | 469 | PDF reports | Q2 2025 |
| WSIB | 462 | PDF reports | Q2 2025 |
| Oregon PERS | 402 | PDF reports | Q3 2025 |
| NY Common | 727 | PDF asset listings | Q1 2024–2025 |

### Key Metrics

- **{n_funds:,}** unique funds tracked across **{n_pensions}** pension systems
- **{n_cross:,}** funds appear in 2+ pension systems (cross-referenced)
- **{total_bn:.0f}B** in total commitments
- **62%** of records include Net IRR (NY Common does not publish IRR)
- **97%** of records include Net Multiple (TVPI)

### Limitations

- NY Common Retirement Fund does not disclose IRR; their records have Net Multiple only.
- Performance metrics reflect values as reported by each pension system and may differ
  slightly for the same fund due to different reporting dates and methodologies.
- Texas TRS and Florida SBA are partially covered due to access restrictions.
""".format(
                        n_funds=total_funds,
                        n_pensions=total_pensions,
                        n_cross=cross_pension_count,
                        total_bn=total_commitment_bn,
                    ))

    # ══════════════════════════════════════════════════════════════════
    # TAB 2 — Board Intelligence
    # ══════════════════════════════════════════════════════════════════
    with tab2:
        render_board_intelligence()

    # ── Footer ─────────────────────────────────────────────────────────
    st.markdown("")
    st.divider()
    st.markdown(
        "<div style='text-align: center; padding: 16px 0;'>"
        "<p style='font-family: Inter, sans-serif; font-size: 1.3rem; font-weight: 600; "
        "color: #FFFFFF; margin-bottom: 6px;'>Built by Nathan Goldberg</p>"
        "<p style='font-family: Inter, sans-serif; font-size: 1rem; margin-top: 0; margin-bottom: 16px;'>"
        "<a href='mailto:nathanmauricegoldberg@gmail.com' style='color: #0984E3; text-decoration: none;'>nathanmauricegoldberg@gmail.com</a>"
        " &nbsp;&bull;&nbsp; "
        "<a href='https://www.linkedin.com/in/nathan-goldberg-62a44522a' target='_blank' "
        "style='color: #0984E3; text-decoration: none;'>LinkedIn</a></p>"
        "<p style='font-family: Inter, sans-serif; font-size: 0.8rem; color: #94A3B8; margin-top: 0;'>"
        "Pension Fund Alternative Investment Tracker &bull; "
        "Data sourced from official public pension fund disclosures &bull; "
        "Deterministic extraction with provenance tracking</p>"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
