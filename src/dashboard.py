"""Streamlit dashboard for the Pension Fund Alternative Investment Tracker.

Polished, client-facing dashboard for demos and outreach.

Run: streamlit run src/dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Constants ─────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pension_tracker.db"

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


# ── Helpers ───────────────────────────────────────────────────────────────

def fmt_dollars(val, unit="M"):
    if pd.isna(val):
        return ""
    if unit == "B":
        return f"${val / 1000:,.1f}B"
    return f"${val:,.1f}M"


def fmt_irr(val):
    if pd.isna(val):
        return ""
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
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=40, r=20, t=40, b=40),
        **kwargs,
    )
    return fig


# ── Page layout ───────────────────────────────────────────────────────────

def main():
    favicon = Path(__file__).resolve().parent.parent / ".streamlit" / "favicon.png"
    st.set_page_config(
        page_title="Pension Fund Investment Tracker",
        page_icon=str(favicon) if favicon.exists() else ":chart_with_upwards_trend:",
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

    /* KPI cards */
    [data-testid="stMetric"] {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] {
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem !important;
        font-weight: 500;
        color: #64748B !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem !important;
        font-weight: 700;
        color: #1B2A4A !important;
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

    # ── Load data ──────────────────────────────────────────────────────
    if not DB_PATH.exists():
        st.error(f"Database not found at {DB_PATH}. Run the pipeline first.")
        return

    df = load_commitments()
    fund_summary = load_fund_summary()
    gp_summary = load_gp_summary()
    pension_summary = load_pension_summary()

    if df.empty:
        st.warning("No data loaded.")
        return

    total_funds = fund_summary.shape[0]
    total_pensions = df["pension_fund"].nunique()
    total_commitment_bn = df["commitment_mm"].sum() / 1000.0
    total_gps = gp_summary.shape[0]
    cross_pension_count = fund_summary[fund_summary["pension_count"] >= 2].shape[0]

    # ── KPI Row ────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Commitments", f"${total_commitment_bn:,.0f}B")
    c2.metric("Funds Tracked", f"{total_funds:,}")
    c3.metric("Pension Systems", f"{total_pensions}")
    c4.metric("General Partners", f"{total_gps:,}")
    c5.metric("Cross-Pension Funds", f"{cross_pension_count:,}")

    st.markdown("")

    # ── Pension System Overview ────────────────────────────────────────
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

    # ── Charts Row 1: Vintage + Strategy ──────────────────────────────
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

    # ── Charts Row 2: GP bar + Performance scatter ────────────────────
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
            textposition="outside",
        ))
        plotly_dark_layout(fig, height=450, showlegend=False,
                          xaxis_title="Total Commitments ($B)",
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        section_header("Performance: IRR vs Net Multiple")

        perf_data = df.dropna(subset=["net_irr", "net_multiple"]).copy()
        perf_data = perf_data[
            (perf_data["net_irr"].between(-0.5, 1.0)) &
            (perf_data["net_multiple"].between(0, 5.0))
        ]

        if not perf_data.empty:
            fig = px.scatter(
                perf_data, x="net_irr", y="net_multiple",
                color="pension_fund",
                size="commitment_mm",
                size_max=15,
                opacity=0.6,
                color_discrete_sequence=PALETTE,
                labels={
                    "net_irr": "Net IRR",
                    "net_multiple": "Net Multiple (TVPI)",
                    "pension_fund": "Pension System",
                    "commitment_mm": "Commitment ($M)",
                },
                hover_data={"fund_name": True, "vintage_year": True},
            )
            plotly_dark_layout(fig, height=450)
            fig.update_xaxes(tickformat=".0%")
            fig.add_hline(y=1.0, line_dash="dash", line_color="#64748B", opacity=0.5)
            fig.add_vline(x=0, line_dash="dash", line_color="#64748B", opacity=0.5)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient performance data for scatter plot.")

    # ── Fund Search ────────────────────────────────────────────────────
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

    # ── Top Funds Table ────────────────────────────────────────────────
    section_header("Top 25 Funds by Total Commitment")

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

    st.markdown("")

    # ── Cross-Pension Comparison ───────────────────────────────────────
    section_header("Cross-Pension Fund Comparison")
    st.markdown(
        '<p class="main-subtitle">'
        'Funds committed to by multiple pension systems — see how the same fund '
        'is reported across different LPs'
        '</p>',
        unsafe_allow_html=True,
    )

    cross_funds = fund_summary[fund_summary["pension_count"] >= 2].copy()

    if cross_funds.empty:
        st.info("No funds found in multiple pension systems.")
    else:
        col1, col2 = st.columns([1, 2])

        with col1:
            st.write(f"**{len(cross_funds)}** funds appear in 2+ pension systems")

            # Distribution chart
            dist = cross_funds["pension_count"].value_counts().sort_index()
            fig = go.Figure(go.Bar(
                x=[f"{n} pensions" for n in dist.index],
                y=dist.values,
                marker_color=ACCENT_BLUE,
                text=dist.values,
                textposition="outside",
            ))
            plotly_dark_layout(fig, height=250, showlegend=False,
                              yaxis_title="Number of Funds")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            cross_fund_names = cross_funds["fund_name"].tolist()
            selected_fund = st.selectbox(
                "Select a fund for side-by-side comparison",
                options=cross_fund_names,
                index=0,
            )

            if selected_fund:
                fund_data = df[df["fund_name"] == selected_fund].copy()
                fund_data["commitment_mm"] = fund_data["commitment_mm"].apply(fmt_dollars)
                fund_data["net_irr"] = fund_data["net_irr"].apply(fmt_irr)
                fund_data["net_multiple"] = fund_data["net_multiple"].apply(fmt_multiple)
                fund_data["capital_called_mm"] = fund_data["capital_called_mm"].apply(fmt_dollars)
                fund_data["capital_distributed_mm"] = fund_data["capital_distributed_mm"].apply(fmt_dollars)

                st.dataframe(
                    fund_data[["pension_fund", "commitment_mm", "net_irr", "net_multiple",
                               "capital_called_mm", "capital_distributed_mm", "as_of_date"]],
                    column_config={
                        "pension_fund": "Pension System",
                        "commitment_mm": "Commitment",
                        "net_irr": "Net IRR",
                        "net_multiple": "Net Multiple",
                        "capital_called_mm": "Called",
                        "capital_distributed_mm": "Distributed",
                        "as_of_date": "As Of Date",
                    },
                    use_container_width=True,
                    hide_index=True,
                )

    st.markdown("")

    # ── GP Leaderboard ─────────────────────────────────────────────────
    section_header("GP Leaderboard — Top 25 by Total Commitment")

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

    # ── About / Methodology ───────────────────────────────────────────
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

    # ── Footer ─────────────────────────────────────────────────────────
    st.markdown("")
    st.divider()
    st.markdown(
        "<div style='text-align: center; color: #94A3B8; font-size: 0.8rem; padding: 8px 0;'>"
        "Pension Fund Alternative Investment Tracker &bull; "
        "Data sourced from official public pension fund disclosures &bull; "
        "Deterministic extraction with provenance tracking"
        "<br>"
        "Built by <strong>Nathan Goldberg</strong> &nbsp;|&nbsp; "
        "<a href='mailto:nathanmauricegoldberg@gmail.com' style='color: #0984E3; text-decoration: none;'>nathanmauricegoldberg@gmail.com</a> &nbsp;|&nbsp; "
        "<a href='https://www.linkedin.com/in/nathan-goldberg-62a44522a' target='_blank' style='color: #0984E3; text-decoration: none;'>LinkedIn</a>"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
