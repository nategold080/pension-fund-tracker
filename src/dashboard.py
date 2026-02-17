"""Streamlit dashboard for the Pension Fund Alternative Investment Tracker.

Launch with: python -m src dashboard
Or directly: streamlit run src/dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path("data/pension_tracker.db")


@st.cache_resource
def get_connection():
    """Get a cached database connection."""
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300)
def load_commitments():
    """Load all commitments with joined fund and pension fund data."""
    conn = get_connection()
    query = """
        SELECT c.id, c.commitment_mm, c.vintage_year, c.net_irr, c.net_multiple,
               c.capital_called_mm, c.capital_distributed_mm, c.remaining_value_mm,
               c.dpi, c.as_of_date,
               f.fund_name, f.general_partner, f.asset_class, f.sub_strategy,
               p.name as pension_fund, p.state as pension_state
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        JOIN pension_funds p ON c.pension_fund_id = p.id
        ORDER BY f.fund_name
    """
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=300)
def load_fund_summary():
    """Load fund-level summary aggregated across pension systems."""
    conn = get_connection()
    query = """
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
    """
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=300)
def load_gp_summary():
    """Load GP-level summary."""
    conn = get_connection()
    query = """
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
    """
    return pd.read_sql_query(query, conn)


def main():
    st.set_page_config(
        page_title="Pension Fund Investment Tracker",
        page_icon=None,
        layout="wide",
    )

    st.title("Pension Fund Alternative Investment Tracker")

    if not DB_PATH.exists():
        st.error(f"Database not found at {DB_PATH}. Run the pipeline first.")
        return

    df = load_commitments()
    fund_summary = load_fund_summary()
    gp_summary = load_gp_summary()

    # --- Summary Banner ---
    total_funds = fund_summary.shape[0]
    total_pensions = df["pension_fund"].nunique()
    total_commitment_bn = df["commitment_mm"].sum() / 1000.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Funds Tracked", f"{total_funds:,}")
    col2.metric("Pension Systems", f"{total_pensions}")
    col3.metric("Total Commitments", f"${total_commitment_bn:,.1f}B")

    st.divider()

    # --- Fund Search ---
    st.header("Fund Search")
    search_query = st.text_input(
        "Search by fund name or GP",
        placeholder="e.g. KKR, Blackstone Capital Partners, Apollo...",
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
            st.write(f"Found {results['fund_name'].nunique()} funds, {len(results)} commitment records")

            display_cols = [
                "pension_fund", "fund_name", "general_partner", "vintage_year",
                "commitment_mm", "net_irr", "net_multiple", "as_of_date",
            ]
            display_df = results[display_cols].copy()
            display_df = display_df.rename(columns={
                "pension_fund": "Pension System",
                "fund_name": "Fund Name",
                "general_partner": "GP",
                "vintage_year": "Vintage",
                "commitment_mm": "Commitment ($M)",
                "net_irr": "Net IRR",
                "net_multiple": "Net Multiple",
                "as_of_date": "As Of Date",
            })
            # Format IRR as percentage
            display_df["Net IRR"] = display_df["Net IRR"].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) else ""
            )
            display_df["Net Multiple"] = display_df["Net Multiple"].apply(
                lambda x: f"{x:.2f}x" if pd.notna(x) else ""
            )
            display_df["Commitment ($M)"] = display_df["Commitment ($M)"].apply(
                lambda x: f"${x:,.1f}" if pd.notna(x) else ""
            )

            st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.divider()

    # --- Top Funds Table ---
    st.header("Top 20 Funds by Total Commitment")
    top_funds = fund_summary.head(20).copy()
    top_funds_display = top_funds[["fund_name", "general_partner", "vintage_year",
                                    "pension_count", "total_commitment_mm",
                                    "avg_irr", "avg_multiple"]].copy()
    top_funds_display = top_funds_display.rename(columns={
        "fund_name": "Fund Name",
        "general_partner": "GP",
        "vintage_year": "Vintage",
        "pension_count": "Pensions",
        "total_commitment_mm": "Total Commitment ($M)",
        "avg_irr": "Avg IRR",
        "avg_multiple": "Avg Multiple",
    })
    top_funds_display["Avg IRR"] = top_funds_display["Avg IRR"].apply(
        lambda x: f"{x:.1%}" if pd.notna(x) else ""
    )
    top_funds_display["Avg Multiple"] = top_funds_display["Avg Multiple"].apply(
        lambda x: f"{x:.2f}x" if pd.notna(x) else ""
    )
    top_funds_display["Total Commitment ($M)"] = top_funds_display["Total Commitment ($M)"].apply(
        lambda x: f"${x:,.1f}" if pd.notna(x) else ""
    )
    st.dataframe(top_funds_display, use_container_width=True, hide_index=True)

    st.divider()

    # --- Cross-Pension Comparison ---
    st.header("Cross-Pension Comparison")
    st.caption("Funds committed to by 2+ pension systems -- the data that Preqin charges $15K/year for")

    cross_funds = fund_summary[fund_summary["pension_count"] >= 2].copy()

    if cross_funds.empty:
        st.info("No funds found in multiple pension systems.")
    else:
        st.write(f"{len(cross_funds)} funds appear in 2+ pension systems")

        # Let user pick a fund to see side-by-side
        cross_fund_names = cross_funds["fund_name"].tolist()
        selected_fund = st.selectbox(
            "Select a fund for side-by-side comparison",
            options=cross_fund_names,
            index=0,
        )

        if selected_fund:
            fund_data = df[df["fund_name"] == selected_fund]
            comparison_cols = [
                "pension_fund", "commitment_mm", "net_irr", "net_multiple",
                "capital_called_mm", "capital_distributed_mm", "as_of_date",
            ]
            comp_df = fund_data[comparison_cols].copy()
            comp_df = comp_df.rename(columns={
                "pension_fund": "Pension System",
                "commitment_mm": "Commitment ($M)",
                "net_irr": "Net IRR",
                "net_multiple": "Net Multiple",
                "capital_called_mm": "Called ($M)",
                "capital_distributed_mm": "Distributed ($M)",
                "as_of_date": "As Of Date",
            })
            comp_df["Net IRR"] = comp_df["Net IRR"].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) else ""
            )
            comp_df["Net Multiple"] = comp_df["Net Multiple"].apply(
                lambda x: f"{x:.2f}x" if pd.notna(x) else ""
            )
            for col in ["Commitment ($M)", "Called ($M)", "Distributed ($M)"]:
                comp_df[col] = comp_df[col].apply(
                    lambda x: f"${x:,.1f}" if pd.notna(x) else ""
                )

            st.dataframe(comp_df, use_container_width=True, hide_index=True)

    st.divider()

    # --- Vintage Year Chart ---
    st.header("Commitment Volume by Vintage Year")

    vintage_data = df.dropna(subset=["vintage_year"]).copy()
    vintage_data["vintage_year"] = vintage_data["vintage_year"].astype(int)
    vintage_agg = (
        vintage_data.groupby("vintage_year")
        .agg(
            total_commitment=("commitment_mm", "sum"),
            fund_count=("fund_name", "nunique"),
        )
        .reset_index()
    )
    vintage_agg["total_commitment_bn"] = vintage_agg["total_commitment"] / 1000.0

    fig = px.bar(
        vintage_agg,
        x="vintage_year",
        y="total_commitment_bn",
        labels={
            "vintage_year": "Vintage Year",
            "total_commitment_bn": "Total Commitments ($B)",
        },
        title="",
        hover_data={"fund_count": True, "total_commitment_bn": ":.2f"},
    )
    fig.update_layout(
        xaxis_title="Vintage Year",
        yaxis_title="Total Commitments ($B)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- GP Leaderboard ---
    st.header("GP Leaderboard -- Top 20 by Total Commitment")

    top_gps = gp_summary.head(20).copy()
    top_gps_display = top_gps[["general_partner", "fund_count", "pension_count",
                                "total_commitment_mm", "avg_irr", "avg_multiple",
                                "earliest_vintage", "latest_vintage"]].copy()
    top_gps_display = top_gps_display.rename(columns={
        "general_partner": "General Partner",
        "fund_count": "Funds",
        "pension_count": "Pensions",
        "total_commitment_mm": "Total Commitment ($M)",
        "avg_irr": "Avg IRR",
        "avg_multiple": "Avg Multiple",
        "earliest_vintage": "Earliest Vintage",
        "latest_vintage": "Latest Vintage",
    })
    top_gps_display["Avg IRR"] = top_gps_display["Avg IRR"].apply(
        lambda x: f"{x:.1%}" if pd.notna(x) else ""
    )
    top_gps_display["Avg Multiple"] = top_gps_display["Avg Multiple"].apply(
        lambda x: f"{x:.2f}x" if pd.notna(x) else ""
    )
    top_gps_display["Total Commitment ($M)"] = top_gps_display["Total Commitment ($M)"].apply(
        lambda x: f"${x:,.1f}" if pd.notna(x) else ""
    )
    st.dataframe(top_gps_display, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
