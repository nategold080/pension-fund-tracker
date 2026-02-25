"""Demo analysis queries for the pension fund tracker.

Generates CSV exports answering the questions a placement agent,
emerging GP, or data buyer would actually ask this dataset.

Outputs are saved to data/exports/demo/.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

from src.database import Database

logger = logging.getLogger(__name__)

DEMO_DIR = Path("data/exports/demo")

# Human-readable column names for demo exports
FRIENDLY_HEADERS = {
    "fund_name": "Fund Name",
    "general_partner": "General Partner",
    "vintage_year": "Vintage Year",
    "asset_class": "Asset Class",
    "sub_strategy": "Sub-Strategy",
    "pension_fund": "Pension Fund",
    "pension_fund_name": "Pension Fund",
    "pension_fund_state": "State",
    "commitment_mm": "Commitment ($M)",
    "capital_called_mm": "Capital Called ($M)",
    "capital_distributed_mm": "Distributions ($M)",
    "remaining_value_mm": "Remaining Value ($M)",
    "net_irr": "Net IRR (%)",
    "net_multiple": "Net Multiple (x)",
    "dpi": "DPI (x)",
    "as_of_date": "As-Of Date",
    "source_url": "Source URL",
    "source_document": "Source Document",
    "extraction_method": "Extraction Method",
    "extraction_confidence": "Confidence",
    "pension_count": "# Pension Systems",
    "pensions": "Pension Systems",
    "total_commitment_mm": "Total Commitment ($M)",
    "avg_irr": "Avg Net IRR (%)",
    "avg_multiple": "Avg Net Multiple (x)",
    "avg_commitment_mm": "Avg Commitment ($M)",
    "fund_count": "# Funds",
    "earliest_vintage": "Earliest Vintage",
    "latest_vintage": "Latest Vintage",
}


def _write_csv(filepath: Path, rows: list[dict], fieldnames: list[str],
               irr_fields: list[str] = None, mm_fields: list[str] = None,
               mult_fields: list[str] = None):
    """Write CSV with friendly headers and formatted values."""
    irr_fields = irr_fields or []
    mm_fields = mm_fields or []
    mult_fields = mult_fields or []

    friendly = [FRIENDLY_HEADERS.get(f, f) for f in fieldnames]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(friendly)
        for row in rows:
            out = []
            for field in fieldnames:
                val = row.get(field)
                if val is None:
                    out.append("")
                elif field in irr_fields:
                    out.append(f"{val * 100:.1f}%")
                elif field in mm_fields and isinstance(val, (int, float)):
                    out.append(f"{val:,.1f}")
                elif field in mult_fields and isinstance(val, (int, float)):
                    out.append(f"{val:.2f}x")
                else:
                    out.append(val)
            writer.writerow(out)

    logger.info(f"Wrote {len(rows)} rows to {filepath}")
    return filepath


def generate_emerging_manager_commitments(db: Database) -> Path:
    """Which pension funds committed to first-time funds (vintage 2020+, Fund I or Fund II)?"""
    rows = db.conn.execute("""
        SELECT f.fund_name, f.general_partner, f.vintage_year,
               p.name as pension_fund, p.state as pension_fund_state,
               c.commitment_mm, c.net_irr, c.net_multiple, c.as_of_date
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        JOIN pension_funds p ON c.pension_fund_id = p.id
        WHERE f.vintage_year >= 2020
        AND (
            f.fund_name LIKE '% I' OR f.fund_name LIKE '% I %'
            OR f.fund_name LIKE '% II' OR f.fund_name LIKE '% II %'
            OR f.fund_name LIKE '%Fund 1 %' OR f.fund_name LIKE '%Fund 2 %'
            OR f.fund_name LIKE '%Fund 1' OR f.fund_name LIKE '%Fund 2'
        )
        AND f.fund_name NOT LIKE '% III%'
        AND f.fund_name NOT LIKE '% IV%'
        AND f.fund_name NOT LIKE '% V %'
        AND f.fund_name NOT LIKE '% VI%'
        ORDER BY f.vintage_year DESC, f.fund_name, p.name
    """).fetchall()

    rows = [dict(r) for r in rows]
    filepath = DEMO_DIR / "emerging_manager_commitments.csv"
    return _write_csv(
        filepath, rows,
        ["fund_name", "vintage_year", "pension_fund",
         "pension_fund_state", "commitment_mm", "net_irr", "net_multiple", "as_of_date"],
        irr_fields=["net_irr"], mm_fields=["commitment_mm"], mult_fields=["net_multiple"],
    )


def generate_buyout_performance(db: Database) -> Path:
    """PE funds vintage 2015-2020 with IRR data — performance benchmarking."""
    rows = db.conn.execute("""
        SELECT f.fund_name, f.general_partner, f.vintage_year,
               f.sub_strategy,
               p.name as pension_fund,
               c.commitment_mm, c.net_irr, c.net_multiple, c.as_of_date
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        JOIN pension_funds p ON c.pension_fund_id = p.id
        WHERE f.vintage_year BETWEEN 2015 AND 2020
        AND f.asset_class = 'Private Equity'
        AND c.net_irr IS NOT NULL
        ORDER BY c.net_irr DESC, f.fund_name
    """).fetchall()

    rows = [dict(r) for r in rows]
    filepath = DEMO_DIR / "pe_performance_2015_2020.csv"
    return _write_csv(
        filepath, rows,
        ["fund_name", "vintage_year", "sub_strategy",
         "pension_fund", "commitment_mm", "net_irr", "net_multiple", "as_of_date"],
        irr_fields=["net_irr"], mm_fields=["commitment_mm"], mult_fields=["net_multiple"],
    )


def _extract_gp_from_fund_name(name: str) -> str:
    """Extract the GP/firm name from a fund name.

    Most PE funds follow patterns like 'KKR North America Fund XI',
    'Blackstone Capital Partners VI', 'TPG Growth III'. We extract
    the portion before fund number indicators.
    """
    import re
    # Strip common suffixes first (L.P., L.P.1, LLC, SCSp, etc.)
    cleaned = re.sub(r',?\s*(L\.?P\.?\d?|LLC|Ltd|SCSp|S\.C\.Sp\.?|Cooperatief U\.A\.?)$', '', name, flags=re.I).strip()
    # Remove trailing roman numerals, digits, and fund number patterns
    # Match: Fund IV, Partners III, Capital V, etc. at end
    cleaned = re.sub(
        r'\s+(Fund\s+)?[IVXLC]+(-[A-Z0-9]+)?(\s+\(.*\))?\s*$', '', cleaned
    ).strip()
    # Handle "Fund" at end with no number (e.g., "KKR 2006 Fund", "KKR Millennium Fund")
    cleaned = re.sub(r'\s+Fund\s*$', '', cleaned).strip()
    # Also handle "Partners 2022" style
    cleaned = re.sub(r'\s+\d{4}\s*$', '', cleaned).strip()
    # Remove trailing single letters (A, B, C class designators)
    cleaned = re.sub(r'\s+[A-D]\s*$', '', cleaned).strip()
    # Collapse - trim
    return cleaned.strip() if cleaned else name


def generate_gp_penetration(db: Database) -> Path:
    """Which fund families have raised capital from 3+ pension funds in our database?"""
    # Get all funds with commitments
    rows = db.conn.execute("""
        SELECT f.id, f.fund_name, f.vintage_year,
               c.pension_fund_id, c.commitment_mm, c.net_irr, c.net_multiple
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        WHERE c.commitment_mm IS NOT NULL
    """).fetchall()

    # Group by derived GP name
    from collections import defaultdict
    gp_data = defaultdict(lambda: {
        "fund_ids": set(), "pension_ids": set(), "fund_names": set(),
        "total_mm": 0.0, "irrs": [], "multiples": [],
        "vintages": [],
    })

    for r in rows:
        gp = _extract_gp_from_fund_name(r["fund_name"])
        d = gp_data[gp]
        d["fund_ids"].add(r["id"])
        d["pension_ids"].add(r["pension_fund_id"])
        d["fund_names"].add(r["fund_name"])
        d["total_mm"] += r["commitment_mm"] or 0
        if r["net_irr"] is not None:
            d["irrs"].append(r["net_irr"])
        if r["net_multiple"] is not None:
            d["multiples"].append(r["net_multiple"])
        if r["vintage_year"]:
            d["vintages"].append(r["vintage_year"])

    # Get pension fund names for display
    pf_names = {}
    for pf in db.conn.execute("SELECT id, name FROM pension_funds").fetchall():
        pf_names[pf["id"]] = pf["name"]

    # Filter to 3+ pension systems and build output rows
    output = []
    for gp, d in gp_data.items():
        if len(d["pension_ids"]) >= 3:
            output.append({
                "general_partner": gp,
                "pension_count": len(d["pension_ids"]),
                "fund_count": len(d["fund_ids"]),
                "total_commitment_mm": d["total_mm"],
                "avg_irr": sum(d["irrs"]) / len(d["irrs"]) if d["irrs"] else None,
                "avg_multiple": sum(d["multiples"]) / len(d["multiples"]) if d["multiples"] else None,
                "earliest_vintage": min(d["vintages"]) if d["vintages"] else None,
                "latest_vintage": max(d["vintages"]) if d["vintages"] else None,
                "pensions": ", ".join(sorted(pf_names.get(pid, pid) for pid in d["pension_ids"])),
            })

    output.sort(key=lambda x: (-x["pension_count"], -x["total_commitment_mm"]))

    filepath = DEMO_DIR / "gp_penetration.csv"
    return _write_csv(
        filepath, output,
        ["general_partner", "pension_count", "fund_count", "total_commitment_mm",
         "avg_irr", "avg_multiple", "earliest_vintage", "latest_vintage", "pensions"],
        irr_fields=["avg_irr"], mm_fields=["total_commitment_mm"], mult_fields=["avg_multiple"],
    )


def generate_commitment_trends(db: Database) -> Path:
    """Average commitment size by vintage year and asset class — deployment trends."""
    rows = db.conn.execute("""
        SELECT c.vintage_year,
               COALESCE(f.sub_strategy, 'Unclassified') as sub_strategy,
               COUNT(*) as fund_count,
               AVG(c.commitment_mm) as avg_commitment_mm,
               SUM(c.commitment_mm) as total_commitment_mm,
               COUNT(DISTINCT c.pension_fund_id) as pension_count
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        WHERE c.vintage_year IS NOT NULL
        AND c.vintage_year >= 2000
        AND c.commitment_mm IS NOT NULL
        AND c.commitment_mm < 5000
        GROUP BY c.vintage_year, COALESCE(f.sub_strategy, 'Unclassified')
        ORDER BY c.vintage_year, sub_strategy
    """).fetchall()

    rows = [dict(r) for r in rows]
    filepath = DEMO_DIR / "commitment_trends.csv"
    fields = ["vintage_year", "sub_strategy", "fund_count", "avg_commitment_mm",
              "total_commitment_mm", "pension_count"]
    return _write_csv(
        filepath, rows, fields,
        mm_fields=["avg_commitment_mm", "total_commitment_mm"],
    )


def generate_vc_commitments_by_pension(db: Database) -> Path:
    """Which pension funds are most active in venture capital?"""
    # Detail rows
    rows = db.conn.execute("""
        SELECT p.name as pension_fund, p.state as pension_fund_state,
               f.fund_name, f.general_partner, f.vintage_year,
               c.commitment_mm, c.net_irr, c.net_multiple, c.as_of_date
        FROM commitments c
        JOIN funds f ON c.fund_id = f.id
        JOIN pension_funds p ON c.pension_fund_id = p.id
        WHERE f.sub_strategy = 'Venture Capital'
        ORDER BY p.name, f.vintage_year DESC, f.fund_name
    """).fetchall()

    rows = [dict(r) for r in rows]
    filepath = DEMO_DIR / "vc_commitments_by_pension.csv"
    return _write_csv(
        filepath, rows,
        ["pension_fund", "pension_fund_state", "fund_name",
         "vintage_year", "commitment_mm", "net_irr", "net_multiple", "as_of_date"],
        irr_fields=["net_irr"], mm_fields=["commitment_mm"], mult_fields=["net_multiple"],
    )


def generate_sample_data(db: Database) -> Path:
    """Curated 100-record sample showcasing cross-linking and full field coverage."""
    # Get cross-linked fund IDs (appear in 2+ pension systems)
    cross_fund_ids = db.conn.execute("""
        SELECT fund_id FROM commitments
        GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 2
    """).fetchall()
    cross_fund_ids = {r["fund_id"] for r in cross_fund_ids}

    # For each pension fund, get records prioritizing cross-linked ones with full fields
    pension_ids = ["calpers", "calstrs", "wsib", "oregon", "ny_common"]
    all_selected = []

    for pf_id in pension_ids:
        # First: cross-linked records with most fields populated
        cross_rows = db.conn.execute("""
            SELECT c.*, f.fund_name, f.general_partner, f.asset_class,
                   f.sub_strategy, f.vintage_year as fund_vy,
                   p.name as pension_fund_name, p.state as pension_fund_state
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            WHERE c.pension_fund_id = ?
            AND c.fund_id IN ({})
            AND c.commitment_mm IS NOT NULL
            ORDER BY
                (CASE WHEN c.net_irr IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN c.net_multiple IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN c.capital_called_mm IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN c.capital_distributed_mm IS NOT NULL THEN 1 ELSE 0 END) DESC,
                c.commitment_mm DESC
        """.format(",".join("?" * len(cross_fund_ids))),
            [pf_id] + list(cross_fund_ids),
        ).fetchall()
        cross_rows = [dict(r) for r in cross_rows]

        # Then: non-cross-linked records
        non_cross_rows = db.conn.execute("""
            SELECT c.*, f.fund_name, f.general_partner, f.asset_class,
                   f.sub_strategy, f.vintage_year as fund_vy,
                   p.name as pension_fund_name, p.state as pension_fund_state
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            WHERE c.pension_fund_id = ?
            AND c.fund_id NOT IN ({})
            AND c.commitment_mm IS NOT NULL
            ORDER BY
                (CASE WHEN c.net_irr IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN c.net_multiple IS NOT NULL THEN 1 ELSE 0 END) DESC,
                c.commitment_mm DESC
        """.format(",".join("?" * len(cross_fund_ids))),
            [pf_id] + list(cross_fund_ids),
        ).fetchall()
        non_cross_rows = [dict(r) for r in non_cross_rows]

        # Take up to 12 cross-linked + fill to 20
        selected = cross_rows[:12]
        remaining_needed = 20 - len(selected)
        if remaining_needed > 0:
            selected.extend(non_cross_rows[:remaining_needed])
        # If still not enough cross-linked, backfill
        if len(selected) < 20:
            selected.extend(cross_rows[12:12 + (20 - len(selected))])

        all_selected.extend(selected[:20])

    # Write with friendly headers
    filepath = DEMO_DIR / "sample_data.csv"
    fieldnames = [
        "pension_fund_name", "pension_fund_state", "fund_name",
        "asset_class", "sub_strategy",
        "vintage_year", "commitment_mm", "capital_called_mm",
        "capital_distributed_mm", "remaining_value_mm",
        "net_irr", "net_multiple", "as_of_date",
        "source_url", "extraction_method", "extraction_confidence",
    ]

    return _write_csv(
        filepath, all_selected, fieldnames,
        irr_fields=["net_irr"],
        mm_fields=["commitment_mm", "capital_called_mm", "capital_distributed_mm",
                    "remaining_value_mm"],
        mult_fields=["net_multiple"],
    )


def generate_dataset_readme(db: Database) -> Path:
    """Generate DATASET_README.md for non-technical audience."""
    # Gather stats
    total_commitments = db.conn.execute("SELECT COUNT(*) FROM commitments").fetchone()[0]
    total_funds = db.conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0]

    pf_stats = db.conn.execute("""
        SELECT p.name, p.full_name, p.state, p.total_aum_mm,
               COUNT(c.id) as record_count,
               SUM(c.commitment_mm) as total_commitment_mm,
               MIN(c.vintage_year) as min_vintage,
               MAX(c.vintage_year) as max_vintage,
               c.as_of_date
        FROM pension_funds p
        JOIN commitments c ON c.pension_fund_id = p.id
        GROUP BY p.id
        ORDER BY record_count DESC
    """).fetchall()

    cross_2 = db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT fund_id FROM commitments
            GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 2
        )
    """).fetchone()[0]
    cross_3 = db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT fund_id FROM commitments
            GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 3
        )
    """).fetchone()[0]

    # Field completeness
    fields_check = {
        "commitment_mm": "Commitment Amount",
        "vintage_year": "Vintage Year",
        "capital_called_mm": "Capital Called",
        "capital_distributed_mm": "Distributions",
        "remaining_value_mm": "Remaining Value",
        "net_irr": "Net IRR",
        "net_multiple": "Net Multiple",
    }
    completeness = {}
    for field, label in fields_check.items():
        cnt = db.conn.execute(
            f"SELECT COUNT(*) FROM commitments WHERE {field} IS NOT NULL"
        ).fetchone()[0]
        completeness[label] = f"{cnt / total_commitments * 100:.0f}%" if total_commitments > 0 else "N/A"

    lines = []
    lines.append("# Alternative Investment Commitment Dataset")
    lines.append("")
    lines.append("## What This Dataset Contains")
    lines.append("")
    lines.append("This dataset contains **private equity, venture capital, and alternative investment**")
    lines.append(f"commitment data extracted from **{len(pf_stats)} major U.S. public pension fund** disclosures.")
    lines.append(f"It covers **{total_commitments:,} commitment records** across **{total_funds:,} unique funds**,")
    lines.append(f"with **{cross_2} funds cross-referenced** across two or more pension systems.")
    lines.append("")
    lines.append("The data is sourced entirely from public disclosures required under state transparency")
    lines.append("laws. No proprietary or paywalled data is included.")
    lines.append("")

    lines.append("## Pension Funds Covered")
    lines.append("")
    lines.append("| Pension Fund | State | Total AUM | Records | Commitment Total | Vintages | Data As-Of |")
    lines.append("|---|---|---:|---:|---:|---|---|")
    for pf in pf_stats:
        pf = dict(pf)
        aum = f"${pf['total_aum_mm']/1000:,.0f}B" if pf.get('total_aum_mm') else "N/A"
        total = f"${pf['total_commitment_mm']:,.0f}M" if pf.get('total_commitment_mm') else "N/A"
        vintages = f"{pf['min_vintage']}-{pf['max_vintage']}" if pf.get('min_vintage') else "N/A"
        lines.append(
            f"| {pf['name']} | {pf['state']} | {aum} | {pf['record_count']:,} | "
            f"{total} | {vintages} | {pf.get('as_of_date', 'N/A')} |"
        )
    lines.append("")

    lines.append("## Cross-Referencing Value")
    lines.append("")
    lines.append("The dataset's unique value is **entity resolution across pension systems**.")
    lines.append("When CalPERS, CalSTRS, WSIB, and Oregon all report commitments to the same fund,")
    lines.append("we link those records together under a single canonical fund name.")
    lines.append("")
    lines.append(f"- **{cross_2} funds** appear in 2 or more pension systems")
    lines.append(f"- **{cross_3} funds** appear in 3 or more pension systems")
    lines.append("")
    lines.append("This allows you to:")
    lines.append("- See which GPs have the broadest LP relationships")
    lines.append("- Compare reported performance for the same fund across different LPs")
    lines.append("- Identify emerging managers who have won allocations from major pensions")
    lines.append("- Validate data accuracy by cross-checking the same fund across sources")
    lines.append("")

    lines.append("## Field Definitions")
    lines.append("")
    lines.append("| Field | Description |")
    lines.append("|---|---|")
    lines.append("| Pension Fund | The public pension system reporting this commitment |")
    lines.append("| State | U.S. state of the pension fund |")
    lines.append("| Fund Name | The canonical name of the private equity or alternative fund |")
    lines.append("| Asset Class | Broad category: Private Equity, Private Credit, Real Assets |")
    lines.append("| Sub-Strategy | Where classified: Venture Capital, Growth Equity, Buyout, Credit, etc. |")
    lines.append("| Vintage Year | Year the fund began investing (first capital call) |")
    lines.append("| Commitment ($M) | Total capital the pension committed to the fund, in millions |")
    lines.append("| Capital Called ($M) | Capital the GP has drawn down from the pension's commitment |")
    lines.append("| Distributions ($M) | Capital returned to the pension (realizations + income) |")
    lines.append("| Remaining Value ($M) | Current fair market value of unrealized holdings |")
    lines.append("| Net IRR (%) | Net internal rate of return after fees, as reported by the pension |")
    lines.append("| Net Multiple (x) | Total value to paid-in capital ratio (TVPI), net of fees |")
    lines.append("| As-Of Date | The reporting date for this data point |")
    lines.append("| Source URL | Direct link to the public document this data was extracted from |")
    lines.append("")

    lines.append("## Field Completeness")
    lines.append("")
    lines.append("Not all pension systems report all fields. Here is what to expect:")
    lines.append("")
    lines.append("| Field | Populated |")
    lines.append("|---|---:|")
    for label, pct in completeness.items():
        lines.append(f"| {label} | {pct} |")
    lines.append("")

    lines.append("## Known Limitations")
    lines.append("")
    lines.append("- **NY Common Retirement Fund does not publish IRR or Net Multiple.** Their disclosure")
    lines.append("  includes commitment, contributed, distributed, and fair value, but not performance metrics.")
    lines.append("- **DPI (distributions to paid-in) is not available** from any source in this dataset.")
    lines.append("- **Data as-of dates vary by pension system.** CalPERS data may be as of June 2025")
    lines.append("  while CalSTRS data is as of March 2025. Comparing IRRs across systems requires")
    lines.append("  awareness of these timing differences.")
    lines.append("- **Sub-strategy classification is incomplete.** Many well-known buyout funds appear")
    lines.append("  without a sub-strategy label because not all pension disclosures categorize funds.")
    lines.append("  Roughly 70% of funds have no sub-strategy assigned.")
    lines.append("- **Texas TRS and Florida SBA have limited data.** Texas provides only a portfolio-level")
    lines.append("  summary. Florida's website blocks programmatic access; their data is not included")
    lines.append("  in the default extraction.")
    lines.append("- **Performance data is net of fees** as reported by each pension fund. Different")
    lines.append("  pensions may use slightly different calculation methodologies, which can produce")
    lines.append("  small differences (typically <2 percentage points) for the same fund.")
    lines.append("")

    lines.append("## Methodology")
    lines.append("")
    lines.append("1. **Source identification:** We identified public disclosure pages for each pension fund")
    lines.append("   and downloaded their most recent private equity portfolio reports.")
    lines.append("2. **Deterministic extraction:** All data was extracted using programmatic parsing")
    lines.append("   (HTML table parsing for CalPERS, PDF table extraction for all others). No AI/LLM")
    lines.append("   was used for data extraction, ensuring reproducibility and accuracy.")
    lines.append("3. **Entity resolution:** Fund names vary across pension systems (e.g., \"KKR 2006 Fund\"")
    lines.append("   vs \"KKR 2006 Fund, L.P.\"). We use fuzzy matching with secondary signals (GP name,")
    lines.append("   vintage year, fund number) to link the same fund across systems.")
    lines.append("4. **Quality checks:** Every record is validated against reasonable ranges for commitment")
    lines.append("   size, IRR, and multiples. Outliers are flagged for manual review.")
    lines.append("")

    lines.append("## Files Included")
    lines.append("")
    lines.append("| File | Description |")
    lines.append("|---|---|")
    lines.append("| `sample_data.csv` | 100 curated records (20 per pension system) showcasing cross-linking |")
    lines.append("| `emerging_manager_commitments.csv` | First-time funds (Fund I/II, vintage 2020+) across all pensions |")
    lines.append("| `pe_performance_2015_2020.csv` | PE fund performance for mature 2015-2020 vintages |")
    lines.append("| `gp_penetration.csv` | Fund families (derived GP names) with commitments from 3+ pension systems |")
    lines.append("| `commitment_trends.csv` | Average commitment size by vintage year and strategy |")
    lines.append("| `vc_commitments_by_pension.csv` | Venture capital commitments by pension fund |")
    lines.append("| `DATASET_README.md` | This file |")
    lines.append("")

    lines.append(f"*Generated {datetime.now().strftime('%B %d, %Y')}*")

    filepath = DEMO_DIR / "DATASET_README.md"
    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Wrote DATASET_README.md to {filepath}")
    return filepath


def run_all(db: Database) -> dict:
    """Generate all demo analysis outputs."""
    DEMO_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    results["emerging_managers"] = generate_emerging_manager_commitments(db)
    results["pe_performance"] = generate_buyout_performance(db)
    results["gp_penetration"] = generate_gp_penetration(db)
    results["commitment_trends"] = generate_commitment_trends(db)
    results["vc_commitments"] = generate_vc_commitments_by_pension(db)
    results["sample_data"] = generate_sample_data(db)
    results["dataset_readme"] = generate_dataset_readme(db)

    return results


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    db = Database()
    db.migrate()
    try:
        results = run_all(db)
        print("\n=== Demo Exports Generated ===\n")
        for name, path in results.items():
            print(f"  {name}: {path}")
    finally:
        db.close()
