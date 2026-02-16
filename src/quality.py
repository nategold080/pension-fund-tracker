"""Data quality module for pension fund tracker.

Performs value range checks, completeness scoring, cross-fund consistency,
and generates review queue entries and quality reports.
"""

import logging
from datetime import datetime
from typing import Optional

from src.database import Database

logger = logging.getLogger(__name__)

# Reasonable ranges — widened from original CLAUDE.md defaults to reduce false flags
COMMITMENT_MIN_MM = 0.0      # Some legitimate sub-$1M commitments (e.g. state programs)
COMMITMENT_MAX_MM = 5000.0
IRR_MIN = -0.30              # Early-vintage / distressed strategies can trail further
IRR_MAX = 1.0                # Top-quartile early VC vintages can exceed 50%
MULTIPLE_MIN = 0.0           # Recent-vintage funds at 0.1-0.5x are normal
MULTIPLE_MAX = 10.0          # Mature VC funds can legitimately reach 5-10x
VINTAGE_MIN = 1980           # Some legacy commitments from the 1980s exist
VINTAGE_MAX = datetime.now().year


class QualityChecker:
    """Runs data quality checks and generates reports."""

    def __init__(self, db: Database):
        self.db = db

    def run_all_checks(self) -> dict:
        """Run all quality checks and return a summary.

        Returns:
            Dict with check results and flagged items.
        """
        commitments = self.db.get_commitments_joined()
        if not commitments:
            return {"total_records": 0, "checks": {}, "flags": []}

        # Clear stale flags before re-checking to avoid accumulating duplicates
        self.db.clear_review_items_by_type("value_range")
        self.db.clear_review_items_by_type("low_completeness")

        flags = []
        flags.extend(self._check_value_ranges(commitments))
        flags.extend(self._check_completeness(commitments))
        flags.extend(self._check_cross_fund_consistency())

        # Insert flags into review queue
        for flag in flags:
            if flag.get("commitment_id"):
                self.db.add_review_item(
                    commitment_id=flag["commitment_id"],
                    flag_type=flag["flag_type"],
                    flag_detail=flag["flag_detail"],
                )

        completeness = self._compute_completeness(commitments)
        summary = {
            "total_records": len(commitments),
            "flags_created": len(flags),
            "completeness": completeness,
            "pension_funds": self._per_pension_fund_stats(commitments),
        }
        return summary

    def _check_value_ranges(self, commitments: list[dict]) -> list[dict]:
        """Check that values fall within reasonable ranges."""
        flags = []
        for c in commitments:
            cid = c["id"]
            name = c.get("fund_name", c.get("fund_name_raw", "Unknown"))

            # Commitment size
            if c.get("commitment_mm") is not None:
                v = c["commitment_mm"]
                if v < COMMITMENT_MIN_MM or v > COMMITMENT_MAX_MM:
                    flags.append({
                        "commitment_id": cid,
                        "flag_type": "value_range",
                        "flag_detail": f"Commitment ${v:.1f}M outside range "
                                       f"[${COMMITMENT_MIN_MM}M, ${COMMITMENT_MAX_MM}M] "
                                       f"for {name}",
                    })

            # IRR
            if c.get("net_irr") is not None:
                v = c["net_irr"]
                if v < IRR_MIN or v > IRR_MAX:
                    flags.append({
                        "commitment_id": cid,
                        "flag_type": "value_range",
                        "flag_detail": f"Net IRR {v:.1%} outside range "
                                       f"[{IRR_MIN:.0%}, {IRR_MAX:.0%}] for {name}",
                    })

            # Net multiple
            if c.get("net_multiple") is not None:
                v = c["net_multiple"]
                if v < MULTIPLE_MIN or v > MULTIPLE_MAX:
                    flags.append({
                        "commitment_id": cid,
                        "flag_type": "value_range",
                        "flag_detail": f"Net multiple {v:.2f}x outside range "
                                       f"[{MULTIPLE_MIN}x, {MULTIPLE_MAX}x] for {name}",
                    })

            # Vintage year
            if c.get("vintage_year") is not None:
                v = c["vintage_year"]
                if v < VINTAGE_MIN or v > VINTAGE_MAX:
                    flags.append({
                        "commitment_id": cid,
                        "flag_type": "value_range",
                        "flag_detail": f"Vintage year {v} outside range "
                                       f"[{VINTAGE_MIN}, {VINTAGE_MAX}] for {name}",
                    })

        return flags

    def _check_completeness(self, commitments: list[dict]) -> list[dict]:
        """Flag records with very low field completeness."""
        flags = []
        key_fields = [
            "commitment_mm", "vintage_year", "capital_called_mm",
            "capital_distributed_mm", "net_irr", "net_multiple",
        ]
        for c in commitments:
            populated = sum(1 for f in key_fields if c.get(f) is not None)
            ratio = populated / len(key_fields)
            if ratio < 0.33:
                flags.append({
                    "commitment_id": c["id"],
                    "flag_type": "low_completeness",
                    "flag_detail": f"Only {populated}/{len(key_fields)} key fields populated "
                                   f"({ratio:.0%}) for {c.get('fund_name', 'Unknown')}",
                })
        return flags

    def _check_cross_fund_consistency(self) -> list[dict]:
        """Check that the same fund has consistent attributes across pension systems."""
        flags = []

        # 1. Vintage year consistency
        rows = self.db.conn.execute(
            """SELECT f.id, f.fund_name, f.vintage_year,
                GROUP_CONCAT(DISTINCT c.vintage_year) as vintage_years,
                COUNT(DISTINCT c.pension_fund_id) as pension_count
            FROM funds f
            JOIN commitments c ON f.id = c.fund_id
            GROUP BY f.id
            HAVING pension_count > 1"""
        ).fetchall()

        for row in rows:
            row = dict(row)
            vintages = row["vintage_years"]
            if vintages and "," in str(vintages):
                flags.append({
                    "commitment_id": None,
                    "flag_type": "cross_fund_inconsistency",
                    "flag_detail": f"Fund '{row['fund_name']}' has inconsistent vintage years "
                                   f"across pension systems: {vintages}",
                })

        # 2. Net multiple consistency — flag if same fund's multiples differ by >0.5x
        mult_rows = self.db.conn.execute(
            """SELECT f.fund_name, p.name as pension, c.net_multiple, c.as_of_date
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            WHERE c.net_multiple IS NOT NULL
            AND f.id IN (
                SELECT fund_id FROM commitments
                WHERE net_multiple IS NOT NULL
                GROUP BY fund_id
                HAVING COUNT(DISTINCT pension_fund_id) > 1
            )
            ORDER BY f.fund_name, p.name"""
        ).fetchall()

        # Group by fund
        by_fund = {}
        for r in mult_rows:
            fn = r["fund_name"]
            if fn not in by_fund:
                by_fund[fn] = []
            by_fund[fn].append((r["pension"], r["net_multiple"], r["as_of_date"]))

        for fn, entries in by_fund.items():
            multiples = [e[1] for e in entries]
            if max(multiples) - min(multiples) > 0.5:
                detail_parts = [f"{e[0]}: {e[1]:.2f}x (as of {e[2]})" for e in entries]
                flags.append({
                    "commitment_id": None,
                    "flag_type": "cross_fund_inconsistency",
                    "flag_detail": f"Fund '{fn}' has divergent net multiples: {'; '.join(detail_parts)}",
                })

        return flags

    def generate_cross_fund_report(self) -> str:
        """Generate a detailed cross-fund consistency report.

        Shows funds that appear in multiple pension systems, comparing
        their reported values side by side.
        """
        rows = self.db.conn.execute(
            """SELECT f.fund_name, f.vintage_year, p.name as pension,
                c.commitment_mm, c.net_irr, c.net_multiple, c.as_of_date
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            WHERE f.id IN (
                SELECT fund_id FROM commitments
                GROUP BY fund_id
                HAVING COUNT(DISTINCT pension_fund_id) >= 2
            )
            ORDER BY f.fund_name, p.name"""
        ).fetchall()

        # Group by fund
        by_fund = {}
        for r in rows:
            fn = r["fund_name"]
            if fn not in by_fund:
                by_fund[fn] = {"vintage_year": r["vintage_year"], "pensions": []}
            by_fund[fn]["pensions"].append(dict(r))

        lines = ["# Cross-Fund Consistency Report"]
        lines.append(f"\nFunds appearing in 2+ pension systems: {len(by_fund)}\n")

        # Summary table of funds with the largest multiple divergence
        lines.append("## Largest Multiple Divergences\n")
        lines.append("| Fund | Vintage | Pensions | Min Multiple | Max Multiple | Spread |")
        lines.append("|---|---:|---:|---:|---:|---:|")

        divergences = []
        for fn, data in by_fund.items():
            multiples = [p["net_multiple"] for p in data["pensions"] if p["net_multiple"] is not None]
            if len(multiples) >= 2:
                spread = max(multiples) - min(multiples)
                pensions = ", ".join(sorted(set(p["pension"] for p in data["pensions"])))
                divergences.append((fn, data["vintage_year"], pensions, min(multiples), max(multiples), spread))

        for fn, vy, pensions, mn, mx, spread in sorted(divergences, key=lambda x: -x[5])[:20]:
            lines.append(f"| {fn} | {vy} | {pensions} | {mn:.2f}x | {mx:.2f}x | {spread:.2f}x |")

        # IRR divergences
        lines.append("\n## Largest IRR Divergences\n")
        lines.append("| Fund | Vintage | Pensions | Min IRR | Max IRR | Spread |")
        lines.append("|---|---:|---:|---:|---:|---:|")

        irr_divs = []
        for fn, data in by_fund.items():
            irrs = [p["net_irr"] for p in data["pensions"] if p["net_irr"] is not None]
            if len(irrs) >= 2:
                spread = max(irrs) - min(irrs)
                pensions = ", ".join(sorted(set(p["pension"] for p in data["pensions"])))
                irr_divs.append((fn, data["vintage_year"], pensions, min(irrs), max(irrs), spread))

        for fn, vy, pensions, mn, mx, spread in sorted(irr_divs, key=lambda x: -x[5])[:20]:
            lines.append(f"| {fn} | {vy} | {pensions} | {mn:.1%} | {mx:.1%} | {spread:.1%} |")

        return "\n".join(lines)

    def _compute_completeness(self, commitments: list[dict]) -> dict:
        """Compute field-level completeness percentages."""
        fields = [
            "commitment_mm", "vintage_year", "capital_called_mm",
            "capital_distributed_mm", "remaining_value_mm",
            "net_irr", "net_multiple", "dpi", "as_of_date",
        ]
        total = len(commitments)
        if total == 0:
            return {}
        result = {}
        for f in fields:
            populated = sum(1 for c in commitments if c.get(f) is not None)
            result[f] = round(populated / total * 100, 1)
        return result

    def _per_pension_fund_stats(self, commitments: list[dict]) -> dict:
        """Compute per-pension-fund statistics."""
        by_pf = {}
        for c in commitments:
            pf = c.get("pension_fund_name", "Unknown")
            if pf not in by_pf:
                by_pf[pf] = {"count": 0, "total_commitment_mm": 0.0}
            by_pf[pf]["count"] += 1
            if c.get("commitment_mm"):
                by_pf[pf]["total_commitment_mm"] += c["commitment_mm"]
        # Round totals
        for pf in by_pf:
            by_pf[pf]["total_commitment_mm"] = round(by_pf[pf]["total_commitment_mm"], 1)
        return by_pf

    def generate_report(self) -> str:
        """Generate a Markdown quality report.

        Returns:
            Markdown string with the quality report.
        """
        summary = self.run_all_checks()
        lines = []
        lines.append("# Data Quality Report")
        lines.append(f"\nGenerated: {datetime.now().isoformat()[:19]}")
        lines.append(f"\n## Overview")
        lines.append(f"\n- **Total commitment records:** {summary['total_records']}")
        lines.append(f"- **Quality flags created:** {summary['flags_created']}")

        # Per pension fund
        lines.append(f"\n## Pension Fund Coverage")
        lines.append(f"\n| Pension Fund | Records | Total Commitment ($M) |")
        lines.append(f"|---|---:|---:|")
        for pf, stats in sorted(summary.get("pension_funds", {}).items()):
            lines.append(
                f"| {pf} | {stats['count']} | "
                f"{stats['total_commitment_mm']:,.1f} |"
            )

        # Completeness
        lines.append(f"\n## Field Completeness")
        lines.append(f"\n| Field | Populated % |")
        lines.append(f"|---|---:|")
        for field, pct in sorted(summary.get("completeness", {}).items()):
            lines.append(f"| {field} | {pct}% |")

        # Review queue
        review = self.db.get_review_queue(resolved=False)
        if review:
            lines.append(f"\n## Review Queue ({len(review)} items)")
            lines.append(f"\n| Type | Detail |")
            lines.append(f"|---|---|")
            for item in review[:20]:
                lines.append(f"| {item['flag_type']} | {item['flag_detail'][:80]} |")
            if len(review) > 20:
                lines.append(f"\n*... and {len(review) - 20} more items*")

        return "\n".join(lines)
