"""Export module for pension fund tracker.

Generates CSV exports and Markdown reports from the database.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.database import Database
from src.quality import QualityChecker

logger = logging.getLogger(__name__)

DEFAULT_EXPORT_DIR = Path("data/exports")


class Exporter:
    """Exports data from the database to CSV and Markdown files."""

    def __init__(self, db: Database, export_dir: Optional[Path] = None):
        self.db = db
        self.export_dir = export_dir or DEFAULT_EXPORT_DIR
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def export_commitments_csv(self) -> Path:
        """Export all commitments with joined fund and pension fund names.

        Returns:
            Path to the generated CSV file.
        """
        commitments = self.db.get_commitments_joined()
        if not commitments:
            logger.warning("No commitments to export")
            return None

        filepath = self.export_dir / "commitments.csv"
        fieldnames = [
            "pension_fund_name", "pension_fund_state", "fund_name",
            "general_partner", "asset_class", "sub_strategy",
            "vintage_year", "commitment_mm", "capital_called_mm",
            "capital_distributed_mm", "remaining_value_mm",
            "net_irr", "net_multiple", "dpi", "as_of_date",
            "source_url", "source_document", "extraction_method",
            "extraction_confidence",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for c in commitments:
                row = {k: c.get(k) for k in fieldnames}
                # Format IRR as percentage for readability
                if row.get("net_irr") is not None:
                    row["net_irr"] = f"{row['net_irr']:.4f}"
                writer.writerow(row)

        logger.info(f"Exported {len(commitments)} commitments to {filepath}")
        return filepath

    def export_summary_csv(self) -> Path:
        """Export a summary with one row per fund, aggregated across pension systems.

        Returns:
            Path to the generated CSV file.
        """
        rows = self.db.conn.execute(
            """SELECT
                f.fund_name,
                f.general_partner,
                f.vintage_year,
                f.asset_class,
                f.sub_strategy,
                COUNT(DISTINCT c.pension_fund_id) as pension_fund_count,
                GROUP_CONCAT(DISTINCT p.name) as pension_funds,
                SUM(c.commitment_mm) as total_commitment_mm,
                AVG(c.net_irr) as avg_net_irr,
                AVG(c.net_multiple) as avg_net_multiple
            FROM funds f
            JOIN commitments c ON f.id = c.fund_id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            GROUP BY f.id
            ORDER BY f.fund_name"""
        ).fetchall()

        if not rows:
            logger.warning("No funds to export")
            return None

        filepath = self.export_dir / "fund_summary.csv"
        fieldnames = [
            "fund_name", "general_partner", "vintage_year", "asset_class",
            "sub_strategy", "pension_fund_count", "pension_funds",
            "total_commitment_mm", "avg_net_irr", "avg_net_multiple",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                if d.get("avg_net_irr") is not None:
                    d["avg_net_irr"] = f"{d['avg_net_irr']:.4f}"
                if d.get("avg_net_multiple") is not None:
                    d["avg_net_multiple"] = f"{d['avg_net_multiple']:.4f}"
                if d.get("total_commitment_mm") is not None:
                    d["total_commitment_mm"] = f"{d['total_commitment_mm']:.2f}"
                writer.writerow(d)

        logger.info(f"Exported {len(rows)} fund summaries to {filepath}")
        return filepath

    def export_quality_report(self) -> Path:
        """Generate and export the quality report as Markdown.

        Returns:
            Path to the generated Markdown file.
        """
        checker = QualityChecker(self.db)
        report = checker.generate_report()

        filepath = self.export_dir / "quality_report.md"
        filepath.write_text(report, encoding="utf-8")
        logger.info(f"Exported quality report to {filepath}")
        return filepath

    def export_top_funds_csv(self) -> Path:
        """Export top funds ranked by total commitments received across all pensions.

        Returns:
            Path to the generated CSV file.
        """
        rows = self.db.conn.execute(
            """SELECT
                f.fund_name,
                f.vintage_year,
                f.asset_class,
                f.sub_strategy,
                COUNT(DISTINCT c.pension_fund_id) as pension_count,
                GROUP_CONCAT(DISTINCT p.name) as pensions,
                SUM(c.commitment_mm) as total_commitment_mm,
                AVG(c.net_irr) as avg_irr,
                AVG(c.net_multiple) as avg_multiple
            FROM funds f
            JOIN commitments c ON f.id = c.fund_id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            GROUP BY f.id
            HAVING total_commitment_mm > 0
            ORDER BY total_commitment_mm DESC
            LIMIT 100"""
        ).fetchall()

        if not rows:
            return None

        filepath = self.export_dir / "top_funds.csv"
        fieldnames = ["fund_name", "vintage_year", "asset_class", "sub_strategy",
                      "pension_count", "pensions", "total_commitment_mm",
                      "avg_irr", "avg_multiple"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                if d.get("avg_irr") is not None:
                    d["avg_irr"] = f"{d['avg_irr']:.4f}"
                if d.get("avg_multiple") is not None:
                    d["avg_multiple"] = f"{d['avg_multiple']:.4f}"
                if d.get("total_commitment_mm") is not None:
                    d["total_commitment_mm"] = f"{d['total_commitment_mm']:.2f}"
                writer.writerow(d)

        logger.info(f"Exported top funds to {filepath}")
        return filepath

    def export_cross_pension_matrix_csv(self) -> Path:
        """Export a matrix showing which funds appear in which pension systems.

        Dynamically detects all pension systems in the commitments table.

        Returns:
            Path to the generated CSV file.
        """
        # Discover all pension fund IDs with commitments
        pf_rows = self.db.conn.execute(
            """SELECT DISTINCT p.id, p.name
            FROM pension_funds p
            JOIN commitments c ON c.pension_fund_id = p.id
            ORDER BY p.name"""
        ).fetchall()

        if not pf_rows:
            return None

        pf_ids = [(r["id"], r["name"]) for r in pf_rows]

        # Build dynamic CASE WHEN columns
        case_parts = []
        for pf_id, pf_name in pf_ids:
            col = f"{pf_id}_mm"
            case_parts.append(
                f"MAX(CASE WHEN c.pension_fund_id = '{pf_id}' THEN c.commitment_mm END) as {col}"
            )
        case_sql = ",\n                ".join(case_parts)

        query = f"""SELECT f.fund_name, f.vintage_year, f.asset_class, f.sub_strategy,
                {case_sql},
                COUNT(DISTINCT c.pension_fund_id) as pension_count,
                AVG(c.net_irr) as avg_irr,
                AVG(c.net_multiple) as avg_multiple
            FROM funds f
            JOIN commitments c ON f.id = c.fund_id
            GROUP BY f.id
            HAVING pension_count >= 2
            ORDER BY pension_count DESC, f.fund_name"""

        rows = self.db.conn.execute(query).fetchall()

        if not rows:
            return None

        pf_columns = [f"{pf_id}_mm" for pf_id, _ in pf_ids]
        filepath = self.export_dir / "cross_pension_matrix.csv"
        fieldnames = ["fund_name", "vintage_year", "asset_class", "sub_strategy"] + \
                     pf_columns + ["pension_count", "avg_irr", "avg_multiple"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                for col in pf_columns:
                    if d.get(col) is not None:
                        d[col] = f"{d[col]:.2f}"
                if d.get("avg_irr") is not None:
                    d["avg_irr"] = f"{d['avg_irr']:.4f}"
                if d.get("avg_multiple") is not None:
                    d["avg_multiple"] = f"{d['avg_multiple']:.4f}"
                writer.writerow(d)

        logger.info(f"Exported cross-pension matrix ({len(pf_ids)} pension systems) to {filepath}")
        return filepath

    def export_performance_by_vintage_csv(self) -> Path:
        """Export performance comparison by vintage year.

        Returns:
            Path to the generated CSV file.
        """
        rows = self.db.conn.execute(
            """SELECT
                c.vintage_year,
                COUNT(*) as fund_count,
                AVG(c.commitment_mm) as avg_commitment_mm,
                SUM(c.commitment_mm) as total_commitment_mm,
                AVG(c.net_irr) as avg_irr,
                AVG(c.net_multiple) as avg_multiple,
                MIN(c.net_irr) as min_irr,
                MAX(c.net_irr) as max_irr,
                MIN(c.net_multiple) as min_multiple,
                MAX(c.net_multiple) as max_multiple
            FROM commitments c
            WHERE c.vintage_year IS NOT NULL
            GROUP BY c.vintage_year
            ORDER BY c.vintage_year"""
        ).fetchall()

        if not rows:
            return None

        filepath = self.export_dir / "performance_by_vintage.csv"
        fieldnames = ["vintage_year", "fund_count", "avg_commitment_mm",
                      "total_commitment_mm", "avg_irr", "avg_multiple",
                      "min_irr", "max_irr", "min_multiple", "max_multiple"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                for k in ("avg_commitment_mm", "total_commitment_mm"):
                    if d.get(k) is not None:
                        d[k] = f"{d[k]:.2f}"
                for k in ("avg_irr", "min_irr", "max_irr"):
                    if d.get(k) is not None:
                        d[k] = f"{d[k]:.4f}"
                for k in ("avg_multiple", "min_multiple", "max_multiple"):
                    if d.get(k) is not None:
                        d[k] = f"{d[k]:.4f}"
                writer.writerow(d)

        logger.info(f"Exported performance by vintage to {filepath}")
        return filepath

    def export_top_gps_csv(self) -> Path:
        """Export top GPs ranked by total commitments and fund count.

        Returns:
            Path to the generated CSV file.
        """
        rows = self.db.conn.execute(
            """SELECT
                f.general_partner,
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
            LIMIT 50"""
        ).fetchall()

        if not rows:
            return None

        filepath = self.export_dir / "top_gps.csv"
        fieldnames = ["general_partner", "fund_count", "pension_count",
                      "total_commitment_mm", "avg_irr", "avg_multiple",
                      "earliest_vintage", "latest_vintage"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                if d.get("total_commitment_mm") is not None:
                    d["total_commitment_mm"] = f"{d['total_commitment_mm']:.2f}"
                if d.get("avg_irr") is not None:
                    d["avg_irr"] = f"{d['avg_irr']:.4f}"
                if d.get("avg_multiple") is not None:
                    d["avg_multiple"] = f"{d['avg_multiple']:.4f}"
                writer.writerow(d)

        logger.info(f"Exported top GPs to {filepath}")
        return filepath

    def export_performance_comparison_csv(self) -> Path:
        """Export side-by-side IRR and multiple for funds in 2+ pension systems.

        Returns:
            Path to the generated CSV file.
        """
        rows = self.db.conn.execute(
            """SELECT f.fund_name, f.vintage_year, p.name as pension_fund,
                c.commitment_mm, c.net_irr, c.net_multiple, c.as_of_date
            FROM commitments c
            JOIN funds f ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            WHERE f.id IN (
                SELECT fund_id FROM commitments
                GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 2
            )
            ORDER BY f.fund_name, p.name"""
        ).fetchall()

        if not rows:
            return None

        filepath = self.export_dir / "performance_comparison.csv"
        fieldnames = ["fund_name", "vintage_year", "pension_fund",
                      "commitment_mm", "net_irr", "net_multiple", "as_of_date"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                d = dict(row)
                if d.get("commitment_mm") is not None:
                    d["commitment_mm"] = f"{d['commitment_mm']:.2f}"
                if d.get("net_irr") is not None:
                    d["net_irr"] = f"{d['net_irr']:.4f}"
                if d.get("net_multiple") is not None:
                    d["net_multiple"] = f"{d['net_multiple']:.4f}"
                writer.writerow(d)

        logger.info(f"Exported performance comparison to {filepath}")
        return filepath

    def export_summary_stats_md(self) -> Path:
        """Export summary statistics as a Markdown file.

        Returns:
            Path to the generated Markdown file.
        """
        stats_text = self.export_summary_stats()

        # Also gather review queue info
        review_rows = self.db.conn.execute(
            """SELECT flag_type, COUNT(*) as cnt
            FROM review_queue WHERE resolved = FALSE
            GROUP BY flag_type ORDER BY cnt DESC"""
        ).fetchall()

        # Field completeness
        commitments = self.db.get_commitments_joined()
        fields = [
            "commitment_mm", "vintage_year", "capital_called_mm",
            "capital_distributed_mm", "remaining_value_mm",
            "net_irr", "net_multiple", "dpi", "as_of_date",
        ]
        total = len(commitments) if commitments else 0

        lines = ["# Pension Fund Alternative Investment Tracker - Summary Report"]
        lines.append(f"\nGenerated: {datetime.now().isoformat()[:19]}\n")
        lines.append("```")
        lines.append(stats_text)
        lines.append("```\n")

        # Field completeness table
        if total > 0:
            lines.append("## Field Completeness\n")
            lines.append("| Field | Populated | % |")
            lines.append("|---|---:|---:|")
            for f in fields:
                populated = sum(1 for c in commitments if c.get(f) is not None)
                pct = populated / total * 100
                lines.append(f"| {f} | {populated:,} | {pct:.1f}% |")

        # Review queue
        if review_rows:
            total_unresolved = sum(r["cnt"] for r in review_rows)
            lines.append(f"\n## Review Queue ({total_unresolved} unresolved)\n")
            lines.append("| Flag Type | Count |")
            lines.append("|---|---:|")
            for r in review_rows:
                lines.append(f"| {r['flag_type']} | {r['cnt']} |")

        filepath = self.export_dir / "summary_stats.md"
        filepath.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"Exported summary stats to {filepath}")
        return filepath

    def export_summary_stats(self) -> str:
        """Generate summary statistics as a formatted string.

        Returns:
            Formatted summary string.
        """
        stats = {}

        # Total records
        stats["total_commitments"] = self.db.conn.execute(
            "SELECT COUNT(*) FROM commitments"
        ).fetchone()[0]
        stats["total_funds"] = self.db.conn.execute(
            "SELECT COUNT(*) FROM funds"
        ).fetchone()[0]
        stats["total_pension_funds"] = self.db.conn.execute(
            "SELECT COUNT(*) FROM pension_funds"
        ).fetchone()[0]

        # Cross-links
        stats["cross_linked_2"] = self.db.conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT fund_id FROM commitments
                GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 2
            )"""
        ).fetchone()[0]
        stats["cross_linked_3"] = self.db.conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT fund_id FROM commitments
                GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 3
            )"""
        ).fetchone()[0]
        stats["cross_linked_4"] = self.db.conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT fund_id FROM commitments
                GROUP BY fund_id HAVING COUNT(DISTINCT pension_fund_id) >= 4
            )"""
        ).fetchone()[0]

        # Total AUM
        total_commitment = self.db.conn.execute(
            "SELECT SUM(commitment_mm) FROM commitments"
        ).fetchone()[0] or 0

        # Strategy breakdown
        strategies = self.db.conn.execute(
            """SELECT f.sub_strategy, COUNT(*) as cnt
            FROM funds f WHERE f.sub_strategy IS NOT NULL
            GROUP BY f.sub_strategy ORDER BY cnt DESC"""
        ).fetchall()

        # Per pension fund
        pf_stats = self.db.conn.execute(
            """SELECT p.name, COUNT(*) as cnt, SUM(c.commitment_mm) as total_mm
            FROM commitments c JOIN pension_funds p ON c.pension_fund_id = p.id
            GROUP BY p.id ORDER BY p.name"""
        ).fetchall()

        lines = [
            "=" * 60,
            "PENSION FUND ALTERNATIVE INVESTMENT TRACKER - SUMMARY",
            "=" * 60,
            "",
            f"Total commitment records: {stats['total_commitments']:,}",
            f"Unique funds:             {stats['total_funds']:,}",
            f"Pension systems covered:  {stats['total_pension_funds']}",
            f"Total commitments:        ${total_commitment:,.0f}M",
            "",
            "Cross-system entity resolution:",
            f"  Funds in 2+ systems:  {stats['cross_linked_2']}",
            f"  Funds in 3+ systems:  {stats['cross_linked_3']}",
            f"  Funds in all 4:       {stats['cross_linked_4']}",
            "",
            "Per pension fund:",
        ]
        for pf in pf_stats:
            lines.append(f"  {pf['name']:20s} {pf['cnt']:5d} records  ${pf['total_mm']:>12,.0f}M")

        if strategies:
            lines.append("")
            lines.append("Strategy classification (keyword-based):")
            for s in strategies:
                lines.append(f"  {s['sub_strategy']:35s} {s['cnt']:4d} funds")

        lines.append("")
        lines.append("=" * 60)
        return "\n".join(lines)

    def export_all(self) -> dict:
        """Export all files.

        Returns:
            Dict mapping export type to file path.
        """
        results = {}
        results["commitments"] = self.export_commitments_csv()
        results["summary"] = self.export_summary_csv()
        results["top_funds"] = self.export_top_funds_csv()
        results["top_gps"] = self.export_top_gps_csv()
        results["cross_pension_matrix"] = self.export_cross_pension_matrix_csv()
        results["performance_comparison"] = self.export_performance_comparison_csv()
        results["performance_by_vintage"] = self.export_performance_by_vintage_csv()
        results["quality"] = self.export_quality_report()
        results["summary_stats"] = self.export_summary_stats_md()
        return results
