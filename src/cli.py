"""CLI interface for the pension fund tracker.

Usage:
    python -m src.cli run              # Run all adapters
    python -m src.cli run --fund calpers  # Run single adapter
    python -m src.cli run --force      # Run even if source unchanged
    python -m src.cli status           # Show last run stats
    python -m src.cli export           # Generate CSVs to data/exports/
    python -m src.cli quality          # Print quality report
"""

import logging
import sys

import click

from src.adapters import ADAPTER_REGISTRY, get_adapter, get_all_adapters, get_default_adapters
from src.database import Database
from src.export import Exporter
from src.pipeline import Pipeline
from src.quality import QualityChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--db", default="data/pension_tracker.db", help="Database path")
@click.pass_context
def cli(ctx, db):
    """Pension Fund Alternative Investment Tracker"""
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db


@cli.command()
@click.option("--fund", default=None, help="Run a specific fund adapter (e.g., calpers)")
@click.option("--force", is_flag=True, help="Run even if source data hasn't changed")
@click.option("--all", "run_all", is_flag=True, help="Run all adapters (including Texas TRS, Florida SBA)")
@click.pass_context
def run(ctx, fund, force, run_all):
    """Run the extraction pipeline."""
    db = Database(ctx.obj["db_path"])
    try:
        pipeline = Pipeline(db)

        if fund:
            try:
                adapter = get_adapter(fund)
            except KeyError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)
            adapters = [adapter]
        elif run_all:
            adapters = get_all_adapters()
        else:
            adapters = get_default_adapters()

        if not adapters:
            click.echo("No adapters available.")
            return

        click.echo(f"Running pipeline for {len(adapters)} adapter(s)...")
        results = pipeline.run(adapters, force=force)

        # Print summary
        click.echo("\n--- Pipeline Results ---")
        total_extracted = 0
        total_flagged = 0
        for name, result in results.items():
            status = result.get("status", "unknown")
            extracted = result.get("records_extracted", 0)
            flagged = result.get("records_flagged", 0)
            total_extracted += extracted
            total_flagged += flagged

            if status == "completed":
                click.echo(f"  {name}: {extracted} records extracted, {flagged} flagged")
            elif status == "skipped":
                click.echo(f"  {name}: skipped (source unchanged)")
            elif status == "error":
                click.echo(f"  {name}: ERROR - {result.get('error', 'unknown')[:100]}")

        click.echo(f"\nTotal: {total_extracted} records extracted, {total_flagged} flagged")

    finally:
        db.close()


@cli.command()
@click.pass_context
def status(ctx):
    """Show last run stats for each fund."""
    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        click.echo("--- Last Run Status ---\n")

        for adapter_id in sorted(ADAPTER_REGISTRY.keys()):
            last_run = db.get_last_extraction_run(adapter_id)
            if last_run:
                click.echo(f"  {adapter_id}:")
                click.echo(f"    Status: {last_run['status']}")
                click.echo(f"    Started: {last_run['started_at']}")
                click.echo(f"    Completed: {last_run.get('completed_at', 'N/A')}")
                click.echo(f"    Records: {last_run.get('records_extracted', 0)} extracted, "
                           f"{last_run.get('records_flagged', 0)} flagged")
                if last_run.get("errors"):
                    click.echo(f"    Errors: {last_run['errors'][:100]}")
            else:
                click.echo(f"  {adapter_id}: No runs yet")
            click.echo()

        # Overall counts
        total = db.count_commitments()
        funds = len(db.list_funds())
        click.echo(f"Database totals: {total} commitments, {funds} funds")

    finally:
        db.close()


@cli.command("export")
@click.pass_context
def export_cmd(ctx):
    """Generate CSV exports to data/exports/."""
    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        exporter = Exporter(db)
        results = exporter.export_all()

        click.echo("--- Exports ---")
        for export_type, filepath in results.items():
            if filepath:
                click.echo(f"  {export_type}: {filepath}")
            else:
                click.echo(f"  {export_type}: (no data)")

    finally:
        db.close()


@cli.command()
@click.pass_context
def quality(ctx):
    """Print data quality report."""
    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        checker = QualityChecker(db)
        report = checker.generate_report()
        click.echo(report)

    finally:
        db.close()


@cli.command("summary-stats")
@click.pass_context
def summary_stats(ctx):
    """Print summary statistics for the database."""
    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        exporter = Exporter(db)
        click.echo(exporter.export_summary_stats())
    finally:
        db.close()


@cli.command("cross-fund-report")
@click.pass_context
def cross_fund_report(ctx):
    """Generate cross-fund consistency report comparing values across pension systems."""
    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        checker = QualityChecker(db)
        report = checker.generate_cross_fund_report()
        click.echo(report)
    finally:
        db.close()


@cli.command("spot-check")
@click.option("--count", default=20, help="Number of records to sample")
@click.pass_context
def spot_check(ctx, count):
    """Spot-check random commitment records for manual verification."""
    import random

    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()

        # Get per-pension-fund commitment IDs for stratified sampling
        pf_rows = db.conn.execute(
            """SELECT DISTINCT p.id, p.name
            FROM pension_funds p
            JOIN commitments c ON c.pension_fund_id = p.id
            ORDER BY p.name"""
        ).fetchall()

        if not pf_rows:
            click.echo("No commitments in database.")
            return

        per_pf = max(1, count // len(pf_rows))
        sampled_ids = []

        for pf in pf_rows:
            ids = db.conn.execute(
                "SELECT id FROM commitments WHERE pension_fund_id = ?",
                (pf["id"],),
            ).fetchall()
            ids = [r["id"] for r in ids]
            sampled_ids.extend(random.sample(ids, min(per_pf, len(ids))))

        # Trim to exact count
        if len(sampled_ids) > count:
            sampled_ids = random.sample(sampled_ids, count)

        click.echo(f"=== Spot-Check: {len(sampled_ids)} Random Records ===\n")

        for i, cid in enumerate(sampled_ids, 1):
            row = db.conn.execute(
                """SELECT c.*, f.fund_name, p.name as pension_fund_name
                FROM commitments c
                JOIN funds f ON c.fund_id = f.id
                JOIN pension_funds p ON c.pension_fund_id = p.id
                WHERE c.id = ?""",
                (cid,),
            ).fetchone()

            if not row:
                continue

            r = dict(row)
            irr_str = f"{r['net_irr']:.1%}" if r.get("net_irr") is not None else "N/A"
            mult_str = f"{r['net_multiple']:.2f}x" if r.get("net_multiple") is not None else "N/A"
            commit_str = f"${r['commitment_mm']:.1f}M" if r.get("commitment_mm") is not None else "N/A"

            click.echo(f"  [{i}] {r['pension_fund_name']} | {r['fund_name']}")
            click.echo(f"      Vintage: {r.get('vintage_year', 'N/A')}  "
                       f"Commitment: {commit_str}  "
                       f"IRR: {irr_str}  Multiple: {mult_str}")
            click.echo(f"      Source: {r.get('source_url', 'N/A')}")
            if r.get("source_page"):
                click.echo(f"      Page: {r['source_page']}")
            click.echo()

    finally:
        db.close()


@cli.command("resolve-fuzzy")
@click.pass_context
def resolve_fuzzy(ctx):
    """Auto-resolve high-confidence fuzzy match review items."""
    from rapidfuzz import fuzz
    from src.utils.normalization import extract_fund_number, normalize_fund_name

    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()
        items = db.get_fuzzy_match_details()

        if not items:
            click.echo("No unresolved fuzzy_match review items found.")
            return

        click.echo(f"Found {len(items)} unresolved fuzzy_match review items.\n")
        auto_resolved = 0
        remaining = 0

        for item in items:
            alias = item.get("alias") or ""
            canonical = item.get("canonical_name") or ""

            if not alias or not canonical:
                remaining += 1
                continue

            norm_alias = normalize_fund_name(alias).lower()
            norm_canon = normalize_fund_name(canonical).lower()
            tok = fuzz.token_sort_ratio(norm_alias, norm_canon) / 100.0
            std = fuzz.ratio(norm_alias, norm_canon) / 100.0
            anum = extract_fund_number(alias)
            cnum = extract_fund_number(canonical)

            # Auto-resolve if high confidence match
            nums_ok = (not anum and not cnum) or (anum == cnum)
            if tok > 0.90 and std > 0.75 and nums_ok:
                db.resolve_review_item(item["review_id"])
                auto_resolved += 1
            else:
                remaining += 1
                click.echo(
                    f'  [?] "{alias}" -> "{canonical}"  '
                    f'tok={tok:.3f} std={std:.3f} nums={anum}/{cnum}'
                )

        click.echo(f"\nAuto-resolved: {auto_resolved}")
        click.echo(f"Remaining for manual review: {remaining}")

    finally:
        db.close()


@cli.command("audit-links")
@click.pass_context
def audit_links(ctx):
    """Audit fuzzy-matched fund links across pension systems."""
    from rapidfuzz import fuzz
    from src.utils.normalization import extract_fund_number, normalize_fund_name

    db = Database(ctx.obj["db_path"])
    try:
        db.migrate()

        # Get all fuzzy match aliases
        aliases = db.conn.execute("""
            SELECT fa.alias, f.fund_name, fa.source_pension_fund_id,
                   f.vintage_year, f.id as fund_id
            FROM fund_aliases fa
            JOIN funds f ON fa.fund_id = f.id
            ORDER BY f.fund_name, fa.alias
        """).fetchall()

        if not aliases:
            click.echo("No fuzzy-matched aliases found.")
            return

        click.echo(f"=== Fuzzy Match Audit ({len(aliases)} aliases) ===\n")

        suspect_count = 0
        for a in aliases:
            norm_alias = normalize_fund_name(a["alias"]).lower()
            norm_canon = normalize_fund_name(a["fund_name"]).lower()
            tok = fuzz.token_sort_ratio(norm_alias, norm_canon) / 100.0
            std = fuzz.ratio(norm_alias, norm_canon) / 100.0
            anum = extract_fund_number(a["alias"])
            cnum = extract_fund_number(a["fund_name"])

            # Flag anything that looks potentially wrong
            suspect = False
            if anum and cnum and anum != cnum:
                suspect = True
            if std < 0.75:
                suspect = True

            marker = "  [!]" if suspect else "     "
            if suspect:
                suspect_count += 1

            click.echo(
                f'{marker} "{a["alias"]}" -> "{a["fund_name"]}"  '
                f'tok={tok:.3f} std={std:.3f} nums={anum}/{cnum} '
                f'(from {a["source_pension_fund_id"]}, vy={a["vintage_year"]})'
            )

        # Cross-link summary
        cross = db.conn.execute("""
            SELECT f.fund_name, COUNT(DISTINCT c.pension_fund_id) as pf_count,
                   GROUP_CONCAT(DISTINCT p.name) as pensions
            FROM funds f
            JOIN commitments c ON c.fund_id = f.id
            JOIN pension_funds p ON c.pension_fund_id = p.id
            GROUP BY f.id
            HAVING pf_count >= 2
            ORDER BY pf_count DESC, f.fund_name
        """).fetchall()

        click.echo(f"\n=== Cross-Link Summary ===")
        click.echo(f"Total fuzzy aliases: {len(aliases)}")
        click.echo(f"Suspect matches: {suspect_count}")
        click.echo(f"Funds across 2+ pensions: {len(cross)}")
        click.echo(f"Funds across 3+: {len([c for c in cross if c['pf_count'] >= 3])}")
        click.echo(f"Funds across all 4: {len([c for c in cross if c['pf_count'] >= 4])}")

        # Show the 4-way linked funds
        four_way = [c for c in cross if c["pf_count"] >= 4]
        if four_way:
            click.echo(f"\nFunds linked across all 4 pension systems:")
            for c in four_way:
                click.echo(f'  {c["fund_name"]}')

    finally:
        db.close()


if __name__ == "__main__":
    cli()
