"""Pipeline orchestrator for pension fund data extraction.

Coordinates adapter execution, entity resolution, database insertion,
and quality checking across all pension fund sources.
"""

import logging
import traceback
from typing import Optional

from src.adapters.base import PensionFundAdapter
from src.database import Database
from src.entity_resolution import FundRegistry

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates the full data extraction pipeline."""

    def __init__(self, db: Database):
        self.db = db
        self.db.migrate()
        self.registry = FundRegistry(db)

    def run(
        self,
        adapters: list[PensionFundAdapter],
        force: bool = False,
    ) -> dict:
        """Run the extraction pipeline for the given adapters.

        Args:
            adapters: List of adapter instances to run.
            force: If True, run even if source data hasn't changed.

        Returns:
            Summary dict with results per adapter.
        """
        results = {}

        for adapter in adapters:
            adapter_name = adapter.pension_fund_id
            logger.info(f"=== Starting pipeline for {adapter.pension_fund_name} ===")

            try:
                result = self._run_adapter(adapter, force=force)
                results[adapter_name] = result
                logger.info(
                    f"=== Completed {adapter.pension_fund_name}: "
                    f"{result['records_extracted']} extracted, "
                    f"{result['records_updated']} updated, "
                    f"{result['records_flagged']} flagged ==="
                )
            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                logger.error(f"Pipeline failed for {adapter.pension_fund_name}: {e}")
                results[adapter_name] = {
                    "status": "error",
                    "error": error_msg,
                    "records_extracted": 0,
                    "records_updated": 0,
                    "records_flagged": 0,
                }

        return results

    def _run_adapter(
        self,
        adapter: PensionFundAdapter,
        force: bool = False,
    ) -> dict:
        """Run a single adapter through the pipeline.

        Returns:
            Dict with extraction results.
        """
        # Register the pension fund
        info = adapter.get_pension_fund_info()
        self.db.upsert_pension_fund(**info)

        # Fetch source data
        raw_data = adapter.fetch_source()
        source_hash = adapter.get_source_hash(raw_data)

        # Check if data has changed since last run
        if not force:
            last_run = self.db.get_last_extraction_run(adapter.pension_fund_id)
            if last_run and last_run.get("source_hash") == source_hash:
                logger.info(
                    f"Source data unchanged for {adapter.pension_fund_name}, skipping. "
                    f"Use --force to override."
                )
                return {
                    "status": "skipped",
                    "reason": "source_unchanged",
                    "records_extracted": 0,
                    "records_updated": 0,
                    "records_flagged": 0,
                }

        # Create extraction run record
        run_id = self.db.create_extraction_run(
            pension_fund_id=adapter.pension_fund_id,
            source_url=adapter.source_url,
            source_hash=source_hash,
        )

        try:
            # Parse the data
            records = adapter.parse(raw_data)
            logger.info(f"Parsed {len(records)} records from {adapter.pension_fund_name}")

            records_extracted = 0
            records_updated = 0
            records_flagged = 0

            for record in records:
                # Entity resolution
                fund_id, match_type = self.registry.resolve(
                    fund_name_raw=record["fund_name_raw"],
                    general_partner=record.get("general_partner"),
                    vintage_year=record.get("vintage_year"),
                    source_pension_fund_id=adapter.pension_fund_id,
                )

                # Insert/update commitment
                commitment_id = self.db.upsert_commitment(
                    pension_fund_id=adapter.pension_fund_id,
                    fund_id=fund_id,
                    source_url=record["source_url"],
                    extraction_method=record["extraction_method"],
                    commitment_mm=record.get("commitment_mm"),
                    vintage_year=record.get("vintage_year"),
                    capital_called_mm=record.get("capital_called_mm"),
                    capital_distributed_mm=record.get("capital_distributed_mm"),
                    remaining_value_mm=record.get("remaining_value_mm"),
                    net_irr=record.get("net_irr"),
                    net_multiple=record.get("net_multiple"),
                    dpi=record.get("dpi"),
                    as_of_date=record.get("as_of_date"),
                    source_document=record.get("source_document"),
                    source_page=record.get("source_page"),
                    extraction_confidence=record.get("extraction_confidence"),
                )

                records_extracted += 1

                # Flag low-confidence extractions
                confidence = record.get("extraction_confidence", 1.0)
                if confidence < 0.85:
                    self.db.add_review_item(
                        commitment_id=commitment_id,
                        flag_type="low_confidence",
                        flag_detail=f"Extraction confidence {confidence:.2f} below threshold",
                    )
                    records_flagged += 1

                # Flag fuzzy entity matches
                if match_type == "fuzzy":
                    self.db.add_review_item(
                        commitment_id=commitment_id,
                        flag_type="fuzzy_match",
                        flag_detail=f"Fund '{record['fund_name_raw']}' fuzzy-matched to {fund_id}",
                    )
                    records_flagged += 1

            # Complete the extraction run
            self.db.complete_extraction_run(
                run_id=run_id,
                status="completed",
                records_extracted=records_extracted,
                records_updated=records_updated,
                records_flagged=records_flagged,
            )

            return {
                "status": "completed",
                "run_id": run_id,
                "records_extracted": records_extracted,
                "records_updated": records_updated,
                "records_flagged": records_flagged,
            }

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            self.db.complete_extraction_run(
                run_id=run_id,
                status="error",
                errors=error_msg,
            )
            raise
