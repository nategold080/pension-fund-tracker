"""Pipeline orchestrator for pension fund data extraction.

Coordinates adapter execution, entity resolution, database insertion,
and quality checking across all pension fund sources.
"""

import logging
import traceback

from src.adapters.base import PensionFundAdapter
from src.database import Database
from src.entity_resolution import ConsultingFirmRegistry, FundRegistry

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates the full data extraction pipeline."""

    def __init__(self, db: Database):
        self.db = db
        self.db.migrate()
        self.registry = FundRegistry(db)
        self.consulting_registry = ConsultingFirmRegistry(db)

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

                # Compute DPI if not provided but derivable
                dpi = record.get("dpi")
                if dpi is None:
                    called = record.get("capital_called_mm")
                    distributed = record.get("capital_distributed_mm")
                    if called is not None and distributed is not None and called > 0:
                        dpi = round(distributed / called, 4)

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
                    dpi=dpi,
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

            # Consulting data extraction phase
            consulting_extracted = self._extract_consulting_data(adapter)

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
                "consulting_extracted": consulting_extracted,
            }

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            self.db.complete_extraction_run(
                run_id=run_id,
                status="error",
                errors=error_msg,
            )
            raise

    def _extract_consulting_data(self, adapter: PensionFundAdapter) -> int:
        """Extract and store consulting engagement data from an adapter.

        Returns:
            Number of consulting engagements extracted.
        """
        try:
            consulting_records = adapter.extract_consulting_data()
        except Exception as e:
            logger.warning(
                f"Consulting data extraction failed for {adapter.pension_fund_name}: {e}"
            )
            return 0

        if not consulting_records:
            return 0

        count = 0
        for record in consulting_records:
            firm_name = record.get("consulting_firm_name")
            if not firm_name:
                continue

            # Resolve consulting firm via registry
            match = self.consulting_registry.resolve(firm_name)
            if not match:
                logger.warning(
                    f"Unknown consulting firm '{firm_name}' from "
                    f"{adapter.pension_fund_name}, skipping"
                )
                continue

            firm_id, match_type = match

            self.db.upsert_consulting_engagement(
                consulting_firm_id=firm_id,
                pension_fund_id=adapter.pension_fund_id,
                role=record["role"],
                mandate_scope=record.get("mandate_scope"),
                start_date=record.get("start_date"),
                end_date=record.get("end_date"),
                is_current=record.get("is_current"),
                annual_fee_usd=record.get("annual_fee_usd"),
                fee_basis=record.get("fee_basis"),
                contract_term_years=record.get("contract_term_years"),
                source_url=record.get("source_url", adapter.source_url),
                source_document=record.get("source_document"),
                source_page=record.get("source_page"),
                extraction_method=record.get("extraction_method"),
                extraction_confidence=record.get("extraction_confidence"),
            )
            count += 1

        if count > 0:
            logger.info(
                f"Extracted {count} consulting engagements from "
                f"{adapter.pension_fund_name}"
            )
        return count
