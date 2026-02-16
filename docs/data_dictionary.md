# Data Dictionary

## Table: pension_funds

Information about each pension fund data source.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | Unique identifier (e.g., "calpers") |
| name | TEXT | Short display name (e.g., "CalPERS") |
| full_name | TEXT | Full legal name |
| state | TEXT | Two-letter US state code |
| total_aum_mm | REAL | Total assets under management in millions |
| website_url | TEXT | Main website URL |
| data_source_type | TEXT | Format of data source (html, pdf, csv) |
| disclosure_quality | TEXT | Quality rating (gold, good, limited) |

## Table: funds

Master registry of investment funds with canonical names.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| fund_name | TEXT | Canonical/normalized fund name |
| fund_name_raw | TEXT | Fund name as first encountered in source data |
| general_partner | TEXT | GP name as found in source |
| general_partner_normalized | TEXT | Normalized GP name for matching |
| vintage_year | INTEGER | Year of first capital call |
| asset_class | TEXT | Asset class (e.g., "Private Equity") |
| sub_strategy | TEXT | Sub-strategy (e.g., "Buyout", "Venture Capital") |
| fund_size_mm | REAL | Total fund size in millions (if known) |

## Table: commitments

Individual pension fund commitments to investment funds. One record per (pension_fund, fund, as_of_date) combination.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| pension_fund_id | TEXT FK | References pension_funds.id |
| fund_id | TEXT FK | References funds.id |
| commitment_mm | REAL | Total commitment amount in millions USD |
| vintage_year | INTEGER | Vintage year of the fund |
| capital_called_mm | REAL | Capital called/contributed in millions USD |
| capital_distributed_mm | REAL | Distributions received in millions USD |
| remaining_value_mm | REAL | Remaining NAV / market value in millions USD |
| net_irr | REAL | Net internal rate of return as decimal (0.15 = 15%) |
| net_multiple | REAL | Net TVPI multiple (e.g., 1.5 = 1.5x) |
| dpi | REAL | Distributions to paid-in ratio |
| as_of_date | DATE | Reporting date (YYYY-MM-DD) |
| status | TEXT | Fund status if available |
| source_url | TEXT | URL of the data source (required) |
| source_document | TEXT | Name of the source document |
| source_page | INTEGER | Page number in PDF (if applicable) |
| extraction_method | TEXT | How data was extracted (required). Values: deterministic_html, deterministic_pdf, deterministic_csv, llm_assisted |
| extraction_confidence | REAL | Confidence score: 1.0 for deterministic, lower for LLM |

**Unique constraint**: (pension_fund_id, fund_id, as_of_date) — prevents duplicate records.

## Table: fund_aliases

Maps alternative fund names to canonical fund entries.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| fund_id | TEXT FK | References funds.id |
| alias | TEXT | Alternative name for the fund |
| source_pension_fund_id | TEXT FK | Which pension fund uses this alias |

## Table: gp_aliases

Maps alternative GP names to canonical names.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| canonical_name | TEXT | Canonical GP name |
| alias | TEXT | Alternative GP name |

## Table: extraction_runs

Log of each pipeline execution.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| pension_fund_id | TEXT FK | Which pension fund was processed |
| started_at | TIMESTAMP | When the run started |
| completed_at | TIMESTAMP | When the run finished |
| status | TEXT | running, completed, error, skipped |
| records_extracted | INTEGER | Number of records extracted |
| records_updated | INTEGER | Number of existing records updated |
| records_flagged | INTEGER | Number of records flagged for review |
| errors | TEXT | Error messages if any |
| source_url | TEXT | URL of the source data |
| source_hash | TEXT | SHA256 hash for change detection |

## Table: review_queue

Items flagged for human review.

| Field | Type | Description |
|---|---|---|
| id | TEXT PK | UUID |
| commitment_id | TEXT FK | References commitments.id |
| flag_type | TEXT | Type of flag: low_confidence, fuzzy_match, value_range, low_completeness, cross_fund_inconsistency |
| flag_detail | TEXT | Human-readable description of the issue |
| resolved | BOOLEAN | Whether the flag has been reviewed/resolved |
