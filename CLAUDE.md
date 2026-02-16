# Pension Fund Alternative Investment Extraction Tool

## Project Overview

You are building a **production-grade data pipeline** that extracts, normalizes, and structures alternative investment commitment data (private equity, venture capital, real estate, infrastructure, hedge funds) from U.S. public pension fund disclosures. The end product is a clean, queryable database and a repeatable pipeline — NOT a one-off scrape.

## Core Design Principles

### 1. Deterministic Over Probabilistic
- If a source provides structured data (HTML tables, CSVs, Excel files), parse it deterministically. Do NOT use an LLM to extract data from structured sources.
- LLMs are ONLY for: (a) entity resolution / fuzzy matching of fund names, (b) classifying asset types when the source uses non-standard categories, (c) extracting data from genuinely unstructured PDFs where no table structure exists.
- Every LLM call must be logged with input, output, and confidence score. Any extraction with confidence below 0.85 gets flagged for human review.

### 2. Source-Aware Architecture
- Each pension fund gets its own **adapter module** — a Python class that knows where to find the data, what format it's in, how to parse it, and what fields are available.
- Adapters implement a common interface so the pipeline treats all funds uniformly downstream.
- Adding a new fund means writing a new adapter, NOT modifying core pipeline code.

### 3. Data Quality Over Data Quantity
- Every record must track its **provenance**: source URL, document name, page number (for PDFs), extraction method (deterministic vs. LLM), extraction date, and confidence score.
- The system must produce a **data quality report** after each run: records extracted, fields populated vs. missing, confidence distribution, flagged records needing review.
- Prefer extracting 500 high-confidence records over 2,000 mixed-quality records.

### 4. Idempotent and Resumable
- Running the pipeline twice on the same source should produce identical results, not duplicates.
- If the pipeline fails mid-run (network error, parsing failure), it should resume from where it left off.
- Each run is logged with timestamps, sources processed, records created/updated, and errors.

## Technical Stack

- **Language:** Python 3.11+
- **Database:** SQLite for the POC (with schema designed to migrate to PostgreSQL trivially)
- **PDF parsing:** First try `camelot-py` or `tabula-py` for table extraction. Fall back to `pdfplumber`. LLM extraction is the LAST resort for PDFs.
- **HTML parsing:** `BeautifulSoup4` + `requests`. Use `lxml` parser.
- **Excel/CSV:** `openpyxl` for .xlsx, `pandas` for CSV.
- **Entity resolution:** Build a master fund registry. Use Levenshtein distance + heuristics (GP name + vintage year + approximate size) for fuzzy matching. LLM-assisted disambiguation only for ambiguous cases.
- **LLM calls:** Use the Anthropic API (Claude) with structured output prompts. Cache all LLM responses to avoid redundant API calls.
- **Testing:** Every adapter must have at least one integration test using a saved/cached copy of its source data.

## Database Schema (implement exactly)

```sql
-- Core tables
CREATE TABLE funds (
    id TEXT PRIMARY KEY,
    fund_name TEXT NOT NULL,
    fund_name_raw TEXT NOT NULL,
    general_partner TEXT,
    general_partner_normalized TEXT,
    vintage_year INTEGER,
    asset_class TEXT,
    sub_strategy TEXT,
    fund_size_mm REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pension_funds (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    full_name TEXT,
    state TEXT,
    total_aum_mm REAL,
    website_url TEXT,
    data_source_type TEXT,
    disclosure_quality TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE commitments (
    id TEXT PRIMARY KEY,
    pension_fund_id TEXT REFERENCES pension_funds(id),
    fund_id TEXT REFERENCES funds(id),
    commitment_mm REAL,
    vintage_year INTEGER,
    capital_called_mm REAL,
    capital_distributed_mm REAL,
    remaining_value_mm REAL,
    net_irr REAL,
    net_multiple REAL,
    dpi REAL,
    as_of_date DATE,
    status TEXT,
    source_url TEXT NOT NULL,
    source_document TEXT,
    source_page INTEGER,
    extraction_method TEXT NOT NULL,
    extraction_confidence REAL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pension_fund_id, fund_id, as_of_date)
);

CREATE TABLE fund_aliases (
    id TEXT PRIMARY KEY,
    fund_id TEXT REFERENCES funds(id),
    alias TEXT NOT NULL,
    source_pension_fund_id TEXT REFERENCES pension_funds(id),
    UNIQUE(alias, source_pension_fund_id)
);

CREATE TABLE gp_aliases (
    id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    alias TEXT NOT NULL,
    UNIQUE(alias)
);

CREATE TABLE extraction_runs (
    id TEXT PRIMARY KEY,
    pension_fund_id TEXT REFERENCES pension_funds(id),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT,
    records_extracted INTEGER,
    records_updated INTEGER,
    records_flagged INTEGER,
    errors TEXT,
    source_url TEXT,
    source_hash TEXT
);

CREATE TABLE review_queue (
    id TEXT PRIMARY KEY,
    commitment_id TEXT REFERENCES commitments(id),
    flag_type TEXT,
    flag_detail TEXT,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Pension Fund Adapters to Build (in priority order)

### Tier 1 — Gold Standard (structured, web-accessible data)
1. **CalPERS** — Quarterly-updated HTML/downloadable table of all PE commitments with fund name, vintage, committed, called, distributed, remaining value, net IRR, net multiple. URL: https://www.calpers.ca.gov/page/investments/asset-classes/private-equity/private-equity-holdings
2. **CalSTRS** — Similar disclosure under California law. Check their investments page for PE holdings.
3. **Washington State Investment Board (WSIB)** — Detailed PE holdings with returns on website.
4. **Oregon PERS (OPERF / Oregon State Treasury)** — Detailed disclosure after state legislation.

### Tier 2 — Good Data, More Work Required
5. **Texas Teachers (TRS)** — PE holdings in annual reports (likely PDF).
6. **New York State Common Retirement Fund** — PE fund lists in board materials and CAFRs.
7. **Florida SBA** — PE holdings in board materials.
8. **Pennsylvania PSERS** — Alternative investment data published.
9. **Ohio STRS** — PE commitment data published.
10. **New Jersey Division of Investment** — Alternative investment reports.

## Entity Resolution Strategy

Build a `FundRegistry` class that:
1. Maintains a master list of known funds with canonical names, GP, vintage year, and known aliases.
2. When a new fund name is encountered, attempts to match it:
   - Exact match on canonical name → done
   - Exact match on known alias → done
   - Fuzzy match (Levenshtein ratio > 0.85) on canonical name or alias → flag as probable match, auto-link if GP and vintage also match
   - GP name match + vintage year match + similar fund number → flag as probable match
   - No match → create new fund entry, flag for review
3. Seed the registry using CalPERS data first (cleanest names — use those as canonical).
4. Log all entity resolution decisions for auditability.

## Output Requirements

The pipeline must produce:
1. **The SQLite database** with all tables populated.
2. **A summary report** (Markdown) after each run: funds covered, total commitments extracted, data quality metrics, records flagged for review.
3. **CSV exports** of the core data (commitments joined with fund and pension fund names) for easy sharing.
4. **A simple CLI** that supports: `run` (full pipeline), `run --fund calpers` (single fund), `status` (show last run stats), `export` (generate CSVs), `quality` (show data quality report).

## File Structure

```
pension-fund-tracker/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── setup.py
├── config.yaml
├── src/
│   ├── __init__.py
│   ├── cli.py
│   ├── pipeline.py
│   ├── database.py
│   ├── entity_resolution.py
│   ├── quality.py
│   ├── export.py
│   ├── llm.py
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── calpers.py
│   │   ├── calstrs.py
│   │   ├── wsib.py
│   │   ├── oregon.py
│   │   ├── texas_trs.py
│   │   ├── ny_common.py
│   │   ├── florida_sba.py
│   │   ├── pa_psers.py
│   │   ├── ohio_strs.py
│   │   └── nj_doi.py
│   └── utils/
│       ├── __init__.py
│       ├── pdf_parser.py
│       ├── html_parser.py
│       └── normalization.py
├── tests/
│   ├── __init__.py
│   ├── test_entity_resolution.py
│   ├── test_normalization.py
│   ├── test_quality.py
│   ├── fixtures/
│   │   ├── calpers_sample.html
│   │   └── ...
│   └── adapters/
│       ├── test_calpers.py
│       └── ...
├── data/
│   ├── pension_tracker.db
│   ├── exports/
│   ├── cache/
│   └── logs/
└── docs/
    ├── adding_a_fund.md
    └── data_dictionary.md
```

## Critical Quality Checks

The quality module must check:
- **Value reasonableness:** Commitment sizes typically $10M–$2B. Flag anything outside $1M–$5B.
- **IRR reasonableness:** Most PE fund IRRs between -20% and +50%. Flag outliers.
- **Multiple reasonableness:** Net multiples typically 0.5x–4.0x. Flag outliers.
- **Vintage year reasonableness:** Between 1990 and current year.
- **Completeness:** Track percentage of fields populated per fund, per pension fund.
- **Cross-fund consistency:** If CalPERS and CalSTRS both committed to the same fund, fund-level attributes (GP, vintage, size) should match.
- **Temporal consistency:** Called capital should not decrease between reporting periods (unless recallable distribution).

## What Success Looks Like

A successful build has:
- 4+ pension fund adapters working (at minimum CalPERS + 3 others)
- 500+ commitment records with provenance tracking
- Entity resolution linking the same funds across multiple pension systems
- A data quality report showing 90%+ field completeness for Tier 1 funds
- CSV exports ready to share with a potential buyer
- A CLI that a non-technical person could run
- Documentation sufficient for someone else to add a new fund adapter

## What to Avoid

- Do NOT build a web UI. CLI + CSV exports + SQLite is the product.
- Do NOT try to cover 25+ funds. Depth over breadth. 4-10 funds done excellently beats 25 done poorly.
- Do NOT use Selenium/Playwright unless absolutely necessary. Try static requests first.
- Do NOT store API keys in code. Use environment variables or config.yaml (gitignored).
- Do NOT skip provenance tracking to save time. Every record must know where it came from.
- Do NOT treat LLM extraction as reliable without validation.
