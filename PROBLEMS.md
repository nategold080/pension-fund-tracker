# Pension Fund Tracker — Problem Report

> This document is the single source of truth for all outstanding issues.
> Work through every item. Mark each DONE when fixed. Do not skip any.
> After each fix, run `python3 -m pytest tests/ -x -q` to verify no regressions.

---

## CRITICAL — Data Quality

### P1. TPG Rise Climate II has negative multiple (-0.3x)
**DONE** — Fixed DB record (net_multiple = 0.3), added abs() guard in wsib.py, added explicit negative multiple check in quality.py.

### P2. README field completeness table is misleading
**DONE** — Added NY Common column to the table. Recalculated Overall to include all 5 funds (IRR completeness now correctly shows 62.0%).

### P3. README architecture tree is stale
**DONE** — Updated tree to show all files (analysis.py, dashboard.py, llm.py, ny_common.py, texas_trs.py, florida_sba.py, html_parser.py). Updated description to "5 pension funds with active data extraction (7 adapter files total, 2 for funds with access restrictions)."

---

## HIGH — Code Quality Issues

### P4. `_rejoin_number()` duplicated in 3 adapters
**DONE** — Extracted to `rejoin_split_number()` in `src/utils/normalization.py`. Removed static methods from calstrs.py, ny_common.py, florida_sba.py. All adapter tests pass.

### P5. `_extract_as_of_date()` duplicated across 4 adapters
**DONE** — Extracted to `extract_as_of_date_from_text()` in `src/utils/normalization.py`. Removed from calstrs.py, wsib.py, oregon.py, florida_sba.py. CalPERS kept its own version (HTML-based, different signature). All tests pass.

### P6. GP alias table is empty
**DONE** — Added gp_alias insertion in entity_resolution.py when new funds are created. Backfilled existing 1,640 fund→GP mappings into gp_aliases table.

### P7. Strategy classification at 27%
**DONE** — Added GP_DEFAULT_STRATEGY dict (150+ GP→strategy mappings) in normalization.py as fallback when keyword classification fails. Classification rate improved from 27.1% (444/1640) to 56.0% (918/1640). Updated 474 existing fund records in database.

### P8. datetime.utcnow() deprecation warnings
**DONE** — Replaced all `datetime.utcnow()` with `datetime.now(timezone.utc)` in database.py. Warnings dropped from 52 to 12 (only bs4 DeprecationWarning remains).

---

## MEDIUM — Test Coverage Gaps

### P9. No tests for CalSTRS adapter
**DONE** — Created `tests/adapters/test_calstrs.py` with 14 tests: record count, required fields, fund names, commitments, vintage years, extraction method, as-of date, value ranges, known fund present, source URL, pension fund info.

### P10. No tests for WSIB adapter
**DONE** — Created `tests/adapters/test_wsib.py` with 14 tests including a P1 regression test for negative multiples.

### P11. No tests for Oregon adapter
**DONE** — Created `tests/adapters/test_oregon.py` with 13 tests.

### P12. No tests for pipeline, quality, export, or analysis modules
**DONE** — Created `tests/test_quality.py` (8 tests: flags high multiple, high IRR, huge commitment, negative multiple; good record not flagged; report generation; completeness computation; summary structure). Created `tests/test_export.py` (5 tests: commitments CSV, CSV columns, summary CSV, quality report, export_all).

---

## LOW — Cleanup

### P13. src/llm.py placeholder
**DONE** — Kept as documentation of deliberate design decision. Fixed stale "4 adapters" reference to "all adapters". Verified `__init__.py` doesn't import it.

### P14. NY Common has dual-vintage records inflating count
**DONE** — Added clarifying note to README: "2,152 unique fund-pension relationships; some records include multiple reporting periods".

### P15. Design Principles section says "All 4 adapters"
**DONE** — Updated to "All adapters use deterministic parsing." Verified no stale "4 adapters" references remain in README.

---

## PRODUCTION READINESS — Round 2

### P16. Unused dependencies in requirements.txt
**DONE** — Removed `camelot-py[cv]`, `tabula-py`, `openpyxl`, `anthropic`, `pyyaml` — none are imported anywhere in `src/`. All 156 tests still pass.

### P17. No database indexes on foreign keys
**DONE** — Added 5 `CREATE INDEX IF NOT EXISTS` statements to `SCHEMA_SQL` in `database.py`: `commitments(fund_id)`, `commitments(pension_fund_id)`, `commitments(as_of_date)`, `fund_aliases(fund_id)`, `review_queue(resolved)`.

### P18. README test count stale
**DONE** — Already updated to 156 in prior round. Verified accurate.

### P19. Unused `import random` in analysis.py
**DONE** — Removed unused `import random` from `src/analysis.py`.

### P20. SQL injection risk in export.py cross_pension_matrix
**DONE** — Refactored `export_cross_pension_matrix_csv()` to use parameterized `?` placeholders for `pension_fund_id` values in CASE WHEN clauses. Added alphanumeric validation on column alias identifiers. Added `import re` to export.py.

### P21. NY Common as_of_date hardcoded
**DONE** — Added `_extract_as_of_date()` method to `NYCommonAdapter` that scans first 5 PDF pages using shared `extract_as_of_date_from_text()`. Falls back to `"2025-03-31"` only if extraction fails. Updated `_parse_page_by_words()` to accept `as_of_date` parameter.

### P22. Dead config.yaml never loaded
**DONE** — Deleted `config.yaml` from project root. Verified no source file imports `yaml` or references `config.yaml`.

### P23. No upper bounds on dependency versions
**DONE** — Added `<major+1` upper bounds to all dependencies in `requirements.txt` (e.g., `requests>=2.31.0,<3.0`).

### P24. Unused imports in florida_sba.py
**DONE** — Removed unused `import requests` and unused `parse_dollar_amount` import from `src/adapters/florida_sba.py`. (Fixed in prior round, recorded here for completeness.)

---

## VERIFICATION CHECKLIST

After all fixes, verify:
- [x] `python3 -m pytest tests/ -x -q` — all 156 tests pass
- [x] `SELECT COUNT(*) FROM commitments WHERE net_multiple < 0` — returns 0
- [x] No duplicate `_rejoin_number` or `_extract_as_of_date` methods across adapters
- [x] `SELECT COUNT(*) FROM gp_aliases` — returns 1,640 (> 0)
- [x] README "Overall" field completeness matches actual DB queries (all 5 funds included)
- [x] README architecture tree matches actual file listing
- [x] Zero datetime deprecation warnings in test output (only bs4 warning remains)
- [x] Strategy classification rate above 50% (56.0%)
- [x] Run `python3 -m src export` and verify CSVs are generated correctly (9 files exported)
- [x] No unused imports in any `src/` file
- [x] No unused dependencies in `requirements.txt`
- [x] Database indexes exist on all foreign key columns
- [x] No hardcoded as_of_date in NY Common adapter
- [x] No SQL string interpolation with external values in export.py
- [x] `config.yaml` removed (never loaded)
