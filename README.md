# Pension Fund Alternative Investment Extraction Tool

A production-grade data pipeline that extracts, normalizes, and structures alternative investment commitment data from U.S. public pension fund disclosures. Produces a clean, queryable SQLite database and CSV exports.

## What It Does

- Extracts private equity fund commitment data from **4 major U.S. pension funds**
- Parses HTML tables and PDF documents using deterministic methods (no LLM needed)
- Resolves fund names across pension systems using fuzzy matching (280 cross-linked funds)
- Classifies funds by strategy (Venture Capital, Growth, Credit, etc.) from name keywords
- Tracks full data provenance: every record traces back to its source URL and document
- Produces quality reports flagging outliers and inconsistencies

## Data Coverage

| Pension Fund | State | Source Format | Records | As-of Date | Confidence |
|---|---|---|---:|---|---|
| CalPERS | CA | HTML table | 429 | 2025-03-31 | 1.00 |
| CalSTRS | CA | PDF | 473 | 2025-06-30 | 0.95 |
| WSIB | WA | PDF | 462 | 2025-06-30 | 0.90 |
| Oregon PERS | OR | PDF | 402 | 2025-09-30 | 0.95 |

**Total: 1,766 commitment records across 1,397 unique funds**

### Entity Resolution

- 280 funds cross-linked across 2+ pension systems
- 71 funds cross-linked across 3+ pension systems
- 18 funds cross-linked across all 4 pension systems
- Fund number extraction prevents false positive matches (e.g., Fund V vs Fund VI)

### Field Completeness

| Field | Overall | CalPERS | CalSTRS | WSIB | Oregon |
|---|---:|---:|---:|---:|---:|
| Commitment ($M) | 99.9% | 100% | 99.8% | 100% | 100% |
| Vintage Year | 99.7% | 100% | 100% | 98.7% | 100% |
| Capital Called ($M) | 98.4% | 100% | 96.4% | 97.6% | 100% |
| Distributions ($M) | 94.8% | 100% | 86.9% | 93.7% | 100% |
| Remaining Value ($M) | 84.8% | 100% | 89.4% | 53.5% | 99.3% |
| Net IRR | 87.7% | 62.2% | 100% | 97.2% | 89.3% |
| Net Multiple (TVPI) | 98.0% | 100% | 96.2% | 97.6% | 98.5% |

### Fields Extracted

- Fund name, vintage year, asset class
- Commitment amount ($M)
- Capital called/contributed ($M)
- Capital distributed ($M)
- Remaining/market value ($M)
- Net IRR, net multiple (TVPI)
- As-of date, source URL, extraction method, confidence score

## Installation

```bash
# Clone the repo
git clone <repo-url>
cd pension-fund-tracker

# Install dependencies
pip install -r requirements.txt
```

Requires Python 3.11+.

## Usage

### Run the full pipeline

```bash
# Extract data from all pension funds
python -m src run

# Extract from a single fund
python -m src run --fund calpers

# Force re-extraction even if source hasn't changed
python -m src run --force
```

### Check status

```bash
python -m src status
```

### Export data

```bash
# Generate CSVs to data/exports/
python -m src export
```

Produces:
- `commitments.csv` — all commitment records with fund and pension fund names
- `fund_summary.csv` — one row per fund, aggregated across pension systems
- `quality_report.md` — data quality report

### Quality report

```bash
python -m src quality
```

Shows field completeness, value range flags, and cross-fund consistency checks.

### Audit entity resolution links

```bash
python -m src audit-links
```

Shows all fuzzy-matched fund aliases, flags suspect matches, and reports cross-link statistics.

## Architecture

```
src/
├── cli.py              # CLI interface (click)
├── pipeline.py         # Orchestrates extraction across adapters
├── database.py         # SQLite schema and CRUD operations
├── entity_resolution.py # Fuzzy matching of fund names across sources
├── quality.py          # Data quality checks and reporting
├── export.py           # CSV and Markdown export
├── adapters/
│   ├── base.py         # Abstract adapter interface
│   ├── calpers.py      # CalPERS HTML table parser
│   ├── calstrs.py      # CalSTRS PDF parser
│   ├── wsib.py         # WSIB PDF parser
│   └── oregon.py       # Oregon PERS PDF parser
└── utils/
    ├── normalization.py # Dollar, percentage, date parsing
    └── pdf_parser.py    # PDF text extraction utilities
```

Each pension fund has its own **adapter** that implements a common interface. The pipeline treats all funds uniformly. Adding a new fund means writing a new adapter — see [docs/adding_a_fund.md](docs/adding_a_fund.md).

## Design Principles

1. **Deterministic over probabilistic** — All 4 adapters use deterministic parsing. No LLM calls needed.
2. **Source-aware** — Each fund has its own adapter that knows the exact data format.
3. **Data quality over quantity** — Every record has provenance tracking and confidence scores.
4. **Idempotent** — Running twice produces identical results, not duplicates.

## Database

SQLite database at `data/pension_tracker.db`. Schema designed for easy migration to PostgreSQL.

Key tables: `pension_funds`, `funds`, `commitments`, `fund_aliases`, `extraction_runs`, `review_queue`.

See [docs/data_dictionary.md](docs/data_dictionary.md) for field definitions.

## Testing

```bash
python -m pytest tests/ -v
```

64 tests covering normalization utilities, entity resolution, and the CalPERS adapter.
