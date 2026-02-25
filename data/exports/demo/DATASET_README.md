# Alternative Investment Commitment Dataset

## What This Dataset Contains

This dataset contains **private equity, venture capital, and alternative investment**
commitment data extracted from **6 major U.S. public pension fund** disclosures.
It covers **2,126 commitment records** across **1,610 unique funds**,
with **345 funds cross-referenced** across two or more pension systems.

The data is sourced entirely from public disclosures required under state transparency
laws. No proprietary or paywalled data is included.

## Pension Funds Covered

| Pension Fund | State | Total AUM | Records | Commitment Total | Vintages | Data As-Of |
|---|---|---:|---:|---:|---|---|
| CalSTRS | CA | $338B | 473 | $81,167M | 1998-2025 | 2025-06-30 |
| WSIB | WA | $190B | 462 | $99,966M | 1981-2025 | 2025-06-30 |
| CalPERS | CA | $503B | 429 | $133,560M | 1998-2025 | 2025-03-31 |
| Oregon PERS | OR | $100B | 402 | $58,127M | 1981-2025 | 2025-09-30 |
| New York State Common Retirement Fund | NY | $268B | 359 | $66,265M | 1987-2023 | 2024-03-31 |
| Texas Teacher Retirement System | TX | $200B | 1 | $45,290M | N/A | 2024-08-31 |

## Cross-Referencing Value

The dataset's unique value is **entity resolution across pension systems**.
When CalPERS, CalSTRS, WSIB, and Oregon all report commitments to the same fund,
we link those records together under a single canonical fund name.

- **345 funds** appear in 2 or more pension systems
- **116 funds** appear in 3 or more pension systems

This allows you to:
- See which GPs have the broadest LP relationships
- Compare reported performance for the same fund across different LPs
- Identify emerging managers who have won allocations from major pensions
- Validate data accuracy by cross-checking the same fund across sources

## Field Definitions

| Field | Description |
|---|---|
| Pension Fund | The public pension system reporting this commitment |
| State | U.S. state of the pension fund |
| Fund Name | The canonical name of the private equity or alternative fund |
| Asset Class | Broad category: Private Equity, Private Credit, Real Assets |
| Sub-Strategy | Where classified: Venture Capital, Growth Equity, Buyout, Credit, etc. |
| Vintage Year | Year the fund began investing (first capital call) |
| Commitment ($M) | Total capital the pension committed to the fund, in millions |
| Capital Called ($M) | Capital the GP has drawn down from the pension's commitment |
| Distributions ($M) | Capital returned to the pension (realizations + income) |
| Remaining Value ($M) | Current fair market value of unrealized holdings |
| Net IRR (%) | Net internal rate of return after fees, as reported by the pension |
| Net Multiple (x) | Total value to paid-in capital ratio (TVPI), net of fees |
| As-Of Date | The reporting date for this data point |
| Source URL | Direct link to the public document this data was extracted from |

## Field Completeness

Not all pension systems report all fields. Here is what to expect:

| Field | Populated |
|---|---:|
| Commitment Amount | 100% |
| Vintage Year | 100% |
| Capital Called | 99% |
| Distributions | 96% |
| Remaining Value | 87% |
| Net IRR | 73% |
| Net Multiple | 98% |

## Known Limitations

- **NY Common Retirement Fund does not publish IRR or Net Multiple.** Their disclosure
  includes commitment, contributed, distributed, and fair value, but not performance metrics.
- **DPI (distributions to paid-in) is not available** from any source in this dataset.
- **Data as-of dates vary by pension system.** CalPERS data may be as of June 2025
  while CalSTRS data is as of March 2025. Comparing IRRs across systems requires
  awareness of these timing differences.
- **Sub-strategy classification is incomplete.** Many well-known buyout funds appear
  without a sub-strategy label because not all pension disclosures categorize funds.
  Roughly 70% of funds have no sub-strategy assigned.
- **Texas TRS and Florida SBA have limited data.** Texas provides only a portfolio-level
  summary. Florida's website blocks programmatic access; their data is not included
  in the default extraction.
- **Performance data is net of fees** as reported by each pension fund. Different
  pensions may use slightly different calculation methodologies, which can produce
  small differences (typically <2 percentage points) for the same fund.

## Methodology

1. **Source identification:** We identified public disclosure pages for each pension fund
   and downloaded their most recent private equity portfolio reports.
2. **Deterministic extraction:** All data was extracted using programmatic parsing
   (HTML table parsing for CalPERS, PDF table extraction for all others). No AI/LLM
   was used for data extraction, ensuring reproducibility and accuracy.
3. **Entity resolution:** Fund names vary across pension systems (e.g., "KKR 2006 Fund"
   vs "KKR 2006 Fund, L.P."). We use fuzzy matching with secondary signals (GP name,
   vintage year, fund number) to link the same fund across systems.
4. **Quality checks:** Every record is validated against reasonable ranges for commitment
   size, IRR, and multiples. Outliers are flagged for manual review.

## Files Included

| File | Description |
|---|---|
| `sample_data.csv` | 100 curated records (20 per pension system) showcasing cross-linking |
| `emerging_manager_commitments.csv` | First-time funds (Fund I/II, vintage 2020+) across all pensions |
| `pe_performance_2015_2020.csv` | PE fund performance for mature 2015-2020 vintages |
| `gp_penetration.csv` | Fund families (derived GP names) with commitments from 3+ pension systems |
| `commitment_trends.csv` | Average commitment size by vintage year and strategy |
| `vc_commitments_by_pension.csv` | Venture capital commitments by pension fund |
| `DATASET_README.md` | This file |

*Generated February 12, 2026*