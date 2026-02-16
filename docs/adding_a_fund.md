# Adding a New Pension Fund Adapter

This guide walks through adding a new pension fund data source to the pipeline.

## Step 1: Research the Data Source

Before writing code, inspect the actual data:

1. Find the pension fund's PE/alternative investment disclosure page
2. Determine the format: HTML table, PDF, CSV, Excel
3. Download a sample and inspect the structure
4. Identify available columns (fund name, vintage, commitment, IRR, etc.)
5. Note the as-of date and update frequency

## Step 2: Create the Adapter File

Create `src/adapters/<fund_id>.py`. Use an existing adapter as a template:

- **HTML tables**: Use `calpers.py` as a reference
- **PDF with good table structure**: Use `oregon.py` as a reference
- **PDF with complex layout**: Use `wsib.py` as a reference

## Step 3: Implement the Adapter Class

```python
from src.adapters.base import PensionFundAdapter

class NewFundAdapter(PensionFundAdapter):
    pension_fund_id = "new_fund"       # Unique short ID
    pension_fund_name = "New Fund Name" # Display name
    state = "XX"                        # Two-letter state code
    source_url = "https://..."          # Data source URL

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self._cache_path = Path("data/cache/new_fund/data_file")

    def fetch_source(self) -> bytes | str:
        """Download or load cached source data."""
        # Implement fetching logic
        # Always cache the data for testing

    def parse(self, raw_data) -> list[dict]:
        """Parse raw data into commitment records."""
        # Return list of dicts with these keys:
        # fund_name_raw, general_partner, vintage_year, asset_class,
        # sub_strategy, commitment_mm, capital_called_mm,
        # capital_distributed_mm, remaining_value_mm, net_irr,
        # net_multiple, dpi, as_of_date, source_url, source_document,
        # extraction_method, extraction_confidence
```

### Key Requirements

- All dollar amounts must be in **millions**
- IRR must be a **decimal** (0.15 for 15%, not 15.0)
- Net multiple should be a float (1.5, not "1.5x")
- Set `extraction_method` to describe how data was extracted
- Set `extraction_confidence` to 1.0 for deterministic, 0.9+ for PDF, lower for LLM

## Step 4: Register the Adapter

Add it to `src/adapters/__init__.py`:

```python
from src.adapters.new_fund import NewFundAdapter

ADAPTER_REGISTRY["new_fund"] = NewFundAdapter
```

## Step 5: Test

1. Run the adapter standalone:
   ```bash
   python -m src run --fund new_fund
   ```

2. Check the quality report:
   ```bash
   python -m src quality
   ```

3. Write a test in `tests/adapters/test_new_fund.py` using cached data

## Step 6: Run Full Pipeline

```bash
python -m src run --force
python -m src quality
python -m src export
```

Verify entity resolution is linking funds correctly across pension systems.

## Tips

- **PDF parsing**: Use pdfplumber's `extract_words()` and group by y-coordinate for reliable column assignment. Plain `extract_text()` often concatenates adjacent columns.
- **Column boundaries**: Print word x-coordinates to determine column boundaries empirically.
- **Amounts**: Check whether the source uses raw dollars, thousands, or millions. Convert to millions.
- **Caching**: Always cache fetched data so tests are reproducible without network access.
- **Confidence**: PDF extraction should use 0.90-0.95 confidence. HTML tables use 1.0.
