"""Board meeting intelligence extraction using PageIndex.

Uses PageIndex's tree-structured document indexing to extract forward-looking
signals and structured events from pension fund board meeting PDFs. Complements
the deterministic regex+spaCy pipeline in board_minutes.py by catching nuanced,
qualitative intelligence that pattern matching misses.

Usage:
    # Process all cached board meeting PDFs
    python -m src.pageindex_extraction

    # Process a single PDF
    python -m src.pageindex_extraction data/cache/board_minutes/wsib/board_meeting_nov2025.pdf

    # Just check status of already-submitted documents
    python -m src.pageindex_extraction --status

Requires PAGEINDEX_API_KEY environment variable.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

from pageindex import PageIndexClient

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "board_minutes"
OUTPUT_DIR = PROJECT_ROOT / "data" / "pageindex"
DOC_REGISTRY = OUTPUT_DIR / "document_registry.json"

# ── Extraction queries ────────────────────────────────────────────────────
# These are the questions we ask PageIndex about each meeting document.
# Designed to extract the same categories as board_intelligence.json but
# also surface things the deterministic pipeline can't catch.

EXTRACTION_QUERIES = [
    {
        "id": "commitments",
        "query": (
            "List every investment commitment or fund allocation that was "
            "approved, authorized, or ratified in this meeting. For each one, "
            "provide: the fund name, the dollar amount, the investment strategy "
            "or asset class, the general partner (GP) name, any prior "
            "relationship history mentioned, and the vote outcome. "
            "Format as a structured list."
        ),
    },
    {
        "id": "personnel",
        "query": (
            "List all personnel changes discussed in this meeting: officer "
            "elections, new hires, departures, retirements, committee "
            "appointments or removals, and any open positions or planned "
            "searches. For each, give the person's name, the role or position, "
            "and the vote outcome if applicable."
        ),
    },
    {
        "id": "policy_and_strategy",
        "query": (
            "What policy approvals, strategic asset allocation changes, or "
            "investment plan approvals occurred in this meeting? Include any "
            "changes to target allocations with the specific percentages "
            "(prior vs. new targets), any new asset classes added, and any "
            "annual investment plans approved. Note the vote outcome for each."
        ),
    },
    {
        "id": "dissent",
        "query": (
            "Were there any dissenting votes, formal objections, statements "
            "of concern read into the record, or split votes in this meeting? "
            "If so, who dissented, on what topic, what were their specific "
            "concerns, and what was the final outcome?"
        ),
    },
    {
        "id": "forward_signals",
        "query": (
            "What forward-looking signals or upcoming decisions can be "
            "identified from this meeting? Look for: educational sessions on "
            "new asset classes (suggesting future allocation changes), RFP "
            "announcements, consultant studies underway, advisory votes, "
            "mentions of upcoming reviews or decisions at future meetings, "
            "deployment targets or pacing plans for the coming year, and any "
            "strategic initiative language. These are the signals a fundraising "
            "team would want to know about months before formal decisions."
        ),
    },
    {
        "id": "manager_selections",
        "query": (
            "Were any external investment managers, consultants, or service "
            "providers selected, retained, or terminated in this meeting? "
            "For each, provide the firm name, the mandate or role, and the "
            "vote outcome."
        ),
    },
    {
        "id": "fundraising_insight",
        "query": (
            "If you were an alternative investment fund manager trying to "
            "raise capital from this pension system, what are the most "
            "actionable takeaways from this meeting? Consider: which asset "
            "classes are they increasing exposure to, what strategies are "
            "they prioritizing, are there deployment shortfalls that create "
            "urgency, what is their risk appetite, and are there any explicit "
            "statements about the types of managers or strategies they want "
            "to add? Summarize in 2-3 sentences."
        ),
    },
]


def get_client() -> PageIndexClient:
    """Initialize PageIndex client with API key from environment."""
    api_key = os.environ.get("PAGEINDEX_API_KEY")
    if not api_key:
        print("Error: PAGEINDEX_API_KEY environment variable not set.")
        print("Get your key at https://dash.pageindex.ai/api-keys")
        sys.exit(1)
    return PageIndexClient(api_key=api_key)


def load_registry() -> dict:
    """Load the document registry tracking submitted PDFs and their doc_ids."""
    if DOC_REGISTRY.exists():
        return json.loads(DOC_REGISTRY.read_text())
    return {"documents": {}}


def save_registry(registry: dict):
    """Save the document registry."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DOC_REGISTRY.write_text(json.dumps(registry, indent=2))


def submit_document(client: PageIndexClient, pdf_path: Path, registry: dict) -> str:
    """Submit a PDF to PageIndex and return its doc_id."""
    pdf_key = str(pdf_path.resolve().relative_to(PROJECT_ROOT))

    # Check if already submitted
    if pdf_key in registry["documents"]:
        doc_id = registry["documents"][pdf_key]["doc_id"]
        logger.info(f"Already submitted: {pdf_path.name} -> {doc_id}")
        return doc_id

    logger.info(f"Submitting {pdf_path.name} to PageIndex...")
    result = client.submit_document(str(pdf_path))
    doc_id = result["doc_id"]

    registry["documents"][pdf_key] = {
        "doc_id": doc_id,
        "filename": pdf_path.name,
        "submitted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "processing",
    }
    save_registry(registry)
    logger.info(f"Submitted: {pdf_path.name} -> {doc_id}")
    return doc_id


def wait_for_ready(client: PageIndexClient, doc_id: str, filename: str,
                   timeout: int = 300, poll_interval: int = 10) -> bool:
    """Wait for a document to finish processing."""
    elapsed = 0
    while elapsed < timeout:
        if client.is_retrieval_ready(doc_id):
            logger.info(f"{filename} is ready for queries.")
            return True
        logger.info(f"Waiting for {filename} to process... ({elapsed}s)")
        time.sleep(poll_interval)
        elapsed += poll_interval

    logger.warning(f"Timeout waiting for {filename} after {timeout}s")
    return False


def get_tree_structure(client: PageIndexClient, doc_id: str, filename: str) -> dict | None:
    """Retrieve the tree index for a processed document."""
    try:
        result = client.get_tree(doc_id, node_summary=True)
        tree_path = OUTPUT_DIR / f"{Path(filename).stem}_tree.json"
        tree_path.write_text(json.dumps(result, indent=2))
        logger.info(f"Tree saved to {tree_path}")
        return result
    except Exception as e:
        logger.error(f"Failed to get tree for {filename}: {e}")
        return None


def run_extraction_queries(client: PageIndexClient, doc_id: str,
                           filename: str) -> dict:
    """Run all extraction queries against a processed document."""
    results = {}

    for q in EXTRACTION_QUERIES:
        query_id = q["id"]
        logger.info(f"  Querying [{query_id}] on {filename}...")

        try:
            response = client.chat_completions(
                messages=[{"role": "user", "content": q["query"]}],
                doc_id=doc_id,
                enable_citations=True,
            )
            results[query_id] = {
                "query": q["query"],
                "response": response,
            }
        except Exception as e:
            logger.error(f"  Query [{query_id}] failed: {e}")
            results[query_id] = {
                "query": q["query"],
                "error": str(e),
            }

    # Save raw results
    output_path = OUTPUT_DIR / f"{Path(filename).stem}_extraction.json"
    output_path.write_text(json.dumps(results, indent=2, default=str))
    logger.info(f"Extraction results saved to {output_path}")
    return results


def check_status(client: PageIndexClient, registry: dict):
    """Print status of all submitted documents."""
    if not registry["documents"]:
        print("No documents submitted yet.")
        return

    print(f"\n{'Document':<45} {'Doc ID':<25} {'Status'}")
    print("-" * 90)
    for pdf_key, info in registry["documents"].items():
        doc_id = info["doc_id"]
        try:
            ready = client.is_retrieval_ready(doc_id)
            status = "Ready" if ready else "Processing"
        except Exception as e:
            status = f"Error: {e}"
        print(f"{info['filename']:<45} {doc_id:<25} {status}")


def process_pdf(client: PageIndexClient, pdf_path: Path, registry: dict):
    """Full pipeline for one PDF: submit, wait, get tree, run queries."""
    filename = pdf_path.name
    print(f"\n{'=' * 70}")
    print(f"Processing: {filename}")
    print(f"{'=' * 70}")

    # Step 1: Submit
    doc_id = submit_document(client, pdf_path, registry)

    # Step 2: Wait for processing
    if not wait_for_ready(client, doc_id, filename):
        print(f"  Skipping {filename} — not ready yet. Re-run later.")
        return

    # Update registry status
    pdf_key = str(pdf_path.resolve().relative_to(PROJECT_ROOT))
    registry["documents"][pdf_key]["status"] = "ready"
    save_registry(registry)

    # Step 3: Get tree structure
    print(f"  Retrieving tree index...")
    get_tree_structure(client, doc_id, filename)

    # Step 4: Run extraction queries
    print(f"  Running {len(EXTRACTION_QUERIES)} extraction queries...")
    results = run_extraction_queries(client, doc_id, filename)

    # Step 5: Print summary
    print(f"\n  Extraction complete for {filename}:")
    for query_id, result in results.items():
        if "error" in result:
            print(f"    [{query_id}] ERROR: {result['error']}")
        else:
            # Get the response text — handle different response formats
            resp = result["response"]
            if isinstance(resp, dict):
                text = resp.get("choices", [{}])[0].get("message", {}).get("content", str(resp))
            else:
                text = str(resp)
            preview = text[:120].replace("\n", " ")
            print(f"    [{query_id}] {preview}...")


def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    client = get_client()
    registry = load_registry()

    args = sys.argv[1:]

    # --status flag: just check document status
    if "--status" in args:
        check_status(client, registry)
        return

    # Specific PDF path provided
    if args and not args[0].startswith("--"):
        pdf_path = Path(args[0]).resolve()
        if not pdf_path.exists():
            pdf_path = (PROJECT_ROOT / args[0]).resolve()
        if not pdf_path.exists():
            print(f"File not found: {args[0]}")
            sys.exit(1)
        process_pdf(client, pdf_path, registry)
        return

    # Default: process all cached board meeting PDFs
    if not CACHE_DIR.exists():
        print(f"No board minutes directory at {CACHE_DIR}")
        sys.exit(1)

    pdfs = sorted(CACHE_DIR.rglob("*.pdf"))
    if not pdfs:
        print("No PDF files found.")
        sys.exit(1)

    print(f"Found {len(pdfs)} board meeting documents.")
    print(f"Free tier: 200 pages. Total pages across all PDFs may exceed this.")
    print(f"Documents will be processed in order.\n")

    for pdf_path in pdfs:
        process_pdf(client, pdf_path, registry)

    # Final summary
    print(f"\n{'=' * 70}")
    print("ALL DONE")
    print(f"{'=' * 70}")
    print(f"Results saved to: {OUTPUT_DIR}/")
    print(f"  - *_tree.json     : Hierarchical document structure")
    print(f"  - *_extraction.json: Query results with citations")
    print(f"  - document_registry.json: Tracking submitted documents")


if __name__ == "__main__":
    main()
