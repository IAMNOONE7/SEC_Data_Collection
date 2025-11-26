#!/usr/bin/env python3
"""
Compare consolidated 10-Q XBRL concepts across multiple companies.

High-level flow:
    1. Load 10x_submissions.json (your aggregated submissions file).
    2. For each ticker, pick the LATEST 10-Q filing.
    3. For each chosen filing:
        - Find its XBRL instance URL
        - Download XML
        - Parse contexts + company-level totals
    4. Build:
        - For each ticker: set of concept names it uses
        - Global frequency of each concept across tickers
    5. Print:
        - Concepts common to ALL tickers
        - Top N most common concepts
        - Some unique concepts per ticker (only used by one company)

This will show you which items are named consistently (standard us-gaap/dei, etc.)
and which ones are company-specific extensions (e.g. "20250731:*").
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set

import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# --- Path setup so imports from src/ work the same as in your other scripts ---

BACKEND_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = BACKEND_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))


from backend.src.app.clients.sec_client import SecClient  # type: ignore
from backend.src.app.services.submissions_10x_service import Filing10X  # type: ignore
from backend.src.app.services.filing_download_service import find_instance_xbrl_url  # type: ignore

from backend.src.app.services.xbrl_company_totals_service import (
    parse_contexts,
    extract_company_totals_for_main_period,
    FactRow,
)

# ---------------------------
#   Helper dataclasses
# ---------------------------

@dataclass
class TickerFacts:
    """Simple container to hold all facts and the concept set for one ticker."""
    ticker: str
    filing_date: date
    facts: List[FactRow]
    concepts: Set[str]


# ---------------------------
#   JSON / Filings helpers
# ---------------------------

def load_submissions_json(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(f"Submissions file not found at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def pick_latest_10q_per_ticker(payload: Dict, max_companies: int = 10) -> List[Filing10X]:
    """
    From the large submissions payload, pick at most `max_companies` DISTINCT tickers,
    using the LATEST 10-Q for each ticker.

    Returns a list of Filing10X objects.
    """
    filings = payload.get("filings", [])
    by_ticker: Dict[str, Filing10X] = {}

    for f in filings:
        form = f.get("form") or f.get("form_type")
        if not form or not form.startswith("10-Q"):
            continue

        ticker = f.get("ticker")
        filing_date_str = f.get("filing_date")
        if not ticker or not filing_date_str:
            continue

        # Parse filing_date
        fd = datetime.strptime(filing_date_str, "%Y-%m-%d").date()

        existing = by_ticker.get(ticker)
        if existing is None or fd > existing.filing_date:
            by_ticker[ticker] = Filing10X(
                ticker=ticker,
                cik=f["cik"],
                form=form,
                accession_number=f["accession_number"],
                primary_document=f["primary_document"],
                filing_date=fd,
            )

    # We might have many tickers; just take up to max_companies
    chosen = list(by_ticker.values())
    chosen.sort(key=lambda x: x.ticker)  # stable ordering
    return chosen[:max_companies]


# ---------------------------
#   XBRL processing per filing
# ---------------------------

def fetch_company_totals_for_filing(
    client: SecClient,
    filing: Filing10X,
    limit: int = 1000,
) -> TickerFacts:
    """
    Download and parse the XBRL instance for one filing, then
    extract company-level totals for the main reporting period.

    Returns TickerFacts with:
        - all extracted FactRow objects
        - set of concept names used by this company
    """
    logging.info(
        "Processing %s %s (ticker=%s, date=%s)",
        filing.form,
        filing.accession_number,
        filing.ticker,
        filing.filing_date,
    )

    instance_url = find_instance_xbrl_url(client, filing)
    if not instance_url:
        logging.warning("No XBRL instance found for %s (%s)", filing.ticker, filing.accession_number)
        return TickerFacts(ticker=filing.ticker, filing_date=filing.filing_date, facts=[], concepts=set())

    logging.info("  XBRL instance URL: %s", instance_url)

    xml_text = client.fetch_text(instance_url)
    root = ET.fromstring(xml_text)

    contexts = parse_contexts(root)
    facts = extract_company_totals_for_main_period(
        root=root,
        contexts=contexts,
        ticker=filing.ticker,
        filing_date=filing.filing_date,
        limit=limit,
    )

    concept_set = {f.concept for f in facts}

    logging.info(
        "  Extracted %d consolidated facts (%d distinct concepts) for %s",
        len(facts),
        len(concept_set),
        filing.ticker,
    )

    return TickerFacts(
        ticker=filing.ticker,
        filing_date=filing.filing_date,
        facts=facts,
        concepts=concept_set,
    )


# ---------------------------
#   Comparison logic
# ---------------------------

def build_concept_frequency(ticker_facts: List[TickerFacts]) -> Dict[str, int]:
    """
    For each concept name, count in how many distinct tickers it appears.
    """
    freq: Dict[str, int] = defaultdict(int)
    for tf in ticker_facts:
        for concept in tf.concepts:
            freq[concept] += 1
    return freq


def pretty_print_summary(ticker_facts: List[TickerFacts]) -> None:
    """
    Print a human-readable summary:

        - Tickers and how many concepts each has
        - Concepts common to ALL tickers
        - Top 30 most frequent concepts
        - A few unique concepts per ticker
    """
    if not ticker_facts:
        print("No ticker facts to summarize.")
        return

    # --- Basic per-ticker summary ---
    print("\n=== PER-TICKER CONCEPT COUNTS ===")
    for tf in sorted(ticker_facts, key=lambda t: t.ticker):
        print(f"{tf.ticker:6}  {len(tf.concepts):4} distinct concepts")

    # --- Concepts common to ALL tickers ---
    all_concept_sets = [tf.concepts for tf in ticker_facts]
    common: Set[str] = set.intersection(*all_concept_sets) if all_concept_sets else set()

    print("\n=== CONCEPTS PRESENT IN ALL TICKERS ===")
    if not common:
        print("  (none)")
    else:
        for c in sorted(common):
            print("  ", c)

    # --- Frequency across tickers ---
    freq = build_concept_frequency(ticker_facts)

    print("\n=== TOP 30 MOST COMMON CONCEPTS (by #tickers that use them) ===")
    top = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:30]
    for concept, count in top:
        print(f"  {concept:90}  used by {count} tickers")

    # --- Unique concepts per ticker ---
    print("\n=== SAMPLE OF UNIQUE CONCEPTS PER TICKER (company-specific extensions) ===")
    for tf in sorted(ticker_facts, key=lambda t: t.ticker):
        unique = sorted([c for c in tf.concepts if freq[c] == 1])
        sample = unique[:15]  # limit to avoid flooding
        print(f"\nTicker {tf.ticker}: {len(unique)} unique concepts")
        for c in sample:
            print("   ", c)
        if len(unique) > len(sample):
            print(f"    ... and {len(unique) - len(sample)} more")


# ---------------------------
#          main()
# ---------------------------

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    load_dotenv()

    # Ensure SEC headers are present (polite EDGAR usage)
    os.environ.setdefault("SEC_USER_AGENT", os.getenv("SEC_USER_AGENT", "you@example.com"))
    os.environ.setdefault("SEC_REFERER", os.getenv("SEC_REFERER", "https://github.com/your/repo"))

    submissions_path = BACKEND_ROOT / "data" / "10x_submissions" / "10x_submissions.json"
    logging.info("Loading submissions from %s", submissions_path)

    payload = load_submissions_json(submissions_path)
    filings = pick_latest_10q_per_ticker(payload, max_companies=10)

    if not filings:
        logging.error("No 10-Q filings found in submissions JSON.")
        return 1

    logging.info("Selected %d tickers for comparison: %s",
                 len(filings),
                 ", ".join(sorted({f.ticker for f in filings})))

    client = SecClient()

    ticker_facts: List[TickerFacts] = []
    for filing in filings:
        tf = fetch_company_totals_for_filing(client, filing, limit=1000)
        if tf.facts:
            ticker_facts.append(tf)
        else:
            logging.warning("No facts extracted for ticker %s", filing.ticker)

    if not ticker_facts:
        logging.error("No usable XBRL data extracted for any ticker.")
        return 1

    pretty_print_summary(ticker_facts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
