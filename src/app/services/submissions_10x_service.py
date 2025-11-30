from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple
from dataclasses import asdict
from typing import Set, Tuple

from backend.src.app.clients.sec_client import SecClient


ALLOWED_FORMS_10X: Set[str] = {
    "10-K",
    "10-K/A",
    "10-Q",
    "10-Q/A",
}

############################################################
# Overview
#
# This module works with the SEC submissions to:
#
#   - Represent 10-K / 10-Q filings as a Filing10X.
#   - Fetch all recent 10-K / 10-Q for a company using its CIK.
#   - Batch-fetch filings for many companies, skipping ones that fail.
#   - Detect companies that appear to be missing expected 10-Q filings
#     in the last N years (by simple count).
#   - Merge newly discovered filings into an existing JSON-style payload
#     without creating duplicates.
#
# It’s intentionally focused on the 10-K and 10-Q only.
# You know every Filing10X here is “a major periodic report”.
############################################################

@dataclass(frozen=True)
class Filing10X:
    """
    Represents one 10-K / 10-Q filing for a company.

    frozen=True -> instances are immutable (hashable, safe to use in sets)
    """
    ticker: str
    cik: str
    form: str
    accession_number: str
    primary_document: str
    filing_date: dt.date


def _parse_recent_filings_10x(
    submissions_json: Dict[str, Any],
    cik: str,
    ticker: str,
    since: dt.date,
) -> List[Filing10X]:
    """
    Parse the filings.recent section of the submissions JSON and return
    all 10-K/10-Q after the given date.
    """
    filings = submissions_json.get("filings", {})
    recent = filings.get("recent", {})
    # SEC returns parallel arrays
    forms: Sequence[str] = recent.get("form", [])
    accession_numbers: Sequence[str] = recent.get("accessionNumber", [])
    primary_documents: Sequence[str] = recent.get("primaryDocument", [])
    filing_dates: Sequence[str] = recent.get("filingDate", [])

    # How many complete rows we can safely parse
    n = min(len(forms), len(accession_numbers), len(primary_documents), len(filing_dates))
    if n == 0:
        return []

    results: List[Filing10X] = []

    for i in range(n):
        form = forms[i]
        if form not in ALLOWED_FORMS_10X:
            # Ignore everything outside the 10-K/10-Q
            continue

        date_str = filing_dates[i]
        try:
            fdate = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        if fdate <= since:
            continue

        accn = accession_numbers[i]
        primary_doc = primary_documents[i]

        results.append(
            Filing10X(
                ticker=ticker,
                cik=cik,
                form=form,
                accession_number=accn,
                primary_document=primary_doc,
                filing_date=fdate,
            )
        )

    return results


def fetch_10x_for_company(
    client: SecClient,
    cik: str,
    ticker: str,
    since: dt.date,
) -> List[Filing10X]:
    """
    Fetch and parse all 10-K/10-Q filings for a single company,
    after the given 'since' date
    """
    submissions = client.fetch_submissions_json(cik)
    return _parse_recent_filings_10x(submissions, cik=cik, ticker=ticker, since=since)


def fetch_10x_for_companies(
    client: SecClient,
    companies: Iterable[Tuple[str, str]],  # (ticker, cik)
    since: dt.date,
) -> List[Filing10X]:
    """
    Fetch all 10-K / 10-Q filings for a list of (ticker, cik) pairs.
    Returns a flat list of Filing10X objects.
    """
    results: List[Filing10X] = []

    for ticker, cik in companies:
        try:
            print(ticker)
            filings = fetch_10x_for_company(client, cik=cik, ticker=ticker, since=since)
            results.extend(filings)
        except Exception:
            #log the exception; for now just continue.
            continue

    return results


def detect_missing_10q_filings(
    filings: List[Filing10X],
    years_back: int = 2
) -> Dict[str, List[int]]:
    """
    Detect which companies are missing expected 10-Q filings.
    """
    from collections import defaultdict

    # Filter only 10-Q and 10-Q/A
    ten_q_forms = {"10-Q", "10-Q/A"}
    now = dt.date.today()
    start_year = now.year - years_back

    # counts[ticker][year] -> number of 10-Q filings
    counts = defaultdict(lambda: defaultdict(int))

    for f in filings:
        if f.form in ten_q_forms:
            year = f.filing_date.year
            if year >= start_year:
                counts[f.ticker][year] += 1

    missing = {}

    for ticker, years in counts.items():
        missing_years = []
        for y in range(start_year, now.year + 1):
            expected = 3  # typical pattern: 3 × 10-Q + 1 × 10-K per full year
            actual = years.get(y, 0)
            if actual < expected:
                missing_years.append(y)
        if missing_years:
            missing[ticker] = missing_years

    return missing


def serialize_filing_10x(f: Filing10X) -> Dict[str, Any]:
    """
    Convert Filing10X dataclass to a JSON-serializable dict.
    """
    return {
        **asdict(f),
        "filing_date": f.filing_date.isoformat(),
    }


def merge_filings_into_payload(
    existing_payload: Dict[str, Any],
    new_filings: List[Filing10X],
) -> tuple[Dict[str, Any], int]:

    """ 
    Merge new Filing10X objects into an existing JSON.
    Deduplicates by (ticker, accession_number).
    existing_payload is expected to have:
      {
        "as_of": "...",
        "count": int,
        "filings": [ { ... }, ... ]
      }

    """
    filings_list: List[Dict[str, Any]] = existing_payload.get("filings", [])

    # Build a set of (ticker, accession_number) that we already have
    existing_keys: Set[Tuple[str, str]] = set()
    for f in filings_list:
        t = f.get("ticker")
        accn = f.get("accession_number")
        if t and accn:
            existing_keys.add((t, accn))

    added_count = 0
    for f in new_filings:
        key = (f.ticker, f.accession_number)
        if key in existing_keys:
            # Already present, skip duplicate.
            continue
        filings_list.append(serialize_filing_10x(f))
        existing_keys.add(key)
        added_count += 1

    existing_payload["filings"] = filings_list
    existing_payload["count"] = len(filings_list)
    # Caller updates "as_of"
    return existing_payload, added_count