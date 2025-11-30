from __future__ import annotations
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
import sys
from dotenv import load_dotenv
load_dotenv()


BACKEND_ROOT = Path(__file__).resolve().parent
SRC_ROOT = BACKEND_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from src.app.clients.sec_client import SecClient
from src.app.services.submissions_10x_service import (
    Filing10X,
    fetch_10x_for_companies,
    merge_filings_into_payload
)

############################################################
# Overview
#
# Top-level script for:
#   - Loading the companies watchlist (ticker + CIK) from companies.json
#   - Fetching all 10-K / 10-Q filings since a given date
#   - Merging those filings into a master JSON
#   - Writing/refreshing data/10x_submissions/10x_submissions.json
#
# This ties together:
#   - SecClient         -> polite SEC HTTP client with throttling/backoff
#   - submissions_10x   -> logic for parsing 10-K/10-Q filings
#
# Intended usage:
#   Run periodically (cron/manual) to keep the 10x_submissions.json
#   dataset up to date for downstream consumers.
############################################################

def _ua(par : str) -> str:
    # fall back to a decent UA if not set
    return os.getenv(par, "fallback@example.com")

def load_companies(path: Path) -> List[Tuple[str, str]]:
    """
    Load companies.json and produce a list of (ticker, cik) pairs.

    Expected schema:
    {
      "schema_version": "1.0",
      "as_of": "2025-09-15",
      "companies": [
        {"ticker": "AAPL", "cik": "0000320193", ...},
        ...
      ]
    }
    """
    raw: Dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    companies = raw.get("companies", [])
    out: List[Tuple[str, str]] = []
    for c in companies:
        ticker = c.get("ticker")
        cik = c.get("cik")
        if not ticker or not cik:
            continue
        out.append((ticker, cik))
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    # Polite EDGAR usage: make sure SEC_USER_AGENT is set to something non-empty.
    os.environ.setdefault("SEC_USER_AGENT", _ua("SEC_USER_AGENT"))

    companies_path = BACKEND_ROOT / "companies.json"
    if not companies_path.exists():
        logging.error("companies.json not found at %s", companies_path)
        return 1

    # Only consider filings after this date.
    since = dt.date(2022, 1, 1)
    logging.info("Fetching 10-K / 10-Q for filings after %s", since.isoformat())

    companies = load_companies(companies_path)
    logging.info("Loaded %d companies from watchlist.", len(companies))

    client = SecClient()
    # Pull 10-K / 10-Q filings for all companies.
    filings: List[Filing10X] = fetch_10x_for_companies(client, companies, since)

    # Optional: missing 10-Q detection (kept for later / debugging).
    """
    missing = detect_missing_10q_filings(filings, years_back=2)

    if missing:
        logging.warning("Some companies have missing 10-Q filings:")
        for ticker, years in missing.items():
            logging.warning("  %s missing 10-Q for years: %s", ticker, years)
    """


    logging.info("Total 10-K / 10-Q filings found: %d", len(filings))
    #build or update master JSON
    out_dir = BACKEND_ROOT / "data" / "10x_submissions"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "10x_submissions.json"

    if out_path.exists():
        # Load existing payload
        try:
            existing_payload = json.loads(out_path.read_text(encoding="utf-8"))
            logging.info(
                "Loaded existing payload with %d filings from %s",
                existing_payload.get("count", 0),
                out_path,
            )
        except Exception as e:
            logging.warning(
                "Could not read existing file %s: %s; starting fresh.", out_path, e
            )
            existing_payload = {
                "as_of": dt.date.today().isoformat(),
                "count": 0,
                "filings": [],
            }
    else:
        # Start new payload
        existing_payload = {
            "as_of": dt.date.today().isoformat(),
            "count": 0,
            "filings": [],
        }

    # Merge new filings into existing payload (dedupe by ticker+accession)
    updated_payload, added_count = merge_filings_into_payload(existing_payload, filings)

    # Always refresh metadata
    updated_payload["as_of"] = dt.date.today().isoformat()
    updated_payload["count"] = len(updated_payload.get("filings", []))

    logging.info("New filings added this run: %d", added_count)

    logging.info(
        "After merge: %d total filings stored in %s",
        updated_payload["count"],
        out_path,
    )

    # Write updated payload back to disk
    logging.info("Writing output: %s", out_path)
    out_path.write_text(json.dumps(updated_payload, indent=2), encoding="utf-8")

    logging.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
