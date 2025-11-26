from __future__ import annotations
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List
import sys
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

BACKEND_ROOT = Path(__file__).resolve().parent
SRC_ROOT = BACKEND_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from app.clients.sec_client import SecClient  # type: ignore
from app.services.submissions_10x_service import Filing10X  # type: ignore
from src.app.services.filing_download_service import find_instance_xbrl_url  # NEW
from src.app.services.xbrl_company_totals_service import (  # type: ignore
    parse_contexts,
    extract_company_totals_for_main_period,
    print_by_context,
)


def load_submissions_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Submissions file not found at {path}")
    return json.loads(path.read_text(encoding="utf-8"))

def pick_first_10q(payload: Dict[str, Any]) -> Filing10X:
    filings: List[Dict[str, Any]] = payload.get("filings", [])
    for f in filings:
        form = f.get("form") or f.get("form_type")
        if form and form.startswith("10-Q"):
            filing_date_str = f.get("filing_date")
            filing_date = (
                dt.datetime.strptime(filing_date_str, "%Y-%m-%d").date()
                if filing_date_str
                else dt.date.today()
            )
            return Filing10X(
                ticker=f["ticker"],
                cik=f["cik"],
                form=form,
                accession_number=f["accession_number"],
                primary_document=f["primary_document"],
                filing_date=filing_date,
            )
    raise RuntimeError("No 10-Q filings found in payload.")



def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Polite EDGAR usage; .env should already have these, but we set fallbacks
    os.environ.setdefault("SEC_USER_AGENT", os.getenv("SEC_USER_AGENT", "you@example.com"))
    os.environ.setdefault("SEC_REFERER", os.getenv("SEC_REFERER", "https://github.com/your/repo"))

    submissions_path = BACKEND_ROOT / "data" / "10x_submissions" / "10x_submissions.json"
    logging.info("Loading submissions from %s", submissions_path)

    payload = load_submissions_json(submissions_path)
    filing = pick_first_10q(payload)

    logging.info(
        "Selected 10-Q filing: ticker=%s cik=%s form=%s accession=%s primary=%s",
        filing.ticker,
        filing.cik,
        filing.form,
        filing.accession_number,
        filing.primary_document,
    )

    client = SecClient()

    # Try to locate the XBRL instance document
    logging.info("Searching for XBRL instance document...")
    instance_url = find_instance_xbrl_url(client, filing)

    if not instance_url:
        logging.error("No XBRL instance document found for this filing.")
        return 1

    logging.info("Found instance XBRL: %s", instance_url)

    text = client.fetch_text(instance_url)

    # Save raw XBRL for manual inspection
    target_dir = BACKEND_ROOT / "data" / "10x_raw_xbrl" / filing.ticker.upper()
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{filing.ticker.upper()}_{filing.accession_number}_{filing.form}_instance.xml"
    out_path = target_dir / filename
    out_path.write_text(text, encoding="utf-8", errors="ignore")

    logging.info("Saved XBRL instance to %s", out_path)


    # Parse and inspect
    root = ET.fromstring(text)
    contexts = parse_contexts(root)

    rows = extract_company_totals_for_main_period(
        root=root,
        contexts=contexts,
        ticker=filing.ticker,
        filing_date=filing.filing_date,
        limit=500,
    )

    print_by_context(rows, contexts)

    print("\nOpen this file in an editor to explore tags and values:")
    print(f"  {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
