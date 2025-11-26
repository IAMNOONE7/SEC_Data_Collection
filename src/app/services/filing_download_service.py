
from __future__ import annotations

from pathlib import Path
from typing import Dict
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from backend.src.app.clients.sec_client import SecClient
from backend.src.app.services.submissions_10x_service import Filing10X

def is_instance_xbrl(snippet: str) -> bool:
    snippet = snippet.lower()

    return (
        "<xbrl" in snippet                      # unprefixed instance
        or "<xbrli:xbrl" in snippet             # prefixed instance
        or "<ix:nonfraction" in snippet         # inline XBRL facts
        or "<ix:header" in snippet              # inline XBRL header
    )

def build_primary_document_url(filing: Filing10X) -> str:
    """
    Build the URL to the primary document (e.g., 10-Q or 10-K HTML)
    for a given Filing10X.

    Pattern:
        https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_document}
    """
    cik_int = str(int(filing.cik))  # normalize to unpadded integer string
    acc_nodash = filing.accession_number.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/"
    return base + filing.primary_document


def download_primary_html(
    client: SecClient,
    filing: Filing10X,
    target_dir: Path,
) -> Path:
    """
    Download the primary HTML document for a filing and save it to target_dir.

    Returns the path to the saved file.
    """
    target_dir.mkdir(parents=True, exist_ok=True)

    url = build_primary_document_url(filing)
    html = client.fetch_text(url)

    # build filename like: AAPL_0000320193-24-000010_10-Q.html
    safe_ticker = filing.ticker.upper()
    fname = f"{safe_ticker}_{filing.accession_number}_{filing.form}.html"
    out_path = target_dir / fname

    out_path.write_text(html, encoding="utf-8")
    return out_path


def build_filing_base_dir(filing: Filing10X) -> str:
    """
    Base directory for all files of a filing, e.g.:

    https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/
    """
    cik_int = str(int(filing.cik))
    acc_nodash = filing.accession_number.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/"


def find_instance_xbrl_url(client: SecClient, filing: Filing10X) -> Optional[str]:
    """
    Try to locate the XBRL instance document for a 10-Q/10-K filing.

    Heuristic:
      - Fetch the headers page {base}{accession}-index-headers.html
      - Collect all .xml links
      - For each, fetch a small snippet and check for '<xbrli:xbrl'
    """
    base_dir = build_filing_base_dir(filing)
    index_url = f"{base_dir}{filing.accession_number}-index.html"

    html = client.fetch_text(index_url)
    soup = BeautifulSoup(html, "html.parser")

    xml_links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".xml"):
            xml_links.append(urljoin(index_url, href))

    if not xml_links:
        return None

    # 1) Try any '*htm.xml' first (like a-20250731_htm.xml)
    preferred = [u for u in xml_links if u.lower().endswith("htm.xml")]
    others = [u for u in xml_links if u not in preferred]
    ordered = preferred + others

    # 2) For each candidate, look for <xbrli:xbrl> in the first few KB
    for url in ordered:
        try:
            r = client._get(url)
            snippet = r.content[:8192].decode("utf-8", errors="ignore")
            if is_instance_xbrl(snippet):
                return url
        except Exception:
            continue

    return None