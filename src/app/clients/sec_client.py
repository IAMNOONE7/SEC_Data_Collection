from __future__ import annotations
import datetime as dt
import gzip
import io
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ---- config
_SEC_BASE = "https://www.sec.gov"
_UA = os.getenv("SEC_USER_AGENT", "you@example.com")
_REFERER = os.getenv("SEC_REFERER", "https://github.com/your/repo")
_REQ_TIMEOUT = int(os.getenv("SEC_REQ_TIMEOUT", "30"))
_BACKOFFS = [0.5, 1.0, 2.0, 4.0]  # seconds
_MIN_DELAY_BETWEEN_CALLS = float(os.getenv("SEC_MIN_DELAY_S", "0.2"))  # gentle pacing
_last_call = 0.0

########################################################################################################################
# Overview
#
# This module implements a lightweight SEC/EDGAR client
# with built-in throttling, retry logic, and helpers
# for working with SEC filings.
#
# Key responsibilities:
#
# 1. **Rate-Limited HTTP Client**
#    - Uses a shared `_throttle()` to ensure we never hit SEC too fast.
#    - Centralized `_get()` ensures all requests behave consistently.
#
# 2. **Daily Index Support (Legacy)**
#    - Can fetch SEC’s old daily master index files.
#
# 3. **Submissions JSON API (Primary path)**
#    - Fetches modern per-company JSON filings using CIK numbers.
#
# 4. **Filing Discovery Helpers**
#    - For a given accession number, parses the `*-index-headers.html`
#      page to locate the main XML and HTML filing documents.
#
# 5. **Utility Routines**
#    - Small helpers to format accession numbers (add dashes),
#      and to fetch arbitrary text via the same safe HTTP path.
#
# In short:
# The `SecClient` class provides a polite way to talk to
# the SEC website, with convenience tools to go from a CIK or index
# entry → to full filing URLs (XML/HTML) → to usable content.
########################################################################################################################

def _throttle() -> None:
    """
    Simple global throttle to avoid hitting SEC too fast.
    """
    global _last_call
    now = time.monotonic()
    wait = _MIN_DELAY_BETWEEN_CALLS - (now - _last_call)
    if wait > 0:
        time.sleep(wait)
    _last_call = time.monotonic()


def quarter_of(month: int) -> int:
    return (month - 1) // 3 + 1 #SEC quarter (1-4)


def build_daily_index_urls(date: dt.date) -> List[str]:
    """
    This is kept for backwards compatibility
    EDGAR daily master index
    """
    y, q = date.year, quarter_of(date.month)
    ymd = date.strftime("%Y%m%d")
    base = f"{_SEC_BASE}/Archives/edgar/daily-index/{y}/QTR{q}/master.{ymd}.idx"
    return [base, base + ".gz"]


class SecClient:
    """
    Core SEC HTTP client
     - adds the right headers once
     - centralizes retry
     - reuses a single session for connection pooling
    """

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.s = session or requests.Session()
        self.s.headers.update(
            {
                "User-Agent": _UA,
                "Accept-Encoding": "gzip, deflate",
                "Referer": _REFERER,
            }
        )

    # All HTTP traffic to SEC should go through this method so we have
    # a single place to tweak rate limits and retry behavior.
    def _get(self, url: str) -> requests.Response:
        for i, backoff in enumerate([0.0] + _BACKOFFS):
            _throttle()
            try:
                r = self.s.get(url, timeout=_REQ_TIMEOUT)
                # Retry on 429/5xx
                if r.status_code in (429, 500, 502, 503, 504):
                    if i == len(_BACKOFFS):
                        r.raise_for_status()
                    time.sleep(backoff)
                    continue
                r.raise_for_status()
                return r
            except requests.RequestException:
                if i == len(_BACKOFFS):
                    raise
                time.sleep(backoff)
        raise RuntimeError("unreachable")

    # ---------------------------------------------------------------------
    # 1) Daily index (kept for compatibility, not used for 10-K/10-Q now)
    # ---------------------------------------------------------------------
    def fetch_daily_index_text(self, date: dt.date) -> Optional[str]:
        for url in build_daily_index_urls(date):
            try:
                r = self._get(url)
                content = r.content
                is_gz = url.endswith(".gz") or (
                    len(content) >= 2 and content[:2] == b"\x1f\x8b"
                )
                if is_gz:
                    with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                        return gz.read().decode("utf-8", errors="replace")
                return r.text
            except Exception:
                # try the next URL (.gz or plain)
                continue
        return None

    # ---------------------------------------------------------------------
    # 2) Submissions JSON API (used for per-company 10-K/10-Q)
    # ---------------------------------------------------------------------
    def fetch_submissions_json(self, cik: str) -> Dict[str, Any]:
        """
        Fetch the SEC submissions JSON for a single company.

        Uses:
                https://data.sec.gov/submissions/CIK##########.json

        """
        cik_padded = str(int(cik)).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
        r = self._get(url)
        return r.json()

    # ---------------------------------------------------------------------
    # 3) Helpers for finding XML/HTML filings in an accession folder
    # ---------------------------------------------------------------------
    def extract_xml_html_from_headers_page(
        self, cik: str, accession_dash: str, timeout: int = 30
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Given a CIK and an accession number with dashes, locate the primary
        XML and HTML filing URLs under the /Archives/edgar/data/... directory.
        """
        # SEC folder path uses unpadded CIK plus accession without dashes.
        cik_num = str(int(cik))  # unpadded
        accession_number = accession_dash.replace("-", "")
        base_dir = f"{_SEC_BASE}/Archives/edgar/data/{cik_num}/{accession_number}/"
        url = f"{base_dir}{accession_dash}-index-headers.html"

        r = self._get(url)
        content = r.text

        # If <TEXT>...</TEXT> wrapping is present, narrow the soup to that section.
        # This reduces noise from the rest of the HTML.
        m = re.search(r"<TEXT\b[^>]*>(.*?)</TEXT>", content, flags=re.I | re.S)
        fragment = m.group(1) if m else content

        soup = BeautifulSoup(fragment, "html.parser")
        xml_url = html_url = None

        # Strategy:
        # - Iterate over all <a> tags.
        # - Use the link text to detect *.xml and *.htm(l).
        # - Build absolute URLs, bail out once we have both.
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            abs_url = urljoin(url, href)
            text = (a.get_text() or "").strip().lower()
            # Case 1: link text says ".xml"
            if xml_url is None and text.endswith(".xml"):
                xml_url = abs_url
            # Case 2: link text says ".htm" or ".html"
            if html_url is None and (text.endswith(".htm") or text.endswith(".html")):
                html_url = abs_url
            if xml_url and html_url:
                break
        return xml_url, html_url

    def enrich_rows_with_xml_html(self, rows):
        """
        rows: List[Tuple[company, form, cik, datefiled, filename]]
        returns: List[Tuple[company, form, cik, datefiled, filename, xml_url, html_url]]
        """
        out = []
        for company, form, cik, datefiled, filename in rows:
            try:
                parts = filename.split("/")
                cik_from_path = parts[2]
                accession = parts[3].replace(".txt", "")
                xml_url, html_url = self.extract_xml_html_from_headers_page(
                    cik_from_path, accession
                )
            except Exception:
                xml_url = html_url = None
            out.append((company, form, cik, datefiled, filename, xml_url, html_url))
        return out

    # ---------------------------------------------------------------------
    # 4) Misc helpers reused elsewhere
    # ---------------------------------------------------------------------
    def fetch_text(self, url: str) -> str:
        return self._get(url).text


    @staticmethod
    def _accession_with_dashes(accession_nodash: str) -> str:
        """000164085225000004 -> 0001640852-25-000004"""
        s = "".join(ch for ch in accession_nodash if ch.isdigit())
        if len(s) < 18:  # guard
            return accession_nodash
        return f"{s[:10]}-{s[10:12]}-{s[12:]}"  # 10-2-6
