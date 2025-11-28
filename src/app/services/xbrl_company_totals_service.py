"""
xbrl_company_totals_service.py
--------------------------------

This module provides tools for parsing SEC XBRL **instance documents** for 10-Q/10-K filings
and extracting *company-level consolidated totals*.

XBRL filings include hundreds–thousands of "facts". Each fact belongs to a “context”
that determines:

  • The reporting period (startDate/endDate or instant)
  • Dimensional qualifiers (“segments”), e.g.:
        - us-gaap:StatementBusinessSegmentsAxis
        - srt:ProductOrServiceAxis
        - srt:ConsolidationItemsAxis

To retrieve *consolidated company totals*, we typically must:

  - Select contexts WITHOUT any dimensions
    (because dimensions break totals into segments)

  - Match the context's end/instant date to dei:DocumentPeriodEndDate
    (the primary period for the filing)

This module handles all the above and outputs clean FactRow objects.

You will most commonly use:
    parse_contexts()
    extract_company_totals_for_main_period()
    print_by_context()   (debug helper)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Dict, List, Iterable
import xml.etree.ElementTree as ET
from collections import defaultdict

# XBRL base namespaces
XBRLI_NS = "http://www.xbrl.org/2003/instance"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"

# We normalize namespaces → short aliases for easier reading.
NS_ALIASES: Dict[str, str] = {
    "http://fasb.org/us-gaap/2025": "us-gaap",
    "http://fasb.org/us-gaap/2024": "us-gaap",
    "http://xbrl.sec.gov/dei/2025": "dei",
    "http://xbrl.sec.gov/dei/2024": "dei",
    "http://xbrl.sec.gov/dei/2023": "dei",
    "http://xbrl.sec.gov/dei/2022": "dei",
    "http://xbrl.sec.gov/country/2025": "country",
    "http://xbrl.org/2006/xbrldi": "xbrldi",
    "http://www.xbrl.org/2003/instance": "xbrl",
}


@dataclass
class ContextInfo:
    """
    Represents one <xbrli:context> in the XBRL instance.

    A context determines:
        • Reporting period (start/end OR instant)
        • Dimensional breakdown (if present)
        • Entity (CIK is always the same in SEC filings)

    Example context:

        <context id="c-3">
            <entity>
                <identifier>0001234567</identifier>
                <segment>
                    <xbrldi:explicitMember dimension="srt:ProductOrServiceAxis">
                        us-gaap:ProductMember
                    </xbrldi:explicitMember>
                </segment>
            </entity>
            <period>
                <startDate>2025-05-01</startDate>
                <endDate>2025-07-31</endDate>
            </period>
        </context>
    """
    id: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    instant: Optional[date] = None
    # dimension -> member, e.g. "srt:ProductOrServiceAxis" -> "us-gaap:ServiceOtherMember"
    dims: Dict[str, str] = field(default_factory=dict)


@dataclass
class FactRow:
    """
       Represents one flattened XBRL fact.

       Example:
           us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax
           value=1230000000
           context=c-11
           period_end=2025-07-31
       """
    ticker: str
    filing_date: date
    context_id: str
    concept: str          # e.g. "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
    value: str
    period_start: Optional[date]
    period_end: Optional[date]
    instant: Optional[date]


def _split_tag(tag: str) -> tuple[Optional[str], str]:
    """
    Split '{namespace}localname' into (namespace, localname).
    If no namespace, returns (None, tag).
    """
    if tag and tag[0] == "{":
        uri, local = tag[1:].split("}", 1)
        return uri, local
    return None, tag


def _parse_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    return datetime.strptime(d, "%Y-%m-%d").date()


def parse_contexts(root: ET.Element) -> Dict[str, ContextInfo]:
    """
    Build a mapping context_id -> ContextInfo from <xbrli:context> elements.
    """
    contexts: Dict[str, ContextInfo] = {}

    for ctx in root.findall(f".//{{{XBRLI_NS}}}context"):
        ctx_id = ctx.attrib.get("id")
        if not ctx_id:
            continue

        info = ContextInfo(id=ctx_id)

        # --- period ---
        period = ctx.find(f"{{{XBRLI_NS}}}period")
        if period is not None:
            start_el = period.find(f"{{{XBRLI_NS}}}startDate")
            end_el = period.find(f"{{{XBRLI_NS}}}endDate")
            inst_el = period.find(f"{{{XBRLI_NS}}}instant")

            if inst_el is not None:
                info.instant = _parse_date(inst_el.text)
            else:
                info.start_date = _parse_date(start_el.text if start_el is not None else None)
                info.end_date = _parse_date(end_el.text if end_el is not None else None)

        # --- dimensions (/entity/segment/explicitMember) ---
        segment = ctx.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}segment")
        if segment is not None:
            for mem in segment.findall(f".//{{{XBRLDI_NS}}}explicitMember"):
                dim = mem.attrib.get("dimension")  # e.g. "srt:ProductOrServiceAxis"
                member = (mem.text or "").strip()  # e.g. "us-gaap:ServiceOtherMember"
                if dim and member:
                    info.dims[dim] = member

        contexts[ctx_id] = info

    return contexts


def get_document_period_end(root: ET.Element) -> Optional[date]:
    """
    Look up dei:DocumentPeriodEndDate.

    This identifies the "primary" reporting period of the filing —
    crucial for filtering out irrelevant contexts (historical, segment-level, etc.).
    """
    target_local = "DocumentPeriodEndDate"
    for el in root.iter():
        uri, local = _split_tag(el.tag)
        if local != target_local:
            continue

        txt = (el.text or "").strip()
        if not txt:
            continue
        try:
            return date.fromisoformat(txt)
        except ValueError:
            continue
    return None


def is_company_total_context(ctx: ContextInfo) -> bool:
    """
    Heuristic:
        A context represents *company-wide consolidated totals*
        if it has **no dimensions**.

    When dimensions ARE present, they split data into segments like:
        - Business segments
        - Product lines
        - Geographies
        - Consolidation/Elimination adjustments

    For consolidated totals, all dims must be empty.
    """
    return not ctx.dims


def _concept_name(el: ET.Element) -> str:
    """
       Convert raw XML tag into a human-readable concept:
           "{http://fasb.org/us-gaap/2025}Revenue" → "us-gaap:Revenue"
    """
    uri, local = _split_tag(el.tag)
    if not uri:
        return local
    prefix = NS_ALIASES.get(uri, uri.split("/")[-1])
    return f"{prefix}:{local}" if prefix else local


def extract_company_totals_for_main_period(
    root: ET.Element,
    contexts: Dict[str, ContextInfo],
    ticker: str,
    filing_date: date,
    limit: int = 300,
) -> List[FactRow]:
    """
    Extract consolidated company-wide totals for the document's primary reporting period.

    Selection criteria:

        (1) Element has a valid contextRef
        (2) context has NO dimensions (i.e., consolidated totals)
        (3) context's endDate or instant == DocumentPeriodEndDate
        (4) Value exists & is not empty
        (5) Concept is not an explicitMember or dimensional metadata

    Parameters
    ----------
    root : XML root element
    contexts : dict of parsed ContextInfo
    ticker : stock ticker symbol
    filing_date : official filing date of the form
    limit : maximum number of extracted facts (to prevent flooding output)

    Returns
    -------
    List[FactRow]
    """
    doc_end = get_document_period_end(root)
    rows: List[FactRow] = []

    for el in root.iter():
        # Must have contextRef attribute
        ctx_id = el.attrib.get("contextRef")
        if not ctx_id:
            continue

        # Skip explicitMember and other pure-dimensional elements
        if XBRLDI_NS in el.tag:
            continue

        # Must have a text value
        txt = (el.text or "").strip()
        if not txt:
            continue

        # Context must exist
        ctx = contexts.get(ctx_id)
        if not ctx:
            continue

        # Only company totals (no dimensions)
        if not is_company_total_context(ctx):
            continue

        # Restrict to main reporting period (if we could find it)
        if doc_end is not None:
            end_or_inst = ctx.end_date or ctx.instant
            if end_or_inst != doc_end:
                continue

        concept = _concept_name(el)

        rows.append(
            FactRow(
                ticker=ticker,
                filing_date=filing_date,
                context_id=ctx.id,
                concept=concept,
                value=txt,
                period_start=ctx.start_date,
                period_end=ctx.end_date,
                instant=ctx.instant,
            )
        )

        if len(rows) >= limit:
            break

    return rows


# ---------------------- DEBUG / INSPECTION HELPERS ----------------------


def _group_rows_by_context(rows: Iterable[FactRow]):
    """Group extracted facts by context_id."""
    grouped = defaultdict(list)
    for r in rows:
        grouped[r.context_id].append(r)
    return grouped


def print_by_context(rows: List[FactRow], contexts: Dict[str, ContextInfo], max_facts_per_ctx: int = 30) -> None:
    """
    Pretty-print grouped facts for debugging.
    Shows:
        - context start/end/instant
        - dimensions (if any)
        - truncated fact values

    This is a diagnostic helper — not meant for production output.
    """
    grouped = _group_rows_by_context(rows)

    for ctx_id, facts in sorted(grouped.items()):
        ctx = contexts.get(ctx_id)

        if ctx is None:
            start = end = inst = None
            dims_dict: Dict[str, str] = {}
        else:
            start = ctx.start_date
            end = ctx.end_date
            inst = ctx.instant
            dims_dict = ctx.dims

        print(f"\n=== CONTEXT {ctx_id} ===")
        print(f"  start={start}  end={end}  instant={inst}")

        if dims_dict:
            dim_str = "; ".join(f"{dim}={member}" for dim, member in dims_dict.items())
        else:
            dim_str = "(no dimensions)"
        print(f"  dims: {dim_str}")
        print("  facts:")

        for r in facts[:max_facts_per_ctx]:
            v = r.value
            if len(v) > 50:
                v = v[:47] + "..."
            print(f"    {r.concept:80} {v:>15}")

        if len(facts) > max_facts_per_ctx:
            print(f"    ... ({len(facts) - max_facts_per_ctx} more)")


    return