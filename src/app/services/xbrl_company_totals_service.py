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

############################################################
# Overview
#
# This module extracts company-wide consolidated totals from
# SEC XBRL instance documents (10-Q / 10-K).
#
# XBRL filings contain many contexts (periods, segments, products,
# geographies, ...). To get clean consolidated totals
# (Revenue, Net Income, Assets…), we must:
#
#   - Parse all <context> definitions
#   - Keep only contexts with no dimensions (true consolidated data)
#   - Match the context’s endDate/instant to the filing’s
#     dei:DocumentPeriodEndDate (the main reporting period)
#   - Collect only facts tied to those contexts
#
# This module does exactly that and outputs simple FactRow objects
# for downstream processing.
#
# Main functions:
#   parse_contexts()
#   extract_company_totals_for_main_period()
#   print_by_context()   # debug helper
############################################################


@dataclass
class ContextInfo:
    """
    Represents a single <xbrli:context>.

    Contains:
        - period (start/end or instant)
        - dimensional qualifiers (optional)
        - unique context id
    """
    id: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    instant: Optional[date] = None
    dims: Dict[str, str] = field(default_factory=dict)


@dataclass
class FactRow:
    """
    Represents a single extracted fact belonging to a
    consolidated main-period context.
    """
    ticker: str
    filing_date: date
    context_id: str
    concept: str          # "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
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
    Read all <xbrli:context> elements and convert them to ContextInfo.
    """
    contexts: Dict[str, ContextInfo] = {}

    # Find every context in the instance document.
    for ctx in root.findall(f".//{{{XBRLI_NS}}}context"):
        ctx_id = ctx.attrib.get("id")
        if not ctx_id:
            continue

        info = ContextInfo(id=ctx_id)

        # period
        period = ctx.find(f"{{{XBRLI_NS}}}period")
        if period is not None:
            start_el = period.find(f"{{{XBRLI_NS}}}startDate")
            end_el = period.find(f"{{{XBRLI_NS}}}endDate")
            inst_el = period.find(f"{{{XBRLI_NS}}}instant")

            # Instance period (single-day fact)
            if inst_el is not None:
                info.instant = _parse_date(inst_el.text)
            # Duration period (start/end)
            else:
                info.start_date = _parse_date(start_el.text if start_el is not None else None)
                info.end_date = _parse_date(end_el.text if end_el is not None else None)

        # dimensions (/entity/segment/explicitMember)
        segment = ctx.find(f"{{{XBRLI_NS}}}entity/{{{XBRLI_NS}}}segment")
        if segment is not None:
            for mem in segment.findall(f".//{{{XBRLDI_NS}}}explicitMember"):
                dim = mem.attrib.get("dimension")  # "srt:ProductOrServiceAxis"
                member = (mem.text or "").strip()  # "us-gaap:ServiceOtherMember"
                if dim and member:
                    info.dims[dim] = member

        contexts[ctx_id] = info

    return contexts


def get_document_period_end(root: ET.Element) -> Optional[date]:
    """
    Locate the dei:DocumentPeriodEndDate fact.
    This defines the main reporting period.
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
    Company-level totals have NO dimensions.
    Dimensions indicate segment-level reporting.
    """
    return not ctx.dims


def _concept_name(el: ET.Element) -> str:
    """
       Convert raw XML tag into a human-readable concept:
           "{http://fasb.org/us-gaap/2025}Revenue" -> "us-gaap:Revenue"
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
    """"
    Extract consolidated totals for the main reporting period.

    Conditions for selecting a fact:
        - Must reference a valid contextRef
        - Context must have NO dimensions
        - Context period must match DocumentPeriodEndDate
        - Fact must have non-empty value
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
    """
    Group extracted facts by context_id.
    """
    grouped = defaultdict(list)
    for r in rows:
        grouped[r.context_id].append(r)
    return grouped


def print_by_context(rows: List[FactRow], contexts: Dict[str, ContextInfo], max_facts_per_ctx: int = 30) -> None:
    """
    Pretty-print extracted facts grouped by context.
    Useful for sanity-checking parsed data.
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