from __future__ import annotations
import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# ticker -> bing_metric -> sec_concept -> list[relative_error]

load_dotenv()

BACKEND_ROOT = Path(__file__).resolve().parent
SRC_ROOT = BACKEND_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from app.clients.sec_client import SecClient  # type: ignore
from app.services.submissions_10x_service import Filing10X  # type: ignore
from src.app.services.filing_download_service import find_instance_xbrl_url  # type: ignore
from src.app.services.xbrl_company_totals_service import (  # type: ignore
    parse_contexts,
    extract_company_totals_for_main_period,
    print_by_context,
)
EvidenceType = Dict[str, Dict[str, Dict[str, List[float]]]]

############################################################
# Overview
#
# This script is a top-level tool to learn a mapping between:
#
#   Bing scraped metrics  <->  SEC XBRL concepts
#
# for each company (ticker).
#
# Idea:
#   - For many 10-Q filings:
#       - Download the XBRL instance from EDGAR
#       - Extract company-wide totals for the main reporting period
#       - Load the matching quarter from our Bing financials JSON
#       - Compare SEC vs Bing numbers and collect "evidence" whenever
#         two values match within some tolerance
#
#   - After looping all filings, the accumulated evidence lets us
#     infer, per ticker, which Bing metric name most likely corresponds
#     to which SEC concept (e.g. "Total revenue" = "us-gaap:Revenues").
#
# Why this exists:
#   Every company uses slightly different naming conventions on Bing,
#   and XBRL tagging is flexible. This code is a practical, data-driven
#   way to figure out "what means what" per company, not an elegant
#   universal solution. It relies on numeric alignment, not semantics.
#
#   - It works as of today (11/30/2025), but may break if either side changes
#     their format or conventions.
############################################################
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_submissions_json(path: Path) -> Dict[str, Any]:
    """
    Load the aggregated 10x_submissions.json file (master 10-K/10-Q dataset).
    """
    if not path.exists():
        raise FileNotFoundError(f"Submissions file not found at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def pick_10q_filings(payload: Dict[str, Any], limit: int = 10) -> List[Filing10X]:
    """
    From the master submissions payload, pick up to `limit` 10-Q filings
    and convert them into Filing10X objects.

    This is our working set for building the Bing–SEC mapping.
    """
    filings = payload.get("filings", [])
    out = []

    for f in filings:
        if len(out) >= limit:
            break

        form = f.get("form") or f.get("form_type")
        if not (form and form.startswith("10-Q")):
            continue

        filing_date_str = f.get("filing_date")
        filing_date = (
            dt.datetime.strptime(filing_date_str, "%Y-%m-%d").date()
            if filing_date_str else dt.date.today()
        )

        out.append(
            Filing10X(
                ticker=f["ticker"],
                cik=f["cik"],
                form=form,
                accession_number=f["accession_number"],
                primary_document=f["primary_document"],
                filing_date=filing_date,
            )
        )

    return out


def _local_name(tag: str) -> str:
    """
    '{ns}Name' -> 'Name'.
    """
    if tag.startswith("{"):
        return tag.split("}", 1)[1]
    return tag

def build_company_sec_mapping(
    evidence: Dict[str, Dict[str, Dict[str, List[Any]]]],
    min_hits: int = 1,
    max_mean_err: float = 0.05,
) -> Dict[str, Dict[str, str]]:
    """
    Build final mapping: ticker -> { BingMetric -> SECConcept }.

    Less strict version:
      - For each (ticker, bing_metric) we compute stats per SEC concept:
          - hits = number of quarters where we saw that match
          - mean_err = average relative error
      - We PREFER candidates with hits >= min_hits and mean_err <= max_mean_err.
      - BUT if none satisfy these thresholds, we still keep the best candidate
        overall (highest hits, then lowest mean_err).

    """
    mapping: Dict[str, Dict[str, str]] = {}

    for ticker, metrics in evidence.items():
        ticker_map: Dict[str, str] = {}

        for bing_metric, sec_candidates in metrics.items():
            stats: List[Tuple[str, int, float]] = []  # (sec_concept, hits, mean_err)

            for sec_concept, hits in sec_candidates.items():
                if not hits:
                    continue

                # hits can be a list of floats or list of dicts with "rel_err"
                first = hits[0]
                if isinstance(first, dict):
                    # list of dicts -> use h["rel_err"]
                    errs = [float(h.get("rel_err", 0.0)) for h in hits]
                else:
                    # list of floats (or numbers) -> use directly
                    errs = [float(h) for h in hits]

                if not errs:
                    continue

                mean_err = sum(errs) / len(errs)
                stats.append((sec_concept, len(hits), mean_err))

            if not stats:
                continue

            # Best overall: more hits first, then lower error
            stats.sort(key=lambda x: (-x[1], x[2]))

            # Preferred = satisfy thresholds
            preferred = [
                s for s in stats
                if s[1] >= min_hits and s[2] <= max_mean_err
            ]

            if preferred:
                best_concept, best_hits, best_err = preferred[0]
            else:
                # Fallback: still take the best overall candidate
                best_concept, best_hits, best_err = stats[0]

            ticker_map[bing_metric] = best_concept

        if ticker_map:
            mapping[ticker] = ticker_map

    return mapping

def find_single_dei_fact(root: ET.Element, local_name: str) -> Optional[str]:
    """
    Return the (first) text value of a DEI fact with given local name,
    e.g. 'DocumentFiscalPeriodFocus', 'DocumentFiscalYearFocus', ...
    """
    for el in root.iter():
        if _local_name(el.tag) == local_name:
            txt = (el.text or "").strip()
            if txt:
                return txt
    return None


def get_document_meta(root: ET.Element) -> Dict[str, Any]:
    """
    Extract some useful DEI meta we need to align with Bing:
      - DocumentPeriodEndDate
      - DocumentFiscalYearFocus
      - DocumentFiscalPeriodFocus (Q1..Q4)
      - DocumentType
      - AmendmentFlag
    """
    meta: Dict[str, Any] = {}
    meta["period_end"] = find_single_dei_fact(root, "DocumentPeriodEndDate")
    meta["fiscal_year"] = find_single_dei_fact(root, "DocumentFiscalYearFocus")
    meta["fiscal_period"] = find_single_dei_fact(root, "DocumentFiscalPeriodFocus")
    meta["document_type"] = find_single_dei_fact(root, "DocumentType")
    meta["amendment_flag"] = find_single_dei_fact(root, "AmendmentFlag")
    return meta


def load_bing_financials_for_ticker(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Load our pre-scraped Bing JSON for this ticker, or None if missing.
    """
    path = BACKEND_ROOT / "data" / "bing_financials" / f"{ticker}.json"
    if not path.exists():
        logging.warning("No Bing financials JSON for %s at %s", ticker, path)
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def parse_bing_number(s: str) -> Optional[float]:
    """
    Convert strings like '1.86B', '782.00M', '294.00K', '1234', '-' to a float in dollars.
    Returns None if parsing fails / it's clearly not a numeric cell.
    """
    if not s:
        return None
    s = s.replace(",", "").strip()
    if s in {"-", "—"}:
        return None

    multiplier = 1.0
    if s[-1] in {"B", "M", "K"}:
        suffix = s[-1]
        num_part = s[:-1]
        if suffix == "B":
            multiplier = 1_000_000_000
        elif suffix == "M":
            multiplier = 1_000_000
        elif suffix == "K":
            multiplier = 1_000
    else:
        num_part = s

    try:
        return float(num_part) * multiplier
    except ValueError:
        return None


def parse_sec_number(s: str) -> Optional[float]:
    """
    Very simple SEC numeric parser – XBRL facts in dollars, possibly with
    decimals or minus sign.
    """
    if not s:
        return None
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def build_sec_numeric_map(rows: List[Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    """
    Build mapping: SEC_concept -> numeric_value from FactRow-like objects.

    Works with:
      - dicts containing "value" and "concept"
      - dataclass/objects with .value and .concept attributes
    """
    for r in rows:
        # get value as string
        if isinstance(r, dict):
            val_str = str(r.get("value", "")).strip()
            concept = str(r.get("concept", "")).strip()
        else:
            # Assume dataclass / object with attributes
            val_str = str(getattr(r, "value", "")).strip()
            concept = str(getattr(r, "concept", "")).strip()

        if not concept:
            continue

        v = parse_sec_number(val_str)
        if v is None:
            continue

        out[concept] = v

    return out


def get_bing_periods(bing: Dict[str, Any]) -> List[str]:
    """
    Try to get the 'periods' array.

    - Preferred: top-level 'periods'
    - Fallback: inside income_statement / balance_sheet / cash_flow
    """
    if isinstance(bing.get("periods"), list):
        return bing["periods"]

    for key in ("income_statement", "balance_sheet", "cash_flow"):
        stmt = bing.get(key)
        if isinstance(stmt, dict) and isinstance(stmt.get("periods"), list):
            return stmt["periods"]

    return []


def get_bing_all_metrics(bing: Dict[str, Any]) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    """
    Return (periods, metrics) where metrics is a single dict that merges:

      - income_statement
      - balance_sheet
      - cash_flow

    Shape of 'metrics':
        {
          "Revenue": {"Oct 2025 (FQ4)": "1.86B", ...},
          "Net Profit": {...},
          "Current Assets": {...},
          "Free Cash Flow": {...},
          ...
        }
    """
    periods = get_bing_periods(bing)
    merged: Dict[str, Dict[str, str]] = {}

    for stmt_key in ("income_statement", "balance_sheet", "cash_flow"):
        stmt = bing.get(stmt_key)
        if not isinstance(stmt, dict):
            continue

        # Some scrapers stored as {"metrics": {...}}, some as plain dict.
        if isinstance(stmt.get("metrics"), dict):
            source = stmt["metrics"]
        else:
            source = stmt

        for raw_name, per_vals in source.items():
            if not isinstance(per_vals, dict):
                continue  # skip metadata fields etc.

            name = raw_name
            if name in merged:
                # avoid collisions: Revenue -> Revenue (income_statement) etc.
                name = f"{raw_name} ({stmt_key})"
                if name in merged:
                    # very unlikely, but just in case
                    suffix_n = 2
                    while f"{raw_name} ({stmt_key}) #{suffix_n}" in merged:
                        suffix_n += 1
                    name = f"{raw_name} ({stmt_key}) #{suffix_n}"

            merged[name] = per_vals

    return periods, merged


def pick_bing_period_index(periods: List[str], fiscal_year: str, fiscal_period: str) -> Optional[int]:
    """
    MSN uses labels like Jul 2025 (FQ3).
    We have DocumentFiscalYearFocus='2025' and DocumentFiscalPeriodFocus='Q3'.
    So we look for a label that ends with '2025 (FQ3)'.
    """
    if not fiscal_year or not fiscal_period or not periods:
        return None

    suffix = f"{fiscal_year} (F{fiscal_period})"  # e.g. "2025 (FQ3)"
    for idx, label in enumerate(periods):
        if label.endswith(suffix):
            return idx
    return None


def build_bing_column_values(
    periods: List[str],
    metrics: Dict[str, Dict[str, str]],
    period_index: int,
) -> Dict[str, float]:
    """
    For a chosen Bing column (single quarter) build metric_name -> numeric_value_in_dollars.
    """
    if period_index < 0 or period_index >= len(periods):
        return {}

    period_label = periods[period_index]
    out: Dict[str, float] = {}

    for metric_name, per_dict in metrics.items():
        if not isinstance(per_dict, dict):
            continue
        raw_val = per_dict.get(period_label)
        if not raw_val:
            continue
        v = parse_bing_number(raw_val)
        if v is None:
            continue
        out[metric_name] = v

    return out


def find_close_matches_stepup(
    sec_value: float,
    bing_values: Dict[str, float],
    max_tol: float = 0.02,
    step: float = 0.005,
) -> List[Tuple[str, float, float]]:
    """
    Try matching SEC value to Bing values using increasing relative tolerances.

    Steps:
        0.5% → 1.0% → ... → 2.0%

    Returns matches sorted by smallest relative error.
    """
    if sec_value == 0:
        return []

    tolerances = [i * step for i in range(1, int(max_tol / step) + 1)]
    # Example: if max_tol=0.02 and step=0.005 → [0.005,0.01,0.015,0.02]

    for tol in tolerances:
        matches = []
        for mname, bval in bing_values.items():
            if bval == 0:
                continue
            rel_err = abs(bval - sec_value) / abs(sec_value)
            if rel_err <= tol:
                matches.append((mname, bval, rel_err))

        if matches:
            matches.sort(key=lambda x: x[2])
            return matches

    # no tolerance level produced a match
    return []


def compare_sec_with_bing(
    ticker: str,
    rows: List[Dict[str, Any]],
    root: ET.Element,
    evidence: Optional[EvidenceType] = None,
) -> str:
    """
    High-level:
      1. Read DEI meta to know which fiscal year + quarter this 10-Q is.
      2. Load Bing JSON for ticker, pick matching column.
      3. Parse SEC and Bing numbers.
      4. For top N SEC facts (by absolute magnitude), try to find matching
         Bing metrics and print a simple comparison.
    """
    import io
    buffer = io.StringIO()

    def w(s=""):
        buffer.write(s + "\n")
    meta = get_document_meta(root)
    fiscal_year = meta.get("fiscal_year")
    fiscal_period = meta.get("fiscal_period")
    w(f"SEC filing metadata: FY={fiscal_year} FP={fiscal_period}")
    if not (fiscal_year and fiscal_period and fiscal_period.startswith("Q")):
        w("Cannot align SEC period with Bing periods.")
        return buffer.getvalue()

    bing = load_bing_financials_for_ticker(ticker)
    if not bing:
        w("No Bing JSON found.")
        return buffer.getvalue()

    periods, metrics = get_bing_all_metrics(bing)
    # periods: ['Oct 2025 (FQ4)', ...]
    # metrics: Revenue, Net Profit, Current Assets, Free Cash Flow, ...

    p_idx = pick_bing_period_index(periods, fiscal_year, fiscal_period)
    if p_idx is None:
        w("Could not match period.")
        return buffer.getvalue()

    period_label = periods[p_idx]
    w(f"Matched Bing period column: {period_label}")

    bing_col_values = build_bing_column_values(periods, metrics, p_idx)
    sec_map = build_sec_numeric_map(rows)
    # Sort SEC concepts by absolute value, largest first (most informative)
    sec_items = sorted(sec_map.items(), key=lambda kv: abs(kv[1]), reverse=True)
    all_matches: List[Tuple[float, str, float, str, float]] = []

    for concept, sec_val in sec_items:
        matches = find_close_matches_stepup(sec_val, bing_col_values)
        if not matches:
            continue
        for mname, bval, err in matches:
            all_matches.append((err, concept, sec_val, mname, bval))

    # Sort by lowest relative error
    all_matches.sort(key=lambda x: x[0])

    if evidence is not None:
        t_entry = evidence.setdefault(ticker, {})
        for err, concept, _, mname, _ in all_matches:
            m_entry = t_entry.setdefault(mname, {})
            m_entry.setdefault(concept, []).append(err)

    w("\n=== Matches (sorted by lowest error) ===\n")
    for err, concept, sec_val, mname, bval in all_matches:
        w(f"{concept:60} SEC={sec_val:,.0f}  Bing={mname}:{bval:,.0f}  err={err * 100:4.2f}%")

    return buffer.getvalue()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

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
    filings = pick_10q_filings(payload, limit=10313)
    if not filings:
        logging.error("No 10-Q filings found.")
        return 1

    client = SecClient()
    # TXT output file
    out_dir = BACKEND_ROOT / "data" / "sec_bing_compare"
    out_dir.mkdir(parents=True, exist_ok=True)
    txt_path = out_dir / f"compare_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    # Global evidence accumulator across all filings and tickers
    evidence: EvidenceType = {}
    total = len(filings)
    with txt_path.open("w", encoding="utf-8") as fout:
        for idx, filing in enumerate(filings, start=1):
            ticker = filing.ticker
            fout.write("\n\n===========================================\n")
            fout.write(f" TICKER: {ticker} | CIK: {filing.cik} | FORM: {filing.form}\n")
            fout.write("===========================================\n\n")

            logging.info("Processing %s %s (%d/%d)", ticker, filing.form, idx, total)

            try:

                instance_url = find_instance_xbrl_url(client, filing)

                if not instance_url:
                    logging.error("No XBRL found for %s", ticker)
                    fout.write(f"No XBRL found for {ticker}\n")
                    continue

                text = client.fetch_text(instance_url)

                # Save raw XBRL for manual inspection
                target_dir = BACKEND_ROOT / "data" / "10x_raw_xbrl" / ticker.upper()
                target_dir.mkdir(parents=True, exist_ok=True)
                filename = f"{filing.ticker.upper()}_{filing.accession_number}_{filing.form}_instance.xml"
                out_path = target_dir / filename
                out_path.write_text(text, encoding="utf-8", errors="ignore")

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
                # capture comparison output
                sec_vs_bing_text = compare_sec_with_bing(
                    ticker,
                    rows,
                    root,
                    evidence=evidence,  # <--- NEW
                )

                fout.write(sec_vs_bing_text)
                fout.write("\n")

            except Exception as exc:
                # Protect the main loop so one bad filing doesn't kill the run.
                logging.exception("Error processing %s %s", ticker, filing.form)
                fout.write(f"ERROR processing {ticker}: {exc}\n")
                fout.write("Skipping this filing.\n\n")
                continue

    # After processing all filings, build the final per-ticker mapping
    mapping = build_company_sec_mapping(
        evidence,
        min_hits=1,  # require at least 2 quarters
        max_mean_err=0.4  # <= 2% average relative error
    )

    mapping_path = out_dir / "sec_bing_mapping.json"
    mapping_path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")
    logging.info("Finished. Results saved to %s", txt_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
