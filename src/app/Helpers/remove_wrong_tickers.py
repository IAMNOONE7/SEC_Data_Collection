from __future__ import annotations
import json
import logging
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[3]

############################################################
# Overview
#
# This script rebuilds the master `companies.json` file by
# validating which tickers actually have associated scraped data
# and removing entries that are incomplete or duplicated.
#
# Purpose:
#   - Ensure `companies.json` contains only tickers for which
#     we have real `bing_financials/<TICKER>.json` files.
#   - Remove duplicate companies that share the same CIK.
#   - Preserve metadata fields (`schema_version`, `as_of`).
#
# High-level flow:
#   1. Load the existing `companies.json`.
#   2. Detect which tickers have corresponding JSON data scraped.
#   3. Filter the company list:
#        - Drop tickers without scraped financials.
#        - Drop duplicates based on CIK (first entry wins).
#   4. Re-save the cleaned list back into `companies.json`.
#   5. Print a human-readable summary for debugging.
#
# Typical use:
#   This script is run after scraping financial data from Bing.
#   It guarantees consistency between the scraped ticker files
#   and the master company list used by other backend components.
############################################################




def build_companies_updated() -> Path:
    companies_path = BACKEND_ROOT / "companies.json"
    data = json.loads(companies_path.read_text(encoding="utf-8"))
    original_companies = data.get("companies", [])

    # All [ticker].json files we actually have
    bing_dir = BACKEND_ROOT / "data" / "bing_financials"
    bing_dir.mkdir(parents=True, exist_ok=True)
    # Determine which ticker-level scraped JSON files actually exist.
    existing_ticker_files = {p.stem for p in bing_dir.glob("*.json")}

    kept_companies = []
    seen_ciks: set[str] = set()         # Track CIKs to deduplicate

    dropped_missing: list[str] = []     # no [ticker].json
    dropped_duplicates: list[str] = []  # same CIK as earlier ticker

    for comp in original_companies:
        ticker = comp.get("ticker")
        cik = (comp.get("cik") or "").strip()

        if not ticker:
            continue

        # 1) Drop tickers with no scraped bing_financials/<TICKER>.json
        if ticker not in existing_ticker_files:
            dropped_missing.append(ticker)
            continue

        # 2) Deduplicate by CIK – keep the FIRST occurrence
        if cik and cik in seen_ciks:
            dropped_duplicates.append(ticker)
            continue

        # Passed all checks → keep the company.
        kept_companies.append(comp)
        if cik:
            seen_ciks.add(cik)

    # Build updated structure (keep schema_version/as_of)
    updated = {
        "schema_version": data.get("schema_version", "1.0"),
        "as_of": data.get("as_of"),
        "companies": kept_companies,
    }

    out_path = BACKEND_ROOT / "companies.json"
    out_path.write_text(json.dumps(updated, indent=2), encoding="utf-8")

    # --- Reporting ---
    print("=== SUMMARY ===")
    print(f"Original companies: {len(original_companies)}")
    print(f"Kept in companies_updated: {len(kept_companies)}")
    print(f"Dropped (missing JSON): {len(dropped_missing)}")
    print(f"Dropped (duplicate CIK): {len(dropped_duplicates)}")
    print()

    all_dropped = dropped_missing + dropped_duplicates
    if all_dropped:
        print("=== TICKERS NOT INCLUDED IN companies.json ===")
        print("Missing JSON files:")
        for t in dropped_missing:
            print("  -", t)
        print()
        print("Duplicate CIK (kept another ticker for that CIK):")
        for t in dropped_duplicates:
            print("  -", t)
    else:
        print("All tickers made it into companies.json")

    return out_path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    out = build_companies_updated()
    logging.info("Wrote %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())