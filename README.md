# SEC_Data_Collection

---

**SEC_Data_Collection** is a small experimental project created over roughly one week of evenings.  
Its main purpose was to test whether it is possible to:

- collect valid SEC fundamentals,  
- process them into a usable structure,  
- and generate LLM-ready datasets  
- all without using paid APIs or external commercial services.

Some parts of the code are solid (e.g., SEC scraping).  
Some parts are rough or minimally polished due to time constraints.  
Still, the full pipeline functions **well-ish** as of **December 1, 2025**.


Every module includes a clear **overview comment block**, and throughout the project I added **a large number of comments** explaining the logic so readers can understand each step and follow the data flow easily.

Additionally, if you want to quickly preview what the processed data looks like, visit **/data/Examples**

---

## `build_dataset.py` – Fundamentals + Price Feature Builder

This script turns **Bing fundamentals JSON + Yahoo Finance prices** into clean, weekly, model-ready datasets.  
It starts by parsing each `bing_financials/<TICKER>.json` into a **quarterly fundamentals DataFrame**, normalizing things like `1.86B`, `782.00M`, or `51.5%` into real numeric values.  
Period labels such as `"Oct 2025 (FQ4)"` are converted into structured fields: `fiscal_year`, `fiscal_quarter`, and an exact `period_end_date`.  

For prices, it pulls **daily data from Yahoo Finance**, then resamples to **weekly average close** (week-ending Friday) for each ticker.  
Using an as-of merge (`merge_asof`), it attaches the **latest known quarter** to each week and forward-fills fundamentals so short gaps don’t create missing rows.  
On top of that, it engineers key features per week:  
- TTM fundamentals: `revenue_ttm`, `eps_ttm`, `fcf_ttm`, `net_income_ttm`  
- Growth rates: `rev_qoq`, `rev_yoy`, `eps_qoq`, `eps_yoy`  
- Valuation: `market_cap`, `pe_ttm`, `ps_ttm`, `fcf_yield_ttm`  
- Momentum/trend: weekly returns, SMAs, and `price_vs_sma` ratios  

To keep numbers stable and LLM-friendly, weekly prices are **rounded to whole integers**, and all other floats are **rounded to 4 decimal places**.  
Each ticker produces two CSVs:  
- `data/out/{TICKER}_Data.csv` – full engineered feature set with ticker  
- `data/llm_out/{TICKER}_LLM_Data.csv` – compact view for GPT, where `ticker` is replaced by **sector** (via `companies.json`) and only curated columns are kept.  

In practice, this script gives you a consistent **(sector, week)** panel of fundamentals, valuation, and price action that’s suitable for classical models *and* LLM training.


---

## `make_sec_bing_dict.py` – Learning Bing <-> SEC Metric Mappings

This script tries to **learn a mapping** between Bing/MSN financial metric names and **SEC XBRL concepts** on a per-ticker basis.  
It loads your aggregated **10x_submissions.json**, picks a (potentially large) set of 10-Q filings, and for each filing:  
- Finds the corresponding **XBRL instance** on EDGAR  
- Extracts company-wide consolidated facts for the main reporting period  
- Loads the matching **Bing financials JSON** for that ticker and quarter  
- Compares SEC numeric values against Bing’s values and looks for close matches  

For each match, it records **evidence** (relative errors over multiple quarters) in an `evidence` structure keyed by:  
`ticker -> bing_metric -> sec_concept -> [relative_errors...]`.  
After all filings are processed, `build_company_sec_mapping()` turns that evidence into a final mapping:  
`ticker -> { "Bing metric name" -> "SEC XBRL concept" }`, stored as `sec_bing_mapping.json`.  

Because Bing often **rounds or abbreviates values** (K/M/B) and sometimes shows slightly different numbers than SEC, the mapping is “**half-way good**”:  
- It’s useful for **rough alignment** and automated labeling  
- It’s *not* a perfect one-to-one ground truth and should be sanity-checked for edge cases  

Along the way, the script also writes a human-readable comparison log (`sec_bing_compare/...txt`) so you can manually inspect which concepts matched which Bing rows and with what error.  
In short, this is a **data-driven guesser** that helps bridge scraped Bing labels and precise SEC tags, good enough for experiments and LLM feature engineering.

---

## `fetch_10x_from_submissions.py` – Refreshing 10-K / 10-Q Filings

This script keeps your local **10-K / 10-Q dataset** up to date.

### What it does
- Loads all `(ticker, cik)` pairs from **`companies.json`**
- Fetches recent 10-K / 10-Q filings using **SecClient**
- Merges them into **`data/10x_submissions/10x_submissions.json`**
- Deduplicates by `(ticker, accession_number)`
- Updates metadata and writes the refreshed file

### Why it's useful
- Produces one clean, unified JSON of all filings you care about  
- Simplifies downstream steps (downloading filings, parsing XBRL, etc.)  
- Avoids re-fetching or re-processing old filings

### Summary
Run this periodically to maintain a reliable, always-current index of all 10-K / 10-Q filings for your watchlist.

---

## `inspect_10q_xml.py` – Hands-On XBRL Playground

This script is a **debug/inspection tool** for working with real SEC XBRL data.  

### What it does
- Loads the aggregated **`10x_submissions/10x_submissions.json`** file  
- Picks the *first* available **10-Q** filing and wraps it into a `Filing10X` object  
- Uses `SecClient` + `find_instance_xbrl_url()` to locate the correct **XBRL instance XML** on EDGAR  
- Downloads that instance file and saves it under  
  `data/10x_raw_xbrl/<TICKER>/<TICKER>_<accession>_<form>_instance.xml`  
- Parses the XML, extracts all **company-wide consolidated totals** for the main reporting period  
- Prints the extracted facts **grouped by context** via `print_by_context()` so you can see what’s going on

### Why it’s useful
- Gives you a concrete, real-world 10-Q to experiment with  
- Lets you visually inspect contexts, dimensions, and facts before building more automation  
- Helps confirm that `parse_contexts()` and `extract_company_totals_for_main_period()` are behaving as expected  

Run this when you want to say:  
> “Take one actual filing, find its XBRL instance, and show me the clean consolidated numbers.”

---

## `sec_client.py` – Lightweight SEC / EDGAR HTTP Client
This module provides a safe and polite way to download data from the **SEC EDGAR** system without hitting rate limits or dealing with inconsistent responses.

### What it does

- Centralizes all SEC requests through a single `_get()` method  
- Adds:
  - polite throttling  
  - retry logic with exponential backoff  
  - required SEC headers (`User-Agent`, `Referer`)  
- Ensures every request behaves consistently and avoids 429/5xx failures

### Supported features

- **Submissions JSON API**  
  Fetch complete filing histories for any CIK using: https://data.sec.gov/submissions/CIK##########.json

- **Filing discovery helpers**  
Locate XML/HTML documents inside filing directories using the `*-index-headers.html` page

- **Utility helpers**  
- Accession number formatting  
- Fetch plain text through the same safe pipeline  
- Quarter detection helpers

### Why it matters

By routing all communication through one robust, rate-limited client, the rest of the project can focus on parsing and analyzing filings, not handling network problems.

### Bottom line

`sec_client.py` makes SEC scraping:
**safe, predictable, and API-key-free** — ideal for automated pipelines or large-scale data collection.

---

## `scrape_bing_data.py` – Scraping Financials from MSN/Bing

This module collects quarterly **Income Statement**, **Balance Sheet**, and **Cash Flow** data from MSN/Bing when no stable API is available.

### What it does

- Launches an Edge Selenium browser
- Navigates to MSN Money and closes:
  - privacy consent popups  
  - app-install dialogs  
- Searches for each ticker using the main search bar
- Opens the Financials tab
- Scrapes three sections:
  - Income Statement  
  - Balance Sheet  
  - Cash Flow  
- Forces the Quarterly view so all metrics align across periods
- Extracts periods like `Oct 2025 (FQ4)` and maps metrics to them

### Reliability handling

- The function `scrape_many_tickers_and_save()` scrapes multiple tickers in a row  
- If any ticker crashes the browser, it restarts the WebDriver automatically
- One bad page never stops the batch

### Limitations (important)

- MSN/Bing frequently changes class names and HTML structure  
- Because scraping depends on CSS selectors, this script can break unexpectedly  
- Still, it’s currently (as of 12/1/2025) the only reliable free way to gather uniform quarterly fundamentals

### Bottom line

`scrape_bing_data.py` is a practical “get the raw data” tool:  
a bit fragile, but extremely useful for building a clean, normalized fundamentals dataset **without paying for any API**.

---

## `filing_download_service.py` – Find & Download the Right SEC Filing Files

This module’s job is simple:  
**Given a 10-K or 10-Q filing, find the correct XML file that contains the real XBRL facts.**

### What it does

- **Builds SEC URLs** from a `Filing10X` object  
  (CIK + accession → `/Archives/edgar/data/.../`)

- **Downloads the primary HTML filing**  
  (the same HTML you see on sec.gov)

- **Scans the filing directory for `.xml` files**

- **Detects which XML is the actual XBRL instance**  
  (the one containing `<xbrl>` or inline XBRL tags)

- **Returns that XML URL** so the rest of the system can parse the facts.

### How it finds the instance XBRL

1. Load the filing’s index page.  
2. Collect all XML links.  
3. Try “better candidates” first (those ending in `htm.xml`).  
4. For each file:  
   - download only the first few KB  
   - check for XBRL tags  
5. Stop as soon as a real instance document is found.

### Why it exists

SEC filings contain many XML files — schemas, labels, presentations, calculations…  
Only **one** is the real data file.  
This module makes sure we pick the right one quickly and reliably.

### Bottom line

`filing_download_service.py` turns  
**“Here’s a filing accession number”**  
into  
**“Here’s the exact XML file containing the actual financial facts.”**

Clean, fast, minimal, and safe for automated pipelines.

---

## `submissions_10x_service.py` – Fetch & Manage 10-K / 10-Q Filings

This module is responsible for working with the SEC *submissions JSON* and extracting clean, structured information about a company’s 10-K and 10-Q filings.

### What it does

- Defines a lightweight, immutable `Filing10X` dataclass  
  (ticker, CIK, form type, accession, primary document, filing date)
- Fetches all recent **10-K / 10-Q** filings for a company using `SecClient`
- Batch-fetches filings for many companies while safely skipping failures
- Detects companies missing expected 10-Q filings in recent years
- Merges new filings into an existing JSON payload without duplicates

### Key features

- **Strictly focused on 10-K and 10-Q**  
- Parses SEC’s “parallel array” structure in `filings.recent`
- Filters filings by date (e.g., “only filings after 2022-01-01”)
- Each filing is returned as a clean `Filing10X` instance

### Missing filing detection

- Checks whether a company has the typical 3× 10-Q per year  
- Flags years where the company filed fewer than expected  
- Useful for spotting data gaps or unusual reporting patterns

### JSON merging

- `merge_filings_into_payload()` deduplicates by `(ticker, accession_number)`  
- Ensures no repeated filings when refreshing data  
- Updates the payload count and returns how many filings were added

### Bottom line

`submissions_10x_service.py` takes raw SEC submission JSON  
and turns it into a clean, deduplicated, structured list of all recent 10-K/10-Q filings — perfect for downstream XBRL extraction or analytics.

---

## `xbrl_company_totals_service.py` – Extracting Consolidated XBRL Facts

This module takes a raw SEC XBRL instance document (10-K / 10-Q) and turns it into a clean list of company-level consolidated facts.  
It starts by parsing all `<xbrli:context>` elements via `parse_contexts()`, capturing each context’s dates and any dimensional qualifiers (segments, products, geographies, etc.).  
Contexts with dimensions are treated as segment-level data; `is_company_total_context()` flags only those with **no dimensions** as true consolidated company totals.  
`get_document_period_end()` hunts for the `dei:DocumentPeriodEndDate` fact, which defines the main reporting period the filing is about.  
The core function `extract_company_totals_for_main_period()` then walks through every fact in the XBRL, keeping only those that:  
- point to a valid context,  
- use a dimension-free (company total) context,  
- match the main reporting period end date,  
- and have a non-empty value.  

Each selected fact is normalized into a `FactRow` (ticker, filing date, context ID, concept name, value, and period dates).  
You can limit the number of extracted facts per filing (via `limit`) to keep downstream processing focused on the most relevant data.  
For debugging, `print_by_context()` groups facts by context and prints a human-friendly summary of periods, dimensions, and concepts — very useful when sanity-checking unfamiliar filings.  
In short, this module isolates the **core consolidated numbers** (revenue, net income, assets, etc.) from the noisy XBRL universe of segments and dimensions, making them much easier to use for analytics or ML.

---

## Project Motivation

---

This project exists because of three goals:

1. Learn how to gather SEC data efficiently without paying for commercial APIs.  
2. Create a reproducible dataset suitable for LLM training.  
3. Experiment with building a custom data pipeline combining fundamentals + market data.

Some of the code was written quickly (with AI help), so expect a few rough edges.  
The goal was working results, not perfect architecture.

---

## Disclaimer

---

- Not every piece of the codebase is fully polished or production-ready.  
- Some bugs or odd cases may appear — this is expected given the experimental nature.  
- The SEC may change XML structures, rate limits, or endpoints, which may break scraping.  
- The project currently works **as of 12/1/2025**, but future changes are possible.

---