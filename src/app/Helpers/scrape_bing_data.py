from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import logging
import json
import time
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[3]

############################################################
# Since there is no stable API, scraping was the only way to
# consistently discover how each company names its Income
# Statement, Balance Sheet, and Cash Flow items.
#
# I am fully aware that this is not the most elegant or
# future-proof solution. This scraper is primarily a
# practical engineering tool to extract the naming
# conventions I need for later processing and normalization.
#
# IMPORTANT:
#   - The scraper depends heavily on MSN/Bing’s current HTML
#     structure and CSS selectors.
#   - It works today (11/30/2025), but it may break if
#     Microsoft updates the UI or changes class names.
#============================================================
# Overview
#
# This module uses Selenium to scrape quarterly financial
# statements (Income Statement, Balance Sheet, Cash Flow)
# from MSN/Bing for a list of tickers.
#
# Main responsibilities:
#   - Drive an Edge browser instance and navigate to MSN Money.
#   - Close privacy / install popups that can block the UI.
#   - Switch between Financials tabs (Income, Balance, Cash Flow)
#     and force the "Quarterly" view.
#   - Parse the financial tables into a structured dict
#   - Save each ticker’s data to its own JSON file in:
#       backend/data/bing_financials/<TICKER>.json
#   - Handle per-ticker failures by restarting the WebDriver
#     so that one broken page doesn’t kill the whole batch.
############################################################

def close_privacy_and_popups(driver):
    try:
        # PRIVACY CONSENT BOX
        buttons = driver.find_elements(By.CSS_SELECTOR, "button")
        for btn in buttons:
            label = btn.text.strip().lower()
            if label in ("i accept", "accept", "reject all", "reject"):
                try:
                    btn.click()
                    print("Privacy window closed.")
                    time.sleep(0.7)
                    break
                except:
                    pass
    except:
        pass

    try:
        # --- MSN MONEY INSTALL POPUP
        close_btns = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Close']")
        for c in close_btns:
            try:
                c.click()
                print("Install popup closed.")
                time.sleep(0.5)
                break
            except:
                pass
    except:
        pass

def _ensure_quarterly_view(driver, wait: WebDriverWait) -> None:
    """
    Make sure the 'Quarterly' toggle is active.
    Works across Income Statement, Balance Sheet, Cash Flow.
    """
    try:
        # Find the button whose visible text contains "Quarterly"
        quarterly_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(normalize-space(.), 'Quarterly')]")
            )
        )
    except TimeoutException:
        logging.warning("Could not find Quarterly toggle – staying in current view.")
        return

    # Small optimization: only click if it's not already the selected view.
    classes = quarterly_btn.get_attribute("class") or ""
    if "selected" not in classes.lower():
        quarterly_btn.click()
        time.sleep(1.0)  # small pause so the table can refresh

def extract_periods(driver, wait: WebDriverWait) -> list[str]:
    """
    Extract period labels from the Financials table header
    ('Oct 2025 (FQ4)', 'Jul 2025 (FQ3)', ...).
    """
    # wait until some table header row exists
    header_row = wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, "table thead tr")
        )
    )

    # pick only <th> that look like period headers
    header_cells = header_row.find_elements(
        By.CSS_SELECTOR,
        "th[class*='tableHeader-']"
    )

    periods: list[str] = []
    for th in header_cells:
        title = (th.get_attribute("title") or "").strip()
        text  = th.text.strip()
        # Prefer the title attribute if present; otherwise fall back to visible text.
        value = title or text
        if value:
            periods.append(value)

    return periods

def extract_financial_table(driver, periods):
    """
    Parse the currently visible financials table into a nested dict.
    """
    rows_data = {}

    for row in driver.find_elements(By.CSS_SELECTOR, "tbody tr"):

        # Extract metric name
        name_td = row.find_elements(By.CSS_SELECTOR, "td:first-child")
        if not name_td:
            continue

        name_raw = name_td[0].text.strip()
        if not name_raw:
            continue

        # Some rows might contain multiple lines; we only take the first line as label.
        metric_name = name_raw.splitlines()[0].strip()

        # Extract values from data cells
        data_tds = row.find_elements(By.CSS_SELECTOR, "td:not(:first-child)")

        values = []
        for td in data_tds:
            # Some cells wrap the value in nested <div>s.
            divs = td.find_elements(By.TAG_NAME, "div")

            if len(divs) >= 1:
                # Value is always first <div>
                val = divs[0].text.strip()
            else:
                # No divs -> pure value inside <td>
                val = td.text.strip()

            values.append(val)

        # Remove empty values
        values = [v for v in values if v != ""]

        if not values:
            # Empty row
            continue

        # Normalize length to match period count
        if len(values) > len(periods):
            values = values[:len(periods)]
        elif len(values) < len(periods):
            values += [""] * (len(periods) - len(values))

        rows_data[metric_name] = dict(zip(periods, values))

    return rows_data

def scrape_income_statement(driver):
    """
    Navigate to Income Statement tab, ensure quarterly view, and scrape the table.
    """
    button = driver.find_element(By.CSS_SELECTOR, 'button[title="Income Statement"]')
    driver.execute_script("arguments[0].click();", button)

    wait = WebDriverWait(driver, 20)
    _ensure_quarterly_view(driver, wait)

    time.sleep(0.5)  # allow rerender

    periods = extract_periods(driver, wait)
    table = extract_financial_table(driver, periods)
    return periods, table

def scrape_balance_sheet(driver):
    """
    Same as scrape_income_statement, but for the Balance Sheet tab.
    """
    button = driver.find_element(By.CSS_SELECTOR, 'button[title="Balance Sheet"]')
    driver.execute_script("arguments[0].click();", button)
    wait = WebDriverWait(driver, 20)
    _ensure_quarterly_view(driver, wait)

    time.sleep(0.5)

    periods = extract_periods(driver, wait)
    table = extract_financial_table(driver, periods)
    return periods, table

def scrape_cash_flow(driver):
    """
    Same as scrape_income_statement, but for the Cash Flow tab.
    """
    button = driver.find_element(By.CSS_SELECTOR, 'button[title="Cash Flow"]')
    driver.execute_script("arguments[0].click();", button)
    wait = WebDriverWait(driver, 20)
    _ensure_quarterly_view(driver, wait)
    time.sleep(0.5)

    periods = extract_periods(driver, wait)
    table = extract_financial_table(driver, periods)
    return periods, table


def scrape_bing_financials_for_driver(driver, wait: WebDriverWait, ticker: str) -> dict:
    """
    Core scraper: assumes driver + wait are already initialized and reused.

    For a single ticker:
      - focuses the MSN Money search box,
      - searches for the ticker,
      - opens the Financials tab,
      - scrapes Income Statement, Balance Sheet, and Cash Flow in Quarterly view.
    """
    close_privacy_and_popups(driver)
    # Locate the main search input inside the top bar.
    search_input = wait.until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                'input[placeholder="Search stocks, ETFs, & more"]'
            )
        )
    )

    # Type ticker and submit
    search_input.clear()
    search_input.send_keys(ticker)
    time.sleep(0.5)  # tiny pause to let autosuggest appear

    # simplest: just press ENTER
    search_input.send_keys(Keys.RETURN)

    financials_btn = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, 'button[title="Financials"]')
        )
    )

    # We want to be on the Financials tab before scraping
    financials_btn.click()
    time.sleep(1.0)

    income_periods, income = scrape_income_statement(driver)
    balance_periods, balance = scrape_balance_sheet(driver)
    cash_periods, cash = scrape_cash_flow(driver)

    # we assume periods are the same across all three tables
    return {
        "ticker": ticker,
        "periods": income_periods,
        "income_statement": income,
        "balance_sheet": balance,
        "cash_flow": cash,
    }

def scrape_many_tickers_and_save(tickers: list[str]) -> None:
    """
    Scrape Bing financials for many tickers.
    - Restarts WebDriver if one ticker breaks it.
    - Saves EACH TICKER into its own JSON file.
    - No single failure can destroy the whole batch.

    Files stored in:
        backend/data/bing_financials/<TICKER>.json
    """

    out_dir = BACKEND_ROOT / "data" / "bing_financials"
    out_dir.mkdir(parents=True, exist_ok=True)

    def init_driver():
        """
        Initialize WebDriver + WebDriverWait, navigate to MSN Money,
        and close early popups.
        """
        d = webdriver.Edge()
        w = WebDriverWait(d, 20)
        d.get("https://www.msn.com/en-US/money?id=a6qja2")
        time.sleep(2)
        close_privacy_and_popups(d)
        return d, w

    driver, wait = init_driver()

    total = len(tickers)
    for idx, ticker in enumerate(tickers, start=1):
        logging.info("Scraping %s (%d/%d)", ticker, idx, total)

        # where to save this ticker
        ticker_path = out_dir / f"{ticker}.json"

        # skip if already scraped
        if ticker_path.exists():
            logging.info("Skipping %s because %s already exists", ticker, ticker_path.name)
            continue

        # attempt 1
        try:
            data = scrape_bing_financials_for_driver(driver, wait, ticker)
        except Exception:
            logging.exception("Ticker %s failed — restarting driver and retrying once", ticker)

            # restart driver
            try:
                driver.quit()
            except:
                pass

            driver, wait = init_driver()

            # attempt 2
            try:
                data = scrape_bing_financials_for_driver(driver, wait, ticker)
            except Exception:
                logging.exception("Ticker %s FAILED even after restart — skipping", ticker)
                continue   # go to next ticker

        # success -> save per ticker
        ticker_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        logging.info("Saved %s → %s", ticker, ticker_path.name)

        time.sleep(0.5)  # polite pause

    # cleanup
    try:
        driver.quit()
    except:
        pass

    logging.info("Finished scraping %d tickers.", total)



def print_metrics(metric_dict: dict, periods: list[str]):
    """
    Helper to pretty-print a metric dict with
    Useful for debugging
    """
    print("Metric".ljust(30), " | ", " | ".join(periods))
    print("-" * 120)

    for metric, vals in metric_dict.items():
        row_vals = [vals.get(p, "") for p in periods]
        print(metric.ljust(30), " | ", " | ".join(row_vals))


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    companies_path = BACKEND_ROOT /"companies.json"

    data = json.loads(companies_path.read_text(encoding="utf-8"))

    # Extract tickers as a flat list
    tickers = [c["ticker"] for c in data.get("companies", [])][198:278]

    out_path = scrape_many_tickers_and_save(tickers)

    # The code below assumes a single JSON file keyed by ticker,
    # which is no longer true with the per-ticker file strategy.
    # Keeping it here as a reminder / template for future quick checks.

    """
    print("\nSaved Bing financials to:")
    print(f"  {out_path}")
    print("\nQuick check (first ticker):")

    first = tickers[0]
    data = json.loads(out_path.read_text(encoding="utf-8"))[first]
    print(f"Ticker: {first}")
    print("Periods:", data["periods"])
    print("Income statement metrics:", list(data["income_statement"].keys())[:10])
    """
    return 0


if __name__ == "__main__":
    raise SystemExit(main())