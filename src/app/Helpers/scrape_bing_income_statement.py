# scrape_bing_financials.py

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

TICKER = "AAPL"

def scrape_bing_financials(ticker: str) -> tuple[list[str], list[tuple[str, list[str]]]]:
    driver = webdriver.Edge()

    try:
        # 1. Load Bing page
        url = f"https://www.bing.com/search?q={ticker}"
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        # Wait for the tab bar
        tabs = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.fin_l3tc")
            )
        )

        # Find the <a title="Financials">
        financials_btn = tabs.find_element(
            By.CSS_SELECTOR,
            "a.fin_l3tab[title='Financials']"
        )

        driver.execute_script("arguments[0].click();", financials_btn)

        # 3) Click the "Quarterly" button in the Financials widget
        quarterly_btn = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[contains(@class,'selectButton')"
                " and normalize-space(text())='Quarterly']"
            ))
        )

        # Optional: skip click if already selected
        cls = quarterly_btn.get_attribute("class") or ""
        if "selectedButton" not in cls:
            driver.execute_script("arguments[0].click();", quarterly_btn)

        wait = WebDriverWait(driver, 20)

        container = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div[style*='incomeStatementTable'] div.fianceDataTable-DS-unknown1-1")
            )
        )

        table = container.find_element(By.TAG_NAME, "table")

        # 2) Read header quarters (skip first label column)
        header_cells = table.find_elements(By.CSS_SELECTOR, "thead th")
        periods = [h.text.strip() for h in header_cells[1:] if h.text.strip()]

        result = {"periods": periods, "metrics": {}}

        # 3) Iterate over rows
        for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
            # metric name is in the first td, inside a div
            name_divs = row.find_elements(By.CSS_SELECTOR, "td:first-child div")
            if not name_divs:
                continue

            raw_name = name_divs[0].text.strip()
            if not raw_name:
                continue

            # many rows have "Revenue\nGrowth YoY" etc → take first line only
            metric_name = raw_name.splitlines()[0].strip()

            # data cells: all tds except the first one
            value_divs = row.find_elements(By.CSS_SELECTOR, "td:not(:first-child) > div")
            raw_values = [v.text.strip() for v in value_divs]

            """            
            
            # KEEP ONLY the “main values” (every 2nd element)
            values = raw_values[0::2]

            # align with number of periods
            if len(values) > len(periods):
                values = values[:len(periods)]
            elif len(values) < len(periods):
                values += [""] * (len(periods) - len(values))

            result["metrics"][metric_name] = dict(zip(periods, values))
            """
            # --- FALLBACK: values directly in <td>, not wrapped in <div> ---
            # (e.g. Gross Margin %, Net Profit %, Diluted Average Shares, etc.)
            if not raw_values:
                cells = row.find_elements(By.CSS_SELECTOR, "td:not(:first-child)")
                raw_values = [c.text.strip() for c in cells if c.text.strip()]

            if not raw_values:
                continue

                # --- decide pattern for this metric row ---
            if len(raw_values) == len(periods):
                values = raw_values
            elif len(raw_values) == 2 * len(periods):
                values = raw_values[0::2]
            else:
                values = raw_values[:len(periods)]

                # align length exactly with periods
            if len(values) < len(periods):
                values += [""] * (len(periods) - len(values))
            elif len(values) > len(periods):
                values = values[:len(periods)]

            result["metrics"][metric_name] = dict(zip(periods, values))

        return result

    finally:
        driver.quit()

def print_financial_table(ticker: str, periods: list[str], metrics: dict[str, dict[str, str]]) -> None:
    """
    Pretty-print the scraped financials in a readable table format.
    """

    print("\n==============================================================")
    print(f"                   {ticker} — Income Statement")
    print("==============================================================\n")

    # Print header row
    header_row = ["Metric"] + periods
    print(" | ".join(f"{h:>20}" for h in header_row))
    print("-" * (25 * len(header_row)))

    # Print each metric row
    for metric_name, values_dict in metrics.items():
        row = [metric_name]

        # order values according to periods
        for p in periods:
            row.append(values_dict.get(p, ""))

        print(" | ".join(f"{col:>20}" for col in row))

    print("\n==============================================================\n")


def main():
    result = scrape_bing_financials("AAPL")
    print_financial_table("AAPL", result["periods"], result["metrics"])


if __name__ == "__main__":
    main()
