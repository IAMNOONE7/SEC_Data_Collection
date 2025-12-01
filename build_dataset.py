from __future__ import annotations
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta
import calendar


BACKEND_ROOT = Path(__file__).resolve().parent
DATA_ROOT = BACKEND_ROOT / "data"

# ===============================================
#  Script overview
# ===============================================
# This script builds a model-ready dataset of stock fundamentals + prices.
#
# Pipeline:
#   1) Parse Bing-style fundamentals JSON (income statement, balance sheet,
#      cash flow) into a quarterly DataFrame with numeric values.
#   2) Parse period labels (e.g. "Oct 2025 (FQ4)") into fiscal year/quarter
#      and an exact period_end_date.
#   3) Fetch daily prices from Yahoo Finance, resample them to weekly
#      average close prices per ticker.
#   4) Merge weekly prices with the latest known quarterly fundamentals
#      (using an as-of merge and forward-fill), then engineer features:
#         - TTM aggregates (revenue_ttm, eps_ttm, fcf_ttm, net_income_ttm)
#         - Growth rates (rev_qoq, rev_yoy, eps_qoq, eps_yoy)
#         - Valuation metrics (market_cap, pe_ttm, ps_ttm, fcf_yield_ttm)
#         - Price momentum and trend features (returns, SMAs, price vs SMA)
#   5) Round prices and features to stable numeric precision to avoid
#      noisy floats in downstream models.
#   6) Build two CSV outputs per ticker:
#         - data/out/{TICKER}_Data.csv    -> full engineered feature set
#         - data/llm_out/{TICKER}_LLM_Data.csv -> compact subset for GPT,
#           where ticker is replaced by sector (from companies.json).
#
# Purpose for GPT training:
#   - Provide consistent financial time series where each row
#     describes a (sector, week) with fundamentals, valuation, and momentum.
#   - Let GPT learn relationships between fundamentals, price action,
#     and derived ratios without being distracted by raw messy inputs
#=================================================================================================

UNIT_PATTERN = re.compile(r"^\s*([+-]?\d*\.?\d+)\s*([KMBT]?)\s*$")


def parse_numeric(value: Any) -> Optional[float]:
    """
    Parse values like:
      "1.86B" -> 1.86e9
      "843.00M" -> 8.43e8
      "-407.00M" -> -4.07e8
      "1.18" -> 1.18
      "51.5%" -> 0.515
      "-" or None -> None
    """
    if value is None:
        return None

    if isinstance(value, (int, float)):
        # Already numeric
        if math.isnan(value):
            return None
        return float(value)

    s = str(value).strip()
    if s == "" or s == "-":
        return None

    # Percent (convert to fraction)
    if s.endswith("%"):
        inner = s[:-1]
        num = parse_numeric(inner)
        return num / 100.0 if num is not None else None

    m = UNIT_PATTERN.match(s)
    if not m:
        # Fallback: try plain float
        try:
            return float(s)
        except ValueError:
            raise ValueError(f"Cannot parse numeric value from: {value!r}")

    number_str, unit = m.groups()
    base = float(number_str)
    unit = unit.upper()

    if unit == "K":
        base *= 1e3
    elif unit == "M":
        base *= 1e6
    elif unit == "B":
        base *= 1e9
    elif unit == "T":
        base *= 1e12
    # else: no unit

    return base


PERIOD_PATTERN = re.compile(r"^([A-Za-z]{3}) (\d{4}) \(FQ(\d)\)$")


def parse_period_label(label: str) -> Dict[str, Any]:
    """
    "Oct 2025 (FQ4)" -> {
        "fiscal_year": 2025,
        "fiscal_quarter": 4,
        "period_end_date": date(2025, 10, 31)
    }
    """
    m = PERIOD_PATTERN.match(label.strip())
    if not m:
        raise ValueError(f"Unexpected period label format: {label!r}")

    month_str, year_str, fq_str = m.groups()
    year = int(year_str)
    fiscal_quarter = int(fq_str)

    # Map month abbreviation to month number
    try:
        month = datetime.strptime(month_str, "%b").month
    except ValueError:
        raise ValueError(f"Unknown month in period label: {label!r}")

    # Use last day of that month as period end date
    last_day = calendar.monthrange(year, month)[1]
    period_end_date = date(year, month, last_day)

    return {
        "fiscal_year": year,
        "fiscal_quarter": fiscal_quarter,
        "period_end_date": period_end_date,
    }

@dataclass
class FundamentalsQuarter:
    ticker: str
    period_label: str
    fiscal_year: int
    fiscal_quarter: int
    period_end_date: date
    # We'll keep metrics in a dict so we can expand to columns later
    metrics: Dict[str, Optional[float]]


def parse_fundamentals_json(path: Path) -> pd.DataFrame:
    """
    Parse one JSON file with fundamentals (like A.json) into a tidy DataFrame:
    columns: ticker, period_label, fiscal_year, fiscal_quarter, period_end_date
    """
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    ticker = data["ticker"]
    periods: List[str] = data["periods"]

    # Collect all metric trees (income_statement, balance_sheet, cash_flow)
    # Each is dict[metric_name][period_label] = value_str
    metric_sources: Dict[str, Dict[str, Dict[str, Any]]] = {
        "income_statement": data.get("income_statement", {}),
        "balance_sheet": data.get("balance_sheet", {}),
        "cash_flow": data.get("cash_flow", {}),
    }

    # Flatten metrics into: metric_name -> {period_label: value}
    flat_metrics: Dict[str, Dict[str, Any]] = {}

    for group_name, group in metric_sources.items():
        for metric_name, per_period_values in group.items():
            full_name = metric_name  # you can later prefix with group_name if needed
            if full_name not in flat_metrics:
                flat_metrics[full_name] = {}
            for period_label, raw_val in per_period_values.items():
                flat_metrics[full_name][period_label] = raw_val

    # Build rows per period
    rows: List[FundamentalsQuarter] = []

    for period_label in periods:
        period_meta = parse_period_label(period_label)

        # Collect metric values for this period
        metrics_for_period: Dict[str, Optional[float]] = {}
        for metric_name, per_period in flat_metrics.items():
            raw_value = per_period.get(period_label, None)
            metrics_for_period[metric_name] = parse_numeric(raw_value)

        rows.append(
            FundamentalsQuarter(
                ticker=ticker,
                period_label=period_label,
                fiscal_year=period_meta["fiscal_year"],
                fiscal_quarter=period_meta["fiscal_quarter"],
                period_end_date=period_meta["period_end_date"],
                metrics=metrics_for_period,
            )
        )

    # Convert to DataFrame
    records: List[Dict[str, Any]] = []
    for r in rows:
        base = {
            "ticker": r.ticker,
            "period_label": r.period_label,
            "fiscal_year": r.fiscal_year,
            "fiscal_quarter": r.fiscal_quarter,
            "period_end_date": r.period_end_date,
        }
        base.update(r.metrics)
        records.append(base)

    df = pd.DataFrame(records)
    df.sort_values(["ticker", "period_end_date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_yahoo_prices_weekly(
    ticker: str,
    start_date: date,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """
       Fetch daily prices from Yahoo Finance and resample to weekly average close.
       Works for both single-index and MultiIndex columns from yfinance.
       """
    if end_date is None:
        end_date = date.today()

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    df = yf.download(
        ticker,
        start=start_str,
        end=end_str,
        auto_adjust=True,
        progress=False,
    )

    if df.empty:
        raise RuntimeError(f"No price data returned from Yahoo for {ticker}")

    # Ensure datetime index
    df = df.copy()
    df.index = pd.to_datetime(df.index)

    # Handle both column structures

    if isinstance(df.columns, pd.MultiIndex):
        # Columns look like ('Close','A'), ('Open','A'), ...
        # We want the Close column for *this* ticker as a Series
        try:
            close_series = df[("Close", ticker)]
        except KeyError:
            # Fallback: sometimes first level is ticker, second is field
            close_series = df[(ticker, "Close")]
    else:
        # Normal single-level columns: 'Open','High','Low','Close',...
        close_series = df["Close"]

    # Weekly average (week ending Friday)
    weekly = (
        close_series
        .resample("W-FRI")
        .mean()
        .rename("weekly_avg_close")
        .to_frame()
        .reset_index()
    )

    weekly.rename(columns={"Date": "week_end_date"}, inplace=True)
    weekly["ticker"] = ticker
    weekly = weekly[["ticker", "week_end_date", "weekly_avg_close"]]

    return weekly

def add_quarter_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input: quarterly fundamentals (one row per (ticker, quarter))
    Output: same, plus TTM and growth features (still quarterly).

    Why this is important for a model:
    - Raw quarterly numbers (Revenue, EPS, etc.) are noisy and not directly comparable.
    - TTM (trailing twelve months) smooths seasonality and gives a more stable signal.
    - Growth rates (QoQ, YoY) tell the model whether the business is accelerating or slowing,
      which is often more predictive than absolute levels.
    """
    df = df.copy()
    df.sort_values(["ticker", "period_end_date"], inplace=True)
    g = df.groupby("ticker", group_keys=False)

    # Shares outstanding (feature for market cap, P/E, etc.)
    if "Diluted Average Shares" in df.columns:
        df["shares_outstanding"] = df["Diluted Average Shares"]
    else:
        df["shares_outstanding"] = pd.NA

    # TTM fundamentals (rolling 4 quarters)
    # For each fundamental, we build TTM (sum over 4 quarters).
    for src, ttm in [
        ("Revenue", "revenue_ttm"),
        ("Diluted EPS", "eps_ttm"),
        ("Free Cash Flow", "fcf_ttm"),
        ("Net Profit", "net_income_ttm"),
    ]:
        if src in df.columns:
            df[ttm] = (
                g[src]
                # Rolling window of 4 quarters per ticker.
                .rolling(window=4, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )

    # QoQ and YoY growth features
    # Growth is often more predictive than levels:
    # - QoQ captures short-term acceleration or deceleration.
    # - YoY removes seasonality by comparing same quarter in previous year.

    if "Revenue" in df.columns:
        df["rev_qoq"] = g["Revenue"].pct_change(1, fill_method=None)
        df["rev_yoy"] = g["Revenue"].pct_change(4, fill_method=None)

    if "Diluted EPS" in df.columns:
        df["eps_qoq"] = g["Diluted EPS"].pct_change(1, fill_method=None)
        df["eps_yoy"] = g["Diluted EPS"].pct_change(4, fill_method=None)

    return df


def build_weekly_feature_frame(
    fundamentals_df: pd.DataFrame,
    weekly_prices_df: pd.DataFrame,
    max_lag_days: int = 120,
) -> pd.DataFrame:
    """
    Returns one row per (ticker, week_end_date) with:
      - weekly_avg_close
      - last known fundamentals (TTM, growth, etc.)
      - valuation ratios (P/E, P/S, FCF yield) per week
      - basic price momentum features.
    """
    # 1) quarterly fundamentals with engineered features
    q_df = add_quarter_features(fundamentals_df)

    q_df = q_df.copy()
    w_df = weekly_prices_df.copy()

    q_df["period_end_date"] = pd.to_datetime(q_df["period_end_date"])
    w_df["week_end_date"] = pd.to_datetime(w_df["week_end_date"])

    q_df.sort_values(["ticker", "period_end_date"], inplace=True)
    w_df.sort_values(["ticker", "week_end_date"], inplace=True)

    # 2) attach *latest* quarter to each week (backwards in time)
    weekly = pd.merge_asof(
        w_df,
        q_df,
        left_on="week_end_date",
        right_on="period_end_date",
        by="ticker",
        direction="backward",
        tolerance=pd.Timedelta(days=max_lag_days),
    )

    # no early weeks with only Yahoo prices and no fundamentals
    # missing fundamentals inside a ticker forward-filled from last known values
    fundamental_cols = [
        c
        for c in [
            "revenue_ttm",
            "eps_ttm",
            "fcf_ttm",
            "net_income_ttm",
            "shares_outstanding",
        ]
        if c in weekly.columns
    ]

    if fundamental_cols:
        # Drop rows where we have none of these fundamentals
        # removes early history where merge_asof didn't find a quarter
        weekly = weekly.dropna(subset=fundamental_cols, how="all")

        # Forward-fill fundamentals within each ticker to fill small gaps / missing fields
        weekly[fundamental_cols] = (
            weekly
            .groupby("ticker")[fundamental_cols]
            .ffill()
        )

    # 3) valuation ratios per week (using weekly price + TTM fundamentals)
    weekly["price"] = weekly["weekly_avg_close"]

    def safe_div(num, den):
        return num.where((den != 0) & (~den.isna())) / den

    # Market cap = price * shares_outstanding (from latest quarter).
    # This connects price action with company size, important for P/S and FCF yield.
    weekly["market_cap"] = weekly["price"] * weekly["shares_outstanding"]

    if "eps_ttm" in weekly.columns:
        weekly["pe_ttm"] = safe_div(weekly["price"], weekly["eps_ttm"])

    if "revenue_ttm" in weekly.columns:
        weekly["ps_ttm"] = safe_div(weekly["market_cap"], weekly["revenue_ttm"])

    if "fcf_ttm" in weekly.columns:
        weekly["fcf_yield_ttm"] = safe_div(weekly["fcf_ttm"], weekly["market_cap"])

    # 4) weekly price momentum features
    g = weekly.groupby("ticker", group_keys=False)

    # returns
    weekly["ret_1w"] = g["weekly_avg_close"].pct_change(1, fill_method=None)
    weekly["ret_4w"] = g["weekly_avg_close"].pct_change(4, fill_method=None)
    weekly["ret_12w"] = g["weekly_avg_close"].pct_change(12, fill_method=None)

    # SMAs – use transform to preserve index
    weekly["sma_4w"] = g["weekly_avg_close"].transform(
        lambda s: s.rolling(4, min_periods=1).mean()
    )
    weekly["sma_12w"] = g["weekly_avg_close"].transform(
        lambda s: s.rolling(12, min_periods=1).mean()
    )
    weekly["sma_24w"] = g["weekly_avg_close"].transform(
        lambda s: s.rolling(24, min_periods=1).mean()
    )

    # SMA relative to price (kind of trend indicators)
    weekly["price_vs_sma_4w"] = safe_div(
        weekly["weekly_avg_close"] - weekly["sma_4w"], weekly["sma_4w"]
    )
    weekly["price_vs_sma_12w"] = safe_div(
        weekly["weekly_avg_close"] - weekly["sma_12w"], weekly["sma_12w"]
    )
    weekly["price_vs_sma_24w"] = safe_div(
        weekly["weekly_avg_close"] - weekly["sma_24w"], weekly["sma_24w"]
    )

    # FINAL ROUNDING
    # 1) Round stock price to integer
    if "weekly_avg_close" in weekly.columns:
        weekly["weekly_avg_close"] = weekly["weekly_avg_close"].round(0).astype("Int64")

    # 2) Round all other float columns to 4 decimals
    for col in weekly.columns:
        if col != "weekly_avg_close" and pd.api.types.is_float_dtype(weekly[col]):
            weekly[col] = weekly[col].round(4)
    # END ROUNDING


    return weekly

def build_llm_feature_frame(
    weekly: pd.DataFrame,
    companies_json_path: Path | str = Path("companies.json"),
) -> pd.DataFrame:
    """
    Build a compact feature frame for LLM training.

    - Replaces ticker with sector (from companies.json).
    - Keeps only the curated set of numeric features.
    - Assumes values are already rounded upstream.
    """
    weekly = weekly.copy()

    # Load sector mapping from companies.json
    companies_json_path = Path(companies_json_path)
    with companies_json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    ticker_to_sector = {
        c["ticker"]: c["sector"]
        for c in data.get("companies", [])
    }

    # Map ticker -> sector
    weekly["sector"] = weekly["ticker"].map(ticker_to_sector)

    # If some tickers are missing in companies.json, you can either drop them:
    weekly = weekly[~weekly["sector"].isna()].copy()

    # Columns to keep for GPT
    desired_cols = [
        # identifiers
        "sector",
        "week_end_date",

        # price
        "weekly_avg_close",

        # TTM fundamentals
        "revenue_ttm",
        "eps_ttm",
        "fcf_ttm",
        "net_income_ttm",

        # growth
        "rev_qoq",
        "rev_yoy",
        "eps_qoq",
        "eps_yoy",

        # valuation
        "market_cap",
        "pe_ttm",
        "ps_ttm",
        "fcf_yield_ttm",

        # momentum
        "ret_1w",
        "ret_4w",
        "ret_12w",

        # trend indicators
        "sma_4w",
        "sma_12w",
        "sma_24w",
        "price_vs_sma_4w",
        "price_vs_sma_12w",
        "price_vs_sma_24w",

        # optional quality ratios
        "Gross Margin %",
        "Operating Income %",
        "Net Profit %",
    ]

    # Only keep columns that actually exist (in case some are missing)
    existing_cols = [c for c in desired_cols if c in weekly.columns]

    llm_df = weekly[existing_cols].copy()

    return llm_df

def main_single_ticker(json_path: Path) -> None:
    fundamentals_df = parse_fundamentals_json(json_path)
    ticker = fundamentals_df["ticker"].iloc[0]

    # Choose price start date: a bit before earliest period_end_date
    min_period_date: date = fundamentals_df["period_end_date"].min()
    price_start_date = min_period_date - relativedelta(years=1)

    weekly_prices_df = fetch_yahoo_prices_weekly(
        ticker=ticker,
        start_date=price_start_date,
    )

    weekly_features = build_weekly_feature_frame(fundamentals_df, weekly_prices_df)

    # Save everything
    out_dir = DATA_ROOT / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    weekly_features.to_csv(out_dir / f"{ticker}_Data.csv", index=False)

    llm_df = build_llm_feature_frame(weekly_features, companies_json_path="companies.json")

    llm_out_dir = DATA_ROOT /"llm_out"
    llm_out_dir.mkdir(exist_ok=True, parents=True)
    llm_out_path = llm_out_dir / f"{ticker}_LLM_Data.csv"

    llm_df.to_csv(llm_out_path, index=False)


if __name__ == "__main__":
    bing_dir = DATA_ROOT / "bing_financials"

    json_files = sorted(bing_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files.")

    for json_path in json_files:
        print(f"\n=== Processing {json_path.name} ===")

        try:
            main_single_ticker(json_path)
        except Exception as e:
            print(f"ERROR processing {json_path.name}: {e}")
