"""
TTB Wine Production Pipeline — Stage 3: Compute Derived Metrics
================================================================

This module takes the parsed monthly data and computes 7 derived metrics
that provide actionable business intelligence about the US wine industry.

All derived metrics are computed on the monthly time series and joined
back to produce an "enriched" version of the monthly data.

The 7 derived metrics:
1. Withdrawal-to-Production Ratio  — market demand vs supply signal
2. Inventory Coverage Ratio        — months of supply in warehouses
3. YoY Production Change (%)       — production trend
4. YoY Withdrawal Change (%)       — demand trend
5. Export Share (%)                 — export market importance
6. Bulk/Bottled Split (%)          — wholesale vs retail channel mix
7. Active Winery Count YoY Change  — industry growth/consolidation
"""

import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _find_processed_dir() -> Path:
    """Locate the data/processed/ directory."""
    return Path(__file__).resolve().parent.parent / "data" / "processed"


def compute_withdrawal_production_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Metric 1: Withdrawal-to-Production Ratio
    ==========================================
    Formula: taxable_withdrawals_total / production_total

    Interpretation:
    - Ratio > 1.0: More wine is being withdrawn than produced this month,
      meaning inventory is being drawn down. This signals strong demand
      relative to current production.
    - Ratio < 1.0: More wine is being produced than withdrawn, meaning
      inventory is building up. This could signal oversupply.
    - Ratio ≈ 1.0: Market is roughly in equilibrium.

    Note: We use taxable withdrawals (domestic sales) only, not exports,
    because this ratio is meant to reflect domestic market dynamics.
    """
    # Guard against division by zero: if production is 0, ratio is undefined
    ratio = df["withdrawals_taxable_total"] / df["production_total"].replace(0, float("nan"))
    ratio.name = "withdrawal_production_ratio"
    return ratio


def compute_inventory_coverage(df: pd.DataFrame) -> pd.Series:
    """
    Metric 2: Inventory Coverage Ratio
    ====================================
    Formula: stocks_total / trailing_3_month_avg_taxable_withdrawals

    Interpretation:
    This tells you how many months of supply are currently sitting in
    warehouses, based on the recent rate of withdrawals.
    - High coverage (>12): Large inventory buffer, potential oversupply
    - Low coverage (<6): Tight supply, potential shortages
    - Normal range: 8-12 months for the wine industry

    We use a 3-month trailing average for withdrawals to smooth out
    monthly volatility (e.g., holiday spikes in December).
    """
    # Compute trailing 3-month average of taxable withdrawals
    # min_periods=1 means we compute the average even if we only have 1-2
    # months of data (important for the start of the time series)
    trailing_avg = df["withdrawals_taxable_total"].rolling(window=3, min_periods=1).mean()

    # Guard against division by zero
    coverage = df["stocks_total"] / trailing_avg.replace(0, float("nan"))
    coverage.name = "inventory_coverage_months"
    return coverage


def compute_yoy_production_change(df: pd.DataFrame) -> pd.Series:
    """
    Metric 3: Year-over-Year Production Change (%)
    =================================================
    Formula: (production_this_month - production_same_month_last_year)
             / production_same_month_last_year * 100

    Interpretation:
    - Positive: Production is growing vs last year (expansion)
    - Negative: Production is contracting (could be weather, demand drop, etc.)

    We compare same month (not sequential month) to control for seasonality —
    wine production is highly seasonal (harvest in Sep-Nov, bottling cycles).
    """
    # Sort by year/month to ensure correct ordering
    df_sorted = df.sort_values(["year", "month"]).reset_index(drop=True)

    # Create a lookup for prior year's same-month production
    # We merge the DataFrame with itself, offset by 12 months
    prior_year = df_sorted[["year", "month", "production_total"]].copy()
    prior_year["year"] = prior_year["year"] + 1  # Shift forward so it aligns with current year
    prior_year = prior_year.rename(columns={"production_total": "production_prior_year"})

    merged = df_sorted.merge(prior_year, on=["year", "month"], how="left")

    # Compute percentage change, guarding against division by zero
    pct_change = (
        (merged["production_total"] - merged["production_prior_year"])
        / merged["production_prior_year"].replace(0, float("nan"))
        * 100
    )
    pct_change.name = "yoy_production_change_pct"
    return pct_change


def compute_yoy_withdrawal_change(df: pd.DataFrame) -> pd.Series:
    """
    Metric 4: Year-over-Year Withdrawal Change (%)
    =================================================
    Formula: (withdrawals_this_month - withdrawals_same_month_last_year)
             / withdrawals_same_month_last_year * 100

    Interpretation:
    - Positive: Demand is growing (more wine being pulled to market)
    - Negative: Demand is contracting (distributors pulling less wine)

    This is arguably the most important demand signal in the dataset.
    Same-month comparison controls for seasonality (holiday spikes, etc.).
    """
    df_sorted = df.sort_values(["year", "month"]).reset_index(drop=True)

    prior_year = df_sorted[["year", "month", "withdrawals_taxable_total"]].copy()
    prior_year["year"] = prior_year["year"] + 1
    prior_year = prior_year.rename(columns={"withdrawals_taxable_total": "withdrawals_prior_year"})

    merged = df_sorted.merge(prior_year, on=["year", "month"], how="left")

    pct_change = (
        (merged["withdrawals_taxable_total"] - merged["withdrawals_prior_year"])
        / merged["withdrawals_prior_year"].replace(0, float("nan"))
        * 100
    )
    pct_change.name = "yoy_withdrawal_change_pct"
    return pct_change


def compute_export_share(df: pd.DataFrame) -> pd.Series:
    """
    Metric 5: Export Share (%)
    ===========================
    Formula: withdrawals_export_total / (withdrawals_taxable_total + withdrawals_export_total) * 100

    Interpretation:
    - Rising export share: International markets are becoming more important
      for US wine producers. Could indicate strong global demand or weak
      domestic demand.
    - Falling export share: Domestic market is absorbing more of production.

    We use total withdrawals (taxable + export) as the denominator to get
    the true share of all wine leaving bonded warehouses.
    """
    total_withdrawals = df["withdrawals_taxable_total"] + df["withdrawals_export_total"]
    # Guard against division by zero
    share = df["withdrawals_export_total"] / total_withdrawals.replace(0, float("nan")) * 100
    share.name = "export_share_pct"
    return share


def compute_bulk_bottled_split(df: pd.DataFrame) -> pd.Series:
    """
    Metric 6: Bulk Share of Taxable Withdrawals (%)
    ==================================================
    Formula: withdrawals_taxable_bulk / withdrawals_taxable_total * 100

    Interpretation:
    - High bulk share: More wine is moving through wholesale/private-label
      channels. This often indicates price competition and commoditization.
    - Low bulk share: More wine is moving as finished product (bottled),
      suggesting stronger brand/premium positioning.

    The bulk vs bottled split is a key structural indicator of market
    positioning in the wine industry.
    """
    share = (
        df["withdrawals_taxable_bulk"]
        / df["withdrawals_taxable_total"].replace(0, float("nan"))
        * 100
    )
    share.name = "bulk_share_pct"
    return share


def compute_winery_count_yoy(df: pd.DataFrame) -> pd.Series:
    """
    Metric 7: Active Winery Count Year-over-Year Change
    =====================================================
    Formula: active_wineries_this_month - active_wineries_same_month_last_year

    Interpretation:
    - Positive: More wineries are operating this year vs last year (industry
      growth, new entrants outpacing closures)
    - Negative: Fewer wineries operating (consolidation, closures)
    - Zero: Stable industry size

    We compute the absolute change rather than percentage because the
    absolute number of new entrants/exits is more actionable for market
    analysis than the percentage change.
    """
    df_sorted = df.sort_values(["year", "month"]).reset_index(drop=True)

    prior_year = df_sorted[["year", "month", "active_wineries"]].copy()
    prior_year["year"] = prior_year["year"] + 1
    prior_year = prior_year.rename(columns={"active_wineries": "wineries_prior_year"})

    merged = df_sorted.merge(prior_year, on=["year", "month"], how="left")

    change = merged["active_wineries"] - merged["wineries_prior_year"]
    change.name = "winery_count_yoy_change"
    return change


def enrich_monthly(monthly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all 7 derived metrics and join them to the monthly data.

    This is the main entry point for the transform stage. It takes the
    parsed monthly DataFrame and returns an enriched version with 7
    additional columns.

    Missing values are handled gracefully:
    - Division by zero -> NaN (not infinity)
    - Missing prior-year data -> NaN for YoY metrics (first year has no prior)
    - Sparse early months -> NaN for any metric that depends on missing data
    """
    logger.info("Computing derived metrics...")

    # Ensure the data is sorted chronologically before computing metrics
    # This is critical for rolling-window calculations (inventory coverage)
    df = monthly_df.sort_values(["year", "month"]).reset_index(drop=True)

    # -----------------------------------------------------------------------
    # Compute each metric and add it as a new column
    # -----------------------------------------------------------------------

    # Metric 1: Withdrawal-to-Production Ratio
    df["withdrawal_production_ratio"] = compute_withdrawal_production_ratio(df)
    logger.info("  ✓ Metric 1: Withdrawal-to-Production Ratio")

    # Metric 2: Inventory Coverage (months of supply)
    df["inventory_coverage_months"] = compute_inventory_coverage(df)
    logger.info("  ✓ Metric 2: Inventory Coverage Ratio")

    # Metric 3: YoY Production Change (%)
    # This requires a merge with prior year data, so it returns a fresh Series
    df["yoy_production_change_pct"] = compute_yoy_production_change(df).values
    logger.info("  ✓ Metric 3: YoY Production Change")

    # Metric 4: YoY Withdrawal Change (%)
    df["yoy_withdrawal_change_pct"] = compute_yoy_withdrawal_change(df).values
    logger.info("  ✓ Metric 4: YoY Withdrawal Change")

    # Metric 5: Export Share (%)
    df["export_share_pct"] = compute_export_share(df)
    logger.info("  ✓ Metric 5: Export Share")

    # Metric 6: Bulk/Bottled Split (%)
    df["bulk_share_pct"] = compute_bulk_bottled_split(df)
    logger.info("  ✓ Metric 6: Bulk/Bottled Split")

    # Metric 7: Active Winery Count YoY Change
    df["winery_count_yoy_change"] = compute_winery_count_yoy(df).values
    logger.info("  ✓ Metric 7: Active Winery Count YoY")

    # Round derived metrics to reasonable precision
    # 2 decimal places for ratios and percentages, 0 for counts
    for col in ["withdrawal_production_ratio", "inventory_coverage_months",
                "yoy_production_change_pct", "yoy_withdrawal_change_pct",
                "export_share_pct", "bulk_share_pct"]:
        if col in df.columns:
            df[col] = df[col].round(2)

    if "winery_count_yoy_change" in df.columns:
        df["winery_count_yoy_change"] = df["winery_count_yoy_change"].round(0)

    logger.info(f"  Enriched monthly data: {len(df)} rows with {len(df.columns)} columns")
    return df


def generate_trend_files(enriched_df: pd.DataFrame) -> dict:
    """
    Generate focused trend files for specific use cases.

    These are subsets of the enriched monthly data, each focused on a
    specific analytical theme. They're convenient for loading into
    dashboards or feeding into specific analyses.
    """
    processed_dir = _find_processed_dir()
    trends = {}

    # -----------------------------------------------------------------------
    # Withdrawals trend — the primary demand signal
    # Includes both taxable (domestic) and export withdrawals with YoY change
    # -----------------------------------------------------------------------
    withdrawal_cols = [
        "year", "month",
        "withdrawals_taxable_total", "withdrawals_taxable_bottled",
        "withdrawals_taxable_bulk", "withdrawals_export_total",
        "yoy_withdrawal_change_pct", "export_share_pct", "bulk_share_pct",
    ]
    available_cols = [c for c in withdrawal_cols if c in enriched_df.columns]
    withdrawals_trend = enriched_df[available_cols].copy()
    withdrawals_trend.to_csv(processed_dir / "withdrawals_trend.csv", index=False)
    trends["withdrawals_trend"] = withdrawals_trend
    logger.info(f"  Wrote withdrawals_trend.csv: {len(withdrawals_trend)} rows")

    # -----------------------------------------------------------------------
    # Inventory trend — supply/glut indicator
    # Tracks stocks on hand and inventory coverage over time
    # -----------------------------------------------------------------------
    inventory_cols = [
        "year", "month",
        "stocks_total", "stocks_bottled", "stocks_bulk",
        "inventory_coverage_months", "withdrawal_production_ratio",
    ]
    available_cols = [c for c in inventory_cols if c in enriched_df.columns]
    inventory_trend = enriched_df[available_cols].copy()
    inventory_trend.to_csv(processed_dir / "inventory_trend.csv", index=False)
    trends["inventory_trend"] = inventory_trend
    logger.info(f"  Wrote inventory_trend.csv: {len(inventory_trend)} rows")

    # -----------------------------------------------------------------------
    # Production trend — supply side analysis
    # Tracks production by wine type with YoY change
    # -----------------------------------------------------------------------
    production_cols = [
        "year", "month",
        "production_total", "production_low_alc", "production_medium_alc",
        "production_high_alc", "production_sparkling", "production_hard_cider",
        "yoy_production_change_pct",
    ]
    available_cols = [c for c in production_cols if c in enriched_df.columns]
    production_trend = enriched_df[available_cols].copy()
    production_trend.to_csv(processed_dir / "production_trend.csv", index=False)
    trends["production_trend"] = production_trend
    logger.info(f"  Wrote production_trend.csv: {len(production_trend)} rows")

    return trends


def run(monthly_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Run the full transform stage.

    If monthly_df is not provided, reads from data/processed/monthly.csv.
    Returns the enriched DataFrame.
    """
    logger.info("=" * 60)
    logger.info("Stage 3: Computing derived metrics")
    logger.info("=" * 60)

    processed_dir = _find_processed_dir()

    if monthly_df is None:
        monthly_path = processed_dir / "monthly.csv"
        monthly_df = pd.read_csv(monthly_path)
        logger.info(f"  Loaded monthly data: {len(monthly_df)} rows")

    # Compute all 7 derived metrics
    enriched = enrich_monthly(monthly_df)

    # Write the enriched monthly data
    enriched.to_csv(processed_dir / "monthly_enriched.csv", index=False)
    logger.info(f"  Wrote monthly_enriched.csv: {len(enriched)} rows")

    # Generate focused trend files
    generate_trend_files(enriched)

    return enriched


if __name__ == "__main__":
    run()
