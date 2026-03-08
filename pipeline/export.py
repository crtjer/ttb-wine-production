"""
TTB Wine Production Pipeline — Stage 4: Export Summary and Final Outputs
=========================================================================

This module generates the summary.json snapshot that provides a quick
overview of the latest period's key metrics, plus validates that all
expected output files were created.

The summary.json is designed to be consumed by dashboards, alerts, or
monitoring systems that need a quick pulse check on the wine industry
without loading the full time series.
"""

import json
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


def _safe_value(val):
    """
    Convert a value to a JSON-safe type.
    pandas/numpy types (int64, float64, NaN) need conversion to native
    Python types for json.dump to work correctly.
    """
    if pd.isna(val):
        return None
    if hasattr(val, "item"):
        # numpy scalar — convert to Python native type
        return val.item()
    return val


def generate_summary(enriched_df: pd.DataFrame = None) -> dict:
    """
    Generate the summary.json snapshot with latest-period key metrics.

    The summary includes:
    - Latest period identification (year/month)
    - Current vs prior-year comparison for production, withdrawals, stocks
    - Inventory coverage ratio (months of supply)
    - Active winery count
    - Export share percentage

    This provides a quick "state of the industry" snapshot that can be
    consumed by dashboards or alert systems.
    """
    processed_dir = _find_processed_dir()

    if enriched_df is None:
        enriched_path = processed_dir / "monthly_enriched.csv"
        enriched_df = pd.read_csv(enriched_path)

    # -----------------------------------------------------------------------
    # Find the latest period in the dataset
    # We sort by year then month to find the most recent data point
    # -----------------------------------------------------------------------
    df = enriched_df.sort_values(["year", "month"])
    latest = df.iloc[-1]  # Last row = most recent period

    latest_year = int(latest["year"])
    latest_month = int(latest["month"])

    logger.info(f"  Latest period: {latest_year}/{latest_month:02d}")

    # -----------------------------------------------------------------------
    # Find the same month in the prior year for comparison
    # This gives us a seasonality-adjusted comparison point
    # -----------------------------------------------------------------------
    prior_year_data = df[(df["year"] == latest_year - 1) & (df["month"] == latest_month)]

    if not prior_year_data.empty:
        prior = prior_year_data.iloc[0]
    else:
        # If no prior year data exists (e.g., first year in dataset),
        # we still produce the summary but without comparisons
        prior = pd.Series(dtype=float)
        logger.warning(f"  No prior year data for {latest_year-1}/{latest_month:02d}")

    # -----------------------------------------------------------------------
    # Build the summary dict
    # Each section provides current value + prior year + change
    # -----------------------------------------------------------------------
    summary = {
        # Metadata
        "latest_period": {
            "year": latest_year,
            "month": latest_month,
            "label": f"{latest_year}-{latest_month:02d}",
        },
        "data_range": {
            "start_year": int(df["year"].min()),
            "start_month": int(df[df["year"] == df["year"].min()]["month"].min()),
            "end_year": latest_year,
            "end_month": latest_month,
        },

        # Production — total gallons produced this period
        "production": {
            "current": _safe_value(latest.get("production_total")),
            "prior_year": _safe_value(prior.get("production_total")) if not prior.empty else None,
            "yoy_change_pct": _safe_value(latest.get("yoy_production_change_pct")),
            "unit": "gallons",
        },

        # Taxable Withdrawals — gallons shipped to domestic market
        "taxable_withdrawals": {
            "current": _safe_value(latest.get("withdrawals_taxable_total")),
            "prior_year": _safe_value(prior.get("withdrawals_taxable_total")) if not prior.empty else None,
            "yoy_change_pct": _safe_value(latest.get("yoy_withdrawal_change_pct")),
            "unit": "gallons",
        },

        # Stocks on Hand — total inventory at end of period
        "stocks_on_hand": {
            "current": _safe_value(latest.get("stocks_total")),
            "prior_year": _safe_value(prior.get("stocks_total")) if not prior.empty else None,
            "unit": "gallons",
        },

        # Inventory Coverage — how many months of supply in warehouses
        "inventory_coverage": {
            "months": _safe_value(latest.get("inventory_coverage_months")),
            "interpretation": _interpret_coverage(latest.get("inventory_coverage_months")),
        },

        # Active Winery Count
        "active_wineries": {
            "current": _safe_value(latest.get("active_wineries")),
            "yoy_change": _safe_value(latest.get("winery_count_yoy_change")),
        },

        # Export Share
        "export_share": {
            "pct": _safe_value(latest.get("export_share_pct")),
            "interpretation": _interpret_export_share(latest.get("export_share_pct")),
        },

        # Market Balance indicator
        "market_balance": {
            "withdrawal_production_ratio": _safe_value(latest.get("withdrawal_production_ratio")),
            "interpretation": _interpret_ratio(latest.get("withdrawal_production_ratio")),
        },
    }

    return summary


def _interpret_coverage(months) -> str:
    """
    Provide a human-readable interpretation of inventory coverage.
    Wine industry norms: 8-12 months is typical.
    """
    if pd.isna(months) or months is None:
        return "insufficient data"
    if months > 12:
        return "elevated inventory — potential oversupply"
    elif months > 8:
        return "normal inventory levels"
    elif months > 4:
        return "below-average inventory — tightening supply"
    else:
        return "low inventory — potential shortage"


def _interpret_export_share(pct) -> str:
    """Interpret the export share percentage."""
    if pd.isna(pct) or pct is None:
        return "insufficient data"
    if pct > 15:
        return "high export orientation"
    elif pct > 8:
        return "moderate export activity"
    else:
        return "primarily domestic market"


def _interpret_ratio(ratio) -> str:
    """
    Interpret the withdrawal-to-production ratio.
    > 1.0 means drawing down inventory (strong demand or weak production).
    < 1.0 means building inventory (weak demand or strong production).
    """
    if pd.isna(ratio) or ratio is None:
        return "insufficient data"
    if ratio > 1.1:
        return "inventory draw-down — demand exceeds production"
    elif ratio > 0.9:
        return "roughly balanced market"
    else:
        return "inventory build-up — production exceeds demand"


def validate_outputs() -> dict:
    """
    Check that all expected output files exist and have data.
    Returns a dict of {filename: {"exists": bool, "rows": int}}.
    """
    processed_dir = _find_processed_dir()

    expected_files = [
        "monthly.csv",
        "yearly.csv",
        "by_state.csv",
        "monthly_enriched.csv",
        "withdrawals_trend.csv",
        "inventory_trend.csv",
        "production_trend.csv",
        "summary.json",
    ]

    validation = {}
    for fname in expected_files:
        path = processed_dir / fname
        if path.exists():
            if fname.endswith(".csv"):
                try:
                    df = pd.read_csv(path)
                    validation[fname] = {"exists": True, "rows": len(df), "size_bytes": path.stat().st_size}
                except Exception:
                    validation[fname] = {"exists": True, "rows": 0, "size_bytes": path.stat().st_size}
            else:
                validation[fname] = {"exists": True, "size_bytes": path.stat().st_size}
        else:
            validation[fname] = {"exists": False}

    return validation


def run(enriched_df: pd.DataFrame = None) -> dict:
    """
    Run the export stage: generate summary.json and validate all outputs.
    """
    logger.info("=" * 60)
    logger.info("Stage 4: Generating summary and validating outputs")
    logger.info("=" * 60)

    processed_dir = _find_processed_dir()

    # Generate the summary snapshot
    summary = generate_summary(enriched_df)

    # Write summary.json with pretty formatting for readability
    summary_path = processed_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"  Wrote summary.json")

    # Validate all expected outputs exist
    validation = validate_outputs()
    logger.info("  Output validation:")
    all_ok = True
    for fname, info in validation.items():
        if info["exists"]:
            rows = info.get("rows", "n/a")
            size = info.get("size_bytes", 0)
            logger.info(f"    ✓ {fname}: {rows} rows, {size:,} bytes")
        else:
            logger.error(f"    ✗ {fname}: MISSING")
            all_ok = False

    if all_ok:
        logger.info("  All outputs validated successfully!")
    else:
        logger.warning("  Some outputs are missing — check pipeline logs for errors")

    return {"summary": summary, "validation": validation}


if __name__ == "__main__":
    run()
