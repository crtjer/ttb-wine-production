"""
TTB Wine Production Pipeline — Stage 2: Parse and Normalize Raw Data
=====================================================================

This module takes the raw CSV/XLSX files downloaded from TTB and normalizes
them into clean, analysis-ready tables.

The raw TTB data comes in a "long" format where each row is a single
observation (one metric for one time period). We pivot this into a "wide"
format where each row is a time period (year + month) and each column is
a specific metric. This makes the data much easier to work with for
time-series analysis and derived metric computation.

Key transformations:
- Filter out redacted rows (Stat_Redaction == TRUE) — these are suppressed
  for confidentiality when fewer than 3 wineries reported a given metric.
- Map the raw Statistical_Group + Statistical_Category + Statistical_Detail
  hierarchy into clean, descriptive column names.
- Handle missing data: some early months (2012-2013) may not have all metrics.
  We fill these with NaN rather than 0, since 0 would be misleading.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column mapping: maps (Statistical_Group, Statistical_Category, Statistical_Detail)
# patterns to our clean output column names.
#
# The TTB data uses a 3-level hierarchy to describe each metric:
#   Group -> Category -> Detail
#
# For example:
#   "1-Production" -> "Production by Type" -> "Low Alcohol Wine (up to 7%)"
#
# We flatten this into descriptive column names like "production_low_alc".
# The mapping below defines how we extract each metric from the raw data.
# ---------------------------------------------------------------------------
METRIC_MAPPINGS = {
    # -----------------------------------------------------------------------
    # PRODUCTION metrics — total gallons produced, broken out by wine type
    # These come from Statistical_Group "1-Production"
    # -----------------------------------------------------------------------
    "production_total": {
        "group": "1-Production",
        "category": None,  # We sum across all categories for total
        "detail": None,    # Sum across all details
        "agg": "sum",      # Aggregate by summing all production sub-types
    },
    "production_low_alc": {
        "group": "1-Production",
        "detail_contains": "Low Alcohol",
    },
    "production_medium_alc": {
        "group": "1-Production",
        "detail_contains": "Medium",
    },
    "production_high_alc": {
        "group": "1-Production",
        "detail_contains": "High Alcohol",
    },
    "production_sparkling": {
        "group": "1-Production",
        "detail_contains": "Sparkling",
    },
    "production_hard_cider": {
        "group": "1-Production",
        "detail_contains": "Cider",
    },

    # -----------------------------------------------------------------------
    # WITHDRAWAL metrics — gallons removed from bonded premises
    # Taxable = domestic sale; Tax-free = export
    # These are the best proxy for actual market demand/supply
    # -----------------------------------------------------------------------
    # Taxable withdrawal total: found in "0-Category Total" category,
    # detail "2-Taxable Withdrawals" (the detail repeats the sub-group name
    # for category-total rows — this is TTB's convention for aggregate values)
    "withdrawals_taxable_total": {
        "group": "2-Withdrawals",
        "category_contains": "Category Total",
        "detail_contains": "2-Taxable Withdrawals",
        "detail_excludes": "Bulk",  # Exclude the "by Bulk vs Bottled" variant
    },
    # Bottled/Bulk breakdown uses the "by Bulk vs Bottled" sub-category
    "withdrawals_taxable_bottled": {
        "group": "2-Withdrawals",
        "category_contains": "Bulk vs Bottled",
        "detail_contains": "Bottled",
    },
    "withdrawals_taxable_bulk": {
        "group": "2-Withdrawals",
        "category_contains": "Bulk vs Bottled",
        "detail_contains": "Bulk",
    },
    # Tax-free (export) total: "0-Category Total" with detail "3-Tax Free Withdrawals"
    # We exclude "For Export" to get the broader tax-free total
    "withdrawals_export_total": {
        "group": "2-Withdrawals",
        "category_contains": "Category Total",
        "detail_contains": "3-Tax Free Withdrawals",
        "detail_excludes": "Export",  # Exclude "For Export" sub-variant
    },
    "withdrawals_export_bottled": {
        "group": "2-Withdrawals",
        "category_contains": "For Export",
        "detail_contains": "Bottled",
    },
    "withdrawals_export_bulk": {
        "group": "2-Withdrawals",
        "category_contains": "For Export",
        "detail_contains": "Bulk",
    },

    # -----------------------------------------------------------------------
    # STOCKS ON HAND — inventory at end of period
    # Rising stocks = supply building up (downward price pressure)
    # Falling stocks = inventory being drawn down (price support)
    # The total is in "0-Category Total" category with detail
    # "4-Stocks on Hand End-of-Period" (not "by Bulk vs Bottled")
    # -----------------------------------------------------------------------
    "stocks_total": {
        "group": "4-Stocks",
        "category_contains": "Category Total",
        "detail_contains": "4-Stocks on Hand End-of-Period",
        "detail_excludes": "Bulk",
    },
    "stocks_bottled": {
        "group": "4-Stocks",
        "detail_contains": "Bottled",
    },
    "stocks_bulk": {
        "group": "4-Stocks",
        "detail_contains": "2-Bulk",
    },

    # -----------------------------------------------------------------------
    # ACTIVE WINERY COUNT — number of industry members (IMs) reporting
    # This tracks market health: growing count = new entrants, shrinking = consolidation
    # -----------------------------------------------------------------------
    "active_wineries": {
        "group": "01-Count",
        "use_count_ims": True,  # This metric uses Count_IMs, not Value
    },
}


def _find_raw_dir() -> Path:
    """Locate the data/raw/ directory relative to this module."""
    return Path(__file__).resolve().parent.parent / "data" / "raw"


def _find_processed_dir() -> Path:
    """Locate the data/processed/ directory, creating if needed."""
    d = Path(__file__).resolve().parent.parent / "data" / "processed"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names: strip whitespace, since TTB CSVs sometimes
    have trailing spaces in headers (common in government data exports).
    """
    df.columns = df.columns.str.strip()
    return df


def _extract_metric(df: pd.DataFrame, metric_name: str, spec: dict) -> pd.Series:
    """
    Extract a single metric from the long-format TTB data.

    This function filters the raw data based on the metric specification
    (group, category, detail patterns) and aggregates the values for each
    year+month combination.

    Args:
        df: The raw long-format DataFrame
        metric_name: Name of the metric (for logging)
        spec: Dict with filter criteria from METRIC_MAPPINGS

    Returns:
        A Series indexed by (year, month) with the metric values
    """
    mask = pd.Series(True, index=df.index)

    # Filter by Statistical_Group — always required
    group_pattern = spec.get("group", "")
    if group_pattern:
        # Use 'contains' matching because group names may have slight variations
        mask &= df["Statistical_Group"].str.contains(group_pattern, case=False, na=False)

    # Filter by Statistical_Category substring if specified
    if "category_contains" in spec:
        mask &= df["Statistical_Category"].str.contains(
            spec["category_contains"], case=False, na=False
        )

    # Filter by Statistical_Detail substring if specified
    if "detail_contains" in spec:
        mask &= df["Statistical_Detail"].str.contains(
            spec["detail_contains"], case=False, na=False
        )

    # Exclude certain detail patterns — used to disambiguate totals from
    # sub-variants (e.g., "2-Taxable Withdrawals" vs "2-Taxable Withdrawals by Bulk vs Bottled")
    if "detail_excludes" in spec:
        mask &= ~df["Statistical_Detail"].str.contains(
            spec["detail_excludes"], case=False, na=False
        )

    filtered = df[mask]

    if filtered.empty:
        logger.warning(f"  No data found for metric '{metric_name}' — will be NaN")
        return pd.Series(dtype=float, name=metric_name)

    # Decide which value column to use:
    # - Most metrics use "Value" (gallons)
    # - Winery count uses "Count_IMs" (number of reporting members)
    value_col = "Count_IMs" if spec.get("use_count_ims") else "Value"

    # Group by year+month and aggregate
    # For "total" metrics we sum sub-categories; for specific metrics there's
    # usually only one row per period, so sum is equivalent to picking the value
    agg_func = spec.get("agg", "sum")

    result = (
        filtered
        .groupby(["Year", "CY_Month_Number"])[value_col]
        .agg(agg_func)
    )
    result.name = metric_name

    return result


def parse_monthly(raw_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Parse the raw monthly CSV into a clean wide-format table.

    The raw data has one row per (year, month, statistical_group, category, detail).
    We pivot it so each row is one (year, month) with columns for each metric.

    Output columns:
        year, month, production_total, production_low_alc, production_medium_alc,
        production_high_alc, production_sparkling, production_hard_cider,
        withdrawals_taxable_total, withdrawals_taxable_bottled, withdrawals_taxable_bulk,
        withdrawals_export_total, withdrawals_export_bottled, withdrawals_export_bulk,
        stocks_total, stocks_bottled, stocks_bulk, active_wineries
    """
    if raw_path is None:
        raw_path = _find_raw_dir() / "wine_monthly.csv"

    logger.info(f"Parsing monthly data from: {raw_path}")

    # -----------------------------------------------------------------------
    # Step 1: Load the raw CSV
    # TTB CSVs use standard comma-delimited format. Some rows may have
    # trailing whitespace in field values, which we strip below.
    # -----------------------------------------------------------------------
    df = pd.read_csv(raw_path, low_memory=False)
    df = _clean_column_names(df)
    logger.info(f"  Raw data: {len(df):,} rows, columns: {list(df.columns)}")

    # -----------------------------------------------------------------------
    # Step 2: Basic data cleaning
    # - Convert Year and CY_Month_Number to integers (they may come as strings)
    # - Strip whitespace from string columns
    # - Convert Value to numeric (some rows have non-numeric values)
    # -----------------------------------------------------------------------
    # Strip whitespace from all string columns to handle TTB formatting quirks
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Ensure year and month are integers
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["CY_Month_Number"] = pd.to_numeric(df["CY_Month_Number"], errors="coerce")

    # Convert Value to numeric — some rows may have text like "N/A" or blanks
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

    # Convert Count_IMs to numeric as well (used for active winery count)
    if "Count_IMs" in df.columns:
        df["Count_IMs"] = pd.to_numeric(df["Count_IMs"], errors="coerce")

    # -----------------------------------------------------------------------
    # Step 3: Filter out redacted rows
    # TTB suppresses data for confidentiality when fewer than 3 wineries
    # reported a metric. These rows have Stat_Redaction = "TRUE".
    # We exclude them because the values are unreliable/missing.
    # -----------------------------------------------------------------------
    if "Stat_Redaction" in df.columns:
        redacted_count = df["Stat_Redaction"].astype(str).str.upper().eq("TRUE").sum()
        df = df[df["Stat_Redaction"].astype(str).str.upper() != "TRUE"]
        logger.info(f"  Filtered out {redacted_count} redacted rows")

    # Drop rows where year or month is missing (can't place them in time series)
    df = df.dropna(subset=["Year", "CY_Month_Number"])
    df["Year"] = df["Year"].astype(int)
    df["CY_Month_Number"] = df["CY_Month_Number"].astype(int)

    # -----------------------------------------------------------------------
    # Step 4: Extract each metric using the mapping definitions
    # Each metric is extracted independently and then joined together.
    # This approach is more robust than a single complex pivot because
    # different metrics need different filtering logic.
    # -----------------------------------------------------------------------
    logger.info("  Extracting metrics...")

    # Build the complete year×month index so we have rows for every period,
    # even months with no data (they'll get NaN, which is correct)
    all_periods = (
        df[["Year", "CY_Month_Number"]]
        .drop_duplicates()
        .sort_values(["Year", "CY_Month_Number"])
        .set_index(["Year", "CY_Month_Number"])
    )

    result = all_periods.copy()

    for metric_name, spec in METRIC_MAPPINGS.items():
        series = _extract_metric(df, metric_name, spec)
        if not series.empty:
            result = result.join(series, how="left")
        else:
            result[metric_name] = float("nan")
        logger.info(f"    {metric_name}: {series.notna().sum() if not series.empty else 0} values")

    # -----------------------------------------------------------------------
    # Step 5: Reset index to get year and month as regular columns
    # Rename CY_Month_Number -> month for clarity
    # -----------------------------------------------------------------------
    result = result.reset_index()
    result = result.rename(columns={"CY_Month_Number": "month", "Year": "year"})

    # Sort chronologically
    result = result.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(f"  Parsed monthly data: {len(result)} rows, "
                f"date range: {result['year'].min()}/{result['month'].min():02d} "
                f"to {result['year'].max()}/{result['month'].max():02d}")

    return result


def parse_yearly(raw_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Parse the raw yearly CSV into a clean wide-format table.

    The yearly CSV has the same structure as the monthly CSV but aggregated
    at the annual level. We apply the same extraction logic but group by
    Year only (ignoring month).
    """
    if raw_path is None:
        raw_path = _find_raw_dir() / "wine_yearly.csv"

    logger.info(f"Parsing yearly data from: {raw_path}")

    df = pd.read_csv(raw_path, low_memory=False)
    df = _clean_column_names(df)
    logger.info(f"  Raw data: {len(df):,} rows")

    # Clean string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    if "Count_IMs" in df.columns:
        df["Count_IMs"] = pd.to_numeric(df["Count_IMs"], errors="coerce")

    # Filter redacted rows
    if "Stat_Redaction" in df.columns:
        df = df[df["Stat_Redaction"].astype(str).str.upper() != "TRUE"]

    df = df.dropna(subset=["Year"])
    df["Year"] = df["Year"].astype(int)

    # For yearly data, we need a dummy month column for consistent extraction
    # The yearly CSV may or may not have CY_Month_Number; if it does, it's
    # often 0 or blank. We set it to 0 to indicate "full year".
    if "CY_Month_Number" not in df.columns:
        df["CY_Month_Number"] = 0
    else:
        df["CY_Month_Number"] = pd.to_numeric(df["CY_Month_Number"], errors="coerce").fillna(0).astype(int)

    # Build year index
    all_years = df[["Year"]].drop_duplicates().sort_values("Year")

    result = all_years.set_index("Year").copy()

    # Extract each metric, grouping by Year only (summing across all months/details)
    for metric_name, spec in METRIC_MAPPINGS.items():
        mask = pd.Series(True, index=df.index)
        group_pattern = spec.get("group", "")
        if group_pattern:
            mask &= df["Statistical_Group"].str.contains(group_pattern, case=False, na=False)
        if "category_contains" in spec:
            mask &= df["Statistical_Category"].str.contains(
                spec["category_contains"], case=False, na=False
            )
        if "detail_contains" in spec:
            mask &= df["Statistical_Detail"].str.contains(
                spec["detail_contains"], case=False, na=False
            )

        filtered = df[mask]
        value_col = "Count_IMs" if spec.get("use_count_ims") else "Value"

        if not filtered.empty:
            series = filtered.groupby("Year")[value_col].sum()
            series.name = metric_name
            result = result.join(series, how="left")
        else:
            result[metric_name] = float("nan")

    result = result.reset_index().rename(columns={"Year": "year"})
    result = result.sort_values("year").reset_index(drop=True)

    logger.info(f"  Parsed yearly data: {len(result)} rows, "
                f"years: {result['year'].min()} to {result['year'].max()}")

    return result


def parse_state(raw_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Parse the state-level XLSX report into a clean table.

    The Wine_State_Report.xlsx has a different structure from the CSVs —
    it's a cross-tabulated report with states as rows and metrics as columns.
    We normalize it into a long-ish format with columns:
        state, year, production, taxable_withdrawals, stocks_on_hand
    """
    if raw_path is None:
        raw_path = _find_raw_dir() / "wine_state.xlsx"

    logger.info(f"Parsing state data from: {raw_path}")

    try:
        # The state report may have multiple sheets — try reading the first one
        # openpyxl is required for .xlsx files
        df = pd.read_excel(raw_path, engine="openpyxl")
        df = _clean_column_names(df)
        logger.info(f"  Raw state data: {len(df)} rows, columns: {list(df.columns)}")

        # The state XLSX format varies by year. We try to identify key columns
        # by looking for common patterns in the column names.
        # Typical columns: State, Year, Production, Taxable Removals, etc.

        # Normalize column names to lowercase for easier matching
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if "state" in col_lower:
                col_map[col] = "state"
            elif "year" in col_lower:
                col_map[col] = "year"
            elif "production" in col_lower and "total" in col_lower:
                col_map[col] = "production"
            elif "production" in col_lower:
                col_map[col] = "production"
            elif "taxable" in col_lower and ("removal" in col_lower or "withdrawal" in col_lower):
                col_map[col] = "taxable_withdrawals"
            elif "stock" in col_lower:
                col_map[col] = "stocks_on_hand"

        if col_map:
            df = df.rename(columns=col_map)

        # Keep only the columns we mapped, plus any extras
        keep_cols = ["state", "year", "production", "taxable_withdrawals", "stocks_on_hand"]
        available = [c for c in keep_cols if c in df.columns]

        if available:
            df = df[available].copy()
            # Convert numeric columns
            for col in ["year", "production", "taxable_withdrawals", "stocks_on_hand"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            # Drop rows with no state
            if "state" in df.columns:
                df = df.dropna(subset=["state"])

        logger.info(f"  Parsed state data: {len(df)} rows")
        return df

    except Exception as e:
        logger.warning(f"  Could not parse state XLSX: {e}")
        logger.warning("  Returning empty DataFrame — state data will be unavailable")
        return pd.DataFrame(columns=["state", "year", "production", "taxable_withdrawals", "stocks_on_hand"])


def run() -> dict:
    """
    Run the full parse stage: monthly, yearly, and state data.

    Returns a dict of DataFrames keyed by name.
    """
    logger.info("=" * 60)
    logger.info("Stage 2: Parsing and normalizing raw data")
    logger.info("=" * 60)

    processed_dir = _find_processed_dir()
    results = {}

    # Parse monthly data (TTB-002)
    monthly = parse_monthly()
    monthly.to_csv(processed_dir / "monthly.csv", index=False)
    results["monthly"] = monthly
    logger.info(f"  Wrote monthly.csv: {len(monthly)} rows")

    # Parse yearly data (TTB-003)
    yearly = parse_yearly()
    yearly.to_csv(processed_dir / "yearly.csv", index=False)
    results["yearly"] = yearly
    logger.info(f"  Wrote yearly.csv: {len(yearly)} rows")

    # Parse state data (TTB-003)
    state = parse_state()
    state.to_csv(processed_dir / "by_state.csv", index=False)
    results["state"] = state
    logger.info(f"  Wrote by_state.csv: {len(state)} rows")

    return results


if __name__ == "__main__":
    run()
