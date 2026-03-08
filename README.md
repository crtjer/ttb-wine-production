# TTB Wine Production Pipeline

Automated pipeline for downloading, normalizing, and enriching US wine production data from the **Alcohol and Tobacco Tax and Trade Bureau (TTB)**.

## What is TTB?

The TTB is a bureau of the US Department of the Treasury that regulates the alcohol and tobacco industries. Every licensed US winery must submit monthly reports (TTB Form 5120.17) detailing their production, withdrawals, inventory, and operations. The TTB publishes this data in aggregate, providing a comprehensive view of the entire US wine industry.

## What This Pipeline Does

1. **Downloads** 5 source files from TTB (CSV, XLSX, JSON) with ETag-based caching
2. **Parses** raw data into clean, analysis-ready tables (monthly, yearly, by-state)
3. **Computes** 7 derived metrics for business intelligence
4. **Exports** enriched data and a summary snapshot

## Key Metrics

| Metric | Column | What It Tells You |
|---|---|---|
| **Production** | `production_total` | Total gallons of wine produced |
| **Taxable Withdrawals** | `withdrawals_taxable_total` | Gallons shipped to domestic market (demand proxy) |
| **Export Withdrawals** | `withdrawals_export_total` | Gallons exported |
| **Stocks on Hand** | `stocks_total` | End-of-period inventory (supply indicator) |
| **Active Wineries** | `active_wineries` | Number of reporting winery premises |

## Derived Metrics

| Metric | Column | Interpretation |
|---|---|---|
| Withdrawal/Production Ratio | `withdrawal_production_ratio` | >1.0 = inventory draw-down, <1.0 = inventory build |
| Inventory Coverage | `inventory_coverage_months` | Months of supply in warehouses (8-12 normal) |
| YoY Production Change | `yoy_production_change_pct` | Same-month vs prior year production trend |
| YoY Withdrawal Change | `yoy_withdrawal_change_pct` | Demand trend (most important signal) |
| Export Share | `export_share_pct` | % of withdrawals going to export |
| Bulk Share | `bulk_share_pct` | % of taxable withdrawals in bulk (wholesale indicator) |
| Winery Count YoY | `winery_count_yoy_change` | Industry growth vs consolidation |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
bash scripts/run_pipeline.sh

# Force re-download all files (ignore cache)
bash scripts/run_pipeline.sh --force
```

## Output Files

All outputs are written to `data/processed/`:

| File | Description |
|---|---|
| `monthly.csv` | Normalized monthly data (all metrics, wide format) |
| `monthly_enriched.csv` | Monthly data + 7 derived metrics |
| `yearly.csv` | Annual aggregates |
| `by_state.csv` | State-level annual data |
| `summary.json` | Latest-period snapshot (for dashboards/alerts) |
| `withdrawals_trend.csv` | Demand signal time series |
| `inventory_trend.csv` | Supply/inventory time series |
| `production_trend.csv` | Production by wine type time series |

## Project Structure

```
pipeline/
  download.py      — Stage 1: fetch + cache source files
  parse.py         — Stage 2: normalize raw data into clean tables
  transform.py     — Stage 3: compute derived metrics
  export.py        — Stage 4: generate summary + validate outputs

data/
  raw/             — downloaded source files (gitignored)
  processed/       — output files (committed)

scripts/
  run_pipeline.sh  — run full pipeline end-to-end
```

## Data Sources

All data comes from [TTB Wine Statistics](https://www.ttb.gov/regulated-commodities/beverage-alcohol/wine/wine-statistics):

- Monthly production/withdrawal/stocks CSV
- Annual aggregate CSV
- State-level XLSX report
- Full JSON dataset
- Metric mapping/translation XLSX

**Update cadence:** Monthly, approximately 45 days after period end.

## Units

All volume values are in **US wine gallons** (the standard unit used by TTB). One US wine gallon = 3.785 liters.
