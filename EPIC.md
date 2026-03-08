# TTB Wine Production Pipeline — EPIC

## Source
TTB (Alcohol and Tobacco Tax and Trade Bureau) — Wine Production and Operations Report
Derived from TTB Form 5120.17 (Report of Wine Premises Operations), submitted monthly by all licensed US wineries.

## What This Pipeline Provides

Monthly and annual signals on the entire US wine production chain:

| Signal | Business Use |
|---|---|
| **Production volume** (gallons, by wine type) | Supply forecasting, vintage tracking |
| **Taxable Withdrawals** (gallons to market) | Distributor demand proxy — how much wine shipped to commerce |
| **Tax-Free Withdrawals** (export gallons) | Export market tracking |
| **Stocks on Hand** (inventory, bulk vs bottled) | Supply glut / shortage indicator, pricing pressure signal |
| **Industry Member Count** | Active winery count — market health, new entrant tracking |

## Source URLs (direct downloads, no auth required)

- Monthly CSV: `https://www.ttb.gov/system/files/2024-08/Wine_monthly_data_csv.csv`
- Yearly CSV: `https://www.ttb.gov/system/files/2024-08/Wine_yearly_data_csv.csv`
- State Report XLSX: `https://www.ttb.gov/system/files/2024-04/Wine_State_Report.xlsx`
- Full JSON: `https://www.ttb.gov/system/files/2024-08/Wine_Data.json`
- Mapping/Translation XLSX: `https://www.ttb.gov/system/files/images/wine_mapping_2022.xlsx`
- Landing page: `https://www.ttb.gov/regulated-commodities/beverage-alcohol/wine/wine-statistics`

**Update cadence:** Monthly, ~45 days after period end. Annual state reports ~60 days after December filing.

## Raw Data Schema (monthly CSV)

```
CY_Month_Number  — 1–12
Year             — 2012–present
Statistical_Group — Production | Withdrawals | Stocks on Hand | Count of IMs
Statistical_Category — subcategory label
Statistical_Detail — detail label (Low/Medium/High Alcohol, Bottled/Bulk, Sparkling, etc.)
Count_IMs        — number of industry members reporting this data point
Value            — gallons (production/withdrawal/stocks) or count (for IM count)
commodity        — always "Wine"
Stat_Redaction   — TRUE if suppressed for confidentiality (rare)
```

## Key Statistical Groups

**1-Production:** Total gallons produced. Subtypes: Low Alcohol (<7%), Medium Alcohol (7–14%), High Alcohol (>14%), Sparkling, Artificially Carbonated, Hard Cider.

**2-Withdrawals:**
- Taxable Withdrawals = wine withdrawn for domestic sale. Best proxy for market demand.
- Taxable by Bulk vs Bottled = wholesale (bulk) vs retail-ready (bottled)
- Tax-Free Withdrawals = wine exported. Sub: Bottled Exports, Bulk Exports.

**4-Stocks on Hand End-of-Period:** Total inventory held at end of month. Bottled vs Bulk split. Rising stocks = supply building (price pressure down). Falling stocks = draw-down (price support).

**01-Count of IMs:** Number of unique wineries/premises reporting. Tracks industry size.

## Pipeline Architecture

```
pipeline/
  download.py      — fetch all source files from TTB, cache to data/raw/
  parse.py         — normalize raw CSVs into clean structured tables
  transform.py     — compute derived metrics (YoY, rolling averages, inventory ratios)
  export.py        — write final outputs to data/processed/

data/
  raw/             — downloaded source files (gitignored except .gitkeep)
  processed/
    monthly.csv    — normalized monthly data (all groups)
    yearly.csv     — normalized annual data
    by_state.csv   — state-level annual data
    summary.json   — latest period key metrics snapshot
    withdrawals_trend.csv   — taxable withdrawals time series (demand signal)
    inventory_trend.csv     — stocks on hand time series (supply signal)
    production_trend.csv    — production by wine type time series

scripts/
  run_pipeline.sh  — run full pipeline end to end
  update.sh        — incremental update (re-download + reprocess)

README.md
requirements.txt
.gitignore
```

## Derived Metrics to Compute

1. **Withdrawal-to-Production Ratio** — withdrawals / production. >1.0 means inventory draw-down; <1.0 means inventory build.
2. **Inventory Coverage Ratio** — stocks on hand / trailing 3-month average withdrawals. How many months of supply currently sitting in warehouses.
3. **YoY Production Change (%)** — same month vs prior year.
4. **YoY Withdrawal Change (%)** — demand trend signal.
5. **Export Share (%)** — tax-free withdrawals / total withdrawals. Trending up = export market growing.
6. **Bulk/Bottled Split (%)** — bulk withdrawal share. High bulk = more wholesale/private label activity.
7. **Active Winery Count (YoY)** — industry consolidation or growth signal.

## Stories

### Story TTB-001 — Download and cache source files
- Fetch all 5 source files from TTB
- Cache to `data/raw/` with timestamped filenames
- Log file sizes and ETags for change detection
- Skip re-download if file unchanged (ETag/Last-Modified check)
- Output: `data/raw/wine_monthly_YYYYMMDD.csv`, etc.

### Story TTB-002 — Parse and normalize monthly data
- Load raw monthly CSV
- Filter out redacted rows (Stat_Redaction=TRUE)
- Pivot Statistical_Group × Statistical_Detail into clean column-per-metric format
- Output: `data/processed/monthly.csv` with columns: year, month, production_total, production_low_alc, production_medium_alc, production_high_alc, production_sparkling, production_hard_cider, withdrawals_taxable_total, withdrawals_taxable_bottled, withdrawals_taxable_bulk, withdrawals_export_total, withdrawals_export_bottled, withdrawals_export_bulk, stocks_total, stocks_bottled, stocks_bulk, active_wineries

### Story TTB-003 — Parse yearly and state data
- Load yearly CSV and state XLSX
- Normalize to same column schema as monthly
- Output: `data/processed/yearly.csv`, `data/processed/by_state.csv`

### Story TTB-004 — Compute derived metrics
- Compute all 7 derived metrics listed above
- Join to monthly table
- Handle division-by-zero and missing periods gracefully
- Output: enriched `data/processed/monthly_enriched.csv`

### Story TTB-005 — Generate summary snapshot
- Produce `data/processed/summary.json` with:
  - Latest period (year/month)
  - Production (latest vs prior year same month)
  - Taxable withdrawals (latest vs prior year same month)
  - Stocks on hand (latest vs prior year same month)
  - Inventory coverage ratio
  - Active winery count
  - Export share %

### Story TTB-006 — Wire into run script + README
- `scripts/run_pipeline.sh` runs full pipeline
- README: what TTB is, what each metric means, how to run, example output
- Add to cron/heartbeat schedule (monthly update, ~45 days after period end)
