# Power BI — Medical Billing Arbitration Dashboard

## Data Sources

### Option A: Gold Layer on ADLS Gen2 (Parquet)

13 Gold tables in the `gold` container:

```
gold/
  recovery_by_payer/current.parquet
  cpt_analysis/current.parquet
  payer_scorecard/current.parquet
  financial_summary/current.parquet
  claims_aging/current.parquet
  case_pipeline/current.parquet
  deadline_compliance/current.parquet
  underpayment_detection/current.parquet
  win_loss_analysis/current.parquet
  analyst_productivity/current.parquet
  time_to_resolution/current.parquet
  provider_performance/current.parquet
  monthly_trends/current.parquet
```

### Option B: Gold SQL Views (Azure SQL)

13 `gold_*` views in `medbill_oltp` — live queries, always current. Created by `sql/gold_views.sql`.

### Option C: Direct Lake (Fabric)

If using Fabric, connect via OneLake data hub to `medbill_lakehouse` semantic model.

## Setup Steps

### 1. Connect Power BI

**ADLS Gen2:**
1. Power BI Desktop > **Get Data** > **Azure Data Lake Storage Gen2**
2. URL: `https://<STORAGE_ACCOUNT>.dfs.core.windows.net/`
3. Navigate to `gold/` container, select Parquet files

**Azure SQL:**
1. Power BI Desktop > **Get Data** > **Azure SQL Database**
2. Server: `<value from .env>`
3. Database: `medbill_oltp`
4. Select the 13 `gold_*` views

### 2. Import 13 Tables

| Table | Description |
|---|---|
| `recovery_by_payer` | Payer recovery metrics |
| `cpt_analysis` | CPT code billing vs benchmarks |
| `payer_scorecard` | Payer risk assessment |
| `financial_summary` | Overall financial KPIs |
| `claims_aging` | Claim aging buckets |
| `case_pipeline` | Case status + SLA compliance |
| `deadline_compliance` | Deadline met/missed by type |
| `underpayment_detection` | Per-claim arbitration eligibility |
| `win_loss_analysis` | Outcomes by payer, dispute type, ROI |
| `analyst_productivity` | Cases per analyst, win rate, resolution time |
| `time_to_resolution` | Cycle time by status and priority |
| `provider_performance` | Provider billing, disputes, recovery |
| `monthly_trends` | Volume and financial trends over time |

### 3. Apply DAX Measures

Import 44 measures from `dax_measures.dax` into your model.

### 4. Build Report Pages

Use the layout in `report_template.json` (8 pages, 13 tables).

## Report Pages (8)

### Page 1: Executive Summary
- KPI cards: total billed, paid, recovery rate, denial rate
- Recovery by payer (bar chart)
- Claims aging distribution (donut chart)
- Payer scorecard table with risk tier coloring

### Page 2: Payer Analysis
- High risk payer count + revenue at risk cards
- Payer scorecard matrix with conditional formatting
- Payment rate vs denial rate scatter plot

### Page 3: CPT Code Analysis
- CPT payment efficiency + Medicare gap cards
- Billed vs paid vs Medicare comparison (bar chart)
- Full CPT code detail table

### Page 4: Arbitration Pipeline
- Active cases, pipeline value, eligible count cards
- Case pipeline funnel by status
- SLA compliance gauge
- Underpayment detection table with arbitration eligibility

### Page 5: Deadline Compliance
- Overall compliance rate, at risk, missed cards
- Deadline type stacked bar (met/missed/pending/at-risk)
- Deadline detail table

### Page 6: Financial Forecast
- Win rate, total awarded, ROI, projected recovery cards
- Disputed vs awarded by payer (bar chart)
- Dispute type breakdown (donut)
- Monthly billed/paid/awarded trend lines

### Page 7: Operational Efficiency
- Resolved cases, avg resolution days, analyst win rate, total recovered
- Analyst productivity matrix
- Resolution time by status/priority
- Provider performance table

### Page 8: AI Performance
- Monthly disputes, resolved, MoM growth cards
- Volume trend lines (claims, disputes, resolutions)
- Financial trend lines (underpayment, awarded)
- Monthly detail table
