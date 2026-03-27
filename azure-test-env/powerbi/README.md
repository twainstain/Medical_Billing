# Power BI — Medical Billing Arbitration Dashboard

## Data Source: Gold Layer on ADLS Gen2

The Gold Parquet files are in the `gold` container on your storage account:

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
```

## Setup Steps

### 1. Connect Power BI to ADLS Gen2

1. Open Power BI Desktop
2. **Get Data** > **Azure** > **Azure Data Lake Storage Gen2**
3. Enter your storage URL: `https://<STORAGE_ACCOUNT>.dfs.core.windows.net/`
4. Sign in with your Azure account
5. Navigate to `gold/` container and select the Parquet files

### 2. Import Tables

Load each Gold table as a separate query:
- `recovery_by_payer` — Payer recovery metrics
- `cpt_analysis` — CPT code billing analysis
- `payer_scorecard` — Payer risk assessment
- `financial_summary` — Overall financial KPIs
- `claims_aging` — Claim aging buckets
- `case_pipeline` — Case status and SLA compliance
- `deadline_compliance` — Deadline met/missed by type
- `underpayment_detection` — Per-claim arbitration eligibility

### 3. Apply DAX Measures

Import the measures from `dax_measures.dax` into your model.

### 4. Build Report Pages

Use the template layout in `report_template.json` or build from scratch using the suggested page layouts below.

## Suggested Report Pages

### Page 1: Executive Summary
- KPI cards from `financial_summary` (total billed, paid, recovery rate, denial rate)
- Recovery trend by payer (bar chart from `recovery_by_payer`)
- Claims aging distribution (donut chart from `claims_aging`)

### Page 2: Payer Analysis
- Payer scorecard table with conditional formatting on risk_tier
- Recovery rate vs denial rate scatter plot
- Top underpaying payers bar chart

### Page 3: CPT Code Analysis
- CPT code payment ratio heatmap
- Billed vs Paid comparison bar chart
- Medicare/Fair Health rate comparison

### Page 4: Arbitration Pipeline
- Case pipeline funnel (from `case_pipeline`)
- SLA compliance gauges by status
- Underpayment detection table with arbitration eligibility flags
- Deadline compliance stacked bar chart

### Page 5: Operational Deadlines
- Deadline compliance by type
- At-risk deadlines alert table
- Missed vs met trend over time
