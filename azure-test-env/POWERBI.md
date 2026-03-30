# Power BI Report Setup

How to connect Power BI to the Medical Billing Gold layer and build the arbitration analytics report.

---

## Prerequisites

- Completed medallion pipeline: Bronze -> Silver -> Gold (all 8 Gold tables populated)
- One of:
  - **Fabric workspace** with `medbill_lakehouse` (for Direct Lake — recommended)
  - **Azure SQL** with Gold views deployed (for DirectQuery)

---

## Option A: Direct Lake via Power BI Service (Cloud — No Desktop Required)

Direct Lake reads Gold Delta tables natively from the Fabric Lakehouse. Everything is done in the browser.

### 1. Create Semantic Model

1. Open **Microsoft Fabric** -> `medbill-test-lakehouse` workspace
2. Open `medbill_lakehouse`
3. Click **New semantic model**
4. Select all `gold_*` tables:

| Gold Table | Rows | Description |
|---|---|---|
| `gold_recovery_by_payer` | 8 | Recovery metrics per payer |
| `gold_cpt_analysis` | 8 | CPT code payment ratios + benchmarks |
| `gold_payer_scorecard` | 8 | Payer risk tier classification |
| `gold_financial_summary` | 10 | Executive KPIs (key-value format) |
| `gold_claims_aging` | 4 | Claims by aging bucket |
| `gold_case_pipeline` | 7 | Case status distribution + SLA |
| `gold_deadline_compliance` | 5 | Deadline met/missed/at-risk by type |
| `gold_underpayment_detection` | 37 | Per-claim arbitration eligibility |

5. Click **Confirm** to create the semantic model

### 2. Add DAX Measures to Semantic Model

1. In Fabric, open the semantic model you just created
2. Click **Open data model** (or **Manage model**)
3. For each measure in `powerbi/dax_measures.dax`:
   - Select a table -> **New measure**
   - Paste the DAX expression
   - Key measures to add first:

| Category | Measures |
|---|---|
| Financial | Total Billed, Total Paid, Total Underpayment, Recovery Rate % |
| Payer | High Risk Payer Count, Revenue at Risk, Payer Risk Color |
| Claims | Aging Over 90 Days %, Total Unpaid Aged Claims |
| Arbitration | Active Cases, Pipeline Value, Overall Win Rate %, Arbitration ROI % |
| Compliance | SLA Status Color, Deadline compliance thresholds |

### 3. Create Report in Power BI Service

1. From the semantic model page, click **Create report** (or **New report**)
2. This opens the **Power BI report editor** in the browser
3. Build pages using the visual specs below
4. Click **Save** when done

### 4. Alternative: Create Report from Fabric Workspace

1. In your Fabric workspace, click **+ New** -> **Report**
2. Select **Pick a published semantic model**
3. Choose the semantic model from step 1
4. Build the report in the browser editor

---

## Option B: Azure SQL via Power BI Service (Cloud — No Fabric)

Use this if you don't have Fabric but have Power BI Pro/Premium cloud access.

### 1. Deploy Gold Views

```bash
sqlcmd -S medbill-sql-214f9d00.database.windows.net -d medbill_oltp \
  -U medbilladmin -P <password> \
  -i azure-test-env/sql/gold_views.sql
```

### 2. Create Dataflow or Dataset in Power BI Service

1. Go to **app.powerbi.com** -> your workspace
2. Click **+ New** -> **Dataflow Gen2** (or **Semantic model**)
3. **Get Data** -> **Azure SQL Database**
4. Server: `medbill-sql-214f9d00.database.windows.net`
5. Database: `medbill_oltp`
6. Authentication: **Database** (username/password) or **OAuth2**
7. Select all `gold_*` views -> **Load**
8. Create report from the dataset

---

## Report Pages (8)

### Page 1: Executive Summary

| Visual | Type | Data Source | Fields |
|---|---|---|---|
| Total Billed | KPI Card | `financial_summary` | metric_value WHERE metric_name = 'total_billed' |
| Total Paid | KPI Card | `financial_summary` | metric_value WHERE metric_name = 'total_paid' |
| Recovery Rate | KPI Card | `financial_summary` | metric_value WHERE metric_name = 'recovery_rate_pct' |
| Denial Rate | KPI Card | `financial_summary` | metric_value WHERE metric_name = 'denial_rate_pct' |
| Total Underpayment | KPI Card | `financial_summary` | metric_value WHERE metric_name = 'total_underpayment' |
| Recovery by Payer | Clustered Bar | `recovery_by_payer` | Axis: payer_name, Values: total_billed, total_paid |
| Claims Aging | Donut | `claims_aging` | Legend: aging_bucket, Values: claim_count |
| Payer Risk | Table | `payer_scorecard` | payer_name, risk_tier, payment_rate_pct, denial_rate_pct |

### Page 2: Payer Analysis

| Visual | Type | Data Source | Fields |
|---|---|---|---|
| Risk Tier Count | Cards | `payer_scorecard` | Count by risk_tier (low/medium/high) |
| Revenue at Risk | KPI Card | `payer_scorecard` | SUM(total_underpayment) WHERE risk_tier = 'high' |
| Payer Scatter | Scatter Plot | `payer_scorecard` | X: payment_rate_pct, Y: denial_rate_pct, Size: total_claims |
| Scorecard Table | Matrix | `payer_scorecard` | All columns, conditional formatting on risk_tier |

### Page 3: CPT Code Analysis

| Visual | Type | Data Source | Fields |
|---|---|---|---|
| Payment Efficiency | KPI Card | `cpt_analysis` | AVG(payment_ratio_pct) |
| CPT Payment Bars | Clustered Bar | `cpt_analysis` | Axis: cpt_code, Values: avg_paid, medicare_rate |
| Payment Heatmap | Matrix | `cpt_analysis` | Rows: cpt_code, Values: payment_ratio_pct (color scale) |
| Detail Table | Table | `cpt_analysis` | cpt_code, claim_count, total_billed, total_paid, payment_ratio_pct |

### Page 4: Arbitration Pipeline

| Visual | Type | Data Source | Fields |
|---|---|---|---|
| Active Cases | KPI Card | `case_pipeline` | SUM(case_count) |
| Pipeline Value | KPI Card | `case_pipeline` | SUM(total_underpayment) |
| Case Funnel | Funnel | `case_pipeline` | Category: status, Values: case_count |
| SLA Gauges | Gauge | `case_pipeline` | sla_compliance_pct per status |
| Eligible Claims | Table | `underpayment_detection` | WHERE arbitration_eligible = 1 |

### Page 5: Deadline Compliance

| Visual | Type | Data Source | Fields |
|---|---|---|---|
| Compliance Rate | KPI Card | `deadline_compliance` | AVG(compliance_pct) |
| At Risk Count | KPI Card | `deadline_compliance` | SUM(at_risk_count) |
| Stacked Bar | Stacked Bar | `deadline_compliance` | Axis: deadline_type, Values: met_count, missed_count, pending_count |
| Detail Table | Table | `deadline_compliance` | All columns |

### Pages 6-8: Financial Forecast, Operational Efficiency, AI Performance

See `powerbi/report_template.json` for full layout of the additional pages covering win/loss analysis, analyst productivity, provider performance, and monthly trends.

---

## Conditional Formatting

Apply these color rules across all pages:

| Metric | Green | Yellow | Red |
|---|---|---|---|
| Recovery Rate % | >= 80% | 50-80% | < 50% |
| Denial Rate % | <= 10% | 10-30% | > 30% |
| SLA Compliance % | >= 90% | 70-90% | < 70% |
| Risk Tier | low | medium | high |

---

## Refresh Strategy

| Connection Mode | Refresh | Latency |
|---|---|---|
| **Direct Lake** (Fabric) | Automatic on Delta commit | 15-30 min after pipeline runs |
| **DirectQuery** (Azure SQL) | Real-time on each report view | Live (Gold views query OLTP) |

The medallion pipeline runs on a 15-minute CDC cycle:
```
ADF CDC (15 min) -> Bronze notebook -> Silver notebook -> Gold notebook
```

After the Gold notebook completes, Direct Lake reports reflect the new data automatically.

---

## Quick Start Checklist (Cloud)

- [ ] Gold tables populated (run pipeline end-to-end)
- [ ] Semantic model created in Fabric from `medbill_lakehouse`
- [ ] DAX measures added to semantic model via **Open data model**
- [ ] Report created via **Create report** from semantic model
- [ ] 8 report pages built using visual specs above
- [ ] Conditional formatting applied
- [ ] Report shared with team via workspace permissions

---

## Standalone Dashboard (No Power BI License)

If you don't have Power BI access at all, a standalone HTML dashboard is available:

```
azure-test-env/powerbi/dashboard.html
```

Open it in any browser. It includes 5 interactive tabs (Executive, Payers, CPT, Pipeline, Deadlines) with sample data pre-loaded. To connect it to live data, use the AI Agent API endpoints as the data source.
