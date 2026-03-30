# Power BI Report Setup

How to connect Power BI to the Medical Billing Gold layer and build the arbitration analytics report.

---

## Prerequisites

- Power BI Desktop (latest version)
- Completed medallion pipeline: Bronze -> Silver -> Gold (all 8 Gold tables populated)
- One of:
  - **Fabric workspace** with `medbill_lakehouse` (for Direct Lake)
  - **Azure SQL** with Gold views deployed (for DirectQuery)

---

## Option A: Direct Lake (Recommended)

Direct Lake reads Gold Delta tables natively from the Fabric Lakehouse — no data import or scheduled refresh needed.

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

### 2. Connect Power BI Desktop

1. Open **Power BI Desktop**
2. **Get Data** -> **OneLake data hub**
3. Select the semantic model created above
4. Click **Connect**

### 3. Import DAX Measures

Open `powerbi/dax_measures.dax` and add the 44 measures to the model. Key measures:

| Category | Measures |
|---|---|
| Financial | Total Billed, Total Paid, Total Underpayment, Recovery Rate % |
| Payer | High Risk Payer Count, Revenue at Risk, Payer Risk Color |
| Claims | Aging Over 90 Days %, Total Unpaid Aged Claims |
| Arbitration | Active Cases, Pipeline Value, Overall Win Rate %, Arbitration ROI % |
| Compliance | SLA Status Color, Deadline compliance thresholds |

In Power BI Desktop: **Modeling** tab -> **New Measure** -> paste each measure.

### 4. Build Report Pages

Use `powerbi/report_template.json` for layout reference.

---

## Option B: Azure SQL DirectQuery

Use this if you don't have Fabric. Requires the 13 Gold SQL views deployed on Azure SQL.

### 1. Deploy Gold Views

```bash
sqlcmd -S medbill-sql-214f9d00.database.windows.net -d medbill_oltp \
  -U medbilladmin -P <password> \
  -i azure-test-env/sql/gold_views.sql
```

### 2. Connect Power BI Desktop

1. **Get Data** -> **Azure SQL Database**
2. Server: `medbill-sql-214f9d00.database.windows.net`
3. Database: `medbill_oltp`
4. Data Connectivity: **DirectQuery**
5. Select all `gold_*` views

### 3. Import DAX Measures and Build Report

Same as Option A steps 3-4.

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
| **Import** (not recommended) | Scheduled refresh | Depends on schedule |

The medallion pipeline runs on a 15-minute CDC cycle:
```
ADF CDC (15 min) -> Bronze notebook -> Silver notebook -> Gold notebook
```

After the Gold notebook completes, Direct Lake reports reflect the new data automatically.

---

## Quick Start Checklist

- [ ] Gold tables populated (run pipeline end-to-end)
- [ ] Semantic model created in Fabric (Direct Lake) or Gold views deployed (DirectQuery)
- [ ] Power BI Desktop connected to data source
- [ ] 44 DAX measures imported from `powerbi/dax_measures.dax`
- [ ] 8 report pages built using layouts above
- [ ] Conditional formatting applied
- [ ] Published to Power BI Service (optional)
