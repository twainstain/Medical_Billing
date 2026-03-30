# Power BI Report Build Guide (Cloud / Fabric)

Step-by-step instructions to build the 5-page report directly in Power BI Service.
No Desktop required. Uses the `gold semantic model` in Fabric.

---

## Step 1: Add DAX Measures to Semantic Model

1. Open `gold semantic model` in Fabric
2. Click **Open data model** in the top toolbar
3. For each measure below, select the indicated table, then click **New measure** and paste

### Measures on `financial_summary` table

Select `financial_summary` in the model, then add each:

```dax
Total Billed = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_billed")
```

```dax
Total Paid = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_paid")
```

```dax
Total Underpayment = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_underpayment")
```

```dax
Recovery Rate % = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "recovery_rate_pct")
```

```dax
Denial Rate % = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "denial_rate_pct")
```

```dax
Total Claims = CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_claims")
```

### Measures on `payer_scorecard` table

Select `payer_scorecard`, then:

```dax
High Risk Payer Count = CALCULATE(COUNTROWS(payer_scorecard), payer_scorecard[risk_tier] = "high")
```

```dax
Total Revenue at Risk = CALCULATE(SUM(payer_scorecard[total_underpayment]), payer_scorecard[risk_tier] = "high")
```

```dax
Payer Risk Color = SWITCH(SELECTEDVALUE(payer_scorecard[risk_tier]), "low", "#27AE60", "medium", "#F39C12", "high", "#E74C3C", "#95A5A6")
```

### Measures on `recovery_by_payer` table

Select `recovery_by_payer`, then:

```dax
Weighted Recovery Rate = DIVIDE(SUM(recovery_by_payer[total_paid]), SUM(recovery_by_payer[total_billed]), 0) * 100
```

### Measures on `cpt_analysis` table

Select `cpt_analysis`, then:

```dax
CPT Payment Efficiency = DIVIDE(SUM(cpt_analysis[total_paid]), SUM(cpt_analysis[total_billed]), 0) * 100
```

```dax
Avg Medicare Gap = AVERAGEX(FILTER(cpt_analysis, NOT(ISBLANK(cpt_analysis[medicare_rate]))), cpt_analysis[avg_paid] - cpt_analysis[medicare_rate])
```

### Measures on `claims_aging` table

Select `claims_aging`, then:

```dax
Aging Over 90 Days % = DIVIDE(CALCULATE(SUM(claims_aging[claim_count]), claims_aging[aging_bucket] IN {"91-180 days", "180+ days"}), SUM(claims_aging[claim_count]), 0) * 100
```

### Measures on `case_pipeline` table

Select `case_pipeline`, then:

```dax
Active Cases = CALCULATE(SUM(case_pipeline[case_count]), NOT(case_pipeline[status] IN {"closed", "decided"}))
```

```dax
Pipeline Value = SUM(case_pipeline[total_underpayment])
```

```dax
Avg SLA Compliance = AVERAGEX(case_pipeline, case_pipeline[sla_compliance_pct])
```

### Measures on `deadline_compliance` table

Select `deadline_compliance`, then:

```dax
Overall Compliance Rate = DIVIDE(SUM(deadline_compliance[met_count]), SUM(deadline_compliance[total_deadlines]), 0) * 100
```

```dax
Total At Risk = SUM(deadline_compliance[at_risk_count])
```

```dax
Total Missed = SUM(deadline_compliance[missed_count])
```

### Measures on `underpayment_detection` table

Select `underpayment_detection`, then:

```dax
Arbitration Eligible Count = CALCULATE(COUNTROWS(underpayment_detection), underpayment_detection[arbitration_eligible] = 1)
```

```dax
Arbitration Eligible Value = CALCULATE(SUM(underpayment_detection[underpayment_amount]), underpayment_detection[arbitration_eligible] = 1)
```

### Conditional Formatting Measure

Add to `financial_summary`:

```dax
Recovery Rate Status = VAR rate = [Recovery Rate %] RETURN SWITCH(TRUE(), rate >= 80, "Good", rate >= 50, "Warning", "Critical")
```

Add to `case_pipeline`:

```dax
SLA Status Color = VAR compliance = SELECTEDVALUE(case_pipeline[sla_compliance_pct]) RETURN SWITCH(TRUE(), compliance >= 90, "#27AE60", compliance >= 70, "#F39C12", "#E74C3C")
```

4. Click **Save** when all measures are added

---

## Step 2: Create Report

1. Go back to the semantic model overview
2. Click **New report** in the toolbar
3. The report editor opens in the browser

---

## Step 3: Build Pages

### Page 1: Executive Summary

Rename the default page to `Executive Summary`.

**KPI Cards (top row):**

| # | Visual | How to Build |
|---|---|---|
| 1 | Add **Card** visual. Drag `Total Billed` measure into **Fields** well. Format: currency, 0 decimals. |
| 2 | Add **Card** visual. Drag `Total Paid` measure. Format: currency. |
| 3 | Add **Card** visual. Drag `Recovery Rate %` measure. Format: percentage, 1 decimal. |
| 4 | Add **Card** visual. Drag `Denial Rate %` measure. Format: percentage, 1 decimal. |
| 5 | Add **Card** visual. Drag `Total Underpayment` measure. Format: currency. |

Arrange all 5 cards in a row across the top.

**Recovery by Payer (middle-left):**

1. Add **Clustered bar chart**
2. Y-axis: `recovery_by_payer` > `payer_name`
3. X-axis: `recovery_by_payer` > `total_billed` and `total_paid`
4. Title: "Recovery by Payer"
5. Format: data labels ON, currency format

**Claims Aging (middle-right):**

1. Add **Donut chart**
2. Legend: `claims_aging` > `aging_bucket`
3. Values: `claims_aging` > `claim_count`
4. Title: "Claims by Aging Bucket"
5. Format: data labels ON showing percentage

**Payer Risk Table (bottom):**

1. Add **Table** visual
2. Columns: `payer_scorecard` > `payer_name`, `payment_rate_pct`, `denial_rate_pct`, `risk_tier`, `total_underpayment`
3. Conditional formatting on `risk_tier`: Format > Cell elements > Background color > Rules:
   - `low` = green (#27AE60)
   - `medium` = yellow (#F39C12)
   - `high` = red (#E74C3C)

---

### Page 2: Payer Analysis

Click **+** at the bottom to add a new page. Rename to `Payer Analysis`.

**KPI Cards (top):**

| # | Visual |
|---|---|
| 1 | Card: `High Risk Payer Count` |
| 2 | Card: `Total Revenue at Risk` (format: currency) |
| 3 | Card: `Weighted Recovery Rate` (format: percentage) |

**Payer Scatter Plot (middle):**

1. Add **Scatter chart**
2. X-axis: `payer_scorecard` > `payment_rate_pct`
3. Y-axis: `payer_scorecard` > `denial_rate_pct`
4. Size: `payer_scorecard` > `total_underpayment`
5. Legend: `payer_scorecard` > `risk_tier`
6. Details: `payer_scorecard` > `payer_name`
7. Title: "Payer Risk Matrix"

**Payer Detail Matrix (bottom):**

1. Add **Matrix** visual
2. Rows: `payer_scorecard` > `payer_name`
3. Values: `total_claims`, `total_billed`, `total_paid`, `payment_rate_pct`, `denial_rate_pct`, `total_underpayment`, `risk_tier`
4. Conditional formatting on `payment_rate_pct`: Background color gradient (red low, green high)

---

### Page 3: CPT Code Analysis

New page, rename to `CPT Code Analysis`.

**KPI Cards (top):**

| # | Visual |
|---|---|
| 1 | Card: `CPT Payment Efficiency` (format: percentage) |
| 2 | Card: `Avg Medicare Gap` (format: currency) |

**CPT Payment Bars (middle):**

1. Add **Clustered bar chart**
2. Y-axis: `cpt_analysis` > `cpt_code`
3. X-axis: `cpt_analysis` > `avg_paid` and `medicare_rate`
4. Title: "Avg Paid vs Medicare Rate by CPT"
5. Format: data labels ON

**CPT Detail Table (bottom):**

1. Add **Table** visual
2. Columns: `cpt_analysis` > `cpt_code`, `claim_count`, `total_billed`, `total_paid`, `payment_ratio_pct`, `medicare_rate`, `fair_health_rate`
3. Conditional formatting on `payment_ratio_pct`: Background color (red < 50%, yellow 50-80%, green > 80%)
4. Sort by `payment_ratio_pct` ascending (worst first)

---

### Page 4: Arbitration Pipeline

New page, rename to `Arbitration Pipeline`.

**KPI Cards (top):**

| # | Visual |
|---|---|
| 1 | Card: `Active Cases` |
| 2 | Card: `Pipeline Value` (format: currency) |
| 3 | Card: `Arbitration Eligible Count` |
| 4 | Card: `Arbitration Eligible Value` (format: currency) |

**Case Funnel (middle-left):**

1. Add **Funnel** chart
2. Group: `case_pipeline` > `status`
3. Values: `case_pipeline` > `case_count`
4. Title: "Case Pipeline"

**SLA Gauge (middle-right):**

1. Add **Gauge** visual
2. Value: `Avg SLA Compliance` measure
3. Minimum: 0, Maximum: 100, Target: 90
4. Format: conditional color (red < 70, yellow < 90, green >= 90)

**Eligible Claims Table (bottom):**

1. Add **Table** visual
2. Columns: `underpayment_detection` > `claim_id`, `payer_name`, `billed_amount`, `paid_amount`, `qpa_amount`, `underpayment_amount`, `underpayment_pct`, `arbitration_eligible`
3. Filter: `arbitration_eligible` = 1
4. Sort by `underpayment_amount` descending
5. Conditional formatting on `underpayment_pct`: data bars

---

### Page 5: Deadline Compliance

New page, rename to `Deadline Compliance`.

**KPI Cards (top):**

| # | Visual |
|---|---|
| 1 | Card: `Overall Compliance Rate` (format: percentage) |
| 2 | Card: `Total At Risk` |
| 3 | Card: `Total Missed` |

**Stacked Bar (middle):**

1. Add **Stacked bar chart**
2. Y-axis: `deadline_compliance` > `deadline_type`
3. X-axis: `deadline_compliance` > `met_count`, `missed_count`, `pending_count`, `at_risk_count`
4. Title: "Deadline Status by Type"
5. Legend colors: met = green, missed = red, pending = gray, at_risk = yellow

**Deadline Detail Table (bottom):**

1. Add **Table** visual
2. Columns: `deadline_compliance` > `deadline_type`, `total_deadlines`, `met_count`, `missed_count`, `pending_count`, `at_risk_count`, `compliance_pct`
3. Conditional formatting on `compliance_pct`: Background gradient (red low, green high)

---

## Step 4: Format & Theme

1. Click **View** > **Themes** in the toolbar
2. Select a dark theme or customize:
   - Background: `#0f1117`
   - Card background: `#1a1d27`
   - Text: `#e4e6eb`
   - Accent: `#6366f1`
3. Set all card titles to 12pt, values to 24pt bold
4. Enable drop shadows on cards for depth

---

## Step 5: Save & Share

1. Click **Save** in the toolbar
2. Name: `Medical Billing Arbitration Dashboard`
3. Save to the `medbill-test-lakehouse` workspace
4. To share: **File** > **Share** > enter email addresses or create a shareable link

---

## Quick Reference: Table → Visual Map

| Table | Page | Visuals |
|---|---|---|
| `financial_summary` | Executive Summary | 5 KPI cards |
| `recovery_by_payer` | Executive Summary | Clustered bar chart |
| `claims_aging` | Executive Summary | Donut chart |
| `payer_scorecard` | Executive Summary + Payer Analysis | Risk table, scatter, matrix |
| `cpt_analysis` | CPT Code Analysis | Bar chart, detail table |
| `case_pipeline` | Arbitration Pipeline | Funnel, SLA gauge |
| `underpayment_detection` | Arbitration Pipeline | Eligible claims table |
| `deadline_compliance` | Deadline Compliance | Stacked bar, detail table |
