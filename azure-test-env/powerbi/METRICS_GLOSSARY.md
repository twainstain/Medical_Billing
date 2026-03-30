# Metrics Glossary

Reference guide for all Power BI report metrics — what they mean, how they're computed, and example values from the Gold layer.

---

## Financial KPIs (from `financial_summary`)

### Total Billed
- **What**: Total dollar amount billed to insurance payers across all claims
- **Formula**: `SUM(claims.total_billed)`
- **DAX**: `CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_billed")`
- **Example**: `$17,470.00` (60 claims)
- **Use**: Baseline for all recovery calculations

### Total Paid
- **What**: Total dollar amount actually received from payers
- **Formula**: `SUM(remittances.paid_amount)` aggregated per claim
- **DAX**: `CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_paid")`
- **Example**: `$5,740.00`
- **Use**: Compare against Total Billed to see collection shortfall

### Total Underpayment
- **What**: Gap between what was billed and what was paid
- **Formula**: `Total Billed - Total Paid`
- **DAX**: `CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "total_underpayment")`
- **Example**: `$11,730.00`
- **Use**: Total recovery opportunity — this is the money payers owe

### Recovery Rate %
- **What**: Percentage of billed amounts actually collected
- **Formula**: `(Total Paid / Total Billed) × 100`
- **DAX**: `CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "recovery_rate_pct")`
- **Example**: `32.86%` — means 67% of billed amounts are uncollected
- **Thresholds**: Green >= 80%, Yellow 50-80%, Red < 50%

### Denial Rate %
- **What**: Percentage of claims that received a denial code from the payer
- **Formula**: `(Claims with denial code / Total Claims) × 100`
- **DAX**: `CALCULATE(MAX(financial_summary[metric_value]), financial_summary[metric_name] = "denial_rate_pct")`
- **Example**: `16.67%` — 1 out of 6 claims had a denial
- **Thresholds**: Green <= 10%, Yellow 10-30%, Red > 30%
- **Common denial codes**: CO-45 (exceeds fee schedule), CO-197 (no preauthorization), CO-50 (not medically necessary)

### Total Claims
- **What**: Number of distinct claims in the system
- **Formula**: `COUNT(DISTINCT claim_id)` from claim_remittance
- **Example**: `60`

---

## Payer Metrics (from `payer_scorecard` and `recovery_by_payer`)

### Payment Rate %
- **What**: How much of billed amounts a specific payer has paid
- **Formula**: `(Payer Total Paid / Payer Total Billed) × 100`
- **Source column**: `payer_scorecard.payment_rate_pct`
- **Example**: Anthem Blue Cross = 44.71%, Medicare = 0%
- **Use**: Identify chronic underpayers

### Risk Tier
- **What**: Payer risk classification based on payment behavior
- **Formula**:
  - `low` = payment_rate >= 80% AND denial_rate <= 10%
  - `medium` = payment_rate >= 50% OR denial_rate <= 30%
  - `high` = everything else
- **Source column**: `payer_scorecard.risk_tier`
- **Example**: Anthem = medium, Cigna = low (paid in full)
- **Color coding**: Green (low), Yellow (medium), Red (high)

### High Risk Payer Count
- **What**: Number of payers classified as "high" risk
- **Formula**: `COUNT(payers WHERE risk_tier = 'high')`
- **DAX**: `CALCULATE(COUNTROWS(payer_scorecard), payer_scorecard[risk_tier] = "high")`
- **Example**: `2` high risk payers

### Total Revenue at Risk
- **What**: Dollar amount of underpayments from high-risk payers
- **Formula**: `SUM(underpayment WHERE risk_tier = 'high')`
- **DAX**: `CALCULATE(SUM(payer_scorecard[total_underpayment]), payer_scorecard[risk_tier] = "high")`
- **Example**: `$3,600.00`
- **Use**: Quantifies exposure from problematic payers

### Weighted Recovery Rate
- **What**: Overall recovery rate weighted by dollar amount (not per-claim)
- **Formula**: `SUM(all payers total_paid) / SUM(all payers total_billed) × 100`
- **DAX**: `DIVIDE(SUM(recovery_by_payer[total_paid]), SUM(recovery_by_payer[total_billed]), 0) * 100`
- **Example**: `32.86%`
- **Note**: Differs from simple average — a $6,500 claim weighs more than a $320 claim

---

## CPT Code Metrics (from `cpt_analysis`)

### CPT Code
- **What**: Current Procedural Terminology — standardized codes for medical procedures
- **Examples**:
  - `99283` = Emergency department visit (moderate severity)
  - `99285` = Emergency department visit (high severity)
  - `27447` = Total knee replacement
  - `93010` = Electrocardiogram (ECG)
  - `00142` = Anesthesia for procedures on eye

### Payment Ratio %
- **What**: Percentage of billed amount actually paid for this CPT code
- **Formula**: `(SUM paid for CPT / SUM billed for CPT) × 100`
- **Source column**: `cpt_analysis.payment_ratio_pct`
- **Example**: CPT 99285 = 35.5% (severely underpaid), CPT 93010 = 66.3%
- **Use**: Identify which procedures are most underpaid

### Medicare Rate
- **What**: CMS Physician Fee Schedule rate — the Medicare benchmark for this procedure
- **Formula**: Current rate from `fee_schedule` WHERE payer = Medicare AND is_current = 1
- **Source column**: `cpt_analysis.medicare_rate`
- **Example**: CPT 99285 = $395.00, CPT 99283 = $162.00
- **Use**: Under the No Surprises Act (NSA), Medicare rate is a reference point for arbitration

### FAIR Health Rate
- **What**: 80th percentile of usual and customary charges in the geographic area
- **Source column**: `cpt_analysis.fair_health_rate`
- **Example**: Not populated in current seed data (NULL)
- **Use**: Another benchmark for IDR/arbitration — often higher than Medicare

### Avg Medicare Gap
- **What**: Average difference between what was paid and the Medicare rate
- **Formula**: `AVG(avg_paid - medicare_rate)` for CPTs with Medicare rates
- **DAX**: `AVERAGEX(FILTER(cpt_analysis, NOT(ISBLANK(cpt_analysis[medicare_rate]))), cpt_analysis[avg_paid] - cpt_analysis[medicare_rate])`
- **Example**: `-$150.00` (negative means paid below Medicare rate)

### CPT Payment Efficiency
- **What**: Overall payment efficiency across all CPT codes
- **Formula**: `SUM(total_paid all CPTs) / SUM(total_billed all CPTs) × 100`
- **DAX**: `DIVIDE(SUM(cpt_analysis[total_paid]), SUM(cpt_analysis[total_billed]), 0) * 100`
- **Example**: `32.9%`

---

## Claims Aging Metrics (from `claims_aging`)

### Aging Bucket
- **What**: How old a claim is based on days since date of service
- **Buckets**:
  - `0-30 days` — Normal processing window
  - `31-60 days` — Follow-up needed, check ERA status
  - `61-90 days` — Escalation required, approaching timely filing limits
  - `91-180 days` — At risk of timely filing denial
  - `180+ days` — Likely needs appeal or arbitration

### Claim Count per Bucket
- **Source column**: `claims_aging.claim_count`
- **Example**: 0-30 days = 2, 31-60 days = 0, 91-180 days = 4, 180+ days = 54

### Aging Over 90 Days %
- **What**: Percentage of claims older than 90 days
- **Formula**: `(Claims in 91-180 + 180+ buckets) / Total Claims × 100`
- **DAX**: `DIVIDE(CALCULATE(SUM(claims_aging[claim_count]), claims_aging[aging_bucket] IN {"91-180 days", "180+ days"}), SUM(claims_aging[claim_count]), 0) * 100`
- **Example**: `96.7%` — most claims in current data are older (historical seed data)
- **Action**: Claims over 90 days need immediate attention or risk forfeiting recovery rights

---

## Arbitration Pipeline Metrics (from `case_pipeline`)

### Case Status
- **What**: Current stage in the NSA arbitration state machine
- **Stages** (in order):
  1. `open` — New case, not yet reviewed
  2. `in_review` — Analyst reviewing claim and payer response
  3. `negotiation` — Open negotiation period (30 days from dispute filing)
  4. `idr_initiated` — IDR process started (4 business days after negotiation fails)
  5. `idr_submitted` — Evidence submitted to IDR entity (10 business days)
  6. `decided` — IDR entity issued decision (30 business days)
  7. `closed` — Case resolved (won, lost, settled, or withdrawn)

### Active Cases
- **What**: Cases not yet closed or decided
- **Formula**: `SUM(case_count) WHERE status NOT IN ('closed', 'decided')`
- **Example**: `4` active cases

### Pipeline Value
- **What**: Total dollar amount at stake across all cases
- **Formula**: `SUM(total_underpayment)` across all case statuses
- **DAX**: `SUM(case_pipeline[total_underpayment])`
- **Example**: `$14,730.00`

### SLA Compliance %
- **What**: Percentage of regulatory deadlines met for cases in each status
- **Formula**: `(Deadlines met / Total deadlines) × 100` per case status
- **Source column**: `case_pipeline.sla_compliance_pct`
- **Example**: negotiation = 100%, idr_submitted = 75%
- **Thresholds**: Green >= 90%, Yellow 70-90%, Red < 70%

---

## Underpayment Detection Metrics (from `underpayment_detection`)

### Underpayment Amount
- **What**: Dollar amount the payer underpaid on a specific claim
- **Formula**: `billed_amount - paid_amount`
- **Source column**: `underpayment_detection.underpayment_amount`
- **Example**: Claim CLM-2025-0004 = $4,400.00 underpaid

### Underpayment %
- **What**: What percentage of the billed amount was underpaid
- **Formula**: `(underpayment_amount / billed_amount) × 100`
- **Source column**: `underpayment_detection.underpayment_pct`
- **Example**: 67.7% — payer only paid 32.3% of the bill

### QPA (Qualifying Payment Amount)
- **What**: Median in-network rate for the same service in the same area
- **Source column**: `underpayment_detection.qpa_amount`
- **Example**: $3,350.00 for knee replacement (CPT 27447)
- **Use**: Under the No Surprises Act, QPA is the starting point for determining fair payment. If billed > QPA, the provider has a strong arbitration case.

### Arbitration Eligible
- **What**: Whether this claim qualifies for IDR (Independent Dispute Resolution)
- **Formula**: `1 if underpayment_amount > $25 AND billed_amount > QPA, else 0`
- **Source column**: `underpayment_detection.arbitration_eligible`
- **Example**: 37 out of 37 disputes are eligible (all meet the threshold)
- **Why $25**: De minimis threshold — IDR filing costs ~$50, so amounts below $25 aren't worth pursuing

### Arbitration Eligible Count
- **What**: Total number of claims eligible for arbitration
- **DAX**: `CALCULATE(COUNTROWS(underpayment_detection), underpayment_detection[arbitration_eligible] = 1)`
- **Example**: `37`

### Arbitration Eligible Value
- **What**: Total dollar amount recoverable through arbitration
- **DAX**: `CALCULATE(SUM(underpayment_detection[underpayment_amount]), underpayment_detection[arbitration_eligible] = 1)`
- **Example**: `$14,730.00`

---

## Deadline Compliance Metrics (from `deadline_compliance`)

### Deadline Types (NSA regulatory)
- **What**: Mandatory deadlines in the IDR process
- **Types**:
  - `open_negotiation` — 30 days from dispute filing to begin negotiation
  - `idr_initiation` — 4 business days after negotiation fails to initiate IDR
  - `entity_selection` — Select IDR entity within required timeframe
  - `evidence_submission` — 10 business days after IDR initiated to submit evidence
  - `decision` — 30 business days after IDR initiated for entity to issue ruling

### Compliance %
- **What**: Percentage of deadlines met for each deadline type
- **Formula**: `(met_count / total_deadlines) × 100`
- **Source column**: `deadline_compliance.compliance_pct`
- **Example**: open_negotiation = 80%, idr_initiation = 100%

### At Risk Count
- **What**: Deadlines due within 5 days that are still pending
- **Source column**: `deadline_compliance.at_risk_count`
- **Example**: `2` at-risk deadlines
- **Action**: These need immediate analyst attention to avoid forfeiture

### Met / Missed / Pending
- **Met**: Deadline was completed before the due date
- **Missed**: Due date passed without completion — may forfeit arbitration rights
- **Pending**: Not yet due, still within the allowed timeframe

---

## Conditional Formatting Reference

| Metric | Green | Yellow | Red |
|---|---|---|---|
| Recovery Rate % | >= 80% | 50-80% | < 50% |
| Denial Rate % | <= 10% | 10-30% | > 30% |
| Payment Rate % | >= 80% | 50-80% | < 50% |
| SLA Compliance % | >= 90% | 70-90% | < 70% |
| Risk Tier | low | medium | high |
| Underpayment % | < 10% | 10-40% | > 40% |
| Aging Bucket | 0-30 days | 31-90 days | 91+ days |

---

## Data Flow

```
Azure SQL (OLTP)
    │
    ▼ ADF CDC Pipeline (every 15 min)
Bronze (raw CDC events in Delta Lake)
    │
    ▼ nb_silver_transforms notebook
Silver (cleaned, joined, deduplicated)
    │
    ▼ nb_gold_aggregations notebook
Gold (8 pre-aggregated tables)
    │
    ├──▶ Power BI (Direct Lake) ── this report
    └──▶ AI Agent (SQL queries)
```

Metrics refresh automatically when the pipeline runs. Direct Lake picks up new data without manual refresh.
