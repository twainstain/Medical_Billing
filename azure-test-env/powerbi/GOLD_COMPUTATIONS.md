# Gold Layer Computations

How each metric in the Gold notebook (`nb_gold_aggregations`) is computed from Silver tables.

---

## Data Flow

```
Silver Tables (cleaned, joined)          Gold Tables (aggregated)
─────────────────────────────          ──────────────────────────
silver.claim_remittance  ──────┬──────▶  gold.recovery_by_payer
  (claim + remittance joined)  ├──────▶  gold.financial_summary
                               ├──────▶  gold.claims_aging
                               │
silver.claims  ────────────────┼──────▶  gold.cpt_analysis
                               │
silver.payers  ────────────────┤
                               │
silver.claim_remittance  ──────┴──────▶  gold.payer_scorecard

silver.cases  ─────────────────┬──────▶  gold.case_pipeline
silver.deadlines  ─────────────┘

silver.deadlines  ────────────────────▶  gold.deadline_compliance

silver.disputes  ─────────────────────▶  gold.underpayment_detection
```

---

## 1. gold.recovery_by_payer

**Source**: `silver.claim_remittance`
**Group by**: `payer_id`

| Output Column | Computation | PySpark Code |
|---|---|---|
| `payer_name` | Latest payer name per group | `spark_max("payer_name")` |
| `total_claims` | Count of claims per payer | `spark_count("*")` |
| `total_billed` | Sum of billed amounts | `spark_sum("total_billed")` |
| `total_paid` | Sum of paid amounts | `spark_sum("total_paid")` |
| `total_underpayment` | Sum of underpayments | `spark_sum("underpayment")` |
| `recovery_rate_pct` | % of billed that was paid | `total_paid / total_billed × 100` |
| `avg_payment_pct` | Average per-claim payment % | `AVG(total_paid / total_billed × 100)` per row |
| `denial_count` | Claims with denial codes | `spark_sum("has_denial")` |
| `denial_rate_pct` | % of claims with denials | `denial_count / total_claims × 100` |

**Example computation**:
```
Anthem Blue Cross: 3 claims
  Claim 1: billed=$850,  paid=$380  → underpayment=$470
  Claim 4: billed=$6500, paid=$2100 → underpayment=$4400
  Claim 9: billed=$650,  paid=$0    → underpayment=$650

  total_billed = 850 + 6500 + 650 = $8,000
  total_paid = 380 + 2100 + 0 = $2,480
  total_underpayment = 470 + 4400 + 650 = $5,520
  recovery_rate_pct = 2480 / 8000 × 100 = 31.00%
  denial_count = 0 (none had denial codes)
  denial_rate_pct = 0 / 3 × 100 = 0.00%
```

---

## 2. gold.cpt_analysis

**Source**: `silver.claim_remittance` + `silver.claims` + `silver.fee_schedule`
**Group by**: `cpt_code`

### Step 1: Explode CPT codes from claims

Each claim has a JSON array of CPT codes (e.g., `["99285","99283"]`). The notebook explodes this into one row per (claim_id, cpt_code):

```python
claims_cpt = claims
    .withColumn("cpt_arr", from_json(col("cpt_codes"), ArrayType(StringType())))
    .select("claim_id", explode("cpt_arr").alias("cpt_code"))
```

### Step 2: Proportional allocation

A claim with 2 CPT codes splits its financial amounts equally:

```python
# Count CPTs per claim
cpt_per_claim = claims_cpt.groupBy("claim_id").agg(count("*").alias("n_codes"))

# Allocate proportionally
per_code_billed = total_billed / n_codes
per_code_paid = total_paid / n_codes
```

**Example**:
```
Claim 1: total_billed=$850, CPTs=["99285","99283"] (2 codes)
  → 99285: per_code_billed = 850/2 = $425
  → 99283: per_code_billed = 850/2 = $425
```

### Step 3: Aggregate per CPT

| Output Column | Computation |
|---|---|
| `claim_count` | Number of claims using this CPT |
| `total_billed` | Sum of proportionally allocated billed amounts |
| `total_paid` | Sum of proportionally allocated paid amounts |
| `avg_billed` | Average billed per occurrence |
| `avg_paid` | Average paid per occurrence |
| `payment_ratio_pct` | `total_paid / total_billed × 100` |

### Step 4: Join benchmark rates

```python
medicare_rates = fee_schedule
    .filter(payer_id == "MEDICARE" AND is_current == 1)
    .select("cpt_code", "rate" as "medicare_rate")

fair_health_rates = fee_schedule
    .filter(payer_id == "FAIR_HEALTH" AND is_current == 1)
    .select("cpt_code", "rate" as "fair_health_rate")
```

If fee_schedule has no matching rate, the column is NULL.

---

## 3. gold.payer_scorecard

**Source**: `silver.claim_remittance` + `silver.payers`
**Group by**: `payer_id`

| Output Column | Computation |
|---|---|
| `payer_name` | Latest payer name |
| `total_claims` | Count of claims |
| `payment_rate_pct` | `SUM(total_paid) / SUM(total_billed) × 100` |
| `denial_rate_pct` | `SUM(has_denial) / COUNT(*) × 100` |
| `avg_underpayment` | `AVG(underpayment)` per claim |
| `total_underpayment` | `SUM(underpayment)` |
| `risk_tier` | Classification (see below) |
| `payer_type` | From payers table: commercial, medicare, medicaid, tricare |

### Risk tier logic

```python
risk_tier = CASE
    WHEN payment_rate_pct >= 80 AND denial_rate_pct <= 10 THEN "low"
    WHEN payment_rate_pct >= 50 OR denial_rate_pct <= 30  THEN "medium"
    ELSE "high"
```

**Example**:
```
Cigna: payment_rate=93.3%, denial_rate=0%
  → 93.3 >= 80 AND 0 <= 10 → risk_tier = "low"

Medicare Part B: payment_rate=0%, denial_rate=100%
  → 0 < 50 AND 100 > 30 → risk_tier = "high"
```

---

## 4. gold.financial_summary

**Source**: `silver.claim_remittance`
**Format**: Key-value pairs (metric_name, metric_value)

The notebook computes all KPIs in a single pass using `.agg()`, then pivots to rows:

```python
stats = claim_remittance.agg(
    count("*").alias("total_claims"),              # 60
    sum("total_billed").alias("total_billed"),     # $17,470
    sum("total_paid").alias("total_paid"),          # $5,740
    sum("underpayment").alias("total_underpayment"),# $11,730
    sum(WHEN total_paid > 0 THEN 1 ELSE 0).alias("paid_claims"),  # 50
    sum("has_denial").alias("denial_count")         # 10
)
```

Then derived metrics are computed in Python:

| Metric | Formula | Example |
|---|---|---|
| `total_claims` | Direct count | `60` |
| `total_billed` | Direct sum | `$17,470.00` |
| `total_paid` | Direct sum | `$5,740.00` |
| `total_underpayment` | Direct sum | `$11,730.00` |
| `recovery_rate_pct` | `total_paid / total_billed × 100` | `32.86%` |
| `paid_claims` | Claims where total_paid > 0 | `50` |
| `denial_count` | Claims with denial code | `10` |
| `denial_rate_pct` | `denial_count / total_claims × 100` | `16.67%` |
| `avg_billed_per_claim` | `total_billed / total_claims` | `$291.17` |
| `avg_underpayment_per_claim` | `total_underpayment / total_claims` | `$195.50` |

These are stored as rows (not columns) so Power BI can display each as an independent KPI card using a filter on `metric_name`.

---

## 5. gold.claims_aging

**Source**: `silver.claim_remittance`
**Group by**: `aging_bucket` (computed column)

### Step 1: Compute age and bucket

```python
age_days = DATEDIFF(current_date(), date_of_service)

aging_bucket = CASE
    WHEN age_days <= 30   THEN "0-30 days"
    WHEN age_days <= 60   THEN "31-60 days"
    WHEN age_days <= 90   THEN "61-90 days"
    WHEN age_days <= 180  THEN "91-180 days"
    ELSE "180+ days"
```

### Step 2: Aggregate per bucket

| Output Column | Computation |
|---|---|
| `claim_count` | Number of claims in bucket |
| `total_billed` | Sum of billed amounts in bucket |
| `total_unpaid` | Sum of underpayment in bucket |
| `pct_of_total` | `claim_count / total_all_claims × 100` |

**Example** (as of 2026-03-30):
```
Claim CLM-2026-0002: date_of_service=2026-02-20
  age = 38 days → bucket = "31-60 days"

Claim CLM-2025-0001: date_of_service=2025-06-15
  age = 288 days → bucket = "180+ days"
```

---

## 6. gold.case_pipeline

**Source**: `silver.cases` + `silver.deadlines`
**Group by**: `status`

### Step 1: Case aggregation

```python
cases.groupBy("status").agg(
    count("*").alias("case_count"),
    sum("total_billed").alias("total_billed"),
    sum("total_underpayment").alias("total_underpayment"),
    avg("age_days").alias("avg_age_days")
)
```

`age_days` was pre-computed in the Silver layer: `DATEDIFF(current_date, created_date)`

### Step 2: SLA compliance join

Deadlines are joined to cases to compute per-status compliance:

```python
sla = deadlines
    .join(cases on case_id)
    .groupBy(case_status)
    .agg(
        count("*").alias("total_dl"),
        sum(WHEN deadline_status = "met" THEN 1 ELSE 0).alias("met_dl")
    )
    .withColumn("sla_compliance_pct", met_dl / total_dl × 100)
```

**Example**:
```
Status "negotiation": 1 case
  Deadlines for case 1: open_negotiation=met, idr_initiation=met, evidence_submission=pending
  total_dl = 3, met_dl = 2
  sla_compliance_pct = 2/3 × 100 = 66.67%
```

---

## 7. gold.deadline_compliance

**Source**: `silver.deadlines`
**Group by**: `type` (deadline type)

| Output Column | Computation |
|---|---|
| `deadline_type` | Renamed from `type` column |
| `total_deadlines` | `COUNT(*)` per type |
| `met_count` | `SUM(WHEN status = "met" THEN 1 ELSE 0)` |
| `missed_count` | `SUM(WHEN status = "missed" THEN 1 ELSE 0)` |
| `pending_count` | `SUM(WHEN status = "pending" THEN 1 ELSE 0)` |
| `at_risk_count` | `SUM(WHEN is_at_risk = 1 THEN 1 ELSE 0)` |
| `compliance_pct` | `met_count / total_deadlines × 100` |

`is_at_risk` was pre-computed in Silver: `1 if days_remaining <= 5 AND status NOT IN (met, missed)`

**Example**:
```
open_negotiation: 5 deadlines
  met=4, missed=0, pending=0, alerted=1, at_risk=1
  compliance_pct = 4/5 × 100 = 80.00%

evidence_submission: 2 deadlines
  met=1, pending=1, at_risk=0
  compliance_pct = 1/2 × 100 = 50.00%
```

---

## 8. gold.underpayment_detection

**Source**: `silver.disputes`
**Filter**: `underpayment_amount > 0`

This table is NOT aggregated — it's per-claim detail for drill-through.

| Output Column | Computation |
|---|---|
| `claim_id` | Passthrough from disputes |
| `payer_id` | Passthrough |
| `payer_name` | Enriched in Silver from payers table |
| `provider_npi` | Enriched in Silver from claims table |
| `date_of_service` | Enriched in Silver from claims table |
| `billed_amount` | From dispute record |
| `paid_amount` | From dispute record |
| `qpa_amount` | Qualifying Payment Amount from dispute |
| `underpayment_amount` | `billed_amount - paid_amount` (computed column in Azure SQL) |
| `underpayment_pct` | `underpayment_amount / billed_amount × 100` (computed in Gold) |
| `arbitration_eligible` | `1 if underpayment > $25 AND billed > QPA, else 0` (computed in Gold) |
| `dispute_status` | Current dispute status |

### Arbitration eligibility logic

```python
arbitration_eligible = CASE
    WHEN underpayment_amount > 25
     AND billed_amount > COALESCE(qpa_amount, 0)
    THEN 1
    ELSE 0
```

**Why these thresholds**:
- `> $25`: IDR filing costs ~$50 administrative fee, so amounts below $25 have negative ROI
- `billed > QPA`: Under the No Surprises Act, if the provider's charge exceeds the median in-network rate (QPA), there's a strong case that the payer underpaid

**Example**:
```
Dispute on Claim 4 (knee replacement):
  billed_amount = $6,500
  paid_amount = $2,100
  qpa_amount = $3,350
  underpayment_amount = $6,500 - $2,100 = $4,400
  underpayment_pct = 4400 / 6500 × 100 = 67.69%
  arbitration_eligible: $4,400 > $25 ✓ AND $6,500 > $3,350 ✓ → 1 (eligible)
```

---

## Silver → Gold Column Mapping

Where each Gold column originates:

| Gold Table | Gold Column | Silver Source | Silver Column |
|---|---|---|---|
| recovery_by_payer | payer_name | claim_remittance | payer_name (from payers via claims) |
| recovery_by_payer | total_billed | claim_remittance | total_billed (from claims) |
| recovery_by_payer | total_paid | claim_remittance | total_paid (aggregated from remittances) |
| recovery_by_payer | has_denial | claim_remittance | has_denial (1 if any remittance has denial_code) |
| cpt_analysis | cpt_code | claims | cpt_codes JSON array (exploded) |
| cpt_analysis | medicare_rate | fee_schedule | rate WHERE payer=MEDICARE AND is_current=1 |
| payer_scorecard | payer_type | payers | type (commercial, medicare, etc.) |
| financial_summary | all metrics | claim_remittance | Aggregated in single pass |
| claims_aging | age_days | claim_remittance | DATEDIFF(today, date_of_service) |
| case_pipeline | age_days | cases | DATEDIFF(today, created_date) — from Silver |
| case_pipeline | sla_compliance | deadlines | met_count / total per case status |
| deadline_compliance | is_at_risk | deadlines | days_remaining <= 5 — from Silver |
| underpayment_detection | underpayment_pct | disputes | Computed in Gold: amount / billed × 100 |
| underpayment_detection | arbitration_eligible | disputes | Computed in Gold: threshold check |
