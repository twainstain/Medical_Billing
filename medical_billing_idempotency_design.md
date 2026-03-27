# Medical Billing Arbitration — Idempotency in the Data Pipeline

> **Author:** Claude (Opus 4.6) — March 2026
> **Parent docs:**
> - `medical_billing_arbitration_future_architecture.md` — Overall architecture
> - `medical_billing_ingestion_layer_design.md` — Ingestion layer deep-dive

---

## Table of Contents

1. [Why Idempotency Matters](#1-why-idempotency-matters)
2. [The Problem: At-Least-Once Delivery](#2-the-problem-at-least-once-delivery)
3. [Core Principle](#3-core-principle)
4. [Idempotency Patterns by Source](#4-idempotency-patterns-by-source)
   - 4.1 [Claims (EDI 837) — Business Key + Frequency Code](#41-claims-edi-837--business-key--frequency-code)
   - 4.2 [Remittances / ERA (EDI 835) — Trace Number](#42-remittances--era-edi-835--trace-number)
   - 4.3 [Unstructured Documents (EOB, Contracts, Uploads) — Content Hash](#43-unstructured-documents-eob-contracts-uploads--content-hash)
   - 4.4 [Patient Data (FHIR) — Upsert (SCD Type 1)](#44-patient-data-fhir--upsert-scd-type-1)
   - 4.5 [Fee Schedules — SCD Type 2 MERGE](#45-fee-schedules--scd-type-2-merge)
   - 4.6 [Provider Data (NPPES) — SCD Type 2 MERGE](#46-provider-data-nppes--scd-type-2-merge)
   - 4.7 [Historical Backfill — Composite Key](#47-historical-backfill--composite-key)
5. [SCD Type 2 MERGE — Full Worked Example](#5-scd-type-2-merge--full-worked-example)
6. [Where the Check Happens — Architecture Pattern](#6-where-the-check-happens--architecture-pattern)
7. [Edge Cases and Race Conditions](#7-edge-cases-and-race-conditions)
8. [Summary Matrix](#8-summary-matrix)

---

## 1. Why Idempotency Matters

In a medical billing arbitration platform, data accuracy is not a convenience — it is a legal requirement. Arbitration cases hinge on proving:

- What amount was billed
- What amount was paid
- What the contracted or benchmark rate was **at the time of service**
- What evidence was submitted

If the same claim is ingested twice, billed amounts double. If the same remittance is counted twice, paid amounts inflate and underpayment calculations are wrong. If the same fee schedule load runs twice, SCD Type 2 history gets phantom rows that corrupt point-in-time rate lookups.

**Every one of these errors can cause a case to be filed incorrectly, an arbitration argument to be wrong, or an audit to fail.**

Idempotency guarantees that no matter how many times a message, file, or event is processed, the database converges to the same correct state.

---

## 2. The Problem: At-Least-Once Delivery

In distributed systems, **at-least-once delivery** is the standard guarantee. Every component in our pipeline can deliver the same input more than once:

```
┌────────────────────────────┬─────────────────────────────────────────────────────────┐
│ Component                  │ Why Duplicates Happen                                   │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ Azure Event Hub            │ Consumer crashes after processing but before             │
│                            │ checkpointing offset → message replayed from last        │
│                            │ checkpoint                                               │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ Azure Functions            │ Function times out or throws → host retries the          │
│                            │ invocation (configurable in host.json, default: 3)       │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ Blob trigger               │ Blob triggers are eventually consistent; the same        │
│                            │ blob may trigger the function more than once              │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ SFTP poller (timer)        │ Timer function polls every 15 min; if the watermark      │
│                            │ update fails after downloading, the same file is          │
│                            │ re-downloaded on the next poll                            │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ ADF pipeline rerun         │ A failed pipeline is retried from the beginning,         │
│                            │ reloading the same data into the staging table            │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ FHIR webhook               │ EHR system retries webhook delivery if it doesn't        │
│                            │ receive a 200 response within its timeout window          │
├────────────────────────────┼─────────────────────────────────────────────────────────┤
│ Human double-upload        │ Analyst accidentally uploads the same contract or EOB     │
│                            │ PDF twice from the Case Management UI                    │
└────────────────────────────┴─────────────────────────────────────────────────────────┘
```

**Exactly-once delivery is not achievable in distributed systems.** Instead, we design for **exactly-once processing** by making every write operation idempotent.

---

## 3. Core Principle

> **Processing the same input N times must produce the exact same database state as processing it once.**

This is achieved by:

1. **Deriving a natural idempotency key from the source data** — never from our system (no auto-increment IDs or UUIDs as dedup keys, because a retry generates a new UUID but carries the same business data)
2. **Checking the key before writing** — the check must be atomic with the write (same transaction) or use database constraints as a safety net
3. **Distinguishing true duplicates from intentional resubmissions** — a corrected claim is not a duplicate; a new SCD Type 2 rate is not a duplicate of the old rate

---

## 4. Idempotency Patterns by Source

### 4.1 Claims (EDI 837) — Business Key + Frequency Code

**Idempotency key:** `claim_id` (CLM01 segment)
**Discriminator:** `frequency_code` (CLM05-3)

This is the most nuanced pattern because the same `claim_id` can legitimately arrive multiple times with different intent:

```
┌───────────────────┬────────┬───────────────────────────────────────────────┐
│ Frequency Code    │ Value  │ Meaning                                       │
├───────────────────┼────────┼───────────────────────────────────────────────┤
│ Original          │ "1"    │ First submission of this claim                │
│ Replacement       │ "7"    │ Corrected claim — replaces the original       │
│ Void              │ "8"    │ Cancel the claim entirely                     │
└───────────────────┴────────┴───────────────────────────────────────────────┘
```

**Decision logic:**

```python
def resolve_duplicate(existing_status: str | None, frequency_code: str) -> str:
    """
    Given current state in OLTP and the incoming frequency code,
    determine the correct action.

    Returns: "insert", "update", "void", or "skip"
    """
    FREQ_CODE_MAP = {"1": "original", "7": "replacement", "8": "void"}
    action = FREQ_CODE_MAP.get(frequency_code, "original")

    if existing_status is None:
        # Never seen this claim_id before
        return "insert"

    if action == "replacement":
        # Payer sent a corrected version — update the existing claim
        return "update"

    if action == "void":
        # Payer voided the claim — mark as voided, don't delete
        return "void"

    # Same claim_id, same frequency code "1" (original) — true duplicate
    return "skip"
```

**Scenario walkthrough:**

```
Timeline:
─────────────────────────────────────────────────────────────────────────

T1: Claim CLM-1001 arrives (freq=1)
    → existing_status = None → "insert"
    → INSERT INTO claims (claim_id='CLM-1001', status='received', ...)

T2: Azure Function crashes, Event Hub replays CLM-1001 (freq=1)
    → existing_status = 'received' → action = 'original' → "skip"
    → No change to database ✓

T3: Payer sends correction for CLM-1001 (freq=7, new billed amount)
    → existing_status = 'received' → action = 'replacement' → "update"
    → UPDATE claims SET total_billed=..., status='corrected' WHERE claim_id='CLM-1001'

T4: Payer voids CLM-1001 (freq=8)
    → existing_status = 'corrected' → action = 'void' → "void"
    → UPDATE claims SET status='voided' WHERE claim_id='CLM-1001'
```

**Database safety net:**

```sql
-- Unique constraint prevents insert race conditions
ALTER TABLE claims ADD CONSTRAINT uq_claims_claim_id UNIQUE (claim_id);
```

If two Function instances process the same message simultaneously, one INSERT succeeds and the other hits the unique constraint → caught, logged, no duplicate.

---

### 4.2 Remittances / ERA (EDI 835) — Trace Number

**Idempotency key:** `trace_number` (TRN02) + `payer_id`

Every 835 file contains a TRN segment with a trace number unique per payer. Unlike claims, there is no "replacement" concept — a payment correction from the payer comes as a **new 835 with a new trace number**.

```python
# Simple existence check — no frequency code logic needed
cursor.execute("""
    SELECT remit_id FROM remittances
    WHERE trace_number = ? AND payer_id = ?
""", remit["trace_number"], remit["payer_id"])

if cursor.fetchone():
    logging.info(f"Duplicate ERA skipped: trace={remit['trace_number']}")
    return  # Already processed — idempotent skip

# First time seeing this trace number → insert
cursor.execute("""
    INSERT INTO remittances (claim_id, trace_number, payer_id, era_date,
                             paid_amount, allowed_amount, adjustment_reason)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""", ...)
```

**Why append-only works here:**

A claim may have multiple legitimate remittances (partial payment, then adjustment, then final payment). Each has a different trace number, so they naturally insert as separate rows. The underpayment is computed at query time:

```sql
-- Underpayment = billed - sum(all payments for this claim)
SELECT c.total_billed - ISNULL(SUM(r.paid_amount), 0) AS underpayment
FROM claims c
LEFT JOIN remittances r ON r.claim_id = c.claim_id
WHERE c.claim_id = 'CLM-1001'
GROUP BY c.total_billed;
```

**Database safety net:**

```sql
ALTER TABLE remittances
ADD CONSTRAINT uq_remit_trace_payer UNIQUE (trace_number, payer_id);
```

---

### 4.3 Unstructured Documents (EOB, Contracts, Uploads) — Content Hash

**Idempotency key:** `content_hash` (SHA-256 of file bytes)

For unstructured documents there is no business key embedded in the content. The file itself is the key:

```python
import hashlib

content_hash = hashlib.sha256(pdf_bytes).hexdigest()

cursor.execute(
    "SELECT artifact_id FROM evidence_artifacts WHERE content_hash = ?",
    content_hash
)
if cursor.fetchone():
    # Exact same file already processed
    return  # Skip (or return 409 Conflict to the UI)
```

**What this catches:**
- Same file uploaded twice by an analyst (accidental double-click)
- Blob trigger firing twice for the same upload
- Same EOB arriving via both email and portal upload

**What this does NOT catch:**
- Same payment information in two *different* files (e.g., a scanned photocopy vs. the original digital PDF). These have different bytes → different hashes. This is a **semantic dedup** problem handled downstream by claim-matching logic, not at the ingestion level.

**Database safety net:**

```sql
CREATE UNIQUE INDEX uix_evidence_content_hash
ON evidence_artifacts (content_hash)
WHERE content_hash IS NOT NULL;
```

---

### 4.4 Patient Data (FHIR) — Upsert (SCD Type 1)

**Idempotency key:** `patient_id` (MRN from FHIR Patient.identifier)

Patient demographics use **SCD Type 1** (overwrite with latest). The upsert pattern is naturally idempotent:

```python
cursor.execute("SELECT patient_id FROM patients WHERE patient_id = ?",
               patient["patient_id"])

if cursor.fetchone():
    # EXISTS → UPDATE (overwrite with latest demographics)
    cursor.execute("""
        UPDATE patients
        SET first_name = ?, last_name = ?, dob = ?, address_line = ?,
            city = ?, state = ?, zip = ?, insurance_id = ?, last_updated = ?
        WHERE patient_id = ?
    """, patient["first_name"], patient["last_name"], patient["dob"],
         patient["address_line"], patient["city"], patient["state"],
         patient["zip"], patient["insurance_id"],
         datetime.utcnow(), patient["patient_id"])
else:
    # NOT EXISTS → INSERT
    cursor.execute("""
        INSERT INTO patients (patient_id, first_name, last_name, dob, ...)
        VALUES (?, ?, ?, ?, ...)
    """, ...)
```

**Why this is idempotent:**
- Same webhook, same data, processed twice → UPDATE sets the same values = no-op
- Same webhook, but data changed between retries (race condition) → last-write-wins, which is the correct SCD Type 1 behavior
- Different webhook for same patient (address change) → UPDATE overwrites old values, which is the intended behavior

**No history needed** because arbitration never asks "what was the patient's address 6 months ago?" — it asks about rates and payments, not demographics.

---

### 4.5 Fee Schedules — SCD Type 2 MERGE

**Idempotency key:** `payer_id` + `cpt_code` + `modifier` + `geo_region` + `valid_from`

This is the most critical idempotency pattern in the system because fee schedule history is **legal evidence** in arbitration. A duplicate SCD Type 2 row creates a phantom rate period that corrupts point-in-time lookups.

Full worked example in [Section 5](#5-scd-type-2-merge--full-worked-example).

---

### 4.6 Provider Data (NPPES) — SCD Type 2 MERGE

**Idempotency key:** `npi`
**Change-detection columns:** `specialty_taxonomy`, `status`, `state`

Same SCD Type 2 pattern as fee schedules. A new historical row is only created when a tracked attribute actually changes. Rerunning the same monthly NPPES file produces zero new rows.

Full example included in [Section 5](#5-scd-type-2-merge--full-worked-example).

---

### 4.7 Historical Backfill — Composite Key

**Idempotency key:** `claim_id` + `date_of_service` + `provider_npi` + `total_billed`

Backfill data may come from multiple legacy systems with no single reliable identifier. The composite key provides dedup across sources:

```python
cursor.execute("""
    SELECT claim_id FROM claims
    WHERE claim_id = ? AND date_of_service = ?
      AND provider_npi = ? AND total_billed = ?
""", claim["claim_id"], claim["date_of_service"],
     claim["provider_npi"], claim["total_billed"])

if cursor.fetchone():
    return  # Exact match — already loaded from this or another source

# Additional safeguard: mark as historical so workflow engine ignores it
cursor.execute("""
    INSERT INTO claims (claim_id, date_of_service, provider_npi,
                        total_billed, status, source)
    VALUES (?, ?, ?, ?, 'historical', 'backfill')
""", ...)
```

The `source = 'backfill'` flag ensures the workflow engine doesn't fire deadline alerts for a 2023 claim loaded in 2026.

---

## 5. SCD Type 2 MERGE — Full Worked Example

SCD Type 2 (Slowly Changing Dimension Type 2) preserves **full history** by never updating an existing row. Instead, when a value changes, the old row is "closed" (its `valid_to` date is set) and a new row is inserted with the updated value.

This is critical for fee schedules and provider data because arbitration requires proving **what the rate or provider status was at a specific date of service**.

### 5.1 The Fee Schedule Table Schema

```sql
CREATE TABLE fee_schedule (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    payer_id        VARCHAR(50)   NOT NULL,
    cpt_code        VARCHAR(10)   NOT NULL,
    modifier        VARCHAR(10)   NOT NULL DEFAULT '',
    geo_region      VARCHAR(20)   NOT NULL,
    rate            DECIMAL(10,2) NOT NULL,
    rate_type       VARCHAR(30)   NOT NULL,  -- 'medicare_pfs', 'fair_health_p80', 'contracted'
    valid_from      DATE          NOT NULL,
    valid_to        DATE          NOT NULL DEFAULT '9999-12-31',
    is_current      BIT           NOT NULL DEFAULT 1,
    source          VARCHAR(100),
    loaded_date     DATETIME2     NOT NULL DEFAULT GETUTCDATE(),

    -- Composite key for idempotency: prevents duplicate SCD rows
    CONSTRAINT uq_fee_schedule_natural_key
        UNIQUE (payer_id, cpt_code, modifier, geo_region, valid_from, rate_type)
);

CREATE INDEX ix_fee_schedule_lookup
ON fee_schedule (payer_id, cpt_code, modifier, geo_region, is_current)
INCLUDE (rate, valid_from, valid_to);
```

### 5.2 The Staging Table

ADF loads raw source data here first. It is truncated before each load.

```sql
CREATE TABLE stg_fee_schedule (
    cpt_code        VARCHAR(10)   NOT NULL,
    modifier        VARCHAR(10),
    geo_region      VARCHAR(20)   NOT NULL,
    rate            DECIMAL(10,2) NOT NULL,
    rate_type       VARCHAR(30)   NOT NULL,
    effective_date  DATE          NOT NULL
);
```

### 5.3 Initial State — Before Any Load

```
fee_schedule table: (empty)
┌────┬──────────┬──────────┬──────────┬──────┬──────────────┬────────────┬────────────┬─────────┐
│ id │ payer_id │ cpt_code │ geo_reg  │ rate │ rate_type    │ valid_from │ valid_to   │ current │
├────┴──────────┴──────────┴──────────┴──────┴──────────────┴────────────┴────────────┴─────────┤
│ (no rows)                                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.4 First Load — Q1 2025 Medicare Rates

ADF downloads the CMS Physician Fee Schedule file and loads it into `stg_fee_schedule`:

```
stg_fee_schedule:
┌──────────┬──────────┬──────────┬────────┬──────────────┬────────────────┐
│ cpt_code │ modifier │ geo_reg  │ rate   │ rate_type    │ effective_date │
├──────────┼──────────┼──────────┼────────┼──────────────┼────────────────┤
│ 99213    │ (null)   │ CA-01    │ 120.00 │ medicare_pfs │ 2025-01-01     │
│ 99214    │ (null)   │ CA-01    │ 175.00 │ medicare_pfs │ 2025-01-01     │
│ 99213    │ 25       │ CA-01    │ 132.00 │ medicare_pfs │ 2025-01-01     │
└──────────┴──────────┴──────────┴────────┴──────────────┴────────────────┘
```

The SCD Type 2 MERGE stored procedure runs:

```sql
CREATE PROCEDURE usp_merge_fee_schedule_scd2
    @payer_id VARCHAR(50) = 'MEDICARE'
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    -- ═══════════════════════════════════════════════════════════════════
    -- STEP 1: Close existing current rows where the rate has CHANGED
    -- ═══════════════════════════════════════════════════════════════════
    -- Find rows in fee_schedule that:
    --   (a) are currently active (is_current = 1)
    --   (b) have a matching key in staging
    --   (c) BUT the rate is different
    -- For these rows: set valid_to to the day before the new effective date,
    -- and mark them as no longer current.

    UPDATE fs
    SET fs.valid_to   = DATEADD(DAY, -1, stg.effective_date),
        fs.is_current = 0
    FROM fee_schedule fs
    INNER JOIN stg_fee_schedule stg
        ON  fs.cpt_code   = stg.cpt_code
        AND fs.modifier   = ISNULL(stg.modifier, '')
        AND fs.geo_region = stg.geo_region
        AND fs.rate_type  = stg.rate_type
    WHERE fs.payer_id   = @payer_id
      AND fs.is_current = 1
      AND fs.rate       <> stg.rate;       -- Only if rate actually changed

    -- Rows affected: count for audit logging
    DECLARE @closed_count INT = @@ROWCOUNT;

    -- ═══════════════════════════════════════════════════════════════════
    -- STEP 2: Insert NEW or CHANGED rows
    -- ═══════════════════════════════════════════════════════════════════
    -- Insert a row from staging if there is NO matching current row
    -- in fee_schedule with the SAME rate. This handles:
    --   (a) Brand new CPT codes → no match at all → insert
    --   (b) Changed rates → old row was closed in Step 1, so no current
    --       match → insert the new rate
    --   (c) Unchanged rates → current row still exists with same rate
    --       → NOT EXISTS matches → SKIP (idempotent!)

    INSERT INTO fee_schedule
        (payer_id, cpt_code, modifier, geo_region, rate, rate_type,
         valid_from, valid_to, is_current, source, loaded_date)
    SELECT
        @payer_id,
        stg.cpt_code,
        ISNULL(stg.modifier, ''),
        stg.geo_region,
        stg.rate,
        stg.rate_type,
        stg.effective_date,     -- valid_from = the new effective date
        '9999-12-31',           -- valid_to = open-ended (current)
        1,                      -- is_current = true
        'CMS_PFS_' + CONVERT(VARCHAR(4), YEAR(stg.effective_date)),
        GETUTCDATE()
    FROM stg_fee_schedule stg
    WHERE NOT EXISTS (
        -- Skip if there's already a current row with the EXACT SAME rate
        SELECT 1 FROM fee_schedule fs
        WHERE fs.payer_id   = @payer_id
          AND fs.cpt_code   = stg.cpt_code
          AND fs.modifier   = ISNULL(stg.modifier, '')
          AND fs.geo_region = stg.geo_region
          AND fs.rate_type  = stg.rate_type
          AND fs.is_current = 1
          AND fs.rate       = stg.rate      -- Same rate = nothing changed
    );

    DECLARE @inserted_count INT = @@ROWCOUNT;

    -- ═══════════════════════════════════════════════════════════════════
    -- STEP 3: Audit log
    -- ═══════════════════════════════════════════════════════════════════

    INSERT INTO audit_log (entity_type, entity_id, action, timestamp,
                           old_value, new_value)
    VALUES ('fee_schedule', @payer_id + '_BATCH', 'scd2_merge', GETUTCDATE(),
            CONCAT('closed:', @closed_count),
            CONCAT('inserted:', @inserted_count));

    COMMIT TRANSACTION;

    -- Return summary for ADF pipeline logging
    SELECT @closed_count AS rows_closed, @inserted_count AS rows_inserted;
END;
```

**Result after first load:**

```
fee_schedule table:
┌────┬──────────┬──────────┬────┬──────────┬────────┬──────────────┬────────────┬────────────┬─────────┐
│ id │ payer_id │ cpt_code │mod │ geo_reg  │ rate   │ rate_type    │ valid_from │ valid_to   │ current │
├────┼──────────┼──────────┼────┼──────────┼────────┼──────────────┼────────────┼────────────┼─────────┤
│ 1  │ MEDICARE │ 99213    │    │ CA-01    │ 120.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
│ 2  │ MEDICARE │ 99214    │    │ CA-01    │ 175.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
│ 3  │ MEDICARE │ 99213    │ 25 │ CA-01    │ 132.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
└────┴──────────┴──────────┴────┴──────────┴────────┴──────────────┴────────────┴────────────┴─────────┘

Audit: closed=0, inserted=3
```

### 5.5 Idempotency Test — Rerun the Same Q1 2025 Load

The ADF pipeline reruns (maybe it failed on a subsequent step and retried from the beginning). The same staging data is loaded again. The MERGE procedure executes:

**Step 1 — Close changed rows:**

```sql
-- Looking for: is_current = 1 AND rate <> stg.rate
-- Row 1: rate=120.00, stg.rate=120.00 → SAME → no update
-- Row 2: rate=175.00, stg.rate=175.00 → SAME → no update
-- Row 3: rate=132.00, stg.rate=132.00 → SAME → no update
-- Result: 0 rows updated ✓
```

**Step 2 — Insert new/changed rows:**

```sql
-- Looking for: NOT EXISTS (current row with same rate)
-- 99213/CA-01: current row exists with rate=120.00, stg.rate=120.00 → EXISTS → SKIP
-- 99214/CA-01: current row exists with rate=175.00, stg.rate=175.00 → EXISTS → SKIP
-- 99213+25/CA-01: current row exists with rate=132.00, stg.rate=132.00 → EXISTS → SKIP
-- Result: 0 rows inserted ✓
```

**Result: Table is UNCHANGED.** The rerun is a no-op. This is idempotency.

```
fee_schedule table: (identical to before)
┌────┬──────────┬──────────┬────┬──────────┬────────┬──────────────┬────────────┬────────────┬─────────┐
│ id │ payer_id │ cpt_code │mod │ geo_reg  │ rate   │ rate_type    │ valid_from │ valid_to   │ current │
├────┼──────────┼──────────┼────┼──────────┼────────┼──────────────┼────────────┼────────────┼─────────┤
│ 1  │ MEDICARE │ 99213    │    │ CA-01    │ 120.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
│ 2  │ MEDICARE │ 99214    │    │ CA-01    │ 175.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
│ 3  │ MEDICARE │ 99213    │ 25 │ CA-01    │ 132.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │
└────┴──────────┴──────────┴────┴──────────┴────────┴──────────────┴────────────┴────────────┴─────────┘

Audit: closed=0, inserted=0 (no-op confirmed)
```

### 5.6 Rate Change — Q1 2026 Medicare Rates

CMS publishes new 2026 rates. ADF loads to staging:

```
stg_fee_schedule:
┌──────────┬──────────┬──────────┬────────┬──────────────┬────────────────┐
│ cpt_code │ modifier │ geo_reg  │ rate   │ rate_type    │ effective_date │
├──────────┼──────────┼──────────┼────────┼──────────────┼────────────────┤
│ 99213    │ (null)   │ CA-01    │ 135.00 │ medicare_pfs │ 2026-01-01     │  ← CHANGED ($120→$135)
│ 99214    │ (null)   │ CA-01    │ 175.00 │ medicare_pfs │ 2026-01-01     │  ← UNCHANGED
│ 99213    │ 25       │ CA-01    │ 140.00 │ medicare_pfs │ 2026-01-01     │  ← CHANGED ($132→$140)
│ 99215    │ (null)   │ CA-01    │ 245.00 │ medicare_pfs │ 2026-01-01     │  ← NEW CODE
└──────────┴──────────┴──────────┴────────┴──────────────┴────────────────┘
```

**Step 1 — Close changed rows:**

```sql
-- Row 1 (99213): current rate=120.00, stg rate=135.00 → DIFFERENT → close it
--   SET valid_to = 2025-12-31, is_current = 0
-- Row 2 (99214): current rate=175.00, stg rate=175.00 → SAME → skip
-- Row 3 (99213+25): current rate=132.00, stg rate=140.00 → DIFFERENT → close it
--   SET valid_to = 2025-12-31, is_current = 0
-- Result: 2 rows closed
```

**Step 2 — Insert new/changed rows:**

```sql
-- 99213/CA-01: no current row anymore (was closed in Step 1) → NOT EXISTS → INSERT
-- 99214/CA-01: current row exists with rate=175.00 → EXISTS → SKIP (unchanged)
-- 99213+25/CA-01: no current row anymore (was closed in Step 1) → NOT EXISTS → INSERT
-- 99215/CA-01: never existed → NOT EXISTS → INSERT (new code)
-- Result: 3 rows inserted
```

**Result after Q1 2026 load:**

```
fee_schedule table:
┌────┬──────────┬──────────┬────┬──────────┬────────┬──────────────┬────────────┬────────────┬─────────┐
│ id │ payer_id │ cpt_code │mod │ geo_reg  │ rate   │ rate_type    │ valid_from │ valid_to   │ current │
├────┼──────────┼──────────┼────┼──────────┼────────┼──────────────┼────────────┼────────────┼─────────┤
│ 1  │ MEDICARE │ 99213    │    │ CA-01    │ 120.00 │ medicare_pfs │ 2025-01-01 │ 2025-12-31 │ 0       │ ← closed
│ 2  │ MEDICARE │ 99214    │    │ CA-01    │ 175.00 │ medicare_pfs │ 2025-01-01 │ 9999-12-31 │ 1       │ ← unchanged
│ 3  │ MEDICARE │ 99213    │ 25 │ CA-01    │ 132.00 │ medicare_pfs │ 2025-01-01 │ 2025-12-31 │ 0       │ ← closed
│ 4  │ MEDICARE │ 99213    │    │ CA-01    │ 135.00 │ medicare_pfs │ 2026-01-01 │ 9999-12-31 │ 1       │ ← new rate
│ 5  │ MEDICARE │ 99213    │ 25 │ CA-01    │ 140.00 │ medicare_pfs │ 2026-01-01 │ 9999-12-31 │ 1       │ ← new rate
│ 6  │ MEDICARE │ 99215    │    │ CA-01    │ 245.00 │ medicare_pfs │ 2026-01-01 │ 9999-12-31 │ 1       │ ← new code
└────┴──────────┴──────────┴────┴──────────┴────────┴──────────────┴────────────┴────────────┴─────────┘

Audit: closed=2, inserted=3
```

### 5.7 Idempotency Test — Rerun the Q1 2026 Load

ADF pipeline reruns with the same 2026 staging data:

**Step 1:** Looks for current rows where rate differs from staging.
- Row 2 (99214): rate=175.00 vs stg=175.00 → same → skip
- Rows 4,5,6: these are the new 2026 rows, their rates match staging → skip
- Rows 1,3: already `is_current = 0` → not matched by `WHERE is_current = 1` → skip
- **Result: 0 rows closed**

**Step 2:** Looks for staging rows with no matching current row at same rate.
- 99213/CA-01: row 4 exists with rate=135.00, stg=135.00 → EXISTS → skip
- 99214/CA-01: row 2 exists with rate=175.00, stg=175.00 → EXISTS → skip
- 99213+25/CA-01: row 5 exists with rate=140.00, stg=140.00 → EXISTS → skip
- 99215/CA-01: row 6 exists with rate=245.00, stg=245.00 → EXISTS → skip
- **Result: 0 rows inserted**

**Table is UNCHANGED.** Rerun is a no-op.

### 5.8 Querying Historical Rates — The Arbitration Use Case

Now the system can answer: **"What was Medicare's rate for CPT 99213 in CA-01 on June 15, 2025?"**

```sql
SELECT rate, valid_from, valid_to
FROM fee_schedule
WHERE payer_id   = 'MEDICARE'
  AND cpt_code   = '99213'
  AND modifier   = ''
  AND geo_region = 'CA-01'
  AND rate_type  = 'medicare_pfs'
  AND '2025-06-15' BETWEEN valid_from AND valid_to;

-- Result:
-- ┌────────┬────────────┬────────────┐
-- │ rate   │ valid_from │ valid_to   │
-- ├────────┼────────────┼────────────┤
-- │ 120.00 │ 2025-01-01 │ 2025-12-31 │
-- └────────┴────────────┴────────────┘
-- Answer: $120.00 (the 2025 rate, not today's $135.00)
```

If the SCD Type 2 merge were not idempotent, a duplicate rerun could have created a second row for the 2025 rate with a different `valid_from`, making this query return two rows or the wrong rate — corrupting the arbitration evidence.

### 5.9 SCD Type 2 for Providers — Same Pattern, Different Change-Detection Columns

```sql
CREATE PROCEDURE usp_merge_providers_scd2
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    -- Step 1: Close rows where tracked attributes changed
    UPDATE p
    SET p.valid_to   = DATEADD(DAY, -1, GETUTCDATE()),
        p.is_current = 0
    FROM providers p
    INNER JOIN stg_providers stg ON p.npi = stg.npi
    WHERE p.is_current = 1
      AND (
          p.specialty_taxonomy <> stg.specialty_taxonomy  -- specialty changed
          OR p.status          <> stg.status              -- active→deactivated
          OR p.state           <> stg.state               -- relocated
      );

    -- Step 2: Insert new/changed rows
    INSERT INTO providers
        (npi, name_first, name_last, specialty_taxonomy,
         state, city, zip, status, deactivation_date,
         valid_from, valid_to, is_current)
    SELECT
        stg.npi, stg.name_first, stg.name_last, stg.specialty_taxonomy,
        stg.state, stg.city, stg.zip, stg.status, stg.deactivation_date,
        GETUTCDATE(),       -- valid_from = now
        '9999-12-31',       -- valid_to = open-ended
        1                   -- is_current = true
    FROM stg_providers stg
    WHERE NOT EXISTS (
        SELECT 1 FROM providers p
        WHERE p.npi = stg.npi
          AND p.is_current = 1
          AND p.specialty_taxonomy = stg.specialty_taxonomy
          AND p.status             = stg.status
          AND p.state              = stg.state
    );

    COMMIT TRANSACTION;
END;
```

**Provider idempotency follows the same logic:**

- Rerun same monthly NPPES file → all attributes match → 0 closed, 0 inserted
- Provider changes specialty → old row closed, new row inserted
- Rerun after the change → new row already exists with matching attributes → 0 closed, 0 inserted

**Arbitration use case:** "Was Dr. Smith in-network with Aetna at the time of service?" requires knowing Dr. Smith's specialty and practice location *at that date* — SCD Type 2 provides this.

---

## 6. Where the Check Happens — Architecture Pattern

```
Message arrives (possibly for the 2nd or 3rd time)
    │
    ▼
Parse (stateless — safe to repeat any number of times)
    │
    ▼
Validate (stateless — safe to repeat any number of times)
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│           IDEMPOTENCY GATE (application layer)              │
│                                                             │
│   SELECT ... WHERE idempotency_key = ?                      │
│       │                                                     │
│       ├── EXISTS + no meaningful change → SKIP              │
│       │   (log for metrics, return success, emit nothing)   │
│       │                                                     │
│       ├── EXISTS + intentional resubmission → UPDATE        │
│       │   (e.g., claim replacement freq_code=7)             │
│       │                                                     │
│       └── NOT EXISTS → INSERT                               │
│                                                             │
│   Database UNIQUE constraint = safety net for race          │
│   conditions (catches concurrent inserts that both pass     │
│   the SELECT check)                                         │
└─────────────────────────────────────────────────────────────┘
    │
    ▼ (only on INSERT or UPDATE — never on SKIP)
Emit downstream event
    │
    ▼
Workflow engine, CDC, analytics
```

### Why the Check is in Application Code, Not Just the Database

| Approach | Pro | Con |
|---|---|---|
| **Application-level check only** | Can distinguish duplicate vs. replacement vs. void; controls downstream event emission | Race condition: two threads both pass the SELECT, both INSERT |
| **Database constraint only** | Eliminates race conditions | Cannot distinguish duplicate from replacement; throws exception on every duplicate (noisy); cannot prevent duplicate downstream events |
| **Both (our approach)** | Application handles business logic (freq codes, SCD merge); DB constraint catches race conditions | Slightly more code — but correct |

The application-level check handles the **business logic** (is this a duplicate, a correction, or a new record?). The database constraint handles the **race condition** (what if two Function instances process the same message at the exact same millisecond?).

### Event Emission is Part of Idempotency

A critical detail: **downstream events are only emitted on INSERT or UPDATE, never on SKIP.** If the same claim message is processed twice:

```
T1: Process claim CLM-1001 → INSERT → emit "claim.insert" event → workflow starts
T2: Process claim CLM-1001 (retry) → SKIP → NO event emitted → workflow NOT duplicated
```

Without this, the workflow engine would start two parallel claim-to-dispute pipelines for the same claim.

---

## 7. Edge Cases and Race Conditions

### 7.1 Concurrent Processing of the Same Message

**Scenario:** Two Azure Function instances pick up the same Event Hub message simultaneously (rare but possible during partition rebalancing).

```
Instance A                              Instance B
    │                                       │
    ▼                                       ▼
SELECT claim_id = 'CLM-1001'           SELECT claim_id = 'CLM-1001'
→ NOT EXISTS                            → NOT EXISTS (race: A hasn't committed yet)
    │                                       │
    ▼                                       ▼
INSERT claim_id = 'CLM-1001'           INSERT claim_id = 'CLM-1001'
→ SUCCESS (commits first)              → UNIQUE CONSTRAINT VIOLATION (caught)
                                            │
                                            ▼
                                        Exception caught → log → return success
                                        (the record exists, which is the correct state)
```

**Handling in code:**

```python
try:
    cursor.execute("INSERT INTO claims (...) VALUES (...)", ...)
    conn.commit()
except pyodbc.IntegrityError as e:
    if "uq_claims_claim_id" in str(e):
        # Race condition: another instance already inserted this claim
        # The correct state exists — treat as a successful skip
        logging.info(f"Claim {claim_id} already inserted by concurrent process — skipping")
        conn.rollback()
    else:
        raise  # Different integrity error — re-raise
```

### 7.2 Partial Failure Mid-Batch (ADF)

**Scenario:** ADF is loading 100K fee schedule rows to staging. Pipeline fails at row 60K. ADF retries from the beginning.

**Why this is safe:**

```sql
-- The ADF pipeline truncates staging before each load
"preCopyScript": "TRUNCATE TABLE stg_fee_schedule"
```

The staging table is always in a clean state before loading. The MERGE procedure then runs against a complete staging table. If the MERGE itself fails mid-transaction, the `BEGIN TRANSACTION ... COMMIT` ensures atomicity — either all SCD changes apply, or none do.

### 7.3 Out-of-Order Message Processing

**Scenario:** Claim CLM-1001 arrives with frequency_code=1 (original). A replacement (freq=7) arrives. Due to Event Hub partition rebalancing, the replacement is processed BEFORE the original.

```
T1: Process replacement (freq=7) for CLM-1001
    → existing_status = None (original hasn't been processed yet)
    → resolve_duplicate(None, "7") → "insert"
    → INSERT with status='corrected' (it's a replacement, so mark accordingly)

T2: Process original (freq=1) for CLM-1001
    → existing_status = 'corrected'
    → resolve_duplicate('corrected', "1") → "skip"
    → The original is skipped because a newer version already exists
```

This produces the correct final state: the corrected version is in the database. The original is irrelevant because the correction supersedes it.

### 7.4 SCD Type 2 — Mid-Year Retroactive Correction

**Scenario:** CMS issues a correction in March 2026 that changes a rate retroactively back to January 1, 2026.

```
Before correction:
┌──────────┬────────┬────────────┬────────────┬─────────┐
│ cpt_code │ rate   │ valid_from │ valid_to   │ current │
├──────────┼────────┼────────────┼────────────┼─────────┤
│ 99213    │ 135.00 │ 2026-01-01 │ 9999-12-31 │ 1       │
└──────────┴────────┴────────────┴────────────┴─────────┘

Correction staging data: rate should be 137.50, effective 2026-01-01

After MERGE:
Step 1: close row (rate 135.00 ≠ 137.50)
  → valid_to = 2025-12-31, is_current = 0
Step 2: insert corrected row
  → rate=137.50, valid_from=2026-01-01, valid_to=9999-12-31, is_current=1

Result:
┌──────────┬────────┬────────────┬────────────┬─────────┐
│ cpt_code │ rate   │ valid_from │ valid_to   │ current │
├──────────┼────────┼────────────┼────────────┼─────────┤
│ 99213    │ 120.00 │ 2025-01-01 │ 2025-12-31 │ 0       │  (2025 rate — untouched)
│ 99213    │ 135.00 │ 2026-01-01 │ 2025-12-31 │ 0       │  (original 2026 — closed)
│ 99213    │ 137.50 │ 2026-01-01 │ 9999-12-31 │ 1       │  (corrected 2026)
└──────────┴────────┴────────────┴────────────┴─────────┘
```

Note: the original 2026 row (135.00) is preserved with `is_current = 0` for audit purposes — we never delete history. The `valid_to` date overlap (both rows claim 2026-01-01 as valid_from) is disambiguated by `is_current`: only the corrected row is active.

For arbitration queries, add `AND is_current = 1` or use the `valid_to = '9999-12-31'` convention to get the authoritative rate.

---

## 8. Summary Matrix

| # | Source | Idempotency Key | Check Type | On True Duplicate | On Intentional Resend | DB Safety Net |
|---|---|---|---|---|---|---|
| 1 | Claims (837) | `claim_id` + `frequency_code` | SELECT before INSERT | Skip | Update (freq=7) or Void (freq=8) | `UNIQUE (claim_id)` |
| 2 | ERA (835) | `trace_number` + `payer_id` | SELECT before INSERT | Skip | N/A (new trace = new record) | `UNIQUE (trace_number, payer_id)` |
| 3 | EOB (PDF) | `content_hash` (SHA-256) | SELECT before INSERT | Skip (or 409 to UI) | N/A (different file = different hash) | `UNIQUE INDEX (content_hash)` |
| 4 | Patient (FHIR) | `patient_id` (MRN) | Upsert (SCD Type 1) | UPDATE = no-op if same data | UPDATE with new values | `PRIMARY KEY (patient_id)` |
| 5 | Fee Schedules | `payer+cpt+mod+geo+valid_from` | SCD2 MERGE (NOT EXISTS) | 0 rows inserted (no-op) | New SCD row if rate changed | `UNIQUE (composite)` |
| 6 | Providers (NPPES) | `npi` | SCD2 MERGE (NOT EXISTS) | 0 rows inserted (no-op) | New SCD row if attrs changed | `UNIQUE (npi, valid_from)` |
| 7 | Contracts | `content_hash` | SELECT before INSERT | Return 409 Conflict | N/A | `UNIQUE INDEX (content_hash)` |
| 8 | Documents | `content_hash` | SELECT before INSERT | Skip | N/A | `UNIQUE INDEX (content_hash)` |
| 9 | Historical Backfill | `claim_id+dos+npi+billed` | SELECT before INSERT | Skip | N/A | Composite UNIQUE |

### The Guarantee

> No matter how many times any message, file, webhook, or pipeline runs, the OLTP database converges to the same correct state — and downstream events are emitted exactly once per logical change.
