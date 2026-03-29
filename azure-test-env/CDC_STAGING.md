# CDC & Staging Tables — Deep Dive

> Part of the [Architecture Reference](ARCHITECTURE.md) for the Medical Billing Arbitration platform.

## What is CDC?

**Change Data Capture (CDC)** is a pattern for detecting and copying only the data that changed
since the last sync. Instead of copying entire tables every time, you track a "watermark"
(timestamp) and only extract rows where `updated_at > last_watermark`.

```
Full copy (slow, wasteful):              CDC / incremental (fast, efficient):
─────────────────────────                ────────────────────────────────────

Every 15 min:                            Every 15 min:
  SELECT * FROM claims                     SELECT * FROM claims
  → 10,000 rows copied                    WHERE updated_at > '2025-03-29 14:00:00'
  → 10,000 rows written to Parquet         → 12 rows copied (only changes)
  → Same data copied repeatedly            → 12 rows written to Parquet
                                           → Update watermark to '2025-03-29 14:15:00'
```

**Why we need it:** The OLTP database (Azure SQL) is the operational store. The Lakehouse
(ADLS Bronze/Silver/Gold) is the analytics store. CDC bridges them — it copies changes from
OLTP → Bronze Parquet every 15 minutes, keeping analytics data fresh without hammering the
operational database.

---

## Our CDC Architecture

We use **watermark-based CDC** (not native SQL Server CDC). Two systems run it:

```
┌─────────────────────────────────────────────────────────────┐
│ OLTP (Azure SQL)                                             │
│                                                              │
│  claims (updated_at) ──┐                                     │
│  remittances (created_at) ──┤                                │
│  cases (last_activity) ──┤     cdc_watermarks table          │
│  disputes (updated_at) ──┤     tracks last sync per table    │
│  deadlines (created_at) ──┤                                  │
│  ... 12 tables total ──────┘                                 │
│                                                              │
└──────────┬──────────────────────────────────┬────────────────┘
           │                                  │
     ADF Pipeline                      Azure Functions
     (every 15 min)                    (every 4 hours)
           │                                  │
           ▼                                  ▼
┌──────────────────┐               ┌──────────────────┐
│ ADLS Bronze      │               │ ADLS Bronze      │
│ (Parquet files)  │               │ (Parquet files)  │
│ date-partitioned │               │ date-partitioned │
└──────────────────┘               └──────────────────┘
```

| Method | File | Schedule | Tables | Used By |
|---|---|---|---|---|
| **ADF pipeline** | `adf/pipelines/pl_cdc_incremental_copy.json` | Every 15 min (trigger `trg_cdc_15min`) | 9 tables | Fabric Lakehouse (Bronze → Silver → Gold) |
| **Azure Functions** | `functions/olap/bronze.py` | Every 4 hours (function #13) | 12 tables | Functions-based OLAP pipeline |

Both use the same `cdc_watermarks` table and same pattern. ADF is the primary (runs more frequently); Functions is the fallback.

---

## The cdc_watermarks Table

This is the heart of CDC — it tracks where each table's sync left off:

```sql
-- sql/staging_tables.sql:10
CREATE TABLE cdc_watermarks (
    source_table    NVARCHAR(128)   PRIMARY KEY,    -- e.g., 'claims', 'disputes'
    last_sync_ts    DATETIME2       NOT NULL,        -- last synced timestamp
    rows_synced     INT             NOT NULL,        -- rows in last sync
    total_inserts   BIGINT          NOT NULL,        -- lifetime inserts
    total_updates   BIGINT          NOT NULL,        -- lifetime updates
    total_deletes   BIGINT          NOT NULL,        -- lifetime deletes (watermark CDC can't detect)
    sync_status     VARCHAR(20)     NOT NULL,        -- 'ok', 'error', 'running'
    updated_at      DATETIME2       NOT NULL         -- when watermark was last updated
);
```

**Example state after several syncs:**

```
source_table     | last_sync_ts            | rows_synced | total_inserts | sync_status
─────────────────┼─────────────────────────┼─────────────┼───────────────┼────────────
claims           | 2025-03-29 14:15:00.000 | 3           | 847           | ok
remittances      | 2025-03-29 14:15:00.000 | 1           | 612           | ok
cases            | 2025-03-29 14:00:00.000 | 0           | 126           | ok
disputes         | 2025-03-29 14:15:00.000 | 2           | 98            | ok
deadlines        | 2025-03-29 14:00:00.000 | 0           | 214           | ok
fee_schedule     | 2025-03-29 06:00:00.000 | 15          | 430           | ok
```

---

## How CDC Works — Step by Step

### ADF Pipeline (`pl_cdc_incremental_copy`)

```
For each of 9 tables (parallel, 4 at a time):
    │
    ├── 1. Lookup_Watermark
    │      SELECT last_sync_ts FROM cdc_watermarks WHERE source_table = 'claims'
    │      → returns '2025-03-29 14:00:00'
    │
    ├── 2. Lookup_MaxTimestamp
    │      SELECT MAX(updated_at) AS max_ts, COUNT(*) AS row_count
    │      FROM claims WHERE updated_at > '2025-03-29 14:00:00'
    │      → returns max_ts='2025-03-29 14:12:35', row_count=3
    │
    ├── 3. IfChanged (row_count > 0?)
    │      YES → continue
    │      NO  → skip this table
    │
    ├── 4. Copy_To_Bronze
    │      SELECT *, '2025-03-29T14:15:00Z' AS _cdc_timestamp,
    │             'I' AS _cdc_operation, 'claims' AS _source_table
    │      FROM claims WHERE updated_at > '2025-03-29 14:00:00'
    │      → Write to: bronze/claims/year=2025/month=03/day=29/*.parquet
    │
    └── 5. Update_Watermark
           EXEC usp_update_cdc_watermark 'claims', '2025-03-29 14:12:35', 3
           → cdc_watermarks.last_sync_ts = '2025-03-29 14:12:35'
```

**ADF code reference:** `adf/pipelines/pl_cdc_incremental_copy.json`

### Azure Functions Bronze Extraction (`olap/bronze.py`)

Same logic, different runtime:

```python
# functions/olap/bronze.py

CDC_TABLES = {
    "claims":       {"pk": "claim_id",    "watermark": "updated_at"},
    "claim_lines":  {"pk": "line_id",     "watermark": "created_at"},
    "remittances":  {"pk": "remit_id",    "watermark": "created_at"},
    "patients":     {"pk": "patient_id",  "watermark": "updated_at"},
    "providers":    {"pk": "npi",         "watermark": "created_at"},
    "payers":       {"pk": "payer_id",    "watermark": "created_at"},
    "fee_schedule": {"pk": "id",          "watermark": "loaded_date"},
    "cases":        {"pk": "case_id",     "watermark": "last_activity"},
    "disputes":     {"pk": "dispute_id",  "watermark": "updated_at"},
    "deadlines":    {"pk": "deadline_id", "watermark": "created_at"},
    "evidence_artifacts": {"pk": "artifact_id", "watermark": "uploaded_date"},
    "audit_log":    {"pk": "log_id",      "watermark": "timestamp"},
}

def extract_table(table, config):
    last_watermark = _get_watermark(table)          # read from cdc_watermarks
    query = f"""
        SELECT *, '{now}' AS _cdc_timestamp, 'I' AS _cdc_operation
        FROM {table}
        WHERE {config['watermark']} > ?             -- only rows changed since last sync
    """
    df = pd.read_sql(query, conn, params=[last_watermark])
    write_parquet("bronze", path, df)               # append to Bronze container
    _update_watermark(table, max_ts, len(df))       # advance the watermark
```

---

## Tables Tracked by CDC

| Table | Watermark Column | Why This Column |
|---|---|---|
| `claims` | `updated_at` | Claims can be updated (status change, correction) |
| `claim_lines` | `created_at` | Lines are append-only (never updated) |
| `remittances` | `created_at` | Remittances are append-only |
| `patients` | `updated_at` | Demographics can be updated (SCD Type 1) |
| `providers` | `created_at` | New provider versions inserted (SCD Type 2) |
| `payers` | `created_at` | Reference data, rarely changes |
| `fee_schedule` | `loaded_date` | New rate versions inserted (SCD Type 2) |
| `cases` | `last_activity` | Updated on every case transition |
| `disputes` | `updated_at` | Updated on status changes |
| `deadlines` | `created_at` | New deadlines created; status changes update `alerted_date`/`completed_date` but not `created_at` — this means status changes are NOT captured by CDC |
| `evidence_artifacts` | `uploaded_date` | New documents added |
| `audit_log` | `timestamp` | Append-only log |

### Known Limitation: Watermark CDC Can't Detect Deletes

```
Watermark-based CDC only detects:
  ✓ INSERTs (new row → watermark column > last sync)
  ✓ UPDATEs (changed row → watermark column bumped)
  ✗ DELETEs (row removed → no watermark to read)

Mitigation: We don't delete rows. Closed cases stay with status='closed'.
Voided claims stay with status='void'. Soft deletes only.
```

To detect deletes, you'd need **native SQL Server CDC** (`sys.sp_cdc_enable_table`), which
maintains a change log at the database engine level. The schema has this commented out:

```sql
-- sql/schema.sql (bottom — uncomment for native CDC)
-- EXEC sys.sp_cdc_enable_db;
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'claims', @role_name=NULL;
```

---

## Staging Tables

### What Are They?

Staging tables are **temporary landing zones** for batch data. They exist because you can't
do an SCD Type 2 merge directly from a CSV file — you need the data in SQL first so you can
compare old vs new rows.

```
Without staging:                         With staging:
─────────────────                        ────────────────

CSV file                                 CSV file
  │                                        │
  ▼                                        ▼
INSERT INTO fee_schedule                 INSERT INTO stg_fee_schedule  (raw dump)
  Problem: how do you detect                │
  which rates changed?                      ▼
  How do you close old rows?             EXEC usp_merge_fee_schedule
                                            │
                                            ├── Compare stg vs current fee_schedule
                                            ├── Rate changed? → close old (is_current=0)
                                            ├── Insert new version (is_current=1)
                                            ├── Unchanged? → skip
                                            └── Clean stg_fee_schedule
```

### Three Staging Tables

| Table | Source | Target | Merge Procedure | Pipeline |
|---|---|---|---|---|
| `stg_fee_schedule` | CSV from ADLS Bronze `fee-schedules/` | `fee_schedule` | `usp_merge_fee_schedule` | ADF `pl_batch_fee_schedule` + Functions timer #5 |
| `stg_providers` | NPPES CSV from ADLS Bronze | `providers` | `usp_merge_providers` | ADF `pl_batch_providers` |
| `stg_backfill_claims` | Historical CSV imports | `claims` + `claim_lines` | Code in `ingest/backfill.py` | Manual |

### stg_fee_schedule

```sql
-- sql/staging_tables.sql:27
CREATE TABLE stg_fee_schedule (
    payer_id        NVARCHAR(50),   -- payer name or ID (resolved during merge)
    cpt_code        VARCHAR(10),    -- procedure code
    modifier        VARCHAR(10),    -- modifier (e.g., '25' for separate E/M)
    geo_region      NVARCHAR(50),   -- geographic region (e.g., 'NY', 'CA-01')
    rate            DECIMAL(12,2),  -- the contracted rate
    rate_type       VARCHAR(30),    -- 'contracted', 'medicare', 'fair_health'
    valid_from      DATE,           -- effective date
    valid_to        DATE,           -- '9999-12-31' for current
    source          NVARCHAR(100),  -- 'CMS PFS 2026', 'Contract 2025'
    batch_id        NVARCHAR(100),  -- groups rows from same upload
    loaded_at       DATETIME2       -- when row landed in staging
);
```

### stg_providers

```sql
-- sql/staging_tables.sql:50
CREATE TABLE stg_providers (
    npi             CHAR(10),       -- National Provider Identifier
    tin             CHAR(9),        -- Tax ID
    name_first      NVARCHAR(100),
    name_last       NVARCHAR(100),
    name_full       NVARCHAR(200),  -- computed or provided
    specialty       NVARCHAR(100),  -- taxonomy code → specialty name
    state           NVARCHAR(2),
    city            NVARCHAR(100),
    zip             NVARCHAR(10),
    status          VARCHAR(20),    -- 'active' or 'deactivated'
    deactivation_date DATE,
    batch_id        NVARCHAR(100),
    loaded_at       DATETIME2
);
```

### stg_backfill_claims

```sql
-- sql/staging_tables.sql:74
CREATE TABLE stg_backfill_claims (
    claim_id        NVARCHAR(50),
    patient_id      NVARCHAR(50),
    provider_npi    CHAR(10),
    payer_id        NVARCHAR(50),
    date_of_service DATE,
    total_billed    DECIMAL(12,2),
    cpt_codes       NVARCHAR(500),  -- semicolon-separated (split during processing)
    diagnosis_codes NVARCHAR(500),
    batch_id        NVARCHAR(100),
    loaded_at       DATETIME2
);
```

---

## Stored Procedures (SCD Type 2 Merge)

### usp_merge_fee_schedule

The most important merge — handles rate history for arbitration. Called by ADF after loading
`stg_fee_schedule`:

```sql
-- sql/staging_tables.sql:94
CREATE OR ALTER PROCEDURE usp_merge_fee_schedule @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    -- Step 1: Find current rates where the rate CHANGED
    --         Close them: set valid_to = yesterday, is_current = 0
    UPDATE fee_schedule
    SET valid_to = DATEADD(DAY, -1, stg.valid_from), is_current = 0
    FROM fee_schedule fs
    JOIN stg_fee_schedule stg ON (same payer, CPT, modifier, geo_region)
    WHERE fs.is_current = 1 AND fs.rate != stg.rate;

    -- Step 2: Insert new version for changed rates + brand new codes
    INSERT INTO fee_schedule (payer_id, cpt_code, ..., is_current=1)
    SELECT ... FROM stg_fee_schedule stg
    WHERE NOT EXISTS (current row with same rate);

    -- Step 3: Update watermark
    -- Step 4: Clean staging table
END;
```

**Why this matters for arbitration:** When disputing a claim from June 2025, we need the
rate that was effective in June 2025 — not today's rate. SCD Type 2 preserves that history:

```
fee_schedule for Anthem, CPT 99285, NY:

id | rate    | valid_from  | valid_to    | is_current
───┼─────────┼─────────────┼─────────────┼───────────
1  | $450.00 | 2024-01-01  | 2024-12-31  | 0          ← historical
2  | $475.00 | 2025-01-01  | 2025-12-31  | 0          ← historical
3  | $498.00 | 2026-01-01  | 9999-12-31  | 1          ← current

Claim from June 2025 → use row 2 ($475) not row 3 ($498)
Query: WHERE valid_from <= '2025-06-15' AND valid_to >= '2025-06-15'
```

### usp_merge_providers

Same pattern but simpler — updates name/specialty/state if changed, inserts new NPIs:

```sql
-- sql/staging_tables.sql:172
CREATE OR ALTER PROCEDURE usp_merge_providers @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    -- Update existing where data changed
    UPDATE providers SET name=stg.name, specialty=stg.specialty, state=stg.state
    FROM providers p JOIN stg_providers stg ON p.npi = stg.npi
    WHERE (p.name != stg.name OR p.specialty != stg.specialty OR p.state != stg.state);

    -- Insert new providers
    INSERT INTO providers (npi, tin, name, specialty, state)
    SELECT ... FROM stg_providers stg
    WHERE NOT EXISTS (SELECT 1 FROM providers p WHERE p.npi = stg.npi);
END;
```

### usp_update_cdc_watermark

Generic watermark updater — called by ADF after each CDC copy:

```sql
-- sql/staging_tables.sql:238
CREATE OR ALTER PROCEDURE usp_update_cdc_watermark
    @source_table NVARCHAR(128),
    @new_watermark NVARCHAR(50),
    @rows_synced INT
AS
BEGIN
    MERGE cdc_watermarks AS tgt
    USING (SELECT @source_table AS source_table) AS src
    ON tgt.source_table = src.source_table
    WHEN MATCHED THEN UPDATE SET last_sync_ts = @new_watermark, rows_synced = @rows_synced, ...
    WHEN NOT MATCHED THEN INSERT (source_table, last_sync_ts, rows_synced, ...) VALUES (...);
END;
```

---

## Data Flow: Full CDC + Staging Pipeline

```
1. OLTP (real-time)           2. CDC (every 15 min)        3. Staging (daily batch)
─────────────────────         ─────────────────────        ─────────────────────────

Claims/remittances            ADF: pl_cdc_incremental      ADF: pl_batch_fee_schedule
arrive via Event Hub           │                            │
      │                        ├── Read cdc_watermarks      ├── Copy CSV from Bronze
      ▼                        ├── SELECT WHERE updated_at  │   → stg_fee_schedule
Azure Functions                │   > last_watermark          ├── EXEC usp_merge_fee_schedule
      │                        ├── Write to Bronze Parquet  │   → close old rates
      ▼                        └── Update watermark         │   → insert new rates
Azure SQL (OLTP)                                            └── Clean staging
      │                                                       │
      │                        4. Lakehouse (after CDC)       │
      │                        ─────────────────────          │
      │                        Fabric Notebooks:              │
      │                        nb_bronze_cdc                  │
      │                          → read Bronze Parquet        │
      │                        nb_silver_transforms           │
      │                          → deduplicate, join          │
      │                        nb_gold_aggregations           │
      │                          → aggregate for BI           │
      │                                                       │
      └──── Gold SQL Views ──── AI Agent queries these ───────┘
             (live, no CDC)     directly from OLTP
```

**Important:** The AI agent's Gold SQL views (`sql/gold_views.sql`) query OLTP directly —
they don't use CDC or the Lakehouse. They're always current. CDC/Lakehouse is for
Power BI (Direct Lake mode) and historical analytics.

---

## Code Reference Summary

| File | Purpose |
|---|---|
| `sql/staging_tables.sql` | Creates `cdc_watermarks`, 3 staging tables, 3 stored procedures |
| `adf/pipelines/pl_cdc_incremental_copy.json` | ADF pipeline: ForEach 9 tables → Lookup watermark → Copy if changed → Update watermark |
| `adf/pipelines/pl_batch_fee_schedule.json` | ADF pipeline: CSV → `stg_fee_schedule` → `usp_merge_fee_schedule` |
| `adf/pipelines/pl_batch_providers.json` | ADF pipeline: CSV → `stg_providers` → `usp_merge_providers` |
| `functions/olap/bronze.py` | Functions-based CDC: same pattern as ADF, 12 tables, every 4 hours |
| `functions/olap/lake.py` | ADLS Gen2 read/write helpers (Parquet) |
| `functions/ingest/fee_schedules.py` | Timer trigger: reads CSV from Bronze, calls staging merge |
| `functions/ingest/providers.py` (local) | Batch provider ingestion with staging merge |
| `sql/schema.sql` (bottom) | Native CDC enable commands (commented out) |

---

## Watermark CDC vs Native CDC

| Feature | Watermark (our approach) | Native SQL Server CDC |
|---|---|---|
| **Setup** | Just needs a timestamp column | `sp_cdc_enable_table` per table |
| **Detects inserts** | Yes | Yes |
| **Detects updates** | Yes (if timestamp column updated) | Yes (tracks all columns) |
| **Detects deletes** | No | Yes |
| **Performance impact** | None (reads existing columns) | Slight (reads transaction log) |
| **Cost** | Free | Requires SQL Server Standard+ (not free tier) |
| **Available on free tier** | Yes | No |
| **Complexity** | Simple | Moderate (change tables, cleanup jobs) |

**Why we use watermark:** Azure SQL free tier doesn't support native CDC. Watermark is simple,
free, and sufficient for our use case (we don't delete rows — only soft-delete via status changes).

**When to upgrade:** If you need delete detection or column-level change tracking, enable
native CDC. Requires upgrading from Azure SQL free tier.
