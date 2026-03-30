# Orchestration — Pipelines, Workflows & Scheduling

> Part of the [Architecture Reference](ARCHITECTURE.md) for the Medical Billing Arbitration platform.

## Overview

The system has **three orchestration layers**, each handling a different type of work:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION LAYERS                         │
│                                                                     │
│  1. ADF Pipelines         Data movement (CDC, batch loads)         │
│     (Azure Data Factory)  Runs on schedule or trigger               │
│                                                                     │
│  2. Durable Functions     Business workflow (disputes, deadlines)   │
│     (Azure Functions)     Runs on events, timers, HTTP calls        │
│                                                                     │
│  3. OLAP Pipeline         Analytics refresh (Bronze→Silver→Gold)    │
│     (Azure Functions)     Runs on timer or manual trigger            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

| Layer | What | When | Code |
|---|---|---|---|
| **ADF** | Move data between OLTP ↔ Lakehouse | Scheduled (15 min, daily) | `adf/pipelines/*.json` |
| **Durable Functions** | Arbitration workflows (create disputes, manage cases, monitor deadlines) | Event-driven + timer | `functions/workflow/*.py` |
| **OLAP Functions** | Compute Gold aggregations | Timer (4h) + manual | `functions/olap/*.py` |

---

## 1. ADF Pipelines (Data Movement)

### Pipeline Catalog

```
Azure Data Factory: medbill-adf
│
├── pl_master_orchestrator         Chains: CDC → Bronze NB → Silver NB → Gold NB
│   └── Sequential: each step waits for previous to complete
│
├── pl_cdc_incremental_copy        CDC: OLTP → Bronze Parquet (every 15 min)
│   └── ForEach 9 tables (parallel, 4 at a time)
│
├── pl_batch_fee_schedule          CSV → staging → SCD2 merge (daily 6 AM)
│   └── List CSVs → Filter → ForEach → Copy to staging → Merge → Archive
│
└── pl_batch_providers             NPPES CSV → staging → merge
    └── Same pattern as fee schedule
```

### pl_master_orchestrator — The Main Chain

Chains all data processing in the correct order:

```
Run_CDC_Incremental  →  Run_Bronze_Notebook  →  Run_Silver_Notebook  →  Run_Gold_Notebook
       │                        │                        │                       │
       ▼                        ▼                        ▼                       ▼
  OLTP → Bronze           Bronze Parquet          Silver (cleaned,         Gold (aggregated
  Parquet (CDC)           → Delta Lake            deduplicated,            for Power BI)
                          (Fabric notebook)        joined)
```

**Code:** `adf/pipelines/pl_master_orchestrator.json`

Each step uses `"dependencyConditions": ["Succeeded"]` — if CDC fails, notebooks don't run.
The master orchestrator needs Fabric notebooks linked manually in ADF Studio (see [FABRIC_SETUP.md](FABRIC_SETUP.md)).

### pl_cdc_incremental_copy — CDC Pipeline

Extracts changed rows from 9 OLTP tables every 15 minutes:

```
ForEach_CDC_Table (parallel, batch=4):
    │
    ├── Lookup_Watermark
    │   SELECT last_sync_ts FROM cdc_watermarks
    │   WHERE source_table = '{tableName}'
    │   → returns: '2025-03-29 14:00:00'
    │
    ├── Lookup_MaxTimestamp
    │   SELECT MAX({watermarkColumn}), COUNT(*)
    │   FROM {tableName} WHERE {watermarkColumn} > '{last_watermark}'
    │   → returns: max_ts, row_count
    │
    ├── IfChanged (row_count > 0?)
    │   │
    │   └── YES:
    │       ├── Copy_To_Bronze
    │       │   SELECT *, _cdc_timestamp, _cdc_operation, _source_table
    │       │   FROM {tableName} WHERE {watermarkColumn} > '{last_watermark}'
    │       │   → Write to: bronze/{tableName}/year=YYYY/month=MM/day=DD/*.parquet
    │       │
    │       └── Update_Watermark
    │           EXEC usp_update_cdc_watermark '{tableName}', '{max_ts}', {rows_copied}
    │
    └── NO: skip (no changes since last sync)
```

**Tables tracked:**

| Table | Watermark Column | Why |
|---|---|---|
| claims | updated_at | Status changes, corrections |
| claim_lines | created_at | Append-only |
| remittances | created_at | Append-only |
| patients | updated_at | Demographics updates |
| cases | last_activity | Every case action |
| disputes | updated_at | Status changes |
| deadlines | created_at | New deadlines |
| evidence_artifacts | uploaded_date | New documents |
| audit_log | timestamp | Append-only log |

**Trigger:** `trg_cdc_15min` (created in STOPPED state — start when ready)

**Code:** `adf/pipelines/pl_cdc_incremental_copy.json`

**More detail:** [CDC_STAGING.md](CDC_STAGING.md)

### pl_batch_fee_schedule — SCD Type 2 Batch Load

Processes new fee schedule CSV files from ADLS Bronze:

```
Get_FeeSchedule_Files              List files in bronze/fee-schedules/
        │
        ▼
Filter_CSV_Files                   Keep only *.csv files
        │
        ▼
ForEach_CSV (parallel, batch=4)    For each CSV:
        │
        ├── Copy_CSV_To_Staging    Copy CSV → stg_fee_schedule table
        │                          (with column mapping: payer_id, cpt_code, rate, etc.)
        │
        ▼
Exec_SCD2_Merge                    EXEC usp_merge_fee_schedule @batch_id = NULL
        │                          → Close old rates where rate changed
        │                          → Insert new rate versions
        │                          → Update cdc_watermarks
        │
        ▼
Archive_Processed_Files            For each CSV:
        ├── Move_To_Archive        Copy to bronze/fee-schedules/archive/YYYY-MM-DD/
        └── Delete_Original        Delete from bronze/fee-schedules/
```

**Trigger:** `trg_fee_schedule_daily` (daily 6 AM UTC, created STOPPED)

**Code:** `adf/pipelines/pl_batch_fee_schedule.json`

### pl_batch_providers — Provider Batch Load

Same pattern as fee schedule but for NPPES provider data:

```
List CSVs in bronze/providers/ → Filter *.csv → Copy to stg_providers
→ EXEC usp_merge_providers → Archive processed files
```

**Code:** `adf/pipelines/pl_batch_providers.json`

### Triggers

| Trigger | Schedule | Pipeline | State |
|---|---|---|---|
| `trg_cdc_15min` | Every 15 minutes | `pl_cdc_incremental_copy` | STOPPED (start when ready) |
| `trg_fee_schedule_daily` | Daily 6 AM UTC | `pl_batch_fee_schedule` | STOPPED (start when ready) |

**Start triggers:**
```bash
az datafactory trigger start --factory-name medbill-adf \
  --resource-group rg-medbill-test --trigger-name trg_cdc_15min

az datafactory trigger start --factory-name medbill-adf \
  --resource-group rg-medbill-test --trigger-name trg_fee_schedule_daily
```

**Manual pipeline run:**
```bash
az datafactory pipeline create-run --factory-name medbill-adf \
  --resource-group rg-medbill-test --pipeline-name pl_cdc_incremental_copy
```

### ADF Linked Services & Datasets

| Linked Service | Type | Connects To |
|---|---|---|
| `ls_medbill_azure_sql` | AzureSqlDatabase | `medbill-sql-214f9d00.database.windows.net` / `medbill_oltp` |
| `ls_medbill_adls` | AzureBlobFS | `medbillstoreb29b302f.dfs.core.windows.net` (managed identity) |

| Dataset | Type | Used By |
|---|---|---|
| `ds_sql_cdc_source` | AzureSqlTable (parameterized) | CDC pipeline — reads any OLTP table by name |
| `ds_adls_bronze_parquet` | Parquet (parameterized) | CDC pipeline — writes date-partitioned Parquet |
| `ds_adls_bronze_csv` | DelimitedText (parameterized) | Batch pipelines — reads CSV from Bronze |
| `ds_sql_staging` | AzureSqlTable (parameterized) | Batch pipelines — writes to staging tables |

---

## 2. Durable Functions (Business Workflows)

### What Are Durable Functions?

Azure Durable Functions extend regular Azure Functions with **stateful orchestration**.
They coordinate multiple steps, handle retries, and survive restarts. Think of them as
a **workflow engine** built into Azure Functions.

```
Regular Function:     Trigger → run code → done (stateless, single step)

Durable Function:     Trigger → Orchestrator → Activity 1 → Activity 2 → ... → done
                                    │               │              │
                                    │          (can fail +     (can run in
                                    │           auto-retry)     parallel)
                                    │
                              State persisted in Azure Storage
                              (survives restarts, scales to zero)
```

### How They're Registered

Durable Functions use a **Blueprint** pattern — registered separately from regular functions:

```python
# functions/function_app.py

# Blueprint for Durable Functions (orchestrators + activities)
workflow_bp = df.Blueprint()

# Register orchestrators on the blueprint
@workflow_bp.orchestration_trigger(context_name="context")
def claim_to_dispute_orchestrator(context):
    from workflow.orchestrator import claim_to_dispute_orchestrator as impl
    return (yield from impl(context))

# Register activities on the blueprint
@workflow_bp.activity_trigger(input_name="input_data")
def detect_underpaid_claims(input_data):
    from workflow.activities import detect_underpaid_claims as impl
    return impl(input_data)

# Merge into the main app
app.register_functions(workflow_bp)

# Regular functions register directly on app
@app.route(route="workflow/create-disputes", methods=["POST"])
async def create_disputes_function(req, starter):
    # This HTTP trigger STARTS the orchestrator
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new("claim_to_dispute_orchestrator", client_input=body)
```

### Three Orchestrators

#### Orchestrator 1: Claim-to-Dispute Pipeline

Detects underpaid claims and creates disputes, cases, and deadlines.

```
POST /api/workflow/create-disputes
  or: automatically after ingestion detects underpayment
        │
        ▼
claim_to_dispute_orchestrator (workflow/orchestrator.py:22)
        │
        ├── detect_underpaid_claims()              Find claims where billed > paid
        │   → returns: [{claim_id: 1, billed: 850, paid: 380}, ...]
        │
        ├── FAN-OUT (parallel per claim):
        │   ├── create_dispute_and_case(claim_1)   Create case + dispute + assign analyst
        │   ├── create_dispute_and_case(claim_2)   Priority routing: >$500=high, >$200=medium
        │   └── create_dispute_and_case(claim_3)   Returns: {case_id, dispute_id, analyst, priority}
        │
        ├── FAN-OUT (parallel per new case):
        │   ├── set_regulatory_deadlines(case_1)   NSA deadlines: 30d, 34d, 64d from filed_date
        │   ├── set_regulatory_deadlines(case_2)
        │   └── set_regulatory_deadlines(case_3)
        │
        └── FOR EACH new case:
            └── send_analyst_notification(case)    Log notification to assigned analyst
```

**Trigger:** `POST /api/workflow/create-disputes` → `functions/workflow/deadline_monitor.py:36`
**Orchestrator:** `functions/workflow/orchestrator.py:22`

#### Orchestrator 2: Deadline Monitor

Checks for at-risk and missed NSA deadlines every 6 hours.

```
Timer trigger (every 6 hours)
  or: POST /api/workflow/check-deadlines
        │
        ▼
deadline_monitor_orchestrator (workflow/orchestrator.py:89)
        │
        ├── get_at_risk_deadlines({days_ahead: 5})     Deadlines due within 5 days
        │   → returns: [{deadline_id, case_id, type, due_date, days_remaining}, ...]
        │
        ├── get_missed_deadlines()                     Deadlines past due date
        │   → returns: [{deadline_id, case_id, type, due_date}, ...]
        │
        ├── FAN-OUT: send_deadline_alert() for each at-risk
        │   → Updates deadline status to 'alerted'
        │
        ├── FAN-OUT: mark_deadline_missed() for each missed
        │   → Updates deadline status to 'missed'
        │
        └── escalate_missed_deadlines()                Critical missed deadlines
            → Escalates case priority to 'critical'
            → Types: idr_initiation, idr_decision, evidence_submission
```

**Trigger:** Timer `0 0 */6 * * *` → `functions/function_app.py` (function #10)
**Orchestrator:** `functions/workflow/orchestrator.py:89`

#### Orchestrator 3: Case Transition

Manages the NSA workflow state machine with validation and side effects.

```
POST /api/workflow/transition-case
  Body: {"case_id": 1, "target_status": "negotiation", "user_id": "analyst_kpatel"}
        │
        ▼
case_transition_orchestrator (workflow/orchestrator.py:152)
        │
        ├── validate_case_transition()
        │   → Check: is current_status → target_status allowed?
        │   → Valid transitions:
        │       open → in_review
        │       in_review → negotiation, closed
        │       negotiation → idr_initiated, closed
        │       idr_initiated → idr_submitted, closed
        │       idr_submitted → decided, closed
        │       decided → closed
        │   → Returns: {valid: true/false, reason: "..."}
        │
        ├── execute_case_transition()
        │   → UPDATE cases SET status = target_status
        │   → If closing: SET closed_date = now
        │
        ├── Side effects (based on target_status):
        │   ├── → negotiation:    complete_deadline("open_negotiation")
        │   ├── → idr_initiated:  complete_deadline("idr_initiation")
        │   │                     + set_regulatory_deadlines(["evidence_submission"])
        │   ├── → decided:        complete_deadline("idr_decision")
        │   └── → closed:         close_all_deadlines(case_id)
        │
        └── write_audit_log()
            → entity_type="case", action="status_change"
            → old_value, new_value, user_id
```

**Trigger:** `POST /api/workflow/transition-case` → `functions/workflow/deadline_monitor.py:60`
**Orchestrator:** `functions/workflow/orchestrator.py:152`

### 14 Activity Functions

Activities are the atomic units of work called by orchestrators. Each runs independently
and is auto-retried on failure.

| # | Activity | File:Line | What It Does |
|---|---|---|---|
| 1 | `detect_underpaid_claims` | `activities.py:51` | SQL: claims WHERE billed > paid AND no dispute exists |
| 2 | `create_dispute_and_case` | `activities.py:82` | INSERT case + dispute, assign analyst by priority routing |
| 3 | `set_regulatory_deadlines` | `activities.py:185` | INSERT NSA deadlines (30d, 34d, 44d, 37d, 64d) |
| 4 | `get_at_risk_deadlines` | `activities.py:236` | SQL: deadlines WHERE due_date within N days |
| 5 | `get_missed_deadlines` | `activities.py:262` | SQL: deadlines WHERE due_date < today AND status=pending |
| 6 | `mark_deadline_missed` | `activities.py:285` | UPDATE deadlines SET status='missed' |
| 7 | `complete_deadline` | `activities.py:304` | UPDATE deadlines SET status='met', completed_date=now |
| 8 | `close_all_deadlines` | `activities.py:325` | UPDATE all pending deadlines for a case to 'met' |
| 9 | `validate_case_transition` | `activities.py:346` | Check state machine: is transition allowed? |
| 10 | `execute_case_transition` | `activities.py:378` | UPDATE cases SET status, closed_date |
| 11 | `send_deadline_alert` | `activities.py:414` | Log warning + UPDATE deadline status to 'alerted' |
| 12 | `escalate_missed_deadlines` | `activities.py:449` | UPDATE case priority to 'critical' for missed IDR deadlines |
| 13 | `send_analyst_notification` | `activities.py:480` | Log notification (production: Teams/email) |
| 14 | `write_audit_log` | `activities.py:498` | INSERT into audit_log table |

### Priority Routing

When a dispute is created, the analyst is assigned based on underpayment severity:

```python
# workflow/activities.py:14
ANALYST_ROUTING = {
    "high":   "analyst_kpatel",    # $500+ underpayment
    "medium": "analyst_mchen",     # $200-$500
    "low":    "analyst_jsmith",    # <$200
}
```

### NSA Regulatory Deadlines

Each dispute gets regulatory deadlines based on the filed date:

```python
# workflow/activities.py:21
NSA_DEADLINES = {
    "open_negotiation":   30,   # 30 days for open negotiation
    "idr_initiation":     34,   # 4 business days after negotiation period
    "entity_selection":   37,   # 3 business days after IDR initiation
    "evidence_submission": 44,  # 10 business days after IDR initiation
    "idr_decision":       64,   # 30 days after IDR initiation
}
```

---

## 3. OLAP Pipeline (Analytics Refresh)

### Functions-Based Pipeline

Runs Bronze → Silver → Gold using pandas + ADLS Parquet:

```
Timer trigger (every 4 hours) OR HTTP POST /api/olap/run
        │
        ▼
olap_pipeline_function (function_app.py, function #13)
        │
        ├── extract_all_bronze()          olap/bronze.py
        │   → CDC: read changed rows from 12 OLTP tables
        │   → Write append-only Parquet to ADLS Bronze container
        │   → Update cdc_watermarks
        │
        ├── transform_all_silver()        olap/silver.py
        │   → Read Bronze Parquet
        │   → Deduplicate, clean, join (claims ↔ remittances)
        │   → Write Silver Parquet (9 tables)
        │
        └── aggregate_all_gold()          olap/gold.py
            → Read Silver Parquet
            → Compute 13 Gold aggregations
            → Write Gold Parquet (for Power BI Direct Lake)
```

**Manual trigger with layer selection:**
```bash
# All layers
curl -X POST ".../api/olap/run" -d '{"layer": "all"}'

# Single layer
curl -X POST ".../api/olap/run" -d '{"layer": "gold"}'
```

### ADF Master Orchestrator

Same pipeline but orchestrated by ADF (with Fabric notebooks instead of pandas):

```
pl_master_orchestrator:
  Run_CDC_Incremental → Run_Bronze_Notebook → Run_Silver_Notebook → Run_Gold_Notebook
```

---

## Complete Orchestration Map

```
TIME-BASED                              EVENT-BASED                        MANUAL
──────────                              ───────────                        ──────

Every 15 min:                           Claim ingested with               POST /api/workflow/
  ADF: pl_cdc_incremental_copy          underpayment detected:              create-disputes
  → OLTP → Bronze Parquet               → claim_to_dispute_orchestrator   POST /api/workflow/
                                          → create case + dispute            transition-case
Every 4 hours:                            → set deadlines                 POST /api/olap/run
  Functions: olap_pipeline_function       → assign analyst
  → Bronze → Silver → Gold

Every 6 hours:                          Document uploaded to
  Functions: deadline_monitor             Blob Storage:
  → check at-risk deadlines              → ingest_document_function
  → alert / escalate                      → classify + store

Daily 6 AM:                             EDI 835 arrives on
  ADF: pl_batch_fee_schedule              Event Hub:
  → CSV → staging → SCD2 merge           → ingest_remittances_function
                                           → match claim → store payment
```

---

## How to Trigger Each Orchestration

| Orchestration | Automatic | Manual CLI | Manual HTTP |
|---|---|---|---|
| CDC pipeline | `trg_cdc_15min` (start trigger) | `az datafactory pipeline create-run --pipeline-name pl_cdc_incremental_copy` | — |
| Fee schedule batch | `trg_fee_schedule_daily` (start trigger) | `az datafactory pipeline create-run --pipeline-name pl_batch_fee_schedule` | — |
| Master orchestrator | — (manual or custom trigger) | `az datafactory pipeline create-run --pipeline-name pl_master_orchestrator` | — |
| OLAP pipeline | Timer (every 4h, auto) | — | `POST /api/olap/run {"layer":"all"}` |
| Deadline monitor | Timer (every 6h, auto) | — | — |
| Create disputes | — | — | `POST /api/workflow/create-disputes` |
| Case transition | — | — | `POST /api/workflow/transition-case {"case_id":1,"target_status":"negotiation"}` |

---

## Monitoring

### ADF Pipeline Runs

```bash
# Recent runs
az datafactory pipeline-run query-by-factory --factory-name medbill-adf \
  --resource-group rg-medbill-test \
  --last-updated-after 2025-03-01T00:00:00Z \
  --last-updated-before 2025-04-01T00:00:00Z -o table

# Or in ADF Studio: Monitor tab → Pipeline runs
```

### Function Logs

```bash
# Stream live logs
func azure functionapp logstream medbill-func-8df6df9c

# Output includes:
# [timestamp] Deadline monitor timer triggered
# [timestamp] DEADLINE ALERT: Case 3 — open_negotiation due 2026-04-04 (3 days remaining)
# [timestamp] OLAP pipeline timer triggered
# [timestamp] Bronze extraction complete: 12 tables, 45 total rows
```

### Durable Function Status

When you start an orchestration via HTTP, the response includes status URLs:

```json
{
    "id": "abc123",
    "statusQueryGetUri": "https://.../runtime/webhooks/durabletask/instances/abc123",
    "sendEventPostUri": "https://.../runtime/webhooks/durabletask/instances/abc123/raiseEvent/{eventName}",
    "terminatePostUri": "https://.../runtime/webhooks/durabletask/instances/abc123/terminate"
}
```

Query the status URL to see progress:
```bash
curl "https://.../runtime/webhooks/durabletask/instances/abc123?code=<key>"
```

---

## Code Reference Summary

| File | Purpose |
|---|---|
| `adf/pipelines/pl_master_orchestrator.json` | Master chain: CDC → Bronze → Silver → Gold |
| `adf/pipelines/pl_cdc_incremental_copy.json` | CDC: ForEach 9 tables → watermark → copy → update |
| `adf/pipelines/pl_batch_fee_schedule.json` | CSV → staging → SCD2 merge → archive |
| `adf/pipelines/pl_batch_providers.json` | NPPES CSV → staging → merge → archive |
| `functions/workflow/orchestrator.py` | 3 orchestrators: claim-to-dispute, deadline monitor, case transition |
| `functions/workflow/activities.py` | 14 activity functions (SQL queries, status updates, notifications) |
| `functions/workflow/deadline_monitor.py` | Timer + HTTP triggers that start orchestrators |
| `functions/function_app.py` | Registers all triggers, blueprint for Durable Functions |
| `functions/olap/bronze.py` | CDC extraction: OLTP → Bronze Parquet |
| `functions/olap/silver.py` | Silver transforms: deduplicate, join, clean |
| `functions/olap/gold.py` | 13 Gold aggregation functions |
| `sql/staging_tables.sql` | Staging tables + stored procedures (SCD2 merge) |
