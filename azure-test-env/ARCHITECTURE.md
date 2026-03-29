# Azure Test Environment — Architecture, Diagrams & Codebase Reference

> **Deployment:** `azure-test-env/`
> **Reference:** `medical_billing_arbitration_future_architecture.md`
> **Last reviewed:** March 2026

---

## Table of Contents

1. [System Architecture Diagram](#1-system-architecture-diagram)
2. [Data Flow — End-to-End](#2-data-flow--end-to-end)
3. [Ingestion Layer](#3-ingestion-layer)
4. [OLTP Operational Store](#4-oltp-operational-store)
5. [CDC & Lakehouse Pipeline](#5-cdc--lakehouse-pipeline)
6. [Medallion Architecture — Bronze / Silver / Gold](#6-medallion-architecture--bronze--silver--gold)
7. [Workflow Engine](#7-workflow-engine)
8. [Deployment & Infrastructure](#8-deployment--infrastructure)
9. [Alignment with Future Architecture](#9-alignment-with-future-architecture)

---

## 1. System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                       │
│                                                                                   │
│   Clearinghouses     Payer Portals      EHR / PMS          Manual Uploads         │
│   (EDI 837/835)      (ERA + EOB PDF)    (FHIR / HL7v2)    (Contracts, Docs)      │
└──────┬───────────────────┬──────────────────┬──────────────────┬─────────────────┘
       │                   │                  │                  │
       ▼                   ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         AZURE EVENT BUS                                           │
│                                                                                   │
│   ┌─────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────┐  │
│   │ Event Hub:   │  │ Event Hub:       │  │ Event Hub:       │  │ Event Hub:  │  │
│   │ claims       │  │ remittances      │  │ documents        │  │ status-     │  │
│   │              │  │                  │  │                  │  │ changes     │  │
│   └──────┬───────┘  └──────┬───────────┘  └──────┬───────────┘  └──────┬──────┘  │
└──────────┼─────────────────┼─────────────────────┼──────────────────────┼─────────┘
           │                 │                     │                      │
           ▼                 ▼                     ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    AZURE FUNCTIONS  (Ingestion Layer)                              │
│                                                                                   │
│  ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐ │
│  │ Claims Ingest        │ │ Remittance Ingest    │ │ Document Ingest      │ │ Patient Ingest       │ │
│  │ (Event Hub)          │ │ (Event Hub)          │ │ (Blob Trigger)       │ │ (HTTP POST)          │ │
│  │ function_app.py:138  │ │ function_app.py:162  │ │ function_app.py:186  │ │ function_app.py:210  │ │
│  └──────────┬───────────┘ └──────────┬───────────┘ └──────────┬───────────┘ └──────────┬───────────┘ │
│             │                        │                        │                        │              │
│  ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────────────────────────────┐  │
│  │ Fee Schedule         │ │ EOB Ingest           │ │              SHARED SERVICES                 │  │
│  │ (Timer 6AM)          │ │ (Event Hub)          │ │  ┌────────────────┐ ┌────────────────────┐   │  │
│  │ function_app.py:239  │ │ function_app.py:292  │ │  │ dedup          │ │ dlq                │   │  │
│  └──────────┬───────────┘ └──────────┬───────────┘ │  │ dedup.py:12    │ │ dlq.py:15          │   │  │
│             │                        │             │  └────────────────┘ └────────────────────┘   │  │
│             │                        │             │  ┌────────────────┐ ┌────────────────────┐   │  │
│             │                        │             │  │ events         │ │ db (pyodbc pool)   │   │  │
│             │                        │             │  │ events.py:43   │ │ db.py:18           │   │  │
│             │                        │             │  └────────────────┘ └────────────────────┘   │  │
│             │                        │             │  ┌────────────────┐                          │  │
│             │                        │             │  │ audit          │                          │  │
│             │                        │             │  │ audit.py:10    │                          │  │
│             │                        │             │  └────────────────┘                          │  │
│             │                        │             └──────────────────────────────────────────────┘  │
└───────────┼───────────────────┼─────────────────────────────────────────────────┘
            │                   │
            ▼                   ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                      AZURE SQL DATABASE  (OLTP)                                   │
│                                                                                   │
│  ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐ ┌──────────────────────────┐  │
│  │ claims            │ │ claim_lines       │ │ remittances       │ │ patients          │ │ fee_schedule (SCD2)      │  │
│  │ schema.sql:77     │ │ schema.sql:94     │ │ schema.sql:108    │ │ schema.sql:34     │ │ schema.sql:54            │  │
│  │                   │ │                   │ │                   │ │                   │ │ valid_from / valid_to     │  │
│  └────────┬──────────┘ └───────────────────┘ └───────────────────┘ └───────────────────┘ └──────────────────────────┘  │
│           │                                                                                                            │
│  ┌────────┴──────────┐ ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐ ┌──────────────────────────┐  │
│  │ disputes          │ │ cases             │ │ dead_letter_queue │ │ evidence_artifacts│ │ deadlines                │  │
│  │ schema.sql:139    │ │ schema.sql:125    │ │ schema.sql:227    │ │ schema.sql:164    │ │ schema.sql:186           │  │
│  └───────────────────┘ └───────────────────┘ └───────────────────┘ └───────────────────┘ └──────────────────────────┘  │
│                                                                                                                        │
│  ┌───────────────────┐ ┌───────────────────┐ ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ audit_log         │ │ claim_id_alias    │ │ Staging: stg_fee_schedule    staging_tables.sql:26                  │  │
│  │ schema.sql:206    │ │ schema.sql:248    │ │          stg_providers       staging_tables.sql:47                  │  │
│  └───────────────────┘ └───────────────────┘ │          stg_backfill        staging_tables.sql:69                  │  │
│                                              └──────────────────────────────────────────────────────────────────────┘  │
│                                                                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ Stored Procedures:  usp_merge_fee_schedule  staging_tables.sql:88                                              │  │
│  │                     usp_merge_providers     staging_tables.sql:166                                              │  │
│  │                     usp_update_cdc_watermark staging_tables.sql:232                                             │  │
│  └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────┬────────────────────────────────────────────┘
                                      │
                                      │ CDC (watermark-based incremental copy)
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    AZURE DATA FACTORY  (Orchestration)                             │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │ pl_master_orchestrator                                                       │  │
│  │ ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐  │  │
│  │ │ CDC Incr Copy │→ │ Bronze NB    │→ │ Silver NB    │→ │ Gold NB     │  │  │
│  │ └───────────────┘  └───────────────┘  └───────────────┘  └──────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                   │
│  ┌──────────────────────────┐  ┌───────────────────────────────────────────────┐  │
│  │ pl_cdc_incremental_copy  │  │ pl_batch_fee_schedule                         │  │
│  │ 9 tables × watermark     │  │ CSV → stg_fee_schedule → usp_merge (SCD2)    │  │
│  └──────────────────────────┘  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────┬────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                   ADLS GEN2 / ONELAKE  (Lakehouse)                                │
│                                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────────┐   │
│  │  BRONZE (Parquet, append-only, date-partitioned)                           │   │
│  │  nb_bronze_cdc.py → ingest_bronze_table() nb_bronze_cdc.py:44             │   │
│  │  bronze/{table}/year=YYYY/month=MM/day=DD/*.parquet                       │   │
│  │  + _cdc_operation, _cdc_timestamp, _source_table metadata                 │   │
│  ├────────────────────────────────────────────────────────────────────────────┤   │
│  │  SILVER (Delta Lake, MERGE INTO, cleaned + joined)                         │   │
│  │  nb_silver_transforms.py → resolve_cdc_to_current() nb_silver_transforms.py:41  │
│  │                           → write_silver()            nb_silver_transforms.py:68 │
│  │  9 Silver tables: claims, remittances, claim_remittance, fee_schedule,    │   │
│  │                   providers, patients, disputes, cases, deadlines          │   │
│  ├────────────────────────────────────────────────────────────────────────────┤   │
│  │  GOLD (Delta Lake, aggregated for BI)                                      │   │
│  │  nb_gold_aggregations.py → 8 aggregation functions                        │   │
│  │  8 Gold tables: recovery_by_payer, cpt_analysis, payer_scorecard,         │   │
│  │                 financial_summary, claims_aging, case_pipeline,            │   │
│  │                 deadline_compliance, underpayment_detection                │   │
│  └────────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                            │
│                                      ▼                                            │
│                              Power BI (Direct Lake)                               │
└──────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────┐
│                    DURABLE FUNCTIONS  (Workflow Engine)                            │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │ Orchestrators (3)                                      Activities (14)                                    │  │
│  │ ┌──────────────────────────────────────────┐           ┌──────────────────────────────────────────────┐   │  │
│  │ │ claim_to_dispute   orchestrator.py:22    │           │ detect_underpaid_claims  activities.py:51    │   │  │
│  │ │ deadline_monitor   orchestrator.py:89    │           │ create_dispute_and_case  activities.py:82    │   │  │
│  │ │ case_transition    orchestrator.py:152   │           │ set_regulatory_deadlines activities.py:185   │   │  │
│  │ └──────────────────────────────────────────┘           │ validate_case_transition activities.py:346   │   │  │
│  │                                                        │ execute_case_transition  activities.py:378   │   │  │
│  │ Triggers (3)                                           │ send_deadline_alert      activities.py:414   │   │  │
│  │ ┌──────────────────────────────────────────┐           │ escalate_missed_deadlines activities.py:449  │   │  │
│  │ │ deadline_monitor_timer                   │           │ mark_deadline_missed     activities.py:285   │   │  │
│  │ │   deadline_monitor.py:19 (every 6 hours) │           │ complete_deadline        activities.py:304   │   │  │
│  │ │ claim_dispute_http                       │           │ close_all_deadlines      activities.py:325   │   │  │
│  │ │   deadline_monitor.py:36 (POST)          │           │ send_analyst_notification activities.py:480  │   │  │
│  │ │ case_transition_http                     │           │ write_audit_log          activities.py:498   │   │  │
│  │ │   deadline_monitor.py:60 (POST)          │           │ get_at_risk_deadlines    activities.py:236   │   │  │
│  │ └──────────────────────────────────────────┘           │ get_missed_deadlines     activities.py:262   │   │  │
│  │                                                        └──────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**All references use `file_name:line_number` format for direct navigation.**

---

## 1b. How Data Gets Into the System

### Source Systems → Event Hub / Blob / HTTP

Data does not arrive automatically — external systems push it in. In production, these are
clearinghouses, payer portals, EHR systems, and manual uploads. In the test environment,
`functions/sample-events/simulate.py` plays the role of all external systems.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS (external)                            │
│                                                                             │
│  Clearinghouse ─────┐     A clearinghouse (e.g., Availity, Change           │
│  (Availity, Change  │     Healthcare, Trizetto) is a third-party            │
│   Healthcare)       │     intermediary that routes healthcare claims         │
│                     │     between providers and payers. Think of it          │
│  Payer Portal ──────┤     like a postal service for medical billing:         │
│  (Aetna, UHC,      │     the provider submits an EDI 837 claim to the      │
│   Anthem)           │     clearinghouse, which validates format, routes      │
│                     │     it to the correct payer, and returns the           │
│  EHR / PMS ─────────┤     EDI 835 remittance (payment) back.                │
│  (Epic, Cerner)     │                                                       │
│                     │     In our system, the clearinghouse pushes EDI        │
│  Manual Upload ─────┘     files to our Azure Event Hub via API.              │
│                                                                             │
└────────┬────────────────┬────────────────┬────────────────┬─────────────────┘
         │                │                │                │
         ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  HOW THEY PUSH DATA IN:                                                     │
│                                                                             │
│  Method 1: Event Hub SDK (clearinghouse / payer integration)                │
│    from azure.eventhub import EventHubProducerClient, EventData             │
│    client = EventHubProducerClient.from_connection_string(conn, "claims")   │
│    batch = client.create_batch()                                            │
│    batch.add(EventData(edi_837_content))   # EDI file as bytes              │
│    client.send_batch(batch)                # push to Event Hub              │
│                                                                             │
│  Method 2: Azure Logic App (SFTP pickup → Event Hub)                        │
│    Payer drops EDI file on SFTP nightly → Logic App polls SFTP              │
│    → reads file → pushes to Event Hub automatically                         │
│                                                                             │
│  Method 3: HTTP POST (EHR webhook)                                          │
│    EHR system calls POST /api/ingest/patients with FHIR JSON               │
│    → Azure Function handles it directly (no Event Hub needed)               │
│                                                                             │
│  Method 4: Blob Upload (documents, fee schedules)                           │
│    Upload PDF/CSV to Azure Blob Storage container                           │
│    → Blob trigger fires automatically when file lands                       │
│                                                                             │
│  Method 5: Simulator (test environment)                                     │
│    python simulate.py --all                                                 │
│    → Pushes sample EDI/FHIR/CSV to Event Hub, HTTP, and Blob               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### How Azure Functions Get Triggered

You never call the functions yourself. Azure monitors the event sources and calls your
function automatically when data arrives. The decorator in `function_app.py` defines what
to watch:

```
Message lands in Event Hub "claims"
        │
        ▼
Azure Functions runtime detects new message (polling every few seconds)
        │
        ▼
Runtime deserializes message → EventHubEvent object
        │
        ▼
Runtime calls ingest_claims_function(event) ← YOUR CODE
        │
        ├── Success: message acknowledged, removed from queue
        └── Exception: message retried (up to 5x), then dead-lettered
```

**Trigger type determines WHEN your code runs:**

| Trigger Type | What Azure Watches | When Your Function Runs | Used By |
|---|---|---|---|
| **Event Hub** | Messages on a topic | New message arrives | Claims (#1), Remittances (#2), EOB (#6) |
| **Blob Storage** | Files in a container | File uploaded/created | Documents (#3) |
| **HTTP** | Incoming HTTP request | Request hits the URL | Patients (#4), Health (#7), Workflow (#8-10), Agent (#11-12) |
| **Timer** | Cron schedule | Clock hits the schedule | Fee Schedules (#5, daily 6AM), Deadline Monitor (#10, every 6h), OLAP (#13, every 4h) |

**The decorator is the contract** — it tells Azure: "watch this source, and when something happens, call my function":

```python
# "When a message arrives on the 'claims' Event Hub, call this function"
@app.event_hub_message_trigger(event_hub_name="claims", connection="EVENTHUB_CONNECTION_STRING")
def ingest_claims_function(event):
    raw_edi = event.get_body().decode("utf-8")     # get the EDI content
    result = ingest_claims(raw_edi)                # parse → validate → insert → Azure SQL

# "When a file appears in documents/ container, call this function"
@app.blob_trigger(path="documents/{name}", connection="STORAGE_CONNECTION_STRING")
def ingest_document_function(blob):
    content = blob.read()                          # get the file bytes
    result = ingest_document(content, blob.name)   # classify → store → Azure SQL

# "Every day at 6 AM UTC, call this function"
@app.timer_trigger(schedule="0 0 6 * * *")
def ingest_fee_schedule_function(timer):
    # check Bronze container for new CSVs and process them

# "When someone POSTs to /api/ingest/patients, call this function"
@app.route(route="ingest/patients", methods=["POST"])
def ingest_patients_function(req):
    fhir_json = req.get_body().decode("utf-8")     # get the FHIR JSON
    result = ingest_patients(fhir_json)            # normalize → validate → insert
```

---

## 2. Data Flow — End-to-End

### 2.1 Claim Lifecycle Flow

```
EDI 837 File Arrives
        │
        ▼
Event Hub: "claims"
        │
        ▼
┌── Azure Function: ingest_claims_function ──────────────────────────────────────┐
│   function_app.py:138                                                           │
│       │                                                                         │
│       ▼                                                                         │
│   EDI837Parser.parse()                   parsers/edi_837.py:29                  │
│       │  Extracts: claim_id, patient_id, provider_npi, payer_id,               │
│       │            date_of_service, total_billed, CPT lines, Dx codes           │
│       ▼                                                                         │
│   validate_claim()                       validators/validation.py:14            │
│       │  Checks: claim_id present, DOS valid, total_billed > 0, lines exist    │
│       │                                                                         │
│       ├── INVALID ──► send_to_dlq()      shared/dlq.py:15                      │
│       │               (dead_letter_queue table, category: validation_failure)   │
│       │                                                                         │
│       ▼  VALID                                                                  │
│   resolve_claim_duplicate()              shared/dedup.py:12                     │
│       │  Checks frequency_code: 1=original, 7=replacement, 8=void              │
│       │                                                                         │
│       ├── "skip"   ──► already exists, idempotent skip                         │
│       ├── "insert"  ──► _insert_claim()  ingest/claims.py:66                   │
│       ├── "update"  ──► _update_claim()  ingest/claims.py:89                   │
│       └── "void"    ──► _void_claim()    ingest/claims.py:115                  │
│                                                                                 │
│   log_action()                           shared/audit.py:10                     │
│   emit_event("claim.insert")            shared/events.py:43                    │
│       └──► Event Hub: "status-changes"                                          │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼ (written to Azure SQL)
   claims table (schema.sql:77)
   claim_lines table (schema.sql:94)
```

### 2.2 Remittance → Underpayment Detection Flow

```
EDI 835 File Arrives
        │
        ▼
Event Hub: "remittances"
        │
        ▼
┌── Azure Function: ingest_remittances_function ─────────────────────────────────┐
│   function_app.py:162                                                           │
│       │                                                                         │
│       ▼                                                                         │
│   EDI835Parser.parse()                   parsers/edi_835.py:18                  │
│       │  Extracts: payer_claim_id, paid_amount, total_billed,                  │
│       │            adjustments (CARC/RARC), trace_number, service_lines         │
│       ▼                                                                         │
│   validate_remittance()                  validators/validation.py:36            │
│       ▼                                                                         │
│   match_claim_id()                       shared/dedup.py:46                     │
│       │  Resolves payer's claim_id → canonical claim_id                        │
│       │  (direct match → normalized → claim_id_alias table)                    │
│       │                                                                         │
│       ├── NO MATCH ──► send_to_dlq() (category: unmatched_reference)           │
│       │                                                                         │
│       ▼  MATCHED                                                                │
│   check_remittance_duplicate()           shared/dedup.py:27                     │
│       │  (trace_number + claim_id + payer_id)                                  │
│       │                                                                         │
│       ├── DUPLICATE ──► skip                                                   │
│       ▼  NEW                                                                    │
│   INSERT → remittances table                                                    │
│   EDI835Parser.extract_denial_code()     parsers/edi_835.py:79                  │
│       └── denial_code stored for downstream arbitration logic                  │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼ (trigger: underpayment detected when billed > paid)
┌── Durable Function: claim_to_dispute_orchestrator ─────────────────────────────┐
│   workflow/orchestrator.py:22                                                   │
│       │                                                                         │
│       ▼                                                                         │
│   detect_underpaid_claims()              workflow/activities.py:51              │
│       │  SQL: WHERE total_billed > paid_amount AND dispute not exists          │
│       ▼                                                                         │
│   ┌── Fan-out (parallel per claim) ────────────────────────────────────────┐   │
│   │                                                                         │   │
│   │  create_dispute_and_case()           workflow/activities.py:82          │   │
│   │       │  Creates: case (priority routed), dispute, audit entry          │   │
│   │       │  Priority: high ≥ $500, medium ≥ $200, low < $200              │   │
│   │       ▼                                                                 │   │
│   │  set_regulatory_deadlines()          workflow/activities.py:185         │   │
│   │       │  Creates NSA deadlines:                                         │   │
│   │       │    open_negotiation:   filed_date + 30 days                    │   │
│   │       │    idr_initiation:     filed_date + 34 days                    │   │
│   │       │    idr_decision:       filed_date + 64 days                    │   │
│   │       ▼                                                                 │   │
│   │  send_analyst_notification()         workflow/activities.py:480         │   │
│   │       └── (stub — log only in test env)                                 │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│   Output: {cases_created: N, case_ids: [...]}                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼ (written to Azure SQL)
   disputes table (schema.sql:139)
   cases table (schema.sql:125)
   deadlines table (schema.sql:186)
```

### 2.3 CDC → Lakehouse Flow

```
OLTP Tables (Azure SQL)
   │  claims, claim_lines, remittances, patients, cases,
   │  disputes, deadlines, evidence_artifacts, audit_log
   │
   │  Tracked via updated_at / created_at / last_activity columns
   ▼
┌── ADF: pl_cdc_incremental_copy ────────────────────────────────────────────────┐
│                                                                                 │
│   ForEach table in 9 CDC tables (parallel batch of 4):                         │
│       │                                                                         │
│       ├─ Lookup_Watermark ──► cdc_watermarks.last_sync_ts                      │
│       │                       (staging_tables.sql:10)                           │
│       │                                                                         │
│       ├─ Lookup_MaxTimestamp ──► MAX(watermark_col), COUNT(*)                   │
│       │                          WHERE watermark_col > last_sync_ts            │
│       │                                                                         │
│       ├─ IF rows changed:                                                       │
│       │   │                                                                     │
│       │   ├─ Copy_To_Bronze ──► ADLS Parquet                                   │
│       │   │   bronze/{table}/year=YYYY/month=MM/day=DD/{table}_{ts}.parquet    │
│       │   │                                                                     │
│       │   └─ Update_Watermark ──► usp_update_cdc_watermark                     │
│       │                           (staging_tables.sql:232)                      │
│       │                                                                         │
│       └─ ELSE: skip (no changes)                                                │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌── ADF: pl_master_orchestrator (sequential) ────────────────────────────────────┐
│                                                                                 │
│   Step 1: Run_CDC_Incremental ──► pl_cdc_incremental_copy (above)              │
│       │                                                                         │
│       ▼                                                                         │
│   Step 2: Run_Bronze_Notebook                                                   │
│       │   nb_bronze_cdc.py                                                      │
│       │   ingest_bronze_table()           nb_bronze_cdc.py:44                   │
│       │     Read Parquet → Add CDC metadata → Append to Delta                  │
│       │     (_cdc_operation, _cdc_timestamp, _source_table,                    │
│       │      _bronze_loaded_ts, _source_file)                                  │
│       ▼                                                                         │
│   Step 3: Run_Silver_Notebook                                                   │
│       │   nb_silver_transforms.py                                               │
│       │   resolve_cdc_to_current()        nb_silver_transforms.py:41  (latest per PK, no deletes)
│       │   write_silver()                  nb_silver_transforms.py:68  (MERGE INTO Delta)
│       │   9 transforms:
│       │     transform_claims()            nb_silver_transforms.py:123
│       │     transform_remittances()       nb_silver_transforms.py:156
│       │     transform_claim_remittance()  nb_silver_transforms.py:181  (claim ↔ remittance join)
│       │     transform_fee_schedule()      nb_silver_transforms.py:246  (SCD2 current rates)
│       │     transform_disputes()          nb_silver_transforms.py:261
│       │     transform_cases()             nb_silver_transforms.py:313
│       │     transform_deadlines()         nb_silver_transforms.py:359
│       │     transform_patients()          nb_silver_transforms.py:93
│       │     transform_providers()         nb_silver_transforms.py:108
│       ▼                                                                         │
│   Step 4: Run_Gold_Notebook                                                     │
│       nb_gold_aggregations.py                                                   │
│       8 aggregations:                                                           │
│         agg_recovery_by_payer()           nb_gold_aggregations.py:62
│         agg_cpt_analysis()                nb_gold_aggregations.py:101
│         agg_payer_scorecard()             nb_gold_aggregations.py:169
│         agg_financial_summary()           nb_gold_aggregations.py:226
│         agg_claims_aging()                nb_gold_aggregations.py:281
│         agg_case_pipeline()               nb_gold_aggregations.py:315
│         agg_deadline_compliance()         nb_gold_aggregations.py:361
│         agg_underpayment_detection()      nb_gold_aggregations.py:390
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
   Gold Delta Tables → Power BI (Direct Lake)
```

### 2.4 Fee Schedule SCD Type 2 Flow

```
CMS / Payer publishes new rates (CSV)
        │
        ▼
   Uploaded to: bronze/fee-schedules/*.csv
        │
        ▼
┌── ADF: pl_batch_fee_schedule ──────────────────────────────────────────────────┐
│                                                                                 │
│   Get_FeeSchedule_Files ──► list bronze/fee-schedules/                         │
│       │                                                                         │
│       ▼                                                                         │
│   Filter_CSV_Files ──► keep *.csv only                                          │
│       │                                                                         │
│       ▼                                                                         │
│   ForEach CSV (parallel batch of 4):                                            │
│       │                                                                         │
│       ├─ Copy_CSV_To_Staging ──► stg_fee_schedule table                        │
│       │                          (staging_tables.sql:26)                        │
│       │                                                                         │
│       └─ Exec_SCD2_Merge ──► usp_merge_fee_schedule                            │
│                               (staging_tables.sql:88)                           │
│               │                                                                 │
│               ├─ Unchanged rate → SKIP                                          │
│               ├─ Changed rate   → CLOSE old row (valid_to = effective - 1 day) │
│               │                   INSERT new row (valid_from = effective date)  │
│               └─ New CPT code   → INSERT (valid_from = effective date)          │
│                                                                                 │
│   Archive_Processed ──► move to fee-schedules/archive/{date}/                   │
└─────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
   fee_schedule table (schema.sql:54)
   ┌────────┬──────────┬────────┬────────────┬────────────┬─────────┐
   │ payer  │ cpt_code │ rate   │ valid_from │ valid_to   │ current │
   ├────────┼──────────┼────────┼────────────┼────────────┼─────────┤
   │ Aetna  │ 99213    │ $120   │ 2024-01-01 │ 2024-12-31 │ false   │  ← closed
   │ Aetna  │ 99213    │ $135   │ 2025-01-01 │ 9999-12-31 │ true    │  ← current
   └────────┴──────────┴────────┴────────────┴────────────┴─────────┘

   Query at arbitration time:
   SELECT rate FROM fee_schedule
   WHERE payer_id='Aetna' AND cpt_code='99213'
     AND '2024-06-15' BETWEEN valid_from AND valid_to;
   -- Returns: $120 (the rate in effect at date of service)
```

### 2.5 Document Processing Flow

```
Document Uploaded (PDF, image, fax)
        │
        ▼
   Blob Storage: documents/{filename}
        │
        ▼
┌── Azure Function: ingest_document_function ────────────────────────────────────┐
│   function_app.py:186                                                           │
│       │                                                                         │
│       ▼                                                                         │
│   classify_document()                    ingest/documents.py:26                 │
│       │                                                                         │
│       ├── DOC_INTEL_ENDPOINT set?                                              │
│       │   YES → _classify_with_doc_intelligence()   documents.py:38             │
│       │         (Azure Document Intelligence API)                               │
│       │   NO  → _classify_by_filename()             documents.py:62             │
│       │         (pattern match: eob, clinical, contract, appeal, etc.)         │
│       ▼                                                                         │
│   ingest_document()                      ingest/documents.py:82                 │
│       │                                                                         │
│       ├── check_evidence_duplicate()     shared/dedup.py:37                    │
│       │   (SHA-256 content_hash)                                               │
│       │                                                                         │
│       ├── confidence < 0.9?                                                    │
│       │   YES → ocr_status = 'needs_review' (human review queue)               │
│       │   NO  → ocr_status = 'completed'                                       │
│       │                                                                         │
│       └── INSERT → evidence_artifacts table (schema.sql:164)                   │
│           (type, blob_url, extracted_data JSON, classification_confidence)      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.6 Case State Machine Flow

```
┌─────────────────────────────────────────────────────────────────┐
│              NSA ARBITRATION STATE MACHINE                        │
│                                                                   │
│   Triggered via: POST /api/workflow/transition-case               │
│   Orchestrator: case_transition_orchestrator                      │
│                 workflow/orchestrator.py:152                       │
│   Validation:   validate_case_transition                          │
│                 workflow/activities.py:346                         │
│                                                                   │
│      ┌────────┐                                                   │
│      │  open  │◄──── case created by claim_to_dispute_orchestrator│
│      └───┬────┘                                                   │
│          │                                                        │
│          ▼                                                        │
│      ┌───────────┐                                                │
│      │ in_review │  analyst reviews evidence                     │
│      └───┬───────┘                                                │
│          │                                                        │
│          ▼                                                        │
│      ┌─────────────┐                                              │
│      │ negotiation │  open_negotiation deadline (30 days)        │
│      └───┬─────────┘  complete_deadline activities.py:304        │
│          │                                                        │
│          ▼                                                        │
│      ┌───────────────┐                                            │
│      │ idr_initiated │  idr_initiation deadline (4 biz days)    │
│      └───┬───────────┘  + set evidence_submission deadline       │
│          │                                                        │
│          ▼                                                        │
│      ┌───────────────┐                                            │
│      │ idr_submitted │  evidence submitted                       │
│      └───┬───────────┘                                            │
│          │                                                        │
│          ▼                                                        │
│      ┌──────────┐                                                 │
│      │ decided  │  idr_decision deadline (30 biz days)           │
│      └───┬──────┘  complete_deadline activities.py:304           │
│          │                                                        │
│          ▼                                                        │
│      ┌──────────┐                                                 │
│      │ closed   │  close_all_deadlines activities.py:325         │
│      └──────────┘  outcome: won / lost / settled / withdrawn     │
│                                                                   │
│   VALID_TRANSITIONS map: workflow/activities.py:346               │
│   execute_case_transition: workflow/activities.py:378              │
└─────────────────────────────────────────────────────────────────┘
```

### 2.7 Deadline Monitoring Flow

```
Timer Trigger: every 6 hours
   workflow/deadline_monitor.py:19
        │
        ▼
┌── deadline_monitor_orchestrator ───────────────────────────────────────────────┐
│   workflow/orchestrator.py:89                                                   │
│       │                                                                         │
│       ├── get_at_risk_deadlines(threshold_days=5)                              │
│       │   workflow/activities.py:236                                            │
│       │   SQL: WHERE due_date <= NOW + 5 days AND status = 'pending'           │
│       │       │                                                                 │
│       │       ▼                                                                 │
│       │   Fan-out: send_deadline_alert() per deadline                          │
│       │            workflow/activities.py:414                                   │
│       │            Updates status → 'alerted', sets alerted_date               │
│       │                                                                         │
│       ├── get_missed_deadlines()                                               │
│       │   workflow/activities.py:262                                            │
│       │   SQL: WHERE due_date < NOW AND status IN ('pending', 'alerted')       │
│       │       │                                                                 │
│       │       ▼                                                                 │
│       │   Fan-out: mark_deadline_missed() per deadline                         │
│       │            workflow/activities.py:285                                   │
│       │            Updates status → 'missed'                                   │
│       │                                                                         │
│       └── escalate_missed_deadlines()                                          │
│           workflow/activities.py:449                                            │
│           Critical types: idr_initiation, idr_decision, evidence_submission    │
│           Updates case priority → 'critical'                                   │
│                                                                                 │
│   Output: {at_risk_count, missed_count, escalated_count, alerts_sent}          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Ingestion Layer — Codebase Reference

### 3.1 Azure Function Triggers

All triggers registered in `functions/function_app.py`. All paths relative to `functions/`.

| # | Trigger | Type | Data Flow |
|---|---------|------|-----------|
| 1 | Claims | Event Hub (`claims`) | Event Hub → `ingest/claims.py` → `parsers/edi_837.py` → `shared/db.py` → Azure SQL |
| 2 | Remittances | Event Hub (`remittances`) | Event Hub → `ingest/remittances.py` → `parsers/edi_835.py` → `shared/db.py` → Azure SQL |
| 3 | Documents | Blob (`documents/{name}`) | Blob Storage → `ingest/documents.py` → Doc Intelligence → `shared/db.py` → Azure SQL |
| 4 | Patients | HTTP POST `/api/ingest/patients` | HTTP → `ingest/patients.py` → `parsers/fhir_patient.py` → `shared/db.py` → Azure SQL |
| 5 | Fee Schedules | Timer (daily 6 AM) | Timer → ADLS `bronze/fee-schedules/` → `ingest/fee_schedules.py` → SCD2 merge → Azure SQL |
| 6 | EOB | Event Hub (`documents`) | Event Hub → `ingest/remittances.py:ingest_eob()` → `parsers/eob_mock.py` → Azure SQL |
| 7 | Health Check | HTTP GET `/api/health` | HTTP → `shared/db.py:fetchone()` → Azure SQL |
| 8 | Create Disputes | HTTP POST `/api/workflow/create-disputes` | HTTP → `workflow/deadline_monitor.py` → `workflow/orchestrator.py` → `workflow/activities.py` |
| 9 | Transition Case | HTTP POST `/api/workflow/transition-case` | HTTP → `workflow/deadline_monitor.py` → `workflow/orchestrator.py:case_transition` |
| 10 | Deadline Monitor | Timer (every 6 hours) | Timer → `workflow/deadline_monitor.py` → `workflow/orchestrator.py:deadline_monitor` |
| 11 | AI Agent: Ask | HTTP POST `/api/agent/ask` | HTTP → `agent/analyst.py:ask()` → Claude API → `sql/gold_views.sql` → Azure SQL |
| 11b | AI Agent: Common | GET/POST `/api/agent/common` | HTTP → `agent/analyst.py:get_common_analyses()` / `ask_common()` |
| 12 | AI Agent: Web UI | HTTP GET `/api/agent/ui` | HTTP → `agent/ui.html` (static) |
| 13 | OLAP Pipeline | Timer (every 4 hours) | Timer → `olap/bronze.py` → `olap/silver.py` → `olap/gold.py` → ADLS Parquet |
| 14 | OLAP Manual Run | HTTP POST `/api/olap/run` | HTTP → same as #13 |

**Shared infrastructure** (used by all ingestion functions):

| Module | File | Purpose |
|--------|------|---------|
| DB Connection | `shared/db.py` | pyodbc connection to Azure SQL (`SQL_CONNECTION_STRING` env var) |
| Deduplication | `shared/dedup.py` | Claim frequency codes, remittance trace dedup, content hash dedup |
| Dead-Letter Queue | `shared/dlq.py` | Failed records stored for analyst triage |
| Event Publisher | `shared/events.py` | Emit events to Azure Event Hubs for downstream consumers |
| Audit Log | `shared/audit.py` | Log all actions to `audit_log` table |
| Validation | `validators/validation.py` | 5 validators: claim, remittance, patient, fee_schedule, provider |

### 3.2 Parsers

| Parser | File | Class/Function | Line | Input Format |
|--------|------|---------------|------|-------------|
| EDI 837 Claims | `parsers/edi_837.py` | `EDI837Parser` | 6 | X12 837P/837I |
| — | — | `.parse()` | 29 | — |
| — | — | `._parse_claim()` | 66 | — |
| — | — | `.detect_transaction_type()` | 22 | — |
| EDI 835 Remittances | `parsers/edi_835.py` | `EDI835Parser` | 6 | X12 835 ERA |
| — | — | `.parse()` | 18 | — |
| — | — | `.extract_denial_code()` | 79 | — |
| FHIR Patient | `parsers/fhir_patient.py` | `normalize_fhir_patient()` | 6 | FHIR R4 JSON |
| HL7 v2 Patient | `parsers/hl7v2_patient.py` | `parse_hl7v2_message()` | 14 | HL7 v2 ADT |
| — | — | `normalize_hl7v2_patient()` | 33 | — |
| CSV | `parsers/csv_parser.py` | `parse_fee_schedule_csv()` | 8 | CSV |
| — | — | `parse_nppes_csv()` | 24 | CSV |
| — | — | `parse_backfill_csv()` | 60 | CSV |
| EOB (mock) | `parsers/eob_mock.py` | `parse_eob_extractions()` | 45 | JSON |
| — | — | `parse_contract_extraction()` | 61 | JSON |
| Regulations | `parsers/regulation_parser.py` | `parse_regulation_text()` | 13 | Plain text |
| — | — | `compute_document_hash()` | 88 | — |

### 3.3 Validators

All in `validators/validation.py`:

| Validator | Line | Required Fields | Warnings |
|-----------|------|-----------------|----------|
| `validate_claim()` | 14 | claim_id, date_of_service, total_billed > 0, lines[] | missing diagnosis_codes, provider_npi, payer_id |
| `validate_remittance()` | 36 | trace_number, paid_amount, payer_claim_id | missing adjustments |
| `validate_patient()` | 49 | patient_id, dob | missing last_name, insurance_id |
| `validate_fee_schedule_row()` | 62 | cpt_code, geo_region, rate >= 0, effective_date, rate_type | — |
| `validate_provider()` | 77 | npi | missing specialty_taxonomy, state |

### 3.4 Shared Services

| Service | File | Function | Line | Purpose |
|---------|------|----------|------|---------|
| DB Connection | `shared/db.py` | `get_connection()` | 18 | pyodbc connection pool to Azure SQL |
| — | — | `execute_query()` | 42 | Parameterized SQL execution |
| — | — | `fetchone()` / `fetchall()` | 62 / 72 | Dict-based result sets |
| Events | `shared/events.py` | `emit_event()` | 43 | Publish to Event Hubs (4 hubs) |
| — | — | `_get_producer()` | 20 | Cached producer per hub |
| Dedup | `shared/dedup.py` | `resolve_claim_duplicate()` | 12 | Returns insert/update/void/skip |
| — | — | `check_remittance_duplicate()` | 27 | trace_number + claim_id + payer_id |
| — | — | `check_evidence_duplicate()` | 37 | SHA-256 content_hash |
| — | — | `match_claim_id()` | 46 | Payer claim_id → canonical |
| DLQ | `shared/dlq.py` | `send_to_dlq()` | 15 | Write failed records to dead_letter_queue |
| Audit | `shared/audit.py` | `log_action()` | 10 | Insert audit_log row |

---

## 4. OLTP Operational Store — Schema Reference

All tables in `sql/schema.sql`:

| Table | Line | PK | Key Relationships | Row Count (seed) |
|-------|------|----|-------------------|-----------------|
| `payers` | 13 | payer_id | — | 8 |
| `providers` | 22 | npi | — | 6 |
| `patients` | 34 | id (IDENTITY) | payer_id → payers | 10 |
| `fee_schedule` | 54 | id (IDENTITY) | payer_id → payers | 30+ (SCD2) |
| `claims` | 77 | id (IDENTITY) | patient_id, provider_npi, payer_id | 10 |
| `claim_lines` | 94 | id (IDENTITY) | claim_id → claims | 10 |
| `remittances` | 108 | id (IDENTITY) | claim_id → claims | 8 |
| `cases` | 125 | id (IDENTITY) | — | 6 |
| `disputes` | 139 | id (IDENTITY) | claim_id, case_id | 6 |
| `evidence_artifacts` | 164 | id (IDENTITY) | case_id → cases | 10 |
| `deadlines` | 186 | id (IDENTITY) | case_id, dispute_id | 14 |
| `audit_log` | 206 | id (BIGINT) | — | 7 |
| `dead_letter_queue` | 227 | id (IDENTITY) | — | 0 |
| `claim_id_alias` | 248 | payer_claim_id | canonical_claim_id | 0 |

### Staging & CDC (sql/staging_tables.sql)

| Object | Line | Purpose |
|--------|------|---------|
| `cdc_watermarks` | 10 | Track last sync timestamp per source table |
| `stg_fee_schedule` | 26 | Landing zone for CSV fee schedule batch loads |
| `stg_providers` | 47 | Landing zone for NPPES CSV batch loads |
| `stg_backfill_claims` | 69 | Landing zone for historical claim CSVs |
| `usp_merge_fee_schedule` | 88 | SCD Type 2 merge: expire changed → insert new |
| `usp_merge_providers` | 166 | SCD Type 1 merge: upsert |
| `usp_update_cdc_watermark` | 232 | Update watermark after each ADF copy |

### Seed Data (sql/seed_data.sql)

| Section | Line | Data |
|---------|------|------|
| Payers | 9 | 8 payers: Anthem, Aetna, UHC, Cigna, Humana, Medicare, Medicaid NY, TRICARE |
| Providers | 22 | 6 providers with NPI, specialty, facility |
| Patients | 33 | 10 synthetic patients |
| Fee Schedule | 50 | 30+ rates with SCD2 history (2024/2025/2026 rates) |
| Claims | 91 | 10 claims (underpaid, disputed, paid, denied) |
| Claim Lines | 110 | 10 service lines with CPT + ICD-10 |
| Remittances | 137 | 8 payments including underpayments + denials |
| Cases | 153 | 6 cases across lifecycle stages |
| Disputes | 165 | 6 disputes (active + historical) |
| Evidence | 178 | 10 artifacts (EOB, clinical, contract, IDR decision) |
| Deadlines | 193 | 14 deadline entries with SLA status |
| Audit Log | 217 | 7 entries including AI classification actions |

---

## 5. CDC & Lakehouse Pipeline — Codebase Reference

### 5.1 ADF Pipelines

| Pipeline | File | Key Activities |
|----------|------|----------------|
| Master Orchestrator | `adf/pipelines/pl_master_orchestrator.json` | CDC Copy → Bronze NB → Silver NB → Gold NB (sequential) |
| CDC Incremental Copy | `adf/pipelines/pl_cdc_incremental_copy.json` | ForEach 9 tables: Lookup watermark → Copy changed rows → Update watermark |
| Batch Fee Schedule | `adf/pipelines/pl_batch_fee_schedule.json` | List CSVs → Copy to staging → SCD2 merge → Archive |
| Batch Providers | `adf/pipelines/pl_batch_providers.json` | List CSVs → Copy to staging → Merge → Archive |

### 5.2 Linked Services & Datasets

| File | Type | Target |
|------|------|--------|
| `adf/linked-services/ls_azure_sql.json` | AzureSqlDatabase | Azure SQL (Key Vault secret) |
| `adf/linked-services/ls_adls_gen2.json` | AzureBlobFS | ADLS Gen2 storage |
| `adf/datasets/ds_sql_cdc_source.json` | AzureSqlTable | Parameterized (tableName, watermarkColumn) |
| `adf/datasets/ds_adls_bronze_parquet.json` | Parquet | `bronze/{tableName}/year={yy}/month={mm}/day={dd}/` |

### 5.3 CDC Tables Tracked

| Table | Watermark Column | Priority |
|-------|-----------------|----------|
| `claims` | `updated_at` | High |
| `claim_lines` | `created_at` | High |
| `remittances` | `created_at` | High |
| `patients` | `updated_at` | Medium |
| `cases` | `last_activity` | High |
| `disputes` | `updated_at` | High |
| `deadlines` | `created_at` | High |
| `evidence_artifacts` | `uploaded_date` | Medium |
| `audit_log` | `timestamp` | Low |

---

## 6. Medallion Architecture — Codebase Reference

### 6.1 Bronze Layer

**File:** `fabric-notebooks/nb_bronze_cdc.py`

| Function | Line | Purpose |
|----------|------|---------|
| `ingest_bronze_table()` | 44 | Read date-partitioned Parquet from ADLS, add CDC metadata columns, append to Delta |

**CDC metadata added:**
- `_cdc_operation` (I/U/D)
- `_cdc_timestamp`
- `_source_table`
- `_bronze_loaded_ts`
- `_source_file`

**Output:** `Tables/bronze/{table}` — Delta Lake, append-only

### 6.2 Silver Layer

**File:** `fabric-notebooks/nb_silver_transforms.py`

| Function | Line | Purpose |
|----------|------|---------|
| `resolve_cdc_to_current()` | 41 | Apply CDC operations: latest row per PK, exclude deletes |
| `write_silver()` | 68 | MERGE INTO Delta table (upsert) |
| `transform_patients()` | 93 | Clean demographics |
| `transform_providers()` | 108 | Current providers only |
| `transform_claims()` | 123 | Claims + claim_lines denormalized |
| `transform_remittances()` | 156 | Remittances + payer names |
| `transform_claim_remittance()` | 181 | Claims LEFT JOIN remittances (underpayment calc) |
| `transform_fee_schedule()` | 246 | SCD2 current rates only |
| `transform_disputes()` | 261 | Disputes + claim/payer context |
| `transform_cases()` | 313 | Cases + dispute count + financial summary |
| `transform_deadlines()` | 359 | Deadlines + SLA compliance tracking |

**Output:** 9 Silver Delta tables

### 6.3 Gold Layer

**File:** `fabric-notebooks/nb_gold_aggregations.py`

| Function | Line | Gold Table | Key Metrics |
|----------|------|-----------|-------------|
| `agg_recovery_by_payer()` | 62 | `gold_recovery_by_payer` | Claims, billed, paid, underpayment, recovery %, denial % per payer |
| `agg_cpt_analysis()` | 101 | `gold_cpt_analysis` | Avg billed/paid, pay ratio, Medicare/FAIR Health benchmark per CPT |
| `agg_payer_scorecard()` | 169 | `gold_payer_scorecard` | Payment rate, denial rate, avg underpayment, risk tier per payer |
| `agg_financial_summary()` | 226 | `gold_financial_summary` | Total claims/billed/paid/underpayment, recovery %, denial %, averages |
| `agg_claims_aging()` | 281 | `gold_claims_aging` | Claims by aging bucket (0-30, 31-60, 61-90, 91-180, 180+) |
| `agg_case_pipeline()` | 315 | `gold_case_pipeline` | Cases by status, underpayment, avg age, SLA compliance |
| `agg_deadline_compliance()` | 361 | `gold_deadline_compliance` | Met/missed/at-risk counts + compliance % per deadline type |
| `agg_underpayment_detection()` | 390 | `gold_underpayment_detection` | Per-claim: billed, paid, QPA, underpayment, arbitration eligibility |

**Output:** 8 Gold Delta tables → Power BI (Direct Lake)

---

## 7. Workflow Engine — Codebase Reference

### 7.1 Orchestrators

**File:** `functions/workflow/orchestrator.py`

| Orchestrator | Line | Trigger | Activities Called |
|-------------|------|---------|-----------------|
| `claim_to_dispute_orchestrator` | 22 | HTTP POST `/api/workflow/create-disputes` | `detect_underpaid_claims` → fan-out: `create_dispute_and_case`, `set_regulatory_deadlines`, `send_analyst_notification` |
| `deadline_monitor_orchestrator` | 89 | Timer (every 6 hours) | `get_at_risk_deadlines`, `get_missed_deadlines` → fan-out: `send_deadline_alert`, `mark_deadline_missed`, `escalate_missed_deadlines` |
| `case_transition_orchestrator` | 152 | HTTP POST `/api/workflow/transition-case` | `validate_case_transition` → `execute_case_transition` → `complete_deadline` / `close_all_deadlines` → `write_audit_log` → `send_analyst_notification` |

### 7.2 Activities

**File:** `functions/workflow/activities.py`

| Activity | Line | Input | Output |
|----------|------|-------|--------|
| `detect_underpaid_claims` | 51 | optional claim_ids[] | [{claim_id, billed, paid, underpayment}] |
| `create_dispute_and_case` | 82 | {claim_id, billed, paid, underpayment} | {case_id, dispute_id, priority, analyst} |
| `set_regulatory_deadlines` | 185 | {case_id, dispute_id, filed_date} | {deadlines_created: 3} |
| `get_at_risk_deadlines` | 236 | {threshold_days: 5} | [{deadline_id, case_id, type, due_date}] |
| `get_missed_deadlines` | 262 | {} | [{deadline_id, case_id, type, due_date}] |
| `mark_deadline_missed` | 285 | {deadline_id} | {success: true} |
| `complete_deadline` | 304 | {deadline_id} | {success: true} |
| `close_all_deadlines` | 325 | {case_id} | {closed_count: N} |
| `validate_case_transition` | 346 | {case_id, target_status} | {valid: bool, current_status, reason} |
| `execute_case_transition` | 378 | {case_id, target_status} | {success: true, dispute_id} |
| `send_deadline_alert` | 414 | {deadline_id, case_id, type, due_date} | {alerted: true} |
| `escalate_missed_deadlines` | 449 | {case_ids[]} | {escalated_count: N} |
| `send_analyst_notification` | 480 | {case_id, analyst, priority} | {notified: true} |
| `write_audit_log` | 498 | {entity_type, entity_id, action, user_id} | {logged: true} |

### 7.3 HTTP/Timer Triggers

**File:** `functions/workflow/deadline_monitor.py`

| Function | Line | Type | Route/Schedule |
|----------|------|------|---------------|
| `deadline_monitor_timer` | 19 | Timer | `0 0 */6 * * *` (every 6 hours) |
| `claim_dispute_http_trigger` | 36 | HTTP POST | `/api/workflow/create-disputes` |
| `case_transition_http_trigger` | 60 | HTTP POST | `/api/workflow/transition-case` |

### 7.4 Durable Functions Registration

**File:** `functions/function_app.py`

| Registration | Lines | Count |
|-------------|-------|-------|
| Orchestrators | 27-39 | 3 |
| Activities | 47-125 | 14 |
| Blueprint registered to app | 131 | — |

**Durable Functions config** (`functions/host.json`):
- Hub name: `MedBillWorkflow`
- Max concurrent activities: 10
- Max concurrent orchestrators: 5

---

## 8. Deployment & Infrastructure — Codebase Reference

### 8.1 Provisioning Script

**File:** `scripts/provision.sh`

| Step | Line | Resource | Tier |
|------|------|----------|------|
| 1 | 46 | Resource Group (`rg-medbill-test`) | — |
| 2 | 66 | Azure SQL Server + Database | Free (32 GB) |
| 3 | 107 | Storage Account (ADLS Gen2) + 4 containers | LRS |
| 4 | 137 | Event Hubs Namespace + 4 hubs | Basic (~$11/mo) |
| 5 | 161 | Azure Functions App | Consumption (free) |
| 6 | 179 | Azure AI Search | Free (50 MB) |
| 7 | 192 | Document Intelligence | Free (500 pages/mo) |
| 8 | 207 | App Service (FastAPI) | F1 Free |
| 9 | 228 | Azure Data Factory | — |
| 10 | 242 | Write `.env` with connection strings | — |

**Estimated cost: ~$15-20/month** (mostly Event Hubs Basic tier)

### 8.2 Other Scripts

| Script | Purpose |
|--------|---------|
| `scripts/run_sql.sh` | Execute `schema.sql` + `staging_tables.sql` + `seed_data.sql` via sqlcmd |
| `scripts/deploy_functions.sh` | `func azure functionapp publish` |
| `scripts/deploy_adf.sh` | Deploy ADF pipelines + linked services + datasets via `az datafactory` CLI |
| `scripts/teardown.sh` | `az group delete --name rg-medbill-test` |

### 8.3 Test Simulator

**File:** `functions/sample-events/simulate.py`

Sends sample data through the deployed pipeline:
- Event Hub: claims (EDI 837), remittances (EDI 835)
- Blob Storage: documents (PDF)
- HTTP: FHIR patients
- Blob Storage: fee schedule CSVs

Usage: `python simulate.py --all` or `python simulate.py --claims`

### 8.4 Configuration Files

| File | Purpose | Key Settings |
|------|---------|-------------|
| `functions/host.json` | Functions runtime | Durable hub: MedBillWorkflow, timeout: 10 min, App Insights |
| `functions/local.settings.json` | Local dev template | SQL_CONNECTION_STRING, EVENTHUB_*, STORAGE_*, DOC_INTEL_* |
| `functions/requirements.txt` | Python deps | azure-functions, azure-functions-durable, pyodbc, azure-eventhub, azure-storage-blob, azure-ai-formrecognizer, anthropic |

---

## 8.5 AI Analyst Agent (Claude API)

```
User Question (natural language)
        │
        ▼
┌── Azure Function: POST /api/agent/ask ──────────────────────────────────────┐
│   function_app.py:435                                                        │
│       │                                                                      │
│       ▼                                                                      │
│   agent/analyst.py                                                           │
│       │                                                                      │
│       ├── Step 1: Claude API (tool_use)                                     │
│       │   System prompt with Gold view schemas                              │
│       │   → Claude generates T-SQL via execute_sql tool                     │
│       │                                                                      │
│       ├── Step 2: Execute SQL (read-only, SELECT only)                      │
│       │   Queries 8 Gold views in Azure SQL                                 │
│       │   Safety: blocked keywords (INSERT, DROP, etc.)                     │
│       │                                                                      │
│       ├── Step 3: Claude API (analysis)                                     │
│       │   Results + question → human-readable analysis                      │
│       │                                                                      │
│       ├── Step 4: Audit log                                                 │
│       │   Logs question, SQL, model version to audit_log table              │
│       │                                                                      │
│       └── Step 5: Suggest next analyses                                     │
│           Picks 3 relevant common analyses as follow-ups                    │
│                                                                              │
│   Returns: { answer, sql, sql_explanation, data, row_count, model,          │
│              suggested_analyses }                                            │
└──────────────────────────────────────────────────────────────────────────────┘

Gold Views (Azure SQL):
├── gold_recovery_by_payer       — recovery metrics per payer
├── gold_cpt_analysis            — CPT code payment ratios + benchmarks
├── gold_payer_scorecard         — payer behavior / risk tier
├── gold_financial_summary       — overall financial KPIs (key-value)
├── gold_claims_aging            — aging buckets (0-30, 31-60, ..., 180+)
├── gold_case_pipeline           — case status + SLA compliance
├── gold_deadline_compliance     — deadline met/missed/at-risk by type
└── gold_underpayment_detection  — per-claim underpayment + arbitration eligibility
```

**API Endpoints:**

| Method | Route | Auth | Purpose | File:Line |
|--------|-------|------|---------|-----------|
| GET | `/api/agent/ui` | Anonymous | Web chat UI (single-page app) | `function_app.py:528` |
| POST | `/api/agent/ask` | Function | Free-form natural language question | `function_app.py:435` |
| GET | `/api/agent/common` | Function | List 10 pre-built common analyses | `function_app.py:480` |
| POST | `/api/agent/common/{id}` | Function | Run a pre-built analysis by ID | `function_app.py:491` |

**Web UI (`agent/ui.html`):**
- Dark-themed single-page chat interface
- Left sidebar: 10 clickable common analyses + suggested follow-ups
- Welcome screen with 4 quick-start cards
- Chat area: formatted answers with expandable SQL and data table views
- Multi-turn conversation context (last 10 messages)
- Auto-detects API base URL; pass `?code=<key>` for Function auth

**Common Analyses (10 pre-built):**

| ID | Name | Description |
|----|------|-------------|
| `executive_summary` | Executive Summary | Financial KPIs: billed, paid, underpayment, recovery rate |
| `worst_payers` | Worst Performing Payers | Ranked by underpayment + denial rate |
| `arbitration_ready` | Arbitration-Ready Claims | Claims eligible for IDR (underpayment > $25, billed > QPA) |
| `cpt_underpayment` | CPT Code Underpayment | CPT codes vs Medicare/FAIR Health benchmarks |
| `deadline_risk` | Deadline Risk Report | At-risk/missed NSA regulatory deadlines |
| `case_pipeline` | Case Pipeline Status | Cases by status + SLA compliance |
| `aging_analysis` | Claims Aging Analysis | Claims by aging bucket with unpaid amounts |
| `payer_comparison` | Payer Risk Comparison | Side-by-side payer scorecards with risk tiers |
| `recovery_opportunity` | Recovery Opportunity | Total recovery potential estimate |
| `denial_patterns` | Denial Pattern Analysis | Denial rates by payer — systematic behavior |

**Suggested Analyses Flow:**
Every response includes `suggested_analyses` — 3 pre-built analyses recommended as follow-ups
based on keyword affinity between the current question/answer and the common analysis catalog.
Claude also generates 2-3 custom follow-up questions in the answer text itself.

**AI Guardrails:**
- Read-only SQL execution (SELECT only, blocked mutation keywords)
- Queries restricted to `gold_*` views — no access to OLTP tables
- All invocations logged to `audit_log` with model version
- Structured JSON output with confidence-enabling fields (sql, data, row_count)

**Configuration:**
| Setting | Environment Variable | Default |
|---------|---------------------|---------|
| API Key | `ANTHROPIC_API_KEY` | (required) |
| Model | `CLAUDE_MODEL` | `claude-sonnet-4-20250514` |

**Testing:**

| Test File | Tests | Scope |
|-----------|-------|-------|
| `tests/test_agent.py` | 35 | SQL safety (INSERT/DROP/etc. blocked), common analyses catalog (15), ask_common, mocked Claude API flow, Gold schema, UI file |
| `tests/test_gold_views.py` | 36 | All 13 Gold view queries against seed data (SQLite), cross-view referential integrity |
| `tests/test_e2e_pipeline.py` | 32 | Full pipeline: OLTP integrity, financial consistency, Gold view chain, agent response structure, workflow state machine, cross-layer data flow |
| `tests/test_olap_gold.py` | 18 | All 13 OLAP Gold aggregation functions with mocked Silver data, aggregate_all_gold pipeline |
| `scripts/verify_agent.sh` | 7 | Live E2E: health, UI, common list, free-form question, common run, invalid ID, bad request |

Run: `python3 -m pytest tests/ -v` (121 tests)

---

## 9. Alignment with Future Architecture

### Summary Scorecard

| Architecture Section | Future Arch Ref | Alignment | Score |
|---------------------|----------------|-----------|-------|
| Design Principles | Section 1 | 5 aligned, 2 partial (AI layer, compliance) | 86% |
| Architectural Pattern | Section 2 | Event-driven hybrid — fully aligned | 95% |
| Ingestion Layer | Section 5 | All sources implemented; Doc Intelligence partial | 82% |
| OLTP Schema | Section 6 | All 14 tables + extras | 100% |
| OLAP Lakehouse | Section 7 | Bronze/Silver/Gold notebooks implemented | 80% |
| CDC Sync | Section 8 | Watermark-based (not native CDC) | 75% |
| Workflow Engine | Section 9 | 3 orchestrators, 14 activities, NSA state machine | 80% |
| AI Layer | Section 11 | Data Analyst Agent (Claude API) + Doc Intelligence partial | 40% |
| Application Layer | Section 10 | AI Agent Web UI (chat + common analyses) | 35% |
| Analytics / Power BI | Section 12 | Gold data ready, PBI not connected | 35% |
| Security | Section 13 | Audit + Key Vault; no RLS/Purview/PHI redaction | 55% |
| Platform Choice | Section 14 | Fabric selected | 100% |

### Key Gaps

| Priority | Gap | Future Arch Section | What Exists | What's Missing |
|----------|-----|-------------------|-------------|----------------|
| **Critical** | Native CDC | 8.2 | Watermark polling via ADF | `sp_cdc_enable_table` for delete detection |
| **Critical** | Case Management API | 10.1 | App Service provisioned | FastAPI code, endpoints, auth |
| **Critical** | Power BI dataset | 12.1 | Gold Delta tables | Direct Lake dataset + reports |
| **High** | Document Intelligence | 11.1 | Service provisioned + code path | Custom model training, wiring |
| **High** | Notifications | 9.3 | Stub functions (log only) | Email/Teams/SMS integration |
| **High** | Gold notebook parity | 7.1 | 8 agg functions | Some less detailed than local `olap/gold.py` |
| **Medium** | LLM Agents (1 of 5) | 11.2 | Data Analyst Agent (Claude text-to-SQL) | Evidence Assembly, Narrative Drafter, Case Copilot |
| **Medium** | RAG Pipeline | 11.3 | Regulation chunker ready | AI Search index, retrieval, augmentation |
| **Low** | Purview | 13.2 | — | Data lineage, auto-classification |
| **Low** | RLS in Power BI | 13.1 | — | Row-level security per analyst role |
