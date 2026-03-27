# Medical Billing Arbitration — Future Architecture & Design

> **Author:** Claude (Opus 4.6) — March 2026
> **Companion docs:**
> - `medical_billing_arbitration_research.md` — Domain research, challenges, current state analysis
> - `medical_billing_arbitration_architecture.md` — Initial design sketch (ChatGPT)

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Architectural Pattern: Why Not Lambda](#2-architectural-pattern-why-not-lambda)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Data Layer — Storage & Update Patterns](#4-data-layer--storage--update-patterns)
5. [Ingestion Layer](#5-ingestion-layer)
6. [Operational Store (OLTP)](#6-operational-store-oltp)
7. [Analytical Store (OLAP / Lakehouse)](#7-analytical-store-olap--lakehouse)
8. [OLTP-to-OLAP Sync — CDC](#8-oltp-to-olap-sync--cdc)
9. [Workflow & Event Engine](#9-workflow--event-engine)
10. [Application Layer — Case Management](#10-application-layer--case-management)
11. [AI Layer](#11-ai-layer)
12. [Analytics Layer (Power BI)](#12-analytics-layer-power-bi)
13. [Security, Compliance & Governance](#13-security-compliance--governance)
14. [Platform Decision: Fabric vs. Databricks](#14-platform-decision-fabric-vs-databricks)
15. [Migration from Current State](#15-migration-from-current-state)
16. [Implementation Roadmap](#16-implementation-roadmap)

---

## 1. Design Principles

| # | Principle | Rationale |
|---|---|---|
| 1 | **Separate operational from analytical** | Case management (OLTP) and reporting (OLAP) have different performance, consistency, and scaling requirements. Coupling them in one store (current state) forces compromises in both. |
| 2 | **Event-driven core** | Arbitration has strict deadlines (30-day negotiation, 4-day IDR initiation). Events — not batch schedules — should trigger workflows, alerts, and AI processing. |
| 3 | **Single source of truth** | The OLTP operational store is authoritative. The OLAP lakehouse is derived via CDC. No dual-write, no merge logic. |
| 4 | **AI as assistive layer** | Human-in-the-loop for all decisions. AI accelerates preparation, flags risks, and drafts artifacts — but never submits autonomously. |
| 5 | **Incremental migration** | Layer new capabilities on existing Azure investment. Don't rip-and-replace. |
| 6 | **Compliance-first** | HIPAA, audit logging, PHI protection, and data lineage built into every layer from day one. |
| 7 | **History preservation (SCD Type 2)** | Prices, contracts, and fee schedules change over time. Arbitration requires proving what the rate was *at time of service*, not today. |

---

## 2. Architectural Pattern: Why Not Lambda

### Lambda Architecture (rejected)

```
            ┌──► Batch Layer (complete, slow reprocessing) ──► Serving Layer ──┐
Source ─────┤                                                                   ├──► Query
            └──► Speed Layer (real-time, approximate) ─────────────────────────┘
```

Lambda requires **two parallel processing paths** (batch + stream) producing the same results, merged in a serving layer. This means building and maintaining every pipeline twice.

### Why Lambda is Wrong for This Domain

| Factor | Why it doesn't fit |
|---|---|
| **Data velocity** | Arbitration cases move over days/weeks, not milliseconds. Event-driven triggers are sufficient — continuous stream processing is overkill. |
| **Dual-pipeline cost** | Keeping batch and speed layers in sync is Lambda's biggest operational burden. For a team on Power BI + ADF, this is unjustifiable complexity. |
| **Data volume** | Even at 1.2M cases/year nationally, a single provider org's volume is manageable without a speed layer. |
| **Team skillset** | Current team knows ADF + Power BI + Python. Lambda requires Kafka/Flink/Spark Streaming expertise. |

### What We Use Instead: Event-Driven Hybrid (Kappa-Influenced)

```
Source Systems
    │
    ├──► Event-driven ingestion ──► OLTP (Azure SQL)   ◄── Source of truth
    │    (Functions / Event Hubs)        │
    │    New docs, remittances,          │ CDC (Change Data Capture)
    │    status changes                  │ Automatic, continuous
    │                                    ▼
    ├──► Batch ingestion (ADF) ────► OLAP Lakehouse    ◄── Derived, for analytics
    │    Historical loads,           (Bronze→Silver→Gold)
    │    bulk payer data,                │
    │    fee schedule updates            ▼
    │                                Power BI
    └──► AI Layer reads from both stores
         (RAG from OLAP, case context from OLTP)
```

**Key difference from Lambda:** One write path into OLTP. One derived analytical layer via CDC. No speed layer, no merge logic, no dual-pipeline maintenance.

**Key difference from pure Kappa:** We retain batch ingestion (ADF) for bulk historical loads and periodic reference data — but batch writes into the *same* OLTP store, not a parallel path.

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE SYSTEMS                                    │
│  EHR/PMS    Clearinghouses    Payer Portals    ERA/EOB (835)    Contracts   │
│  (HL7/FHIR)  (EDI 837/835)   (API/scrape)     (EDI + PDF)     (PDF/scan)  │
└──────┬──────────┬──────────────┬──────────────────┬──────────────┬──────────┘
       │          │              │                  │              │
       ▼          ▼              ▼                  ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER  (Section 5)                            │
│                                                                             │
│  Azure Data Factory (batch)     Azure Functions (event-driven)              │
│  Event Hubs / Service Bus       SFTP listeners                              │
│  API connectors (FHIR, X12)     Email ingestion (for EOBs)                  │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
┌──────────────────────────┐   ┌──────────────────────────────────┐
│  OPERATIONAL STORE       │   │   ANALYTICAL STORE                │
│  (OLTP) (Section 6)     │   │   (OLAP) (Section 7)             │
│                          │   │                                    │
│  Azure SQL Database      │   │  ADLS Gen2 / OneLake              │
│  ┌────────────────────┐  │   │  ┌──────────────────────────────┐ │
│  │ Cases              │  │──CDC──► Bronze: raw ingested data   │ │
│  │ Claims / Lines     │  │   │  │ Silver: cleaned, conformed   │ │
│  │ Disputes           │  │   │  │ Gold: aggregated, enriched   │ │
│  │ Evidence artifacts │  │   │  └──────────────────────────────┘ │
│  │ Fee schedules (SCD)│  │   │                                    │
│  │ Deadlines / SLAs   │  │   │  Processing: Fabric Lakehouse     │
│  │ Decisions / Awards  │  │   │                                    │
│  │ Audit log          │  │   │                                    │
│  └────────────────────┘  │   │                                    │
└────────────┬─────────────┘   └──────────────┬───────────────────┘
             │                                 │
             ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  WORKFLOW & EVENT ENGINE  (Section 9)                        │
│                                                                             │
│  Azure Durable Functions          Event-driven orchestration                │
│  Triggers:                        Deadline monitors (cron + event)          │
│  - New claim ingested             Case routing rules engine                 │
│  - Remittance received            Notification engine (email/Teams/SMS)     │
│  - Underpayment detected                                                    │
│  - Deadline approaching                                                     │
│  - Document classified                                                      │
│  - Case status changed                                                      │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AI LAYER  (Section 11)                               │
│                                                                             │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────────────┐  │
│  │ Document Intel.  │  │ AI Search (RAG)  │  │ LLM Agents                │  │
│  │ Azure Doc Intel. │  │ Azure AI Search  │  │ Claude API (LangChain)    │  │
│  │ OCR + Classify   │  │ Vector index on  │  │ + Azure OpenAI (fallback) │  │
│  │ + Extract fields │  │ contracts, regs, │  │                           │  │
│  │                  │  │ decisions, EOBs  │  │ 5 Agents (see 11.2)      │  │
│  └─────────────────┘  └──────────────────┘  └───────────────────────────┘  │
│  Guardrails: structured outputs, citations, human approval, audit log,      │
│              confidence scores, PHI redaction                                │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌───────────────────────────┐    ┌────────────────────────────────────────────┐
│ APPLICATION LAYER         │    │ ANALYTICS LAYER  (Section 12)              │
│ (Section 10)              │    │                                            │
│                           │    │ Power BI (retained)                        │
│ Case Management UI        │    │ - Recovery rates, win/loss trends          │
│ (React / Next.js)         │    │ - QPA vs. award analysis                   │
│ + API (FastAPI)           │    │ - Case aging, SLA compliance               │
│                           │    │ - Payer behavior scorecards                │
│ Embedded PBI reports ◄────┼────│ - Revenue impact forecasting               │
│                           │    │                                            │
└───────────────────────────┘    └────────────────────────────────────────────┘
```

---

## 4. Data Layer — Storage & Update Patterns

Different data types have different velocity, sources, and history requirements. The architecture must handle each appropriately.

### 4.1 Data Classification by Update Pattern

```
┌─────────────────┬──────────────┬────────────────┬──────────────────┬───────────────┐
│ Data Type       │ Source       │ Velocity       │ Sync Mechanism   │ History Model │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Fee Schedules / │ Payer files, │ Quarterly /    │ ADF batch load   │ SCD Type 2    │
│ Price Tables    │ CMS, FAIR    │ annual         │ → staging →      │ (CRITICAL for │
│ (QPA, Medicare) │ Health       │                │ SCD merge        │ arbitration)  │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Patient Data    │ EHR / PMS    │ On-change      │ FHIR webhook or  │ SCD Type 1    │
│ (demographics)  │              │                │ nightly batch    │ (overwrite)   │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Claims / Lines  │ Clearinghouse│ Daily (as      │ Event Hub +      │ Append-only   │
│                 │ EDI 837      │ filed)         │ Azure Function   │ + status col  │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Remittances     │ Payer EDI /  │ Daily (as      │ Event Hub +      │ Append-only   │
│ (ERA 835 / EOB) │ PDF          │ received)      │ Azure Function   │               │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Contract Terms  │ Manual /     │ Infrequent     │ UI upload →      │ SCD Type 2    │
│                 │ negotiated   │ (renegotiated) │ OLTP + versioned │ (full history)│
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Case Status /   │ Internal     │ Real-time      │ App writes to    │ Event log +   │
│ Workflow State  │ (app)        │ (user actions) │ OLTP → CDC       │ current state │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Regulations /   │ CMS / state  │ When published │ Manual upload →  │ Versioned     │
│ Rules           │ agencies     │                │ AI Search re-idx │ corpus        │
├─────────────────┼──────────────┼────────────────┼──────────────────┼───────────────┤
│ Provider /      │ NPPES, PECOS │ Monthly        │ ADF batch        │ SCD Type 2    │
│ Facility Data   │ CMS          │                │                  │               │
└─────────────────┴──────────────┴────────────────┴──────────────────┴───────────────┘
```

### 4.2 SCD Type 2 — Why It's Critical for Prices & Contracts

Arbitration requires proving **what the rate was at the time of service**, not what it is today. SCD Type 2 preserves full history:

```sql
-- fee_schedule table (SCD Type 2)
┌────────┬──────────┬────────┬────────────┬────────────┬─────────┐
│ payer  │ cpt_code │ rate   │ valid_from │ valid_to   │ current │
├────────┼──────────┼────────┼────────────┼────────────┼─────────┤
│ Aetna  │ 99213    │ $120   │ 2024-01-01 │ 2024-12-31 │ false   │
│ Aetna  │ 99213    │ $135   │ 2025-01-01 │ 2025-12-31 │ false   │
│ Aetna  │ 99213    │ $142   │ 2026-01-01 │ 9999-12-31 │ true    │
└────────┴──────────┴────────┴────────────┴────────────┴─────────┘

-- Query: "What was Aetna's rate for 99213 on date of service 2025-06-15?"
SELECT rate FROM fee_schedule
WHERE payer = 'Aetna' AND cpt_code = '99213'
  AND '2025-06-15' BETWEEN valid_from AND valid_to;
-- Returns: $135
```

**Without SCD Type 2:** You'd only have today's rate ($142) and lose the evidence trail needed for arbitration.

### 4.3 Fee Schedule / Price Update Flow

```
CMS publishes new Medicare rates (annual)
         │
         ▼
ADF Pipeline (scheduled — quarterly or on-publish)
         │
         ├─► Load new rates into staging table
         │
         ├─► SCD Type 2 merge logic:
         │     - Unchanged rows → skip
         │     - Changed rows → close old row (set valid_to = yesterday),
         │                      insert new row (valid_from = today)
         │     - New codes → insert (valid_from = effective date)
         │
         ├─► Write to OLTP (Azure SQL: fee_schedule table)
         │         │
         │         │ CDC picks up changes automatically
         │         ▼
         │    Lakehouse Silver layer updated
         │         │
         │         ▼
         │    Gold layer: aggregated rate comparison views
         │         │
         │         ▼
         │    Power BI: rate trend dashboards refresh
         │
         └─► AI Search: re-index rate data for RAG queries
              ("What is the Medicare benchmark for CPT 99213 in Texas?")
```

### 4.4 Patient Data Update Flow

```
EHR system: patient address changed
         │
         ▼
Option A: FHIR webhook (real-time)     Option B: Nightly batch (simpler)
         │                                        │
         ▼                                        ▼
Azure Function receives event            ADF pulls delta from EHR
         │                                        │
         └──────────────┬─────────────────────────┘
                        ▼
              Upsert in OLTP (Azure SQL: patients table)
              (SCD Type 1 — overwrite, demographics don't need history)
                        │
                        │ CDC → Lakehouse
                        ▼
              Analytics updated (patient geography, demographics)
```

### 4.5 Sync Mechanism Decision Guide

| Approach | When to Use | Complexity | Current Team Fit |
|---|---|---|---|
| **ADF scheduled batch** | Prices, fee schedules, provider data (periodic bulk) | Low | Already using |
| **CDC (Azure SQL built-in)** | OLTP → Lakehouse for all operational tables | Medium | New — enable per table |
| **Event Hub + Functions** | Claims, remittances, documents (as they arrive) | Medium | New — high value |
| **FHIR webhook** | Patient data from EHR (if EHR supports it) | High | Depends on EHR |
| **Manual UI upload** | Contracts, regulations (infrequent, high-value) | Low | Trivial |

---

## 5. Ingestion Layer

### 5.1 Batch Ingestion (ADF — existing, enhanced)

| Source | Format | Schedule | Pipeline |
|---|---|---|---|
| Medicare fee schedules | CSV/Excel from CMS | Quarterly | ADF → staging → SCD merge → OLTP |
| FAIR Health rates | Licensed data files | Quarterly | ADF → staging → SCD merge → OLTP |
| Payer fee schedules | Varies (CSV, Excel, PDF) | On receipt | ADF → staging (PDF → Doc Intel first) |
| Provider/NPI data | NPPES CSV | Monthly | ADF → staging → upsert OLTP |
| Historical claims backfill | EDI 837 / CSV | One-time + periodic | ADF → parse → OLTP |

### 5.2 Event-Driven Ingestion (new)

| Source | Trigger | Handler | Target |
|---|---|---|---|
| ERA/835 files (EDI) | SFTP drop / Event Hub | Azure Function: parse X12 835 | OLTP: remittances table |
| EOB documents (PDF) | Email / portal / upload | Azure Function → Doc Intelligence → extract | OLTP: remittances + evidence |
| New claims | Clearinghouse feed | Event Hub → Azure Function: parse 837 | OLTP: claims table |
| Patient updates | EHR FHIR webhook | Azure Function: upsert patient | OLTP: patients table |
| Document uploads | Case Management UI | Azure Function → Blob → Doc Intelligence | OLTP: evidence artifacts |

### 5.3 Ingestion Pipeline Pattern

```
Source Event
    │
    ▼
Event Hub / Service Bus (buffer + dedup)
    │
    ▼
Azure Function (parser)
    ├── Structured data (EDI 837/835) → parse → validate → write OLTP
    └── Unstructured (PDF/fax) → Blob Storage → Doc Intelligence → extract → validate → write OLTP
                                                                      │
                                                                      ▼
                                                            Low confidence?
                                                            → Human review queue
```

---

## 6. Operational Store (OLTP)

### 6.1 Technology: Azure SQL Database

Retained from current stack. Appropriate because:
- Team already skilled in SQL Server / Azure SQL
- HIPAA-compliant with transparent data encryption (TDE)
- Built-in CDC support
- Sufficient scale for operational workload (not petabyte analytics)

### 6.2 Core Data Model (simplified ERD)

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐
│   patients   │     │   providers   │     │   payers          │
│──────────────│     │──────────────│     │──────────────────│
│ patient_id   │     │ npi          │     │ payer_id          │
│ name, dob    │     │ tin          │     │ name              │
│ address      │     │ name         │     │ type              │
│ insurance_id │     │ specialty    │     │ state             │
└──────┬───────┘     │ facility_id  │     └────────┬─────────┘
       │             └──────┬───────┘              │
       │                    │                      │
       ▼                    ▼                      ▼
┌──────────────────────────────────────────────────────────────┐
│                         claims                                │
│──────────────────────────────────────────────────────────────│
│ claim_id (PK)                                                 │
│ patient_id (FK) │ provider_npi (FK) │ payer_id (FK)          │
│ date_of_service │ date_filed                                  │
│ facility_id     │ place_of_service                            │
│ total_billed    │ status                                      │
└──────────────────────────┬───────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
┌──────────────────────┐   ┌──────────────────────┐
│    claim_lines       │   │    remittances        │
│──────────────────────│   │──────────────────────│
│ line_id (PK)         │   │ remit_id (PK)        │
│ claim_id (FK)        │   │ claim_id (FK)        │
│ cpt_code             │   │ era_date             │
│ modifier             │   │ paid_amount          │
│ units                │   │ allowed_amount       │
│ billed_amount        │   │ adjustment_reason    │
│ diagnosis_codes      │   │ denial_code          │
└──────────┬───────────┘   │ check_number         │
           │               └──────────┬───────────┘
           │                          │
           ▼                          ▼
┌──────────────────────────────────────────────────────────────┐
│                        disputes                               │
│──────────────────────────────────────────────────────────────│
│ dispute_id (PK)                                               │
│ claim_id (FK)  │ claim_line_id (FK)                           │
│ case_id (FK)   │ dispute_type (appeal / IDR / state_arb)     │
│ status         │ billed_amount │ paid_amount │ requested_amt  │
│ qpa_amount     │ underpayment_amount                          │
│ filed_date     │ open_negotiation_deadline                    │
│ idr_initiation_deadline                                       │
└──────────────────────────┬───────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
┌──────────────────────┐   ┌──────────────────────┐
│      cases           │   │  evidence_artifacts   │
│──────────────────────│   │──────────────────────│
│ case_id (PK)         │   │ artifact_id (PK)     │
│ dispute_ids (FK[])   │   │ case_id (FK)         │
│ assigned_analyst     │   │ type (EOB, clinical, │
│ status               │   │   contract, corr.)   │
│ priority             │   │ blob_url             │
│ created_date         │   │ extracted_data (JSON)│
│ last_activity_date   │   │ classification_conf  │
│ outcome              │   │ ocr_status           │
│ award_amount         │   │ uploaded_date        │
└──────────────────────┘   └──────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                   fee_schedule (SCD Type 2)                    │
│──────────────────────────────────────────────────────────────│
│ id (PK)                                                       │
│ payer_id (FK) │ cpt_code │ modifier │ geo_region              │
│ rate          │ rate_type (contracted / medicare / fair_health)│
│ valid_from    │ valid_to  │ is_current                        │
│ source        │ loaded_date                                    │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                      deadlines                                │
│──────────────────────────────────────────────────────────────│
│ deadline_id (PK)                                              │
│ case_id (FK) │ dispute_id (FK)                               │
│ type (open_negotiation / idr_initiation / entity_selection)  │
│ due_date     │ status (pending / met / missed)               │
│ alerted_date │ completed_date                                 │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                      audit_log                                │
│──────────────────────────────────────────────────────────────│
│ log_id (PK)                                                   │
│ entity_type  │ entity_id  │ action                            │
│ user_id      │ timestamp  │ old_value │ new_value             │
│ ai_agent     │ ai_confidence │ ai_model_version               │
└──────────────────────────────────────────────────────────────┘
```

---

## 7. Analytical Store (OLAP / Lakehouse)

### 7.1 Medallion Architecture on ADLS Gen2 / OneLake

```
┌───────────────────────────────────────────────────────────────────┐
│ BRONZE (Raw)                                                       │
│ - Raw CDC events from OLTP (JSON / Parquet)                       │
│ - Raw EDI files (837, 835)                                        │
│ - Raw PDF OCR outputs                                             │
│ - Raw fee schedule files                                          │
│ - Schema-on-read, append-only, partitioned by date                │
├───────────────────────────────────────────────────────────────────┤
│ SILVER (Cleaned & Conformed)                                       │
│ - Parsed claims, remittances, disputes (normalized)               │
│ - Deduped, validated, typed                                       │
│ - Joined: claim + remittance + dispute (matched)                  │
│ - Fee schedules (point-in-time lookups via SCD)                   │
│ - Delta Lake format (ACID transactions, time travel)              │
├───────────────────────────────────────────────────────────────────┤
│ GOLD (Business-Ready Aggregations)                                 │
│ - Recovery rate by payer × CPT × geography × time                 │
│ - Win/loss rates and trends                                       │
│ - QPA vs. award spread analysis                                   │
│ - Case aging and SLA compliance metrics                           │
│ - Revenue impact and cash flow projections                        │
│ - Payer behavior scorecards                                       │
│ - Operational throughput metrics                                   │
│ - Direct Lake / Power BI consumption layer                        │
└───────────────────────────────────────────────────────────────────┘
```

### 7.2 Processing: Microsoft Fabric Lakehouse

- Fabric Notebooks (PySpark / SQL) for Bronze → Silver → Gold transforms
- Scheduled via Fabric Pipelines (replacement for ADF in OLAP context)
- Direct Lake mode for Power BI — near-real-time BI without import/refresh cycles

---

## 8. OLTP-to-OLAP Sync — CDC

### 8.1 What is CDC (Change Data Capture)

CDC tracks **row-level changes** (inserts, updates, deletes) in the OLTP database and propagates them to the lakehouse — so analytics stay in sync without full table re-loads.

```
OLTP (Azure SQL)                          OLAP (Lakehouse)
┌──────────────────┐                      ┌──────────────────┐
│ Cases table       │                      │ Silver: cases     │
│                   │  CDC detects:        │                   │
│ Row 42: status    │  "Row 42 changed     │ Row 42 updated    │
│ changed from      │──►from OPEN to ──────►│ within minutes    │
│ OPEN → IN_REVIEW  │   IN_REVIEW"         │ (not next day)    │
└──────────────────┘                      └──────────────────┘
```

### 8.2 Implementation

Azure SQL has **built-in CDC** — enabled per table. Changes are written to system change tables that downstream consumers read.

```sql
-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on specific tables
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'cases',
    @role_name     = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'disputes',
    @role_name     = NULL;

-- ... repeat for claims, remittances, deadlines, etc.
```

### 8.3 CDC → Lakehouse Pipeline

```
Azure SQL (CDC enabled)
    │
    │ Change feed (inserts, updates, deletes with timestamps)
    ▼
Fabric Pipeline / ADF (incremental copy)
    │
    │ Reads only changes since last watermark
    ▼
Bronze layer (ADLS Gen2 / OneLake)
    │
    │ Raw change events as Parquet/Delta
    ▼
Fabric Notebook (Silver transform)
    │
    │ Merge changes into Silver tables (MERGE INTO ... WHEN MATCHED ...)
    ▼
Silver layer (cleaned, current state + history)
    │
    ▼
Gold layer (aggregations refresh)
    │
    ▼
Power BI (Direct Lake — no manual refresh needed)
```

### 8.4 CDC-Enabled Tables

| Table | CDC Priority | Reason |
|---|---|---|
| `cases` | High | Case status drives dashboards, SLA reporting |
| `disputes` | High | Core arbitration tracking |
| `claims` | High | Claim lifecycle analytics |
| `remittances` | High | Payment tracking, underpayment detection |
| `deadlines` | High | SLA compliance reporting |
| `evidence_artifacts` | Medium | Document processing metrics |
| `fee_schedule` | Low | Changes infrequently (batch loaded) |
| `audit_log` | Low | Append-only, can batch-sync daily |

---

## 9. Workflow & Event Engine

### 9.1 Technology: Azure Durable Functions

Chosen over Temporal.io / Airflow because:
- Native Azure integration (stays in ecosystem)
- Supports long-running workflows (cases can span months)
- Timer triggers for deadline monitoring
- Lower ops overhead than self-hosted alternatives

### 9.2 Core Workflows

#### Workflow 1: Claim-to-Dispute Pipeline
```
New Claim Ingested (Event Hub trigger)
    │
    ▼
Match with existing patient + provider
    │
    ▼
Wait for Remittance (timer: check daily for 45 days)
    │
    ▼
Remittance Received
    │
    ▼
Compare paid vs. expected (contract rate lookup via SCD Type 2)
    │
    ├── Within tolerance → Close (no dispute)
    │
    └── Underpayment detected
         │
         ▼
    Create Dispute record
         │
         ▼
    Calculate recovery potential (amount × historical win rate for this payer+CPT)
         │
         ├── Below threshold → Flag for review (may not be worth pursuing)
         │
         └── Above threshold → Create Case, assign analyst, start deadline timers
```

#### Workflow 2: Deadline Monitor
```
Case Created
    │
    ▼
Start Timer: Open Negotiation (30 calendar days)
    │
    ├── Day 20 → Alert: "10 days remaining for open negotiation"
    ├── Day 27 → Escalation: "3 days remaining — manager notified"
    ├── Day 30 → Deadline reached
    │       │
    │       ├── Negotiation successful → Close dispute → Record outcome
    │       └── No agreement → Start IDR Initiation Timer (4 business days)
    │               │
    │               ├── Day 2 → Alert: "2 business days to initiate IDR"
    │               └── Day 4 → IDR must be initiated or forfeited
    │
    ▼
IDR Entity Selection Timer (next business day)
    │
    ▼
Evidence Submission Timer (10 business days)
    │
    ▼
Await Decision (30 business days)
```

#### Workflow 3: Document Processing
```
Document Arrives (upload, email, SFTP)
    │
    ▼
Store in Blob Storage
    │
    ▼
Trigger Doc Intelligence (OCR + classification)
    │
    ▼
Confidence > 90%?
    ├── Yes → Auto-link to case, extract fields, store in OLTP
    └── No → Route to Human Review Queue
              │
              ▼
         Human classifies + corrects
              │
              ▼
         Store in OLTP (+ feedback loop to improve model)
```

### 9.3 Notification Engine

| Event | Channel | Recipient |
|---|---|---|
| Case assigned | In-app + Email | Analyst |
| Deadline in 10 days | In-app | Analyst |
| Deadline in 3 days | Email + Teams | Analyst + Manager |
| Deadline missed | Email + Teams + SMS | Manager + Director |
| AI draft ready for review | In-app | Analyst |
| Case decision received | Email | Analyst + Manager |
| High-value underpayment detected | In-app + Email | Manager |

---

## 10. Application Layer — Case Management

### 10.1 Tech Stack

| Component | Technology | Rationale |
|---|---|---|
| Frontend | React / Next.js | Team familiarity, rich component ecosystem |
| API | FastAPI (Python) | Team uses Python; async support; auto-generated OpenAPI docs |
| Hosting | Azure App Service or Container Apps | Managed, HIPAA-eligible |
| Auth | Azure AD (Entra ID) | Existing investment; RBAC integration |
| Real-time | WebSocket (via FastAPI) | Deadline alerts, case status updates |

### 10.2 Key UI Views

| View | Purpose |
|---|---|
| **Case Dashboard** | Filterable list of all cases with status, priority, deadlines, assigned analyst |
| **Case Detail** | Full case timeline: claim → remittance → dispute → evidence → decision |
| **Evidence Viewer** | Document viewer with AI-extracted annotations; side-by-side comparison |
| **Narrative Editor** | Rich text editor with AI-drafted content; inline citations; track changes |
| **Deadline Tracker** | Calendar/Gantt view of all active deadlines across cases |
| **QPA Benchmarking** | Compare payer QPA vs. Medicare, FAIR Health, historical awards |
| **Bulk Actions** | Batch filing, status updates, assignment for high-volume operations |
| **Analytics (embedded PBI)** | Contextual Power BI reports embedded within case views |

### 10.3 Role-Based Access

| Role | Permissions |
|---|---|
| **Analyst** | View/edit assigned cases, upload evidence, review AI drafts |
| **Senior Analyst** | All analyst + approve narratives, initiate IDR filings |
| **Manager** | All senior + reassign cases, view all cases, override priorities |
| **Director** | All manager + view financial analytics, approve strategy changes |
| **External Counsel** | Read-only access to assigned cases + evidence |
| **Admin** | User management, system configuration, audit log access |

---

## 11. AI Layer

### 11.1 Document Intelligence Pipeline

```
Incoming Document (PDF, fax, email attachment)
    │
    ▼
Azure Document Intelligence (OCR + layout extraction)
    │
    ▼
Classification Model (custom-trained)
    ├── EOB / Remittance Advice
    ├── Clinical Record
    ├── Contract / Fee Schedule
    ├── Payer Correspondence
    └── IDR Decision / Award
    │
    ▼
Field Extraction (per document type)
    ├── Claim #, CPT codes, billed/allowed/paid amounts
    ├── Provider NPI/TIN, payer ID, patient ID
    └── Dates, denial codes, adjustment reason codes
    │
    ▼
Validation + Human Review Queue (confidence < threshold)
    │
    ▼
Stored in OLTP + linked to Case
```

**Expected impact:** 80% reduction in processing time, 90% reduction in extraction errors.

### 11.2 LLM Agent Architecture

Five bounded agents — each with a specific role, defined inputs/outputs, and human oversight level:

| # | Agent | Input | Output | Model | Human Oversight |
|---|---|---|---|---|---|
| 1 | **Document Classifier** | Raw document | Classification + extracted fields | Doc Intelligence + Claude (ambiguous) | Review queue for low confidence |
| 2 | **Underpayment Detector** | Claim + remittance + contract | Underpayment flag + recovery estimate | Rules engine + ML | All flags reviewed before dispute |
| 3 | **Evidence Assembly** | Case context + all linked docs | Organized evidence package + gap analysis | Claude API + RAG | Reviewed before submission |
| 4 | **Narrative Drafter** | Evidence + strategy + precedents | Draft arbitration narrative with citations | Claude API | **Mandatory** human edit + approval |
| 5 | **Case Copilot** | Case history + payer patterns | Win probability, strategy recommendation, similar precedents | Claude API + ML classifier | Advisory only (confidence scores shown) |

### 11.3 RAG Architecture

```
Knowledge Corpus:
├── Federal regulations (NSA, interim final rules)
├── State surprise billing laws (50 states)
├── CMS guidance documents & FAQs
├── IDR decision precedents (public CMS data)
├── Internal contract library
├── FAIR Health / Medicare fee schedules
└── Historical case outcomes (internal)

    ──── Chunked & Embedded ────►  Azure AI Search (vector index)
                                        │
                                        ▼
                                   RAG Pipeline
                                   (query → retrieve → augment → generate)
                                        │
                                        ▼
                                   Cited, grounded responses
                                   with source document references
```

### 11.4 AI Guardrails & Compliance

| Guardrail | Implementation |
|---|---|
| **PHI protection** | Redact PHI before sending to external LLM APIs; Azure Private Endpoints where possible |
| **Structured outputs** | Enforce JSON schema for all agent outputs; validate before storing |
| **Source citations** | Every AI-generated claim must reference a source document or data point |
| **Confidence scoring** | All outputs include confidence score; low-confidence → human queue |
| **Human approval gates** | Narratives, evidence packages, case strategy require explicit sign-off |
| **Audit logging** | Every AI invocation: input hash, output, model version, timestamp, reviewer |
| **Model versioning** | Pin model versions; test before upgrade; maintain rollback capability |
| **Bias monitoring** | Track AI recommendations vs. human overrides; flag divergence by payer/provider |

---

## 12. Analytics Layer (Power BI)

### 12.1 Retained & Enhanced

Power BI is **retained** — leverages existing investment, team skills, and Azure AD embedding. What changes is the **data source**: instead of reading from fragmented operational tables, Power BI reads from the Gold layer via Direct Lake mode.

### 12.2 Dashboard Catalog

| Dashboard | Key Metrics | Audience |
|---|---|---|
| **Recovery Overview** | Total recovered, recovery rate, average award vs. QPA | Director, Manager |
| **Case Pipeline** | Open cases by status, aging, SLA compliance % | Manager, Analyst |
| **Payer Scorecards** | Win rate by payer, average underpayment, response time | Manager, Strategy |
| **CPT Code Analysis** | Recovery rate by code, top disputed codes, Medicare comparison | Analyst, Strategy |
| **Deadline Compliance** | Deadlines met/missed, at-risk cases, escalation frequency | Manager, Director |
| **Financial Forecast** | Projected recovery pipeline, cash flow impact, ROI by case type | Director, Finance |
| **AI Performance** | Agent accuracy, human override rate, processing time savings | Admin, Manager |
| **Operational Efficiency** | Cases per analyst, time-to-resolution, throughput trends | Manager, Director |

### 12.3 Embedded Analytics in Case UI

Contextual Power BI visuals embedded directly in the case management application:
- **Case Detail view:** Payer's historical win/loss for this CPT code
- **QPA Benchmarking view:** This payer's QPA vs. Medicare/FAIR Health for this code+geography
- **Evidence view:** Similar case outcomes (embedded scatter plot)

---

## 13. Security, Compliance & Governance

### 13.1 HIPAA Compliance

| Requirement | Implementation |
|---|---|
| **Encryption at rest** | Azure SQL TDE; ADLS encryption; Blob Storage encryption |
| **Encryption in transit** | TLS 1.2+ everywhere |
| **Access control** | Azure AD (Entra ID) + RBAC; row-level security in Power BI |
| **Audit logging** | All data access and modifications logged in audit_log table |
| **BAA** | Azure BAA covers SQL, Blob, Functions, App Service, Fabric |
| **PHI minimization** | LLM calls use redacted data; full PHI stays in Azure boundary |
| **Breach notification** | Azure Security Center alerts + custom monitoring |

### 13.2 Data Governance

| Concern | Solution |
|---|---|
| **Data lineage** | Microsoft Purview (integrated with Fabric) traces data from source → Bronze → Silver → Gold → Power BI |
| **Data quality** | Validation rules at ingestion; Silver layer enforces schema + referential integrity |
| **Data classification** | Purview auto-classifies PHI columns; sensitivity labels propagate to Power BI |
| **Retention** | Configurable per data type; arbitration records retained per state/federal requirements |
| **Access audit** | Who accessed what data, when — queryable via audit_log + Azure AD logs |

---

## 14. Platform Decision: Fabric vs. Databricks

**Recommendation: Microsoft Fabric** for this use case.

| Factor | Fabric | Databricks |
|---|---|---|
| Power BI integration | Native (Direct Lake) | Connector-based |
| Team skillset fit | Aligns with Azure/PBI/Python | Requires Spark expertise |
| Operational complexity | Fully managed, no clusters | More infrastructure |
| Cost at this scale | Lower (unified capacity) | Higher (compute clusters) |
| CDC ingestion | Native Fabric Pipeline support | Requires Delta Live Tables setup |
| Governance | Purview native | Unity Catalog (separate sync) |
| Advanced ML | Adequate (Notebooks + MLflow) | Superior (but overkill here) |

**Re-evaluate for Databricks if:** AI/ML workload grows to require dedicated model training infrastructure, feature stores, or MLOps beyond Fabric Notebooks.

---

## 15. Migration from Current State

### 15.1 What Changes vs. Current

| Component | Current | Proposed | Risk | Approach |
|---|---|---|---|---|
| Data ingestion | ADF batch + manual | ADF + Event Hubs + Functions | Low | Additive — keep ADF, add event layer |
| Storage | Azure SQL + Blob | Azure SQL (OLTP) + ADLS/OneLake (OLAP) | Low | Additive — new lakehouse alongside |
| Processing | SQL transforms in ADF | Fabric Lakehouse (medallion) | Medium | New skill — start with simple transforms |
| Case management | Spreadsheets/manual | React + FastAPI + Azure SQL | High | New build — biggest effort |
| Workflow | Manual | Durable Functions | Medium | New build — start with deadline monitoring |
| Doc intelligence | None | Azure Doc Intelligence | Medium | New capability — start with EOBs |
| AI/LLM | None | Claude API + AI Search (RAG) | Medium | New capability — start with evidence assembly |
| Analytics | Power BI | Power BI (retained) + embedded | Low | Enhancement — redirect data source to Gold |
| Auth | Azure AD + PBI embedding | Azure AD (retained) + app RBAC | Low | Extension |

### 15.2 Migration Strategy: Strangler Fig Pattern

Don't big-bang replace. Wrap the old system progressively:

```
Phase 1: New OLTP + Case UI coexist with old Power BI reports
         (PBI reads from both old and new sources)

Phase 2: Event-driven ingestion runs alongside ADF batch
         (both write to new OLTP; old ADF pipelines still active)

Phase 3: Lakehouse (Bronze/Silver/Gold) replaces direct PBI-to-SQL queries
         (PBI now reads from Gold layer via Direct Lake)

Phase 4: AI layer plugs into new OLTP + Lakehouse
         (old manual processes still available as fallback)

Phase 5: Old direct-to-SQL PBI reports retired
         (full stack on new architecture)
```

---

## 16. Implementation Roadmap

### Phase 1: Foundation (Months 1-3)
**Goal:** Operational case management + enhanced data model

- Design and deploy OLTP schema (Section 6.2) in Azure SQL
- Build case management API (FastAPI on Azure App Service)
- Build core Case Management UI (React) — dashboard, case detail, timeline
- Enable CDC on core OLTP tables
- Set up ADLS Gen2 with Bronze/Silver/Gold structure
- Migrate existing Power BI reports to read from new OLTP

**Exit criteria:** Cases tracked in app, not spreadsheets. Power BI reads from new store.

### Phase 2: Document Intelligence (Months 3-5)
**Goal:** Automated document processing

- Deploy Azure Document Intelligence
- Train custom classification model (EOB, clinical, contract, correspondence)
- Build ingestion pipeline: ingest → OCR → classify → extract → validate → store
- Implement human review queue for low-confidence extractions
- Link extracted documents to cases in OLTP

**Exit criteria:** 80%+ of incoming documents auto-classified and extracted.

### Phase 3: Workflow Automation (Months 4-6)
**Goal:** Event-driven case lifecycle

- Implement Durable Functions for Workflows 1-3 (Section 9.2)
- Build deadline monitoring + alert engine
- Deploy notification engine (email, Teams, in-app)
- Automate case routing by payer, code, dollar amount
- Build SLA compliance dashboard in Power BI

**Exit criteria:** No deadline missed silently. Cases auto-routed on creation.

### Phase 4: AI Agents (Months 5-8)
**Goal:** AI-assisted case preparation

- Deploy Azure AI Search with vector index (contracts, regulations, precedents)
- Build RAG pipeline for regulatory/contract queries
- Implement Evidence Assembly Agent (Claude API + LangChain)
- Implement Narrative Drafting Agent with human review workflow
- Build Case Copilot with win-rate prediction
- Deploy guardrails framework (PHI redaction, confidence scoring, audit)

**Exit criteria:** AI drafts evidence packages and narratives. Humans review + approve.

### Phase 5: Optimization (Months 8-12)
**Goal:** Continuous improvement + advanced analytics

- Train win-rate prediction model on historical outcomes
- Build payer behavior scorecards and negotiation intelligence
- Implement QPA validation tooling
- Build revenue impact forecasting
- Optimize AI agents based on human override patterns
- Complete Fabric migration; retire old direct-to-SQL PBI reports

**Exit criteria:** Full platform operational. Measurable improvements in recovery rate, processing time, and analyst throughput.
