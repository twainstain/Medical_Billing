# Medical Billing Arbitration — Ingestion Layer Design

> **Author:** Claude (Opus 4.6) — March 2026
> **Parent doc:** `medical_billing_arbitration_future_architecture.md`
> **Scope:** Deep-dive into the ingestion layer only — data sources, challenges, resolutions, example code, and execution strategy.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Ingestion Patterns](#2-ingestion-patterns)
3. [Ingestion Principles](#3-ingestion-principles)
4. [Technology per Source — Summary](#4-technology-per-source--summary)
5. [Data Source Deep-Dives](#5-data-source-deep-dives)
   - 5.1 [Claims (EDI 837)](#51-claims-edi-837)
   - 5.2 [Remittances — ERA (EDI 835)](#52-remittances--era-edi-835)
   - 5.3 [Remittances — EOB (PDF)](#53-remittances--eob-pdf)
   - 5.4 [Patient Data (EHR / PMS)](#54-patient-data-ehr--pms)
   - 5.5 [Fee Schedules — Medicare / CMS](#55-fee-schedules--medicare--cms)
   - 5.6 [Fee Schedules — FAIR Health](#56-fee-schedules--fair-health)
   - 5.7 [Fee Schedules — Payer-Specific](#57-fee-schedules--payer-specific)
   - 5.8 [Provider / Facility Data (NPPES)](#58-provider--facility-data-nppes)
   - 5.9 [Contracts (Provider-Payer Agreements)](#59-contracts-provider-payer-agreements)
   - 5.10 [Regulations & Rules](#510-regulations--rules)
   - 5.11 [Document Uploads (Case Management UI)](#511-document-uploads-case-management-ui)
   - 5.12 [Historical Claims Backfill](#512-historical-claims-backfill)
6. [Cross-Cutting Concerns](#6-cross-cutting-concerns)
7. [Execution Strategy — How It All Runs](#7-execution-strategy--how-it-all-runs)
8. [Monitoring & Observability](#8-monitoring--observability)
9. [Infrastructure as Code — Deployment](#9-infrastructure-as-code--deployment)

---

## 1. Architecture Overview

The ingestion layer sits between source systems and the OLTP operational store (Azure SQL). Its job is to receive, parse, validate, and write data into the single source of truth. The OLAP lakehouse is **never written to directly by ingestion** — it is derived from OLTP via CDC.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                      │
│                                                                                  │
│  EHR/PMS      Clearinghouses    Payer Portals     ERA/EOB         Contracts     │
│  (HL7/FHIR)   (EDI 837)        (API/scrape)      (EDI 835+PDF)   (PDF/scan)    │
│                                                                                  │
│  CMS (Medicare fees)   FAIR Health   NPPES   Regulations   Case Mgmt UI         │
└────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬───────────┘
     │          │          │          │          │          │          │
     ▼          ▼          ▼          ▼          ▼          ▼          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                         │
│                                                                                  │
│  ┌─────────────────────────┐    ┌───────────────────────────────────────────┐   │
│  │   EVENT-DRIVEN PATH      │    │   BATCH PATH                              │   │
│  │                           │    │                                           │   │
│  │  Event Hubs / Svc Bus     │    │  Azure Data Factory                      │   │
│  │  Azure Functions          │    │  Scheduled pipelines                     │   │
│  │  SFTP listeners           │    │  Staging tables + SCD merge              │   │
│  │  Email ingestion          │    │                                           │   │
│  │                           │    │                                           │   │
│  │  Sources:                 │    │  Sources:                                │   │
│  │  - Claims (837)           │    │  - Medicare fee schedules                │   │
│  │  - ERA/835                │    │  - FAIR Health rates                     │   │
│  │  - EOB PDFs               │    │  - Payer fee schedules                   │   │
│  │  - Patient updates (FHIR) │    │  - NPPES provider data                  │   │
│  │  - Document uploads       │    │  - Historical claims backfill            │   │
│  └────────────┬──────────────┘    └──────────────────┬──────────────────────┘   │
│               │                                       │                          │
│               ▼                                       ▼                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                     SHARED PIPELINE STAGES                                │   │
│  │                                                                           │   │
│  │  1. Parse (EDI X12 / CSV / FHIR JSON / PDF via Doc Intelligence)         │   │
│  │  2. Validate (schema, business rules, referential integrity)              │   │
│  │  3. Dedup (idempotency key check against OLTP)                           │   │
│  │  4. Write to OLTP (Azure SQL — single source of truth)                   │   │
│  │  5. Emit event (Event Hub — for downstream workflow triggers)            │   │
│  │  6. Audit log (every ingestion action recorded)                          │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                              OLTP (Azure SQL)
                              Single source of truth
                                        │
                                        │ CDC (automatic)
                                        ▼
                              OLAP Lakehouse (derived)
```

---

## 2. Ingestion Patterns

There are two ingestion patterns. The choice depends on the source's velocity and delivery mechanism.

### 2.1 Event-Driven Ingestion (Azure Functions + Event Hubs)

**When to use:** Data arrives unpredictably — as claims are filed, remittances received, documents uploaded, or patients updated.

```
Source event (SFTP drop / webhook / upload / email)
    │
    ▼
Event Hub or Service Bus (buffer, ordering, at-least-once delivery)
    │
    ▼
Azure Function (triggered by Event Hub message)
    │
    ├── Structured (EDI 837/835, FHIR JSON)
    │       → Parse → Validate → Dedup → Write OLTP → Emit downstream event
    │
    └── Unstructured (PDF, fax, scanned image)
            → Store in Blob → Trigger Doc Intelligence (OCR + classify + extract)
                → Confidence check
                    ├── >= 90%  → Write OLTP → Emit downstream event
                    └── < 90%   → Route to Human Review Queue
```

**Key properties:**
- Latency: seconds to minutes
- Idempotent: dedup via source-specific idempotency keys (claim_id, ERA trace number, etc.)
- Dead-letter queue: failed messages go to DLQ for investigation, not dropped

### 2.2 Batch Ingestion (Azure Data Factory)

**When to use:** Data arrives on a known schedule (monthly, quarterly, annual) or in bulk (historical backfill, reference data refreshes).

```
Schedule trigger (or manual trigger for backfill)
    │
    ▼
ADF Pipeline
    │
    ├── Extract: pull from source (SFTP, HTTP, API, file share)
    │
    ├── Load to staging table (raw, no transforms yet)
    │
    ├── Transform + Validate:
    │       - Schema validation
    │       - Business rule checks
    │       - SCD Type 2 merge logic (for fee schedules, contracts)
    │       - Upsert logic (for provider data)
    │
    ├── Write to OLTP (Azure SQL — same target as event-driven)
    │
    └── Post-load: emit event for downstream consumers (AI Search re-index, etc.)
```

**Key properties:**
- Latency: minutes to hours (acceptable for reference data)
- Staging tables: raw data lands first, then merges into operational tables
- Retry: ADF built-in retry + alerting on failure

---

## 3. Ingestion Principles

### 3.1 Six Shared Pipeline Stages

Every data source — regardless of whether it arrives via the event-driven or batch path — passes through the same six stages:

```
Source (any format, any velocity)
    │
    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Stage 1: PARSE                                                      │
│  Transform raw source format into normalized internal representation │
│  EDI X12 → dict  |  CSV → rows  |  FHIR JSON → dict  |  PDF → OCR  │
├──────────────────────────────────────────────────────────────────────┤
│  Stage 2: VALIDATE                                                   │
│  Schema conformance, business rules, referential integrity           │
│  Required fields present? Values in range? Foreign keys exist?       │
├──────────────────────────────────────────────────────────────────────┤
│  Stage 3: DEDUP                                                      │
│  Check idempotency key against OLTP to prevent duplicate records     │
│  Each source has a natural key — never system-generated              │
├──────────────────────────────────────────────────────────────────────┤
│  Stage 4: WRITE TO OLTP                                              │
│  Azure SQL is the single source of truth                             │
│  Insert, update, upsert, or SCD Type 2 merge — depends on source    │
├──────────────────────────────────────────────────────────────────────┤
│  Stage 5: EMIT EVENT                                                 │
│  Publish downstream event to Event Hub (only on insert/update)       │
│  Workflow engine reacts: deadline timers, underpayment detection      │
├──────────────────────────────────────────────────────────────────────┤
│  Stage 6: AUDIT LOG                                                  │
│  Every ingestion action recorded with entity type, entity ID,        │
│  action, user/system, timestamp. No PHI in logs.                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Foundational Principles

| # | Principle | What It Means |
|---|---|---|
| 1 | **One write path** | All data flows into OLTP (Azure SQL) first. The OLAP lakehouse is derived via CDC — never written to directly by ingestion. This eliminates dual-write consistency problems. |
| 2 | **Two patterns, same destination** | Event-driven (for unpredictable arrivals) and batch (for scheduled/bulk loads) both write to the same Azure SQL tables through the same validation and dedup logic. |
| 3 | **Idempotent by design** | Every write operation can be safely repeated. Natural business keys from source data (not system-generated IDs) serve as dedup keys. See `medical_billing_idempotency_design.md` for full detail. |
| 4 | **Parse tolerantly, validate strictly** | Parsers accept optional missing fields without crashing (tolerant). Validators enforce required fields and reject bad records to a dead-letter queue (strict). |
| 5 | **Never drop data silently** | Failed records go to a dead-letter queue or human review queue — never discarded. Every failure is logged and alerted on. |
| 6 | **Downstream events are conditional** | Events are emitted only on insert or update — never on duplicate skip. This prevents the workflow engine from processing the same claim/remittance twice. |
| 7 | **PHI never in logs** | Patient names, DOB, SSN, and medical details are never logged. Only entity IDs and action types appear in logs, error messages, and dead-letter queues. |
| 8 | **Audit everything** | Every ingestion action (insert, update, skip, reject) writes to the `audit_log` table — required for HIPAA compliance and arbitration evidence trails. |

---

## 4. Technology per Source — Summary

### 4.1 At-a-Glance: Source-to-Technology Mapping

| # | Source | Format | Pattern | Trigger | Core Azure Services | Parser / Processor | Write Pattern | Dedup Key |
|---|---|---|---|---|---|---|---|---|
| 1 | Claims (Clearinghouse) | EDI X12 837 | Event-driven | Event Hub | Azure Functions | Python EDI X12 parser (`EDI837Parser`) | Append + frequency code | `claim_id` + `frequency_code` |
| 2 | ERA / Remittances (Payer) | EDI X12 835 | Event-driven | SFTP → Timer → Event Hub | Azure Functions + `paramiko` SSH | Python EDI X12 parser (`EDI835Parser`) | Append | `trace_number` + `payer_id` |
| 3 | EOB / Remittances (Payer) | PDF | Event-driven | Blob trigger | Azure Functions + Doc Intelligence | Per-payer custom OCR models | Append | `content_hash` (SHA-256) |
| 4 | Patient Data (EHR) | FHIR R4 JSON / HL7 v2 | Event-driven + batch safety net | HTTP webhook + ADF nightly | Azure Functions + `python-hl7` | FHIR Patient normalizer | Upsert (SCD Type 1) | `patient_id` (MRN) |
| 5 | Medicare Fee Schedules (CMS) | CSV / Excel | Batch | ADF quarterly schedule | Azure Data Factory | CSV → staging → SQL stored procedure | SCD Type 2 MERGE | `payer+cpt+mod+geo+valid_from` |
| 6 | FAIR Health Rates | Licensed files | Batch | ADF quarterly schedule | Azure Data Factory | Versioned schema config | SCD Type 2 MERGE | `payer+cpt+mod+geo+valid_from` |
| 7 | Payer Fee Schedules | CSV / Excel / PDF | Batch (+ Doc Intel for PDF) | ADF manual trigger | ADF + Doc Intelligence | Per-payer column config | SCD Type 2 MERGE | `payer+cpt+mod+geo+valid_from` |
| 8 | Provider Data (NPPES) | CSV (8GB) | Batch | ADF monthly schedule | ADF + Azure Functions (filter) | CSV filter + bulk staging | SCD Type 2 MERGE | `npi` |
| 9 | Contracts | PDF | Event-driven (UI) | HTTP POST from UI | Azure Functions + Doc Intelligence + Claude LLM | Layout extraction + LLM interpretation | Human-reviewed → SCD Type 2 | `content_hash` (SHA-256) |
| 10 | Regulations & Rules | PDF / HTML | Manual | UI upload | Azure AI Search | Vector embeddings + section chunking | AI Search index (no OLTP write) | N/A (corpus versioning) |
| 11 | Document Uploads | PDF / TIFF / JPEG / Word / Email | Event-driven (UI) | Blob trigger | Azure Functions + Doc Intelligence | Classification model (8 doc types) | Append | `content_hash` (SHA-256) |
| 12 | Historical Claims Backfill | EDI 837 / CSV | Batch | ADF ad-hoc (manual) | Azure Data Factory | Legacy code crosswalk mapping | Append (flagged `source='backfill'`) | `claim_id+dos+npi+billed` |

### 4.2 Technology Stack Summary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION TECHNOLOGY STACK                                │
│                                                                                  │
│  EVENT-DRIVEN                           BATCH                                   │
│  ─────────────                          ─────                                   │
│  Azure Event Hubs (buffer + ordering)   Azure Data Factory (orchestration)      │
│  Azure Service Bus (DLQ + review Qs)    ADF Copy Activities (extract + load)    │
│  Azure Functions (parse + validate)     ADF Stored Proc Activities (SCD merge)  │
│  paramiko (SFTP polling)                Staging tables in Azure SQL             │
│                                                                                  │
│  SHARED                                                                          │
│  ──────                                                                          │
│  Azure SQL Database (OLTP — single source of truth)                             │
│  Azure Doc Intelligence (OCR, classification, table/field extraction)           │
│  Azure Blob Storage (documents, staging files)                                  │
│  Azure Key Vault (all secrets — connection strings, API keys, SFTP creds)       │
│  Azure AI Search (vector index for regulations RAG)                             │
│  Claude API via LangChain (contract interpretation)                             │
│  Azure Monitor + Log Analytics (observability)                                  │
│                                                                                  │
│  LANGUAGES & LIBRARIES                                                           │
│  ─────────────────────                                                           │
│  Python 3.11 (all Azure Functions)                                              │
│  pyodbc (Azure SQL connectivity)                                                │
│  paramiko (SFTP)                                                                │
│  python-hl7 (HL7 v2 legacy parsing)                                            │
│  azure-ai-documentintelligence SDK                                              │
│  azure-eventhub SDK                                                             │
│  hashlib (SHA-256 for document dedup)                                           │
│  T-SQL (SCD Type 2 stored procedures)                                           │
│  Bicep (Infrastructure as Code)                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Ingestion Pattern Decision Guide

| Your Data Looks Like... | Use This Pattern | Why |
|---|---|---|
| Arrives unpredictably, needs fast processing (claims, remittances, webhooks) | **Event-driven** (Event Hub + Azure Function) | Seconds-to-minutes latency; auto-scales with volume |
| Arrives on a known schedule in bulk (fee schedules, provider files) | **Batch** (ADF pipeline) | Efficient for large files; staging + merge prevents partial loads |
| Is a PDF or scanned document | **Event-driven + Doc Intelligence** (Blob trigger → OCR → Function) | Async processing; confidence-based routing to human review |
| Is uploaded manually by staff | **Event-driven** (HTTP trigger or Blob trigger) | Immediate feedback to user; dedup via content hash |
| Needs full history (rates, contracts, provider status) | Either pattern → **SCD Type 2 MERGE** in OLTP | Preserves point-in-time lookups for arbitration evidence |
| Is a one-time migration from legacy systems | **Batch** (ADF ad-hoc) with `source='backfill'` flag | Bulk performance; workflow engine ignores historical records |

---

## 5. Data Source Deep-Dives

### 5.1 Claims (EDI 837)

| Attribute | Detail |
|---|---|
| **Source** | Clearinghouse feed (e.g., Availity, Change Healthcare, Waystar) |
| **Format** | EDI X12 837 (Professional = 837P, Institutional = 837I) |
| **Velocity** | Daily, as claims are filed |
| **Ingestion pattern** | Event-driven (Event Hub + Azure Function) |
| **Target table** | `claims` + `claim_lines` |
| **History model** | Append-only with status column |
| **Idempotency key** | `claim_id` (CLM01 segment) |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **EDI X12 is not human-readable** — fixed-width segment/element format with nested hierarchical loops (ISA/GS/ST/CLM/SV1) | Requires specialized parsing; standard JSON/CSV tools cannot handle it | High |
| **Multiple 837 versions** — 837P (professional) vs. 837I (institutional) have different segment structures and required fields | One parser does not fit both; must detect transaction type and branch | High |
| **Clearinghouse-specific variations** — different clearinghouses may use optional segments differently, add proprietary fields, or use non-standard delimiters | Parsers that work for Availity may break on Change Healthcare feeds | Medium |
| **Duplicate claim submissions** — the same claim may arrive multiple times (resubmissions, corrections, voids) | Must detect and handle duplicates vs. legitimate corrections | High |
| **Missing or malformed segments** — optional segments (e.g., referring provider, prior authorization) may be absent or incorrectly formatted | Parser must be tolerant of missing optional data without failing | Medium |
| **High volume in batch windows** — clearinghouses often deliver files in nightly batches, creating spikes | Event Hub must handle burst; Functions must scale | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| EDI parsing complexity | Use the `py-edi-837` or `pyx12` library for X12 parsing; wrap in a reusable parser class that outputs normalized JSON |
| Multiple 837 versions | Detect transaction set ID (ST01: "837") and implementation guide reference (ST03) to branch to 837P or 837I parser |
| Clearinghouse variations | Abstract parser behind interface; configure per-clearinghouse overrides for delimiter, segment usage |
| Duplicate submissions | Dedup on `claim_id` + `date_filed`; use `frequency_code` (CLM05-3) to detect original, replacement, or void |
| Missing segments | Parse with optional field defaults; validate required fields only; log warnings for missing optional fields |
| Volume spikes | Event Hub partitions (scale consumers); Azure Functions consumption plan auto-scales |

#### Example Code — Azure Function: Claims Ingestion

```python
# function_app.py — Claims ingestion Azure Function (Event Hub triggered)

import json
import logging
import azure.functions as func
import pyodbc
from datetime import datetime

app = func.FunctionApp()

# ── EDI 837 Parser ──────────────────────────────────────────────────────

class EDI837Parser:
    """Parses EDI X12 837 (Professional & Institutional) into normalized dicts."""

    def __init__(self, raw_edi: str):
        self.raw = raw_edi
        self.delimiter = self._detect_delimiter()
        self.segments = self._split_segments()

    def _detect_delimiter(self) -> str:
        # Element separator is character at position 3 of ISA segment
        # Segment terminator is character at position 105 of ISA
        if self.raw[:3] != "ISA":
            raise ValueError("EDI file does not start with ISA segment")
        return self.raw[3]  # element separator (commonly * or |)

    def _split_segments(self) -> list[list[str]]:
        # Segment terminator is the char after ISA16 (position 105)
        seg_terminator = self.raw[105]
        lines = self.raw.split(seg_terminator)
        return [line.strip().split(self.delimiter) for line in lines if line.strip()]

    def _find_segments(self, segment_id: str) -> list[list[str]]:
        return [s for s in self.segments if s[0] == segment_id]

    def detect_transaction_type(self) -> str:
        """Returns '837P' or '837I' based on ST03 implementation reference."""
        st_segments = self._find_segments("ST")
        if not st_segments:
            raise ValueError("No ST segment found")
        st03 = st_segments[0][3] if len(st_segments[0]) > 3 else ""
        if "005010X222" in st03:
            return "837P"  # Professional
        elif "005010X223" in st03:
            return "837I"  # Institutional
        return "837P"  # Default to professional

    def parse(self) -> dict:
        """Parse full 837 into a normalized claim dict."""
        tx_type = self.detect_transaction_type()
        claims = []

        clm_segments = self._find_segments("CLM")
        for clm in clm_segments:
            claim = {
                "claim_id": clm[1],                          # CLM01
                "total_billed": float(clm[2]),                # CLM02
                "facility_type": clm[5] if len(clm) > 5 else None,  # CLM05
                "frequency_code": clm[5].split(":")[2] if len(clm) > 5 and ":" in clm[5] else "1",
                "transaction_type": tx_type,
                "date_of_service": None,
                "diagnosis_codes": [],
                "lines": [],
            }

            # Parse service lines — SV1 (professional) or SV2 (institutional)
            sv_id = "SV1" if tx_type == "837P" else "SV2"
            for sv in self._find_segments(sv_id):
                line = {
                    "cpt_code": sv[1].split(":")[1] if ":" in sv[1] else sv[1],
                    "billed_amount": float(sv[2]) if len(sv) > 2 else 0.0,
                    "units": int(sv[4]) if len(sv) > 4 else 1,
                    "modifier": sv[1].split(":")[2] if len(sv[1].split(":")) > 2 else None,
                }
                claim["lines"].append(line)

            # Parse diagnosis codes from HI segments
            for hi in self._find_segments("HI"):
                for element in hi[1:]:
                    if ":" in element:
                        code = element.split(":")[1]
                        claim["diagnosis_codes"].append(code)

            # Parse dates from DTP segments
            for dtp in self._find_segments("DTP"):
                if dtp[1] == "472":  # Service date qualifier
                    claim["date_of_service"] = dtp[3]

            claims.append(claim)

        return {"transaction_type": tx_type, "claims": claims}


# ── Validation ───────────────────────────────────────────────────────────

def validate_claim(claim: dict) -> list[str]:
    """Returns a list of validation errors (empty = valid)."""
    errors = []
    if not claim.get("claim_id"):
        errors.append("Missing claim_id (CLM01)")
    if not claim.get("total_billed") or claim["total_billed"] <= 0:
        errors.append(f"Invalid total_billed: {claim.get('total_billed')}")
    if not claim.get("lines"):
        errors.append("No service lines found")
    if not claim.get("date_of_service"):
        errors.append("Missing date_of_service (DTP 472)")
    for i, line in enumerate(claim.get("lines", [])):
        if not line.get("cpt_code"):
            errors.append(f"Line {i}: missing cpt_code")
    return errors


# ── Deduplication ────────────────────────────────────────────────────────

FREQ_CODE_MAP = {"1": "original", "7": "replacement", "8": "void"}

def resolve_duplicate(existing_status: str | None, frequency_code: str) -> str:
    """Determine action: 'insert', 'update', 'void', or 'skip'."""
    action = FREQ_CODE_MAP.get(frequency_code, "original")
    if existing_status is None:
        return "insert"
    if action == "replacement":
        return "update"
    if action == "void":
        return "void"
    return "skip"  # Duplicate original — ignore


# ── Database Writer ──────────────────────────────────────────────────────

def write_claim_to_oltp(conn: pyodbc.Connection, claim: dict, action: str):
    """Write parsed claim to Azure SQL. Handles insert, update, and void."""
    cursor = conn.cursor()

    if action == "insert":
        cursor.execute("""
            INSERT INTO claims (claim_id, patient_id, provider_npi, payer_id,
                                date_of_service, date_filed, total_billed, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'received')
        """, claim["claim_id"], claim.get("patient_id"),
             claim.get("provider_npi"), claim.get("payer_id"),
             claim["date_of_service"], datetime.utcnow(),
             claim["total_billed"])

        for line in claim["lines"]:
            cursor.execute("""
                INSERT INTO claim_lines (claim_id, cpt_code, modifier, units,
                                         billed_amount, diagnosis_codes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, claim["claim_id"], line["cpt_code"], line.get("modifier"),
                 line["units"], line["billed_amount"],
                 json.dumps(claim.get("diagnosis_codes", [])))

    elif action == "update":
        cursor.execute("""
            UPDATE claims SET total_billed = ?, status = 'corrected',
                              date_filed = ?
            WHERE claim_id = ?
        """, claim["total_billed"], datetime.utcnow(), claim["claim_id"])

    elif action == "void":
        cursor.execute("""
            UPDATE claims SET status = 'voided' WHERE claim_id = ?
        """, claim["claim_id"])

    conn.commit()

    # Audit log
    cursor.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, timestamp)
        VALUES ('claim', ?, ?, ?)
    """, claim["claim_id"], action, datetime.utcnow())
    conn.commit()


# ── Azure Function Entry Point ───────────────────────────────────────────

@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="claims-ingest",
    connection="EventHubConnection",
    consumer_group="$Default"
)
def ingest_claim(event: func.EventHubEvent):
    """Triggered when a new EDI 837 message arrives on the claims Event Hub."""
    raw_edi = event.get_body().decode("utf-8")
    logging.info(f"Received EDI 837 message, size={len(raw_edi)} bytes")

    try:
        # 1. Parse
        parser = EDI837Parser(raw_edi)
        result = parser.parse()

        conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])

        for claim in result["claims"]:
            # 2. Validate
            errors = validate_claim(claim)
            if errors:
                logging.warning(f"Claim {claim.get('claim_id')} validation failed: {errors}")
                # Write to dead-letter / review queue
                _send_to_dlq(claim, errors)
                continue

            # 3. Dedup
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM claims WHERE claim_id = ?", claim["claim_id"])
            row = cursor.fetchone()
            existing_status = row[0] if row else None
            action = resolve_duplicate(existing_status, claim.get("frequency_code", "1"))

            if action == "skip":
                logging.info(f"Claim {claim['claim_id']} is a duplicate — skipping")
                continue

            # 4. Write to OLTP
            write_claim_to_oltp(conn, claim, action)
            logging.info(f"Claim {claim['claim_id']} — action={action}")

            # 5. Emit downstream event (for workflow engine)
            _emit_event("claim-events", {
                "type": f"claim.{action}",
                "claim_id": claim["claim_id"],
                "timestamp": datetime.utcnow().isoformat()
            })

        conn.close()

    except Exception as e:
        logging.error(f"Claims ingestion failed: {e}")
        raise  # Azure Functions will retry per host.json policy
```

---

### 5.2 Remittances — ERA (EDI 835)

| Attribute | Detail |
|---|---|
| **Source** | Payer clearinghouse, direct payer feed |
| **Format** | EDI X12 835 |
| **Delivery** | SFTP drop or Event Hub |
| **Velocity** | Daily, as payments are processed |
| **Ingestion pattern** | Event-driven (SFTP listener → Event Hub → Azure Function) |
| **Target table** | `remittances` |
| **History model** | Append-only |
| **Idempotency key** | `trace_number` (TRN02) + `payer_id` |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Claim-to-remittance matching** — the 835 references claims by CLM01 but the identifier may not exactly match the 837 claim_id (leading zeros, trimming, payer reformatting) | Remittances can't be linked to claims, breaking the dispute workflow | Critical |
| **Multiple remittances per claim** — partial payments, adjustments, and recoupments result in multiple 835s for one claim | Summing payments incorrectly; missing the final underpayment amount | High |
| **Adjustment reason codes** — 300+ CARC/RARC reason codes with payer-specific interpretations | Must correctly classify denials vs. contractual adjustments vs. patient responsibility | High |
| **SFTP reliability** — payer SFTP servers have variable uptime, may deliver files late or out of order | Missed files = missed payment data = stalled cases | Medium |
| **Proprietary 835 extensions** — some payers add non-standard segments or use non-standard adjustment codes | Parser breaks or misclassifies adjustments | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Claim matching | Normalize claim IDs on both sides (strip leading zeros, uppercase, trim). Maintain a `claim_id_alias` table mapping payer-formatted IDs to canonical claim_ids. Fuzzy match as fallback with human review for unmatched. |
| Multiple remittances | Append all remittances; compute `total_paid` and `underpayment` at query time by summing across remittances per claim. Never overwrite — always append. |
| Adjustment reason codes | Maintain a CARC/RARC lookup table (sourced from X12.org). Classify into buckets: denial, contractual adjustment, patient responsibility, informational. Flag unknown codes for review. |
| SFTP reliability | SFTP watcher function polls on a schedule (every 15 min). Track last-processed file timestamp. Alert if no files received in 24h (configurable). Retry failed downloads with exponential backoff. |
| Proprietary extensions | Lenient parser that ignores unknown segments. Log unknown segments for analysis. Per-payer config for known extensions. |

#### Example Code — Azure Function: ERA 835 Ingestion

```python
# era_ingestion.py — ERA/835 ingestion via SFTP polling + Event Hub

import os
import json
import logging
import paramiko
import azure.functions as func
import pyodbc
from datetime import datetime

app = func.FunctionApp()

# ── SFTP Poller (Timer-Triggered) ────────────────────────────────────────

@app.timer_trigger(
    schedule="0 */15 * * * *",  # Every 15 minutes
    arg_name="timer",
    run_on_startup=False
)
def poll_sftp_for_era(timer: func.TimerRequest):
    """Poll payer SFTP for new 835 files and forward to Event Hub."""
    sftp_config = json.loads(os.environ["SFTP_CONFIG"])  # list of {host, user, key, path}

    for payer_sftp in sftp_config:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=payer_sftp["host"],
                username=payer_sftp["user"],
                key_filename=payer_sftp["key_path"]
            )
            sftp = ssh.open_sftp()

            # Track last processed file via Azure Table Storage / SQL
            last_timestamp = _get_last_processed_timestamp(payer_sftp["host"])

            for file_attr in sftp.listdir_attr(payer_sftp["remote_path"]):
                if file_attr.st_mtime <= last_timestamp:
                    continue  # Already processed

                remote_path = f"{payer_sftp['remote_path']}/{file_attr.filename}"
                raw_edi = sftp.open(remote_path).read().decode("utf-8")

                # Forward to Event Hub for processing
                _send_to_event_hub("era-ingest", {
                    "payer_sftp_host": payer_sftp["host"],
                    "filename": file_attr.filename,
                    "raw_edi": raw_edi,
                    "received_at": datetime.utcnow().isoformat()
                })

                _update_last_processed_timestamp(payer_sftp["host"], file_attr.st_mtime)
                logging.info(f"Forwarded {file_attr.filename} from {payer_sftp['host']}")

            sftp.close()
            ssh.close()

        except Exception as e:
            logging.error(f"SFTP poll failed for {payer_sftp['host']}: {e}")
            _alert_sftp_failure(payer_sftp["host"], str(e))


# ── 835 Parser ───────────────────────────────────────────────────────────

class EDI835Parser:
    """Parses EDI X12 835 into normalized remittance records."""

    def __init__(self, raw_edi: str):
        self.raw = raw_edi
        self.delimiter = raw_edi[3] if raw_edi[:3] == "ISA" else "*"
        seg_terminator = raw_edi[105] if len(raw_edi) > 105 else "~"
        self.segments = [
            s.strip().split(self.delimiter)
            for s in raw_edi.split(seg_terminator) if s.strip()
        ]

    def parse(self) -> list[dict]:
        remittances = []
        current_claim_id = None
        trace_number = None
        payer_id = None

        for seg in self.segments:
            seg_id = seg[0]

            # TRN — Trace number (unique per 835)
            if seg_id == "TRN" and len(seg) > 2:
                trace_number = seg[2]

            # N1*PR — Payer identification
            if seg_id == "N1" and len(seg) > 2 and seg[1] == "PR":
                payer_id = seg[4] if len(seg) > 4 else seg[2]

            # CLP — Claim-level payment
            if seg_id == "CLP":
                current_claim_id = seg[1]  # CLP01 = claim ID as payer knows it
                remittance = {
                    "payer_claim_id": seg[1],
                    "claim_status": seg[2],      # 1=processed, 2=denied, etc.
                    "total_billed": float(seg[3]) if len(seg) > 3 else 0.0,
                    "paid_amount": float(seg[4]) if len(seg) > 4 else 0.0,
                    "patient_responsibility": float(seg[5]) if len(seg) > 5 else 0.0,
                    "trace_number": trace_number,
                    "payer_id": payer_id,
                    "adjustments": [],
                    "era_date": datetime.utcnow().isoformat(),
                }
                remittances.append(remittance)

            # CAS — Adjustment reason codes
            if seg_id == "CAS" and current_claim_id and remittances:
                group_code = seg[1]  # CO=contractual, PR=patient, OA=other, CR=corrections
                i = 2
                while i + 1 < len(seg):
                    adj = {
                        "group_code": group_code,
                        "reason_code": seg[i],
                        "amount": float(seg[i + 1]) if seg[i + 1] else 0.0,
                    }
                    remittances[-1]["adjustments"].append(adj)
                    i += 3  # reason_code, amount, quantity triplets

        return remittances


# ── Claim ID Matching ────────────────────────────────────────────────────

def match_claim_id(conn: pyodbc.Connection, payer_claim_id: str) -> str | None:
    """Resolve payer's claim ID to our canonical claim_id."""
    cursor = conn.cursor()

    # Direct match
    cursor.execute("SELECT claim_id FROM claims WHERE claim_id = ?", payer_claim_id)
    row = cursor.fetchone()
    if row:
        return row[0]

    # Normalized match (strip leading zeros, uppercase)
    normalized = payer_claim_id.lstrip("0").upper().strip()
    cursor.execute("""
        SELECT claim_id FROM claims
        WHERE UPPER(LTRIM(claim_id)) = ? OR claim_id = ?
    """, normalized, normalized)
    row = cursor.fetchone()
    if row:
        return row[0]

    # Check alias table
    cursor.execute("""
        SELECT canonical_claim_id FROM claim_id_alias
        WHERE payer_claim_id = ?
    """, payer_claim_id)
    row = cursor.fetchone()
    if row:
        return row[0]

    return None  # Unmatched — route to human review


# ── Event Hub Handler ────────────────────────────────────────────────────

@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="era-ingest",
    connection="EventHubConnection"
)
def ingest_era(event: func.EventHubEvent):
    """Process an ERA/835 file from the Event Hub."""
    payload = json.loads(event.get_body().decode("utf-8"))
    raw_edi = payload["raw_edi"]

    parser = EDI835Parser(raw_edi)
    remittances = parser.parse()

    conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])
    cursor = conn.cursor()

    for remit in remittances:
        # Dedup: check if this trace_number + payer already exists
        cursor.execute("""
            SELECT remit_id FROM remittances
            WHERE trace_number = ? AND payer_id = ?
        """, remit["trace_number"], remit["payer_id"])
        if cursor.fetchone():
            logging.info(f"Duplicate ERA skipped: trace={remit['trace_number']}")
            continue

        # Match to our canonical claim
        canonical_claim_id = match_claim_id(conn, remit["payer_claim_id"])
        if not canonical_claim_id:
            logging.warning(f"Unmatched claim: payer_claim_id={remit['payer_claim_id']}")
            _send_to_review_queue("unmatched_remittance", remit)
            continue

        # Write to OLTP
        cursor.execute("""
            INSERT INTO remittances
                (claim_id, trace_number, payer_id, era_date, paid_amount,
                 allowed_amount, adjustment_reason, denial_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, canonical_claim_id, remit["trace_number"], remit["payer_id"],
             remit["era_date"], remit["paid_amount"], remit["total_billed"],
             json.dumps(remit["adjustments"]),
             _extract_denial_code(remit["adjustments"]))

        conn.commit()

        # Emit downstream event
        _emit_event("remittance-events", {
            "type": "remittance.received",
            "claim_id": canonical_claim_id,
            "paid_amount": remit["paid_amount"],
            "timestamp": datetime.utcnow().isoformat()
        })

    conn.close()
```

---

### 5.3 Remittances — EOB (PDF)

| Attribute | Detail |
|---|---|
| **Source** | Payer portal download, email attachment, fax, manual upload |
| **Format** | PDF (scanned or digital) |
| **Velocity** | Daily, as received |
| **Ingestion pattern** | Event-driven (upload/email → Blob → Doc Intelligence → Azure Function) |
| **Target table** | `remittances` + `evidence_artifacts` |
| **History model** | Append-only |
| **Idempotency key** | `blob_hash` (SHA-256 of file content) |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Unstructured format** — every payer uses a different EOB layout, fonts, and terminology | No universal template; extraction accuracy varies wildly by payer | Critical |
| **Scanned vs. digital PDF** — scanned documents (fax, print-scan) have OCR noise; digital PDFs may have copy-protected text | Scanned: 85-95% OCR accuracy; digital: text extraction but layout parsing needed | High |
| **Key field extraction** — must extract claim number, paid amount, allowed amount, adjustment reason, patient responsibility, check number | Incorrect extraction means wrong payment data in the system | Critical |
| **Multi-page EOBs** — a single EOB may contain multiple claim payments across pages | Must correlate pages to the correct claim | Medium |
| **Handwritten annotations** — providers or staff sometimes annotate EOBs before uploading | Annotations can confuse OCR and field extraction | Low |

#### Resolution

| Challenge | Resolution |
|---|---|
| Variable payer layouts | Azure Doc Intelligence custom models: train one model per major payer (top 10 payers cover ~80% of volume). Fall back to a generic model for others. |
| Scanned vs. digital | Pre-processing pipeline: detect if PDF has embedded text (digital) or requires OCR (scanned). Route accordingly. Apply image enhancement (deskew, denoise) for scans. |
| Key field extraction | Define a standard extraction schema: `{claim_number, paid_amount, allowed_amount, adjustments[], patient_resp, check_number, date}`. Map per-payer model outputs to this schema. |
| Multi-page EOBs | Doc Intelligence layout analysis handles page segmentation. Group extracted fields by claim boundary markers (claim number changes). |
| Handwritten annotations | Filter low-confidence OCR regions. Annotations typically appear in margins — crop to main content area. |

#### Example Code — Azure Function: EOB PDF Ingestion

```python
# eob_ingestion.py — EOB PDF ingestion via Blob trigger + Doc Intelligence

import os
import hashlib
import json
import logging
import azure.functions as func
import pyodbc
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from datetime import datetime

app = func.FunctionApp()

# ── Doc Intelligence Client ──────────────────────────────────────────────

def get_doc_intel_client() -> DocumentIntelligenceClient:
    return DocumentIntelligenceClient(
        endpoint=os.environ["DOC_INTELLIGENCE_ENDPOINT"],
        credential=AzureKeyCredential(os.environ["DOC_INTELLIGENCE_KEY"])
    )


# ── Extraction Schema Normalizer ─────────────────────────────────────────

def normalize_eob_extraction(raw_fields: dict, payer_id: str | None) -> dict:
    """Map payer-specific Doc Intelligence model output to canonical schema."""
    # Standard schema — fields may come with different names per payer model
    field_mappings = {
        "claim_number": ["ClaimNumber", "Claim_No", "ClaimID", "claim_number"],
        "paid_amount": ["PaidAmount", "PaymentAmount", "AmountPaid", "paid_amount"],
        "allowed_amount": ["AllowedAmount", "Allowed", "allowed_amount"],
        "patient_responsibility": ["PatientResp", "PatientAmount", "patient_resp"],
        "check_number": ["CheckNumber", "Check_No", "EFTNumber", "check_number"],
        "service_date": ["ServiceDate", "DOS", "DateOfService", "service_date"],
        "adjustment_reason": ["AdjustmentReason", "RemarkCode", "DenialReason"],
    }

    normalized = {}
    for canonical_name, possible_keys in field_mappings.items():
        for key in possible_keys:
            if key in raw_fields and raw_fields[key].get("value") is not None:
                val = raw_fields[key]["value"]
                confidence = raw_fields[key].get("confidence", 0.0)
                normalized[canonical_name] = {"value": val, "confidence": confidence}
                break

    return normalized


# ── Blob Trigger: EOB PDF Uploaded ───────────────────────────────────────

@app.blob_trigger(
    arg_name="blob",
    path="eob-uploads/{name}",
    connection="BlobStorageConnection"
)
def ingest_eob_pdf(blob: func.InputStream):
    """Triggered when an EOB PDF is uploaded to the eob-uploads container."""
    pdf_bytes = blob.read()
    blob_name = blob.name
    logging.info(f"Processing EOB PDF: {blob_name}, size={len(pdf_bytes)} bytes")

    # Dedup: hash the file content
    content_hash = hashlib.sha256(pdf_bytes).hexdigest()
    conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])
    cursor = conn.cursor()

    cursor.execute(
        "SELECT artifact_id FROM evidence_artifacts WHERE content_hash = ?",
        content_hash
    )
    if cursor.fetchone():
        logging.info(f"Duplicate EOB skipped: hash={content_hash[:16]}...")
        conn.close()
        return

    # 1. Run Doc Intelligence — try payer-specific model first, then generic
    client = get_doc_intel_client()
    payer_model_id = _detect_payer_model(blob_name)  # from filename convention or metadata
    model_id = payer_model_id or "prebuilt-invoice"   # fallback to generic

    poller = client.begin_analyze_document(
        model_id=model_id,
        body=pdf_bytes,
        content_type="application/pdf"
    )
    result = poller.result()

    # 2. Extract and normalize fields
    raw_fields = {}
    for doc in result.documents:
        for field_name, field_value in doc.fields.items():
            raw_fields[field_name] = {
                "value": field_value.content,
                "confidence": field_value.confidence
            }

    normalized = normalize_eob_extraction(raw_fields, payer_model_id)

    # 3. Confidence check
    avg_confidence = 0.0
    if normalized:
        confidences = [f["confidence"] for f in normalized.values()]
        avg_confidence = sum(confidences) / len(confidences)

    # 4. Store evidence artifact
    cursor.execute("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_conf,
             ocr_status, content_hash, uploaded_date)
        VALUES (?, 'EOB', ?, ?, ?, ?, ?, ?)
    """, None,  # case_id linked later by workflow
         f"eob-uploads/{blob_name}",
         json.dumps(normalized, default=str),
         avg_confidence,
         "completed" if avg_confidence >= 0.9 else "needs_review",
         content_hash,
         datetime.utcnow())
    conn.commit()

    # 5. Route based on confidence
    if avg_confidence >= 0.9:
        # Auto-create remittance record
        claim_id = match_claim_id(conn, normalized.get("claim_number", {}).get("value"))
        if claim_id:
            cursor.execute("""
                INSERT INTO remittances (claim_id, era_date, paid_amount,
                                         allowed_amount, adjustment_reason, source)
                VALUES (?, ?, ?, ?, ?, 'eob_pdf')
            """, claim_id, datetime.utcnow(),
                 _safe_float(normalized.get("paid_amount", {}).get("value")),
                 _safe_float(normalized.get("allowed_amount", {}).get("value")),
                 normalized.get("adjustment_reason", {}).get("value"))
            conn.commit()

            _emit_event("remittance-events", {
                "type": "remittance.received",
                "claim_id": claim_id,
                "source": "eob_pdf",
                "confidence": avg_confidence
            })
        else:
            _send_to_review_queue("unmatched_eob", {"blob": blob_name, "fields": normalized})
    else:
        # Low confidence — route to human review
        logging.warning(f"Low confidence ({avg_confidence:.2f}) for {blob_name} — routing to review")
        _send_to_review_queue("low_confidence_eob", {
            "blob": blob_name,
            "fields": normalized,
            "avg_confidence": avg_confidence
        })

    conn.close()
```

---

### 5.4 Patient Data (EHR / PMS)

| Attribute | Detail |
|---|---|
| **Source** | Electronic Health Record / Practice Management System (e.g., Epic, Cerner, athenahealth) |
| **Format** | HL7 FHIR R4 (JSON) or HL7 v2 (pipe-delimited) |
| **Delivery** | FHIR webhook (real-time) or nightly batch pull |
| **Velocity** | On-change (webhook) or daily (batch) |
| **Ingestion pattern** | Event-driven (FHIR webhook → Azure Function) or Batch (ADF nightly) |
| **Target table** | `patients` |
| **History model** | SCD Type 1 (overwrite — demographics don't need history for arbitration) |
| **Idempotency key** | `patient_id` (FHIR Patient.id or MRN) |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **EHR vendor fragmentation** — Epic, Cerner, athenahealth all expose different FHIR implementations with varying conformance levels | Can't write one integration that works across all EHRs | High |
| **FHIR R4 conformance variability** — some EHRs support only a subset of FHIR resources; Patient resource may lack required fields | Missing insurance, address, or identifier data | High |
| **HL7 v2 legacy systems** — older EHRs still use HL7 v2 (ADT messages), not FHIR | Must maintain two parsers (FHIR + HL7v2) | Medium |
| **PHI sensitivity** — patient data is the most HIPAA-sensitive data in the system | Any logging, error message, or retry queue that leaks PHI is a compliance violation | Critical |
| **Webhook reliability** — EHR webhooks may not guarantee delivery; no standard retry mechanism | Stale patient data if webhooks are lost | Medium |
| **Patient matching / MPI** — patients may have multiple MRNs across systems | Duplicate patient records, fragmented case data | High |

#### Resolution

| Challenge | Resolution |
|---|---|
| EHR vendor fragmentation | Abstract behind a `PatientAdapter` interface. Implement per-vendor adapters (Epic, Cerner, etc.). Use SMART on FHIR for auth. |
| FHIR conformance variability | Define a minimum required field set (name, DOB, identifier). Accept partial data; flag records missing insurance for manual enrichment. |
| HL7 v2 legacy | Use `python-hl7` library for v2 parsing. Map ADT A01/A04/A08 messages to the same normalized patient schema. |
| PHI sensitivity | Encrypt all patient data at rest (Azure SQL TDE) and in transit (TLS). Redact PHI from all logs. Dedicated PHI-aware dead-letter queue (encrypted, auto-purge after 7 days). |
| Webhook reliability | Nightly batch reconciliation job: pull full patient roster from EHR FHIR API, compare against OLTP, upsert deltas. Webhook is fast-path; batch is safety net. |
| Patient matching | Normalize on MRN + DOB + last name. If EHR provides FHIR Patient.id, use as primary key. Flag potential duplicates for manual MPI review. |

#### Example Code — Azure Function: FHIR Webhook Handler

```python
# patient_ingestion.py — FHIR Patient webhook handler

import os
import json
import logging
import azure.functions as func
import pyodbc
from datetime import datetime

app = func.FunctionApp()

# ── FHIR Patient Normalizer ─────────────────────────────────────────────

def normalize_fhir_patient(fhir_resource: dict) -> dict:
    """Extract canonical patient fields from a FHIR Patient resource."""
    names = fhir_resource.get("name", [{}])
    official_name = next((n for n in names if n.get("use") == "official"), names[0])

    identifiers = fhir_resource.get("identifier", [])
    mrn = next(
        (i["value"] for i in identifiers
         if i.get("type", {}).get("coding", [{}])[0].get("code") == "MR"),
        fhir_resource.get("id")
    )

    addresses = fhir_resource.get("address", [{}])
    home_addr = next((a for a in addresses if a.get("use") == "home"), addresses[0] if addresses else {})

    insurance_id = None
    # Insurance may come via Coverage resource reference, not inline
    # For now, extract from extensions if present
    for ext in fhir_resource.get("extension", []):
        if "insurance" in ext.get("url", "").lower():
            insurance_id = ext.get("valueString")

    return {
        "patient_id": mrn,
        "first_name": " ".join(official_name.get("given", [])),
        "last_name": official_name.get("family", ""),
        "dob": fhir_resource.get("birthDate"),
        "gender": fhir_resource.get("gender"),
        "address_line": " ".join(home_addr.get("line", [])),
        "city": home_addr.get("city"),
        "state": home_addr.get("state"),
        "zip": home_addr.get("postalCode"),
        "insurance_id": insurance_id,
        "source_system": "fhir",
        "last_updated": datetime.utcnow().isoformat(),
    }


# ── Upsert (SCD Type 1 — Overwrite) ─────────────────────────────────────

def upsert_patient(conn: pyodbc.Connection, patient: dict):
    cursor = conn.cursor()
    cursor.execute("SELECT patient_id FROM patients WHERE patient_id = ?", patient["patient_id"])

    if cursor.fetchone():
        cursor.execute("""
            UPDATE patients
            SET first_name = ?, last_name = ?, dob = ?, gender = ?,
                address_line = ?, city = ?, state = ?, zip = ?,
                insurance_id = ?, last_updated = ?
            WHERE patient_id = ?
        """, patient["first_name"], patient["last_name"], patient["dob"],
             patient["gender"], patient["address_line"], patient["city"],
             patient["state"], patient["zip"], patient["insurance_id"],
             patient["last_updated"], patient["patient_id"])
    else:
        cursor.execute("""
            INSERT INTO patients (patient_id, first_name, last_name, dob, gender,
                                  address_line, city, state, zip, insurance_id, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, patient["patient_id"], patient["first_name"], patient["last_name"],
             patient["dob"], patient["gender"], patient["address_line"],
             patient["city"], patient["state"], patient["zip"],
             patient["insurance_id"], patient["last_updated"])

    conn.commit()

    # Audit log — note: NO PHI in audit log, only entity reference
    cursor.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, timestamp)
        VALUES ('patient', ?, 'upsert', ?)
    """, patient["patient_id"], datetime.utcnow())
    conn.commit()


# ── FHIR Webhook Endpoint ────────────────────────────────────────────────

@app.route(route="webhooks/fhir/patient", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def fhir_patient_webhook(req: func.HttpRequest) -> func.HttpResponse:
    """Receives FHIR Patient resource updates from EHR webhook."""
    try:
        body = req.get_json()

        # FHIR Subscription notification bundles wrap the resource
        if body.get("resourceType") == "Bundle":
            entries = body.get("entry", [])
            resources = [e["resource"] for e in entries if e.get("resource", {}).get("resourceType") == "Patient"]
        elif body.get("resourceType") == "Patient":
            resources = [body]
        else:
            return func.HttpResponse("Unsupported resource type", status_code=400)

        conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])

        for fhir_patient in resources:
            patient = normalize_fhir_patient(fhir_patient)

            if not patient["patient_id"] or not patient["dob"]:
                logging.warning("Patient missing required fields — skipping")
                continue

            upsert_patient(conn, patient)
            # PHI-safe log: only ID, no names or DOB
            logging.info(f"Patient upserted: id={patient['patient_id']}")

        conn.close()
        return func.HttpResponse("OK", status_code=200)

    except Exception as e:
        # CRITICAL: Do not log the request body (contains PHI)
        logging.error(f"FHIR webhook processing failed: {type(e).__name__}")
        return func.HttpResponse("Processing error", status_code=500)
```

---

### 5.5 Fee Schedules — Medicare / CMS

| Attribute | Detail |
|---|---|
| **Source** | CMS.gov (Physician Fee Schedule, OPPS, ASC payment files) |
| **Format** | CSV / Excel (published on CMS website) |
| **Velocity** | Annual (with quarterly updates) |
| **Ingestion pattern** | Batch (ADF scheduled pipeline) |
| **Target table** | `fee_schedule` (SCD Type 2) |
| **History model** | SCD Type 2 — critical for arbitration (must prove rate at time of service) |
| **Idempotency key** | `payer_id` + `cpt_code` + `modifier` + `geo_region` + `valid_from` |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **File format changes year-over-year** — CMS occasionally changes column names, adds/removes fields, or restructures the file | ADF pipeline breaks silently; wrong columns mapped | High |
| **Geographic rate variations** — Medicare rates vary by MAC (Medicare Administrative Contractor) region, locality, GPCI adjustments | Must correctly associate rates to geographic regions for benchmarking | High |
| **Multiple fee schedules** — Physician (PFS), Outpatient (OPPS), Ambulatory Surgery Center (ASC), Clinical Lab (CLFS) each have different structures | Can't use a single parser for all CMS rate files | Medium |
| **Retroactive corrections** — CMS issues mid-year corrections that change rates back to the start of the year | Must update SCD rows with corrected effective dates without losing the original record for audit | Medium |
| **Large file sizes** — the full PFS national file is 100K+ rows per year | Must process efficiently; staging table approach prevents partial loads from corrupting OLTP | Low |

#### Resolution

| Challenge | Resolution |
|---|---|
| Format changes | Schema validation step in ADF: compare incoming column headers against expected schema. Alert and pause pipeline if mismatch. Maintain a `cms_schema_version` config per file type. |
| Geographic variations | Parse the locality/MAC columns. Store `geo_region` in fee_schedule table. Join on provider's service location for correct rate lookup. |
| Multiple fee schedules | Separate ADF pipelines per CMS file type (PFS, OPPS, ASC, CLFS), each with its own column mapping. Common target: `fee_schedule` with `rate_type` discriminator. |
| Retroactive corrections | On correction load: close the original SCD row (`valid_to = correction_date - 1`). Insert corrected row with original `valid_from`. Insert new row if rate also changed going forward. Audit log the correction. |
| Large files | ADF bulk copy to staging table. SQL MERGE for SCD Type 2 in a single transaction. |

#### Example Code — ADF Pipeline + SQL SCD Type 2 Merge

```json
// ADF Pipeline Definition (conceptual JSON — key activities)
{
  "name": "pipeline_medicare_fee_schedule_load",
  "properties": {
    "activities": [
      {
        "name": "DownloadCMSFile",
        "type": "Copy",
        "inputs": [{ "referenceName": "CMS_PFS_HTTP_Source" }],
        "outputs": [{ "referenceName": "ADLS_Staging_CSV" }],
        "typeProperties": {
          "source": { "type": "HttpSource" },
          "sink": { "type": "AzureBlobFSSink" }
        }
      },
      {
        "name": "ValidateSchema",
        "type": "AzureFunctionActivity",
        "typeProperties": {
          "functionName": "validate_cms_schema",
          "method": "POST",
          "body": {
            "file_path": "@activity('DownloadCMSFile').output.fileName",
            "expected_schema": "pfs_2026"
          }
        }
      },
      {
        "name": "LoadToStaging",
        "type": "Copy",
        "dependsOn": [{ "activity": "ValidateSchema", "conditions": ["Succeeded"] }],
        "inputs": [{ "referenceName": "ADLS_Staging_CSV" }],
        "outputs": [{ "referenceName": "AzureSQL_FeeSchedule_Staging" }],
        "typeProperties": {
          "source": { "type": "DelimitedTextSource" },
          "sink": { "type": "AzureSqlSink", "preCopyScript": "TRUNCATE TABLE stg_fee_schedule" }
        }
      },
      {
        "name": "RunSCDMerge",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [{ "activity": "LoadToStaging", "conditions": ["Succeeded"] }],
        "typeProperties": {
          "storedProcedureName": "usp_merge_fee_schedule_scd2"
        }
      }
    ]
  }
}
```

```sql
-- usp_merge_fee_schedule_scd2.sql
-- SCD Type 2 merge: staging → fee_schedule

CREATE PROCEDURE usp_merge_fee_schedule_scd2
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    -- Step 1: Close existing rows where rate has changed
    UPDATE fs
    SET fs.valid_to = DATEADD(DAY, -1, stg.effective_date),
        fs.is_current = 0
    FROM fee_schedule fs
    INNER JOIN stg_fee_schedule stg
        ON  fs.payer_id   = 'MEDICARE'
        AND fs.cpt_code   = stg.cpt_code
        AND fs.modifier   = ISNULL(stg.modifier, '')
        AND fs.geo_region = stg.geo_region
    WHERE fs.is_current = 1
      AND fs.rate <> stg.rate;

    -- Step 2: Insert new/changed rows
    INSERT INTO fee_schedule
        (payer_id, cpt_code, modifier, geo_region, rate, rate_type,
         valid_from, valid_to, is_current, source, loaded_date)
    SELECT
        'MEDICARE',
        stg.cpt_code,
        ISNULL(stg.modifier, ''),
        stg.geo_region,
        stg.rate,
        stg.rate_type,           -- 'pfs', 'opps', 'asc', 'clfs'
        stg.effective_date,
        '9999-12-31',
        1,
        'CMS_PFS_' + CONVERT(VARCHAR, YEAR(stg.effective_date)),
        GETUTCDATE()
    FROM stg_fee_schedule stg
    WHERE NOT EXISTS (
        SELECT 1 FROM fee_schedule fs
        WHERE fs.payer_id   = 'MEDICARE'
          AND fs.cpt_code   = stg.cpt_code
          AND fs.modifier   = ISNULL(stg.modifier, '')
          AND fs.geo_region = stg.geo_region
          AND fs.is_current = 1
          AND fs.rate       = stg.rate
    );

    -- Step 3: Audit
    INSERT INTO audit_log (entity_type, entity_id, action, timestamp)
    VALUES ('fee_schedule', 'MEDICARE_BATCH', 'scd2_merge', GETUTCDATE());

    COMMIT TRANSACTION;
END;
```

---

### 5.6 Fee Schedules — FAIR Health

| Attribute | Detail |
|---|---|
| **Source** | FAIR Health Inc. (licensed data subscription) |
| **Format** | Licensed data files (CSV / proprietary format) |
| **Velocity** | Quarterly |
| **Ingestion pattern** | Batch (ADF) |
| **Target table** | `fee_schedule` (SCD Type 2, `rate_type = 'fair_health'`) |
| **History model** | SCD Type 2 |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Licensed data** — distribution restricted by contract; cannot store in shared/public locations | Must enforce access controls on the raw files and derived data | High |
| **Proprietary format** — delivery format may change with license renewal; schema is not publicly documented | Parser must be updated each time format changes | Medium |
| **Percentile-based rates** — FAIR Health provides percentile benchmarks (50th, 75th, 80th, 90th) not a single rate | Must store multiple rate tiers per CPT + geo; arbitration typically references 80th percentile | Medium |
| **Geo-granularity mismatch** — FAIR Health uses 3-digit ZIP prefixes; Medicare uses MAC localities | Must map between geo systems for apples-to-apples comparison | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Licensed data | Store raw files in a dedicated ADLS container with restricted Azure RBAC. Encrypt at rest. Audit all access. Include license expiry date in pipeline metadata — block loads after expiry. |
| Proprietary format | Versioned parser configs: `fair_health_schema_v{X}.json` mapping column positions/names. Schema validation step in ADF before load. |
| Percentile rates | Store each percentile as a separate row in `fee_schedule` with `rate_type = 'fair_health_p80'`, etc. Default arbitration lookup uses `fair_health_p80`. |
| Geo-granularity | Maintain a `geo_crosswalk` table mapping 3-digit ZIPs to Medicare localities. Populate from CMS locality files. Join during Silver layer transforms. |

---

### 5.7 Fee Schedules — Payer-Specific

| Attribute | Detail |
|---|---|
| **Source** | Individual payer fee schedule files (from contract negotiations, portals, email) |
| **Format** | Varies — CSV, Excel, PDF |
| **Velocity** | On receipt (irregular — when contracts are renegotiated) |
| **Ingestion pattern** | Batch (ADF) for CSV/Excel; Event-driven (Doc Intelligence) for PDF |
| **Target table** | `fee_schedule` (SCD Type 2, `rate_type = 'contracted'`) |
| **History model** | SCD Type 2 — must prove contracted rate at time of service |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **No standard format** — every payer delivers fee schedules in their own format, column naming, and structure | Must build per-payer column mappings | High |
| **PDF fee schedules** — some payers only provide fee schedules as PDF tables | Requires table extraction from PDF (harder than form extraction) | High |
| **Ambiguous effective dates** — some files don't state when rates take effect; arrived date != effective date | SCD Type 2 effective dates may be wrong, compromising arbitration evidence | Critical |
| **Partial updates** — payers may send only changed rates, not the full schedule | Must merge updates without losing unchanged rates | Medium |
| **Rate discrepancies vs. contracts** — published fee schedule may differ from actual contracted rates in the signed agreement | Data may not match what was legally agreed | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| No standard format | Maintain a `payer_schema_config` table: per-payer column mappings, header row, delimiter. ADF parameterized pipeline reads config at runtime. |
| PDF fee schedules | Route PDFs through Doc Intelligence with custom table extraction model. Extract as structured table → CSV → same ADF pipeline as CSV sources. Human review for first load per payer. |
| Ambiguous effective dates | Require effective date as mandatory metadata on upload. If missing, flag for analyst input before SCD merge proceeds. Never default to upload date. |
| Partial updates | SCD merge logic handles this: unchanged rates remain open (not closed). Only changed or new rates get new rows. |
| Rate discrepancies | Cross-reference against uploaded contract documents. Flag mismatches for analyst review. The `fee_schedule` stores the published rate; the `contracts` store the legal agreement — both are evidence. |

---

### 5.8 Provider / Facility Data (NPPES)

| Attribute | Detail |
|---|---|
| **Source** | NPPES (National Plan and Provider Enumeration System) — CMS |
| **Format** | CSV (monthly full file ~8GB; weekly incremental ~50MB) |
| **Velocity** | Monthly (full) + weekly (incremental) |
| **Ingestion pattern** | Batch (ADF scheduled pipeline) |
| **Target table** | `providers` |
| **History model** | SCD Type 2 (provider specialty, address, and affiliation changes matter for arbitration — out-of-network status depends on provider attributes at time of service) |
| **Idempotency key** | `npi` |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Very large full file** — ~8GB CSV with 7M+ NPIs nationally; only a small subset are relevant to this organization | Loading the entire file wastes time and storage | Medium |
| **Data quality issues** — NPPES data is self-reported by providers; specialty codes, addresses, and taxonomy may be inaccurate or outdated | Provider matching and out-of-network determination may be wrong | High |
| **NPI reactivation / deactivation** — NPIs can be deactivated and reactivated; status changes affect claim validity | Must track NPI status over time | Medium |
| **Multiple taxonomies per NPI** — a provider may have multiple specialty taxonomy codes (primary + secondary) | Must select the correct taxonomy for arbitration context (the specialty relevant to the service) | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Large file | Filter during ADF copy: only load NPIs present in `claims` or `providers` tables, plus a configurable state/taxonomy whitelist. Use ADF mapping data flow with filter transformation. |
| Data quality | Cross-reference with PECOS (Medicare enrollment data) where available. Flag providers whose NPPES specialty doesn't match claim specialty codes. |
| NPI status changes | Store `npi_status` and `deactivation_date` in `providers` table with SCD Type 2 tracking. |
| Multiple taxonomies | Store all taxonomies; mark primary. During arbitration, match taxonomy to the CPT code's relevant specialty via a `cpt_specialty_map` table. |

#### Example Code — ADF Pipeline for NPPES with Filtering

```python
# nppes_ingestion.py — Called by ADF as an Azure Function activity for filtering

import csv
import io
import logging
import azure.functions as func
import pyodbc

app = func.FunctionApp()

@app.route(route="ingest/nppes-filter", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def filter_nppes_file(req: func.HttpRequest) -> func.HttpResponse:
    """
    ADF calls this function after downloading the NPPES CSV to Blob.
    Filters to only relevant NPIs and writes to staging.
    """
    params = req.get_json()
    blob_path = params["blob_path"]
    state_whitelist = set(params.get("state_whitelist", []))  # e.g., ["CA", "TX", "NY"]

    conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])
    cursor = conn.cursor()

    # Get NPIs we already know about (from claims history)
    cursor.execute("SELECT DISTINCT provider_npi FROM claims")
    known_npis = {row[0] for row in cursor.fetchall()}

    # Read NPPES CSV from Blob (via SDK — simplified here)
    nppes_content = _read_blob(blob_path)  # returns string content
    reader = csv.DictReader(io.StringIO(nppes_content))

    staged_count = 0
    batch = []

    for row in reader:
        npi = row.get("NPI", "").strip()
        state = row.get("Provider Business Practice Location Address State Name", "").strip()
        entity_type = row.get("Entity Type Code", "")

        # Filter: only individuals (type 1) in our states or known to us
        if entity_type != "1":
            continue
        if npi not in known_npis and state not in state_whitelist:
            continue

        batch.append({
            "npi": npi,
            "name_first": row.get("Provider First Name", ""),
            "name_last": row.get("Provider Last Name (Legal Name)", ""),
            "specialty_taxonomy": row.get("Healthcare Provider Taxonomy Code_1", ""),
            "state": state,
            "city": row.get("Provider Business Practice Location Address City Name", ""),
            "zip": row.get("Provider Business Practice Location Address Postal Code", "")[:5],
            "status": row.get("NPI Deactivation Reason Code", "") or "active",
            "deactivation_date": row.get("NPI Deactivation Date") or None,
        })

        if len(batch) >= 1000:
            _bulk_insert_staging(cursor, batch)
            staged_count += len(batch)
            batch = []

    if batch:
        _bulk_insert_staging(cursor, batch)
        staged_count += len(batch)

    conn.commit()
    conn.close()

    logging.info(f"NPPES filter complete: {staged_count} providers staged")
    return func.HttpResponse(json.dumps({"staged_count": staged_count}), status_code=200)


def _bulk_insert_staging(cursor, batch: list[dict]):
    """Bulk insert into staging table."""
    cursor.executemany("""
        INSERT INTO stg_providers (npi, name_first, name_last, specialty_taxonomy,
                                    state, city, zip, status, deactivation_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(p["npi"], p["name_first"], p["name_last"], p["specialty_taxonomy"],
           p["state"], p["city"], p["zip"], p["status"], p["deactivation_date"])
          for p in batch])
```

```sql
-- usp_merge_providers_scd2.sql — called by ADF after staging load

CREATE PROCEDURE usp_merge_providers_scd2
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    -- Close rows where specialty or status changed
    UPDATE p
    SET p.valid_to = DATEADD(DAY, -1, GETUTCDATE()),
        p.is_current = 0
    FROM providers p
    INNER JOIN stg_providers stg ON p.npi = stg.npi
    WHERE p.is_current = 1
      AND (p.specialty_taxonomy <> stg.specialty_taxonomy
           OR p.status <> stg.status
           OR p.state <> stg.state);

    -- Insert new/changed rows
    INSERT INTO providers (npi, name_first, name_last, specialty_taxonomy,
                           state, city, zip, status, deactivation_date,
                           valid_from, valid_to, is_current)
    SELECT stg.npi, stg.name_first, stg.name_last, stg.specialty_taxonomy,
           stg.state, stg.city, stg.zip, stg.status, stg.deactivation_date,
           GETUTCDATE(), '9999-12-31', 1
    FROM stg_providers stg
    WHERE NOT EXISTS (
        SELECT 1 FROM providers p
        WHERE p.npi = stg.npi AND p.is_current = 1
          AND p.specialty_taxonomy = stg.specialty_taxonomy
          AND p.status = stg.status
          AND p.state = stg.state
    );

    COMMIT TRANSACTION;
END;
```

---

### 5.9 Contracts (Provider-Payer Agreements)

| Attribute | Detail |
|---|---|
| **Source** | Manual upload by analysts via Case Management UI |
| **Format** | PDF (scanned or digital), occasionally Word docs |
| **Velocity** | Infrequent (when contracts are signed or renegotiated) |
| **Ingestion pattern** | Event-driven (UI upload → Blob → Doc Intelligence → human review) |
| **Target table** | `evidence_artifacts` (the document) + `fee_schedule` (extracted rates, if parseable) |
| **History model** | SCD Type 2 + full document versioning in Blob |
| **Idempotency key** | `content_hash` (SHA-256) |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Contracts are complex legal documents** — rate tables are embedded in pages of legal text, amendments, exhibits, and schedules | Automated extraction is unreliable; rates buried in appendices | Critical |
| **Rate tables in various formats** — some contracts have structured tables; others describe rates in prose ("Provider shall be reimbursed at 150% of Medicare") | Must handle both tabular and narrative rate definitions | High |
| **Amendments and addenda** — contracts are modified over time; the current effective agreement is a stack of original + amendments | Must track which amendment is in effect for a given date of service | High |
| **Confidentiality** — contract terms are highly sensitive business information | Stricter access controls than other data; limited to authorized roles | High |
| **Low volume but high value** — each contract is critical for potentially thousands of arbitration cases | Errors have outsized impact; human review is mandatory | Critical |

#### Resolution

| Challenge | Resolution |
|---|---|
| Complex documents | Doc Intelligence for layout extraction + table detection. LLM (Claude) as a second pass to interpret extracted content and identify rate tables, effective dates, and termination clauses. Always human-verified. |
| Variable rate formats | Two extraction paths: (1) tabular → direct parse to fee_schedule staging, (2) narrative → LLM extracts structured rates from prose (e.g., "150% of Medicare" → `multiplier: 1.5, base: 'medicare'`). Human approval before commit. |
| Amendments | Store each document version with `version_number`, `effective_date`, `supersedes_artifact_id`. Build a contract timeline view in the Case Management UI. |
| Confidentiality | Azure RBAC: only `Manager`, `Director`, and `External Counsel` roles can view contracts. Blob container with separate access policy. Audit all access. |
| High value, low volume | Mandatory human review for every contract upload. No auto-commit to fee_schedule. Analyst reviews extracted data, confirms, then the system commits. |

#### Example Code — Contract Upload Handler

```python
# contract_ingestion.py — Contract upload from Case Management UI

import os
import hashlib
import json
import logging
import azure.functions as func
import pyodbc
from azure.storage.blob import BlobServiceClient
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from datetime import datetime

app = func.FunctionApp()

@app.route(route="ingest/contract", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def upload_contract(req: func.HttpRequest) -> func.HttpResponse:
    """Handle contract PDF upload from Case Management UI."""

    # Auth check — only Manager, Director, External Counsel
    user_role = req.headers.get("X-User-Role")
    if user_role not in ("manager", "director", "external_counsel"):
        return func.HttpResponse("Forbidden: insufficient role", status_code=403)

    file_bytes = req.get_body()
    metadata = json.loads(req.headers.get("X-Contract-Metadata", "{}"))
    payer_id = metadata.get("payer_id")
    effective_date = metadata.get("effective_date")
    provider_npi = metadata.get("provider_npi")
    user_id = req.headers.get("X-User-Id")

    if not payer_id or not effective_date:
        return func.HttpResponse("Missing payer_id or effective_date", status_code=400)

    # Dedup
    content_hash = hashlib.sha256(file_bytes).hexdigest()
    conn = pyodbc.connect(os.environ["AZURE_SQL_CONN_STRING"])
    cursor = conn.cursor()
    cursor.execute(
        "SELECT artifact_id FROM evidence_artifacts WHERE content_hash = ?",
        content_hash
    )
    if cursor.fetchone():
        conn.close()
        return func.HttpResponse(
            json.dumps({"status": "duplicate", "message": "This document has already been uploaded"}),
            status_code=409
        )

    # 1. Store in restricted Blob container
    blob_client = BlobServiceClient.from_connection_string(os.environ["BLOB_CONN_STRING"])
    container = blob_client.get_container_client("contracts-restricted")
    blob_name = f"{payer_id}/{effective_date}/{content_hash[:12]}.pdf"
    container.upload_blob(name=blob_name, data=file_bytes, overwrite=False)

    # 2. Run Doc Intelligence (table extraction)
    doc_client = DocumentIntelligenceClient(
        endpoint=os.environ["DOC_INTELLIGENCE_ENDPOINT"],
        credential=AzureKeyCredential(os.environ["DOC_INTELLIGENCE_KEY"])
    )
    poller = doc_client.begin_analyze_document(
        model_id="prebuilt-layout",  # layout model for table extraction
        body=file_bytes,
        content_type="application/pdf"
    )
    result = poller.result()

    # 3. Extract tables (if any)
    extracted_tables = []
    for table in result.tables:
        rows = []
        for cell in table.cells:
            while len(rows) <= cell.row_index:
                rows.append({})
            rows[cell.row_index][f"col_{cell.column_index}"] = cell.content
        extracted_tables.append(rows)

    # 4. Store artifact with status = needs_review (NEVER auto-commit contracts)
    cursor.execute("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_conf,
             ocr_status, content_hash, uploaded_date, uploaded_by)
        VALUES (?, 'contract', ?, ?, 0.0, 'needs_review', ?, ?, ?)
    """, None,
         f"contracts-restricted/{blob_name}",
         json.dumps({"tables": extracted_tables, "payer_id": payer_id,
                      "effective_date": effective_date, "provider_npi": provider_npi}),
         content_hash,
         datetime.utcnow(),
         user_id)
    conn.commit()

    # 5. Audit
    cursor.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, user_id, timestamp)
        VALUES ('contract', ?, 'upload', ?, ?)
    """, content_hash[:12], user_id, datetime.utcnow())
    conn.commit()
    conn.close()

    return func.HttpResponse(
        json.dumps({
            "status": "uploaded",
            "review_required": True,
            "tables_found": len(extracted_tables),
            "blob_url": f"contracts-restricted/{blob_name}"
        }),
        status_code=201
    )
```

---

### 5.10 Regulations & Rules

| Attribute | Detail |
|---|---|
| **Source** | CMS, state regulatory agencies, Federal Register |
| **Format** | PDF, HTML (regulatory text) |
| **Velocity** | When published (irregular) |
| **Ingestion pattern** | Manual upload → Blob → AI Search re-index |
| **Target** | Azure AI Search vector index (for RAG) + `evidence_artifacts` |
| **History model** | Versioned corpus (point-in-time regulatory snapshots) |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Regulatory text is dense and long** — a single rule may span 100+ pages of Federal Register text | Chunking strategy for RAG must preserve context without exceeding token limits | High |
| **State vs. federal overlap** — some states have their own surprise billing laws that preempt or supplement the NSA | Must correctly scope which regulations apply to a given case based on state | High |
| **Frequent updates with retroactive effect** — interim final rules, proposed rules, and final rules may coexist; effective dates shift | The RAG index must serve the version of the regulation in effect at a given date | Medium |
| **No machine-readable format** — regulations are published as prose, not structured data | Must rely on LLM understanding during RAG retrieval, not exact field matching | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Dense text | Chunk by section/subsection headers (natural boundaries). Overlap chunks by 200 tokens. Store section metadata (title, CFR citation) in each chunk for retrieval context. |
| State vs. federal | Tag each document with `jurisdiction` (federal, state code). During RAG query, filter by the case's state + always include federal. |
| Versioned regulations | Store each version with `effective_date` and `superseded_date`. AI Search index includes these as filterable fields. At query time, filter to regulations effective as of the claim's date of service. |
| Prose format | Azure AI Search with vector embeddings (text-embedding-ada-002 or newer). Hybrid search: vector similarity + keyword BM25. LLM synthesizes answer with citations. |

---

### 5.11 Document Uploads (Case Management UI)

| Attribute | Detail |
|---|---|
| **Source** | Analyst uploads via the Case Management UI |
| **Format** | PDF, TIFF, JPEG, Word, email (.eml/.msg) |
| **Velocity** | Real-time (user-initiated) |
| **Ingestion pattern** | Event-driven (UI upload → Blob → Doc Intelligence → Azure Function) |
| **Target table** | `evidence_artifacts` |
| **Idempotency key** | `content_hash` |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Diverse document types** — clinical records, correspondence, prior auth letters, payer denial letters, appeal submissions, etc. | Classification model must handle 8+ document types | High |
| **File format variety** — PDF, TIFF (fax), JPEG (photo of document), Word, email attachments | Must handle each format's extraction differently | Medium |
| **Linking to correct case** — documents must be associated with the right case; mislinks corrupt the evidence record | Analyst selects case during upload, but bulk uploads may be error-prone | Medium |
| **Large files** — clinical records can be 50+ pages | Processing time for OCR + classification may be long; must not block the UI | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Classification | Custom Doc Intelligence classification model trained on: EOB, clinical record, contract, payer correspondence, prior auth, appeal, arbitration decision, provider correspondence. Confidence threshold of 90% for auto-classify; below that → analyst confirms. |
| Format variety | Pre-processing step normalizes to PDF: TIFF/JPEG → PDF via image-to-pdf conversion. Word → PDF via Azure conversion. Email → extract attachments + body as PDF. Then all documents go through the same Doc Intelligence pipeline. |
| Case linking | Require `case_id` on upload. For bulk uploads, implement a drag-and-drop interface that groups documents by case. Post-upload validation: flag if document content suggests a different case (e.g., different claim number in extracted text). |
| Large files | Async processing: UI upload returns immediately with `processing` status. Blob trigger processes in background. WebSocket notification to UI when processing completes. |

---

### 5.12 Historical Claims Backfill

| Attribute | Detail |
|---|---|
| **Source** | Legacy systems, prior clearinghouse exports, internal databases |
| **Format** | EDI 837, CSV, SQL export |
| **Velocity** | One-time + periodic (migration batches) |
| **Ingestion pattern** | Batch (ADF) |
| **Target table** | `claims` + `claim_lines` |
| **History model** | Append-only with status column |

#### Challenges

| Challenge | Impact | Severity |
|---|---|---|
| **Data quality from legacy systems** — missing fields, inconsistent formats, no standard encoding | Historical claims may be incomplete or malformed | High |
| **Volume** — years of history; potentially millions of rows | Must load efficiently without impacting production OLTP performance | Medium |
| **Duplicate detection across sources** — the same claim may exist in multiple legacy systems | Must dedup across sources, not just within a single load | High |
| **No retroactive workflow triggers** — historical claims should not trigger the real-time workflow engine (e.g., deadline alerts for claims from 2023) | Ingesting historical data must not create false alerts | Medium |
| **Mapping legacy codes** — old systems may use deprecated CPT codes, non-standard payer IDs, or local identifiers | Must map or flag unmappable codes | Medium |

#### Resolution

| Challenge | Resolution |
|---|---|
| Data quality | Pre-load profiling: run data quality checks (null rates, value distributions, format validation) on the extract before loading. Accept with quality flags rather than reject — historical evidence has value even if incomplete. |
| Volume | Load during off-peak hours. Use ADF bulk copy with batch size = 10,000. Disable indexes on target table during load; rebuild after. |
| Cross-source dedup | Dedup key: `claim_id` + `date_of_service` + `provider_npi` + `total_billed`. If exact match exists, skip. If partial match, flag for review. |
| No false triggers | Backfill pipeline sets `status = 'historical'` and `source = 'backfill'`. Workflow engine filters on `source != 'backfill'` for real-time triggers. |
| Legacy code mapping | Maintain a `code_crosswalk` table: `{legacy_code, standard_code, source_system}`. Map during the ADF transform step. Unmappable codes stored as-is with a `mapping_status = 'unmapped'` flag. |

---

## 6. Cross-Cutting Concerns

### 6.1 Idempotency

Every ingestion path must be idempotent — processing the same message twice must not create duplicate records.

| Source | Idempotency Key | Dedup Location |
|---|---|---|
| Claims (837) | `claim_id` + `frequency_code` | OLTP `claims` table |
| ERA (835) | `trace_number` + `payer_id` | OLTP `remittances` table |
| EOB (PDF) | `content_hash` (SHA-256) | OLTP `evidence_artifacts` |
| Patient (FHIR) | `patient_id` (MRN) | OLTP `patients` (upsert) |
| Fee schedules | `payer_id + cpt + modifier + geo + valid_from` | SCD merge logic |
| Provider (NPPES) | `npi` | SCD merge logic |
| Contracts (PDF) | `content_hash` | OLTP `evidence_artifacts` |
| Document uploads | `content_hash` | OLTP `evidence_artifacts` |

### 6.2 Error Handling & Dead-Letter Queues

```
Failed message / unparseable file
    │
    ▼
Dead-Letter Queue (Service Bus DLQ)
    │
    ├── Metadata: source, timestamp, error reason, original payload reference
    │
    ├── Alert: DLQ depth > threshold → PagerDuty / Teams alert
    │
    ├── Retention: 7 days (PHI-containing messages encrypted, auto-purged)
    │
    └── Resolution: analyst reviews in DLQ dashboard → fix → resubmit
```

**Error categories and handling:**

| Category | Example | Action |
|---|---|---|
| **Parse failure** | Malformed EDI, corrupted PDF | DLQ + alert; do not retry automatically |
| **Validation failure** | Missing required field, impossible date | DLQ + log; analyst can fix and resubmit |
| **Transient failure** | Azure SQL timeout, Event Hub throttle | Auto-retry with exponential backoff (3 attempts) |
| **Unmatched reference** | Remittance references unknown claim_id | Review queue; analyst matches manually |
| **Duplicate** | Same claim/remittance reprocessed | Skip silently; log for metrics |

### 6.3 HIPAA & PHI Protection in the Ingestion Layer

| Control | Implementation |
|---|---|
| **Encryption at rest** | Azure SQL TDE; Blob Storage SSE; ADLS encryption |
| **Encryption in transit** | TLS 1.2+ for all connections (SFTP, Event Hub, Functions, SQL) |
| **PHI in logs** | NEVER log patient names, DOB, SSN, or medical details. Log only entity IDs and action types. |
| **PHI in error queues** | DLQ messages containing PHI are encrypted. Auto-purge after 7 days. Access restricted to authorized roles. |
| **Audit trail** | Every ingestion write → `audit_log` entry with entity_type, entity_id, action, user/system, timestamp |
| **Access control** | Azure Functions authenticate via managed identity. No hardcoded credentials. Key Vault for all secrets. |
| **BAA coverage** | All Azure services in the ingestion path must be covered by Microsoft's HIPAA BAA |

### 6.4 Schema Validation

Every ingestion path validates incoming data before writing to OLTP:

```python
# shared/validation.py — Reusable validation framework

from dataclasses import dataclass

@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]
    warnings: list[str]

def validate_claim(claim: dict) -> ValidationResult:
    errors, warnings = [], []

    # Required fields
    if not claim.get("claim_id"):
        errors.append("Missing claim_id")
    if not claim.get("date_of_service"):
        errors.append("Missing date_of_service")
    if not claim.get("total_billed") or claim["total_billed"] <= 0:
        errors.append(f"Invalid total_billed: {claim.get('total_billed')}")
    if not claim.get("lines"):
        errors.append("No service lines")

    # Warnings (accepted but flagged)
    if not claim.get("diagnosis_codes"):
        warnings.append("No diagnosis codes — may affect arbitration eligibility")
    if not claim.get("provider_npi"):
        warnings.append("Missing provider NPI — will need manual enrichment")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )

def validate_remittance(remit: dict) -> ValidationResult:
    errors, warnings = [], []

    if not remit.get("trace_number"):
        errors.append("Missing trace_number")
    if remit.get("paid_amount") is None:
        errors.append("Missing paid_amount")
    if not remit.get("payer_claim_id"):
        errors.append("Missing payer_claim_id (CLP01)")

    if not remit.get("adjustments"):
        warnings.append("No adjustment reason codes — cannot classify denial type")

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings
    )
```

---

## 7. Execution Strategy — How It All Runs

### 7.1 Azure Resource Topology

```
Resource Group: rg-medbill-ingestion
│
├── Event Hub Namespace: evhns-medbill
│   ├── Event Hub: claims-ingest        (partitions: 4)
│   ├── Event Hub: era-ingest           (partitions: 4)
│   ├── Event Hub: claim-events         (downstream triggers)
│   └── Event Hub: remittance-events    (downstream triggers)
│
├── Service Bus Namespace: sb-medbill
│   ├── Queue: eob-review              (human review queue)
│   ├── Queue: unmatched-remittances   (manual matching)
│   └── Queue: dead-letter             (DLQ)
│
├── Function App: func-medbill-ingest  (Consumption Plan — auto-scale)
│   ├── ingest_claim                   (Event Hub trigger: claims-ingest)
│   ├── ingest_era                     (Event Hub trigger: era-ingest)
│   ├── ingest_eob_pdf                 (Blob trigger: eob-uploads)
│   ├── fhir_patient_webhook           (HTTP trigger: POST /webhooks/fhir/patient)
│   ├── upload_contract                (HTTP trigger: POST /ingest/contract)
│   ├── poll_sftp_for_era              (Timer trigger: every 15 min)
│   └── filter_nppes_file              (HTTP trigger: called by ADF)
│
├── Storage Account: stmedbillingest
│   ├── Container: eob-uploads         (EOB PDFs)
│   ├── Container: contracts-restricted (contracts — restricted RBAC)
│   ├── Container: regulations         (regulatory docs)
│   ├── Container: staging             (ADF staging files)
│   └── Container: dead-letter-payloads (DLQ payload backup)
│
├── Azure Data Factory: adf-medbill
│   ├── Pipeline: pl_medicare_fee_schedule    (quarterly)
│   ├── Pipeline: pl_fair_health_rates        (quarterly)
│   ├── Pipeline: pl_payer_fee_schedules      (on-demand)
│   ├── Pipeline: pl_nppes_provider_load      (monthly)
│   ├── Pipeline: pl_historical_backfill      (one-time / ad-hoc)
│   └── Pipeline: pl_patient_batch_reconcile  (nightly — safety net)
│
├── Azure SQL Database: sqldb-medbill-oltp
│   ├── Staging tables: stg_fee_schedule, stg_providers, stg_claims_backfill
│   ├── Operational tables: claims, claim_lines, remittances, patients,
│   │                       providers, fee_schedule, disputes, cases,
│   │                       evidence_artifacts, deadlines, audit_log
│   ├── Reference tables: claim_id_alias, code_crosswalk, geo_crosswalk,
│   │                     payer_schema_config, carc_rarc_lookup
│   └── Stored procedures: usp_merge_fee_schedule_scd2, usp_merge_providers_scd2
│
├── Azure Doc Intelligence: doc-intel-medbill
│   ├── Custom model: eob-aetna, eob-unitedhealth, eob-cigna, ...
│   ├── Custom model: classification-model (8 document types)
│   └── Prebuilt: layout, invoice (fallback)
│
└── Key Vault: kv-medbill
    ├── AZURE-SQL-CONN-STRING
    ├── EVENT-HUB-CONNECTION
    ├── BLOB-CONN-STRING
    ├── DOC-INTELLIGENCE-KEY
    ├── SFTP-CONFIGS
    └── FHIR-CLIENT-CREDENTIALS
```

### 7.2 Execution Flow — Complete Lifecycle Example

Here is how a single claim flows through the entire ingestion layer, end-to-end:

```
1. Clearinghouse files an 837 via Event Hub
                │
                ▼
2. Event Hub: claims-ingest receives message (buffered, ordered by partition)
                │
                ▼
3. Azure Function: ingest_claim is triggered
                │
                ▼
4. EDI837Parser.parse() → normalized claim dict
                │
                ▼
5. validate_claim() → errors? → YES: send to DLQ, STOP
                │                 NO: continue
                ▼
6. resolve_duplicate() → check OLTP for existing claim_id
                │
                ├── "skip" (duplicate original) → log, STOP
                ├── "update" (replacement) → UPDATE claims SET ...
                ├── "void" → UPDATE claims SET status = 'voided'
                └── "insert" (new) → INSERT INTO claims + claim_lines
                                │
                                ▼
7. INSERT INTO audit_log (entity_type='claim', action='insert', ...)
                │
                ▼
8. Emit to Event Hub: claim-events → {"type": "claim.insert", "claim_id": "..."}
                │
                ▼
9. (Downstream — outside ingestion layer)
   Workflow Engine picks up claim.insert event → starts Claim-to-Dispute pipeline
   CDC picks up the INSERT → propagates to Lakehouse Bronze → Silver → Gold
   Power BI refreshes via Direct Lake
```

### 7.3 Scheduling Summary

| Pipeline / Function | Trigger Type | Schedule / Event | Timeout |
|---|---|---|---|
| `ingest_claim` | Event Hub | On message arrival | 5 min |
| `ingest_era` | Event Hub | On message arrival | 5 min |
| `ingest_eob_pdf` | Blob trigger | On file upload | 10 min |
| `fhir_patient_webhook` | HTTP POST | On EHR webhook | 30 sec |
| `upload_contract` | HTTP POST | On UI upload | 2 min |
| `poll_sftp_for_era` | Timer | Every 15 minutes | 5 min |
| `pl_medicare_fee_schedule` | ADF Schedule | Quarterly (Jan, Apr, Jul, Oct 1st) | 2 hours |
| `pl_fair_health_rates` | ADF Schedule | Quarterly (aligned with FAIR Health releases) | 1 hour |
| `pl_payer_fee_schedules` | ADF Manual | On receipt of payer files | 1 hour |
| `pl_nppes_provider_load` | ADF Schedule | Monthly (1st of month) | 3 hours |
| `pl_historical_backfill` | ADF Manual | Ad-hoc | 8 hours |
| `pl_patient_batch_reconcile` | ADF Schedule | Nightly at 2:00 AM UTC | 1 hour |

---

## 8. Monitoring & Observability

### 8.1 Metrics Dashboard (Azure Monitor + Log Analytics)

| Metric | Source | Alert Threshold |
|---|---|---|
| Claims ingested / hour | Function App logs | < 10 during business hours (indicates feed down) |
| ERA files processed / day | Function App logs | 0 for 24h (indicates SFTP or payer issue) |
| DLQ depth | Service Bus metrics | > 50 messages |
| Parse failure rate | Custom metric | > 5% of messages in any 1h window |
| Unmatched remittances | Custom metric | > 10% of ERA records |
| Doc Intelligence confidence (avg) | Custom metric | < 85% average over 1 day |
| ADF pipeline failures | ADF monitoring | Any failure |
| SFTP connectivity | Timer function logs | Any payer unreachable for > 1h |
| End-to-end latency (event → OLTP) | Custom metric | > 5 min (event-driven), > 4h (batch) |

### 8.2 Alerting

```
Metric exceeds threshold
    │
    ▼
Azure Monitor Alert Rule fires
    │
    ├── P1 (DLQ depth, parse failure rate, SFTP down) → PagerDuty + Teams
    ├── P2 (unmatched remittances, low confidence) → Teams
    └── P3 (ADF pipeline failure, volume anomaly) → Email
```

---

## 9. Infrastructure as Code — Deployment

### 9.1 Bicep Template (Key Resources)

```bicep
// infra/ingestion.bicep — Ingestion layer infrastructure

param location string = resourceGroup().location
param environment string = 'prod'

// ── Event Hub Namespace ─────────────────────────────────────────────────

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2023-01-01-preview' = {
  name: 'evhns-medbill-${environment}'
  location: location
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }

  resource claimsHub 'eventhubs' = {
    name: 'claims-ingest'
    properties: { partitionCount: 4, messageRetentionInDays: 7 }
  }

  resource eraHub 'eventhubs' = {
    name: 'era-ingest'
    properties: { partitionCount: 4, messageRetentionInDays: 7 }
  }

  resource claimEventsHub 'eventhubs' = {
    name: 'claim-events'
    properties: { partitionCount: 4, messageRetentionInDays: 3 }
  }

  resource remittanceEventsHub 'eventhubs' = {
    name: 'remittance-events'
    properties: { partitionCount: 4, messageRetentionInDays: 3 }
  }
}

// ── Function App ────────────────────────────────────────────────────────

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: 'func-medbill-ingest-${environment}'
  location: location
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    siteConfig: {
      pythonVersion: '3.11'
      appSettings: [
        { name: 'AZURE_SQL_CONN_STRING', value: '@Microsoft.KeyVault(SecretUri=${keyVault::sqlConnString.properties.secretUri})' }
        { name: 'EventHubConnection', value: '@Microsoft.KeyVault(SecretUri=${keyVault::eventHubConn.properties.secretUri})' }
        { name: 'BlobStorageConnection', value: '@Microsoft.KeyVault(SecretUri=${keyVault::blobConn.properties.secretUri})' }
        { name: 'DOC_INTELLIGENCE_ENDPOINT', value: docIntelligence.properties.endpoint }
        { name: 'DOC_INTELLIGENCE_KEY', value: '@Microsoft.KeyVault(SecretUri=${keyVault::docIntelKey.properties.secretUri})' }
      ]
    }
  }
}

// ── Storage Account ─────────────────────────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: 'stmedbillingest${environment}'
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    encryption: {
      services: { blob: { enabled: true }, file: { enabled: true } }
      keySource: 'Microsoft.Storage'
    }
  }
}

// ── Doc Intelligence ────────────────────────────────────────────────────

resource docIntelligence 'Microsoft.CognitiveServices/accounts@2023-10-01-preview' = {
  name: 'doc-intel-medbill-${environment}'
  location: location
  sku: { name: 'S0' }
  kind: 'FormRecognizer'
  properties: { publicNetworkAccess: 'Disabled' }
}

// ── Key Vault ───────────────────────────────────────────────────────────

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'kv-medbill-${environment}'
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
  }
}
```

### 9.2 Deployment Command

```bash
# Deploy ingestion layer infrastructure
az deployment group create \
  --resource-group rg-medbill-ingestion \
  --template-file infra/ingestion.bicep \
  --parameters environment=prod

# Deploy Function App code
cd functions/
func azure functionapp publish func-medbill-ingest-prod --python
```

---

## Appendix: Source-to-Target Mapping Summary

| # | Source | Format | Pattern | Trigger | Target Table | History | Key Challenge |
|---|---|---|---|---|---|---|---|
| 1 | Claims (Clearinghouse) | EDI 837 | Event-driven | Event Hub | `claims` + `claim_lines` | Append | EDI parsing + dedup |
| 2 | ERA (Payer) | EDI 835 | Event-driven | SFTP → Event Hub | `remittances` | Append | Claim ID matching |
| 3 | EOB (Payer) | PDF | Event-driven | Blob trigger | `remittances` + `evidence_artifacts` | Append | OCR accuracy + payer layout variation |
| 4 | Patient (EHR) | FHIR JSON | Event-driven | HTTP webhook | `patients` | SCD1 | EHR vendor fragmentation + PHI |
| 5 | Medicare fees (CMS) | CSV/Excel | Batch | ADF quarterly | `fee_schedule` | SCD2 | Format changes + geo mapping |
| 6 | FAIR Health | Licensed files | Batch | ADF quarterly | `fee_schedule` | SCD2 | Licensing + percentile rates |
| 7 | Payer fees | CSV/Excel/PDF | Batch | ADF on-demand | `fee_schedule` | SCD2 | No standard format |
| 8 | Provider (NPPES) | CSV (8GB) | Batch | ADF monthly | `providers` | SCD2 | File size + data quality |
| 9 | Contracts | PDF | Event-driven | UI upload | `evidence_artifacts` | SCD2 + versioned | Complex docs, always human-reviewed |
| 10 | Regulations | PDF/HTML | Manual | UI upload | AI Search index | Versioned | Dense text + state/federal overlap |
| 11 | Document uploads | PDF/TIFF/etc. | Event-driven | UI upload | `evidence_artifacts` | Append | Classification accuracy |
| 12 | Historical backfill | EDI 837/CSV | Batch | ADF ad-hoc | `claims` + `claim_lines` | Append | Legacy data quality + no false triggers |
