# ADLS Gen2 / OneLake / Lakehouse — Deep Dive

> Part of the [Architecture Reference](ARCHITECTURE.md) for the Medical Billing Arbitration platform.

## What Are These Three Things?

They're often confused because they overlap. Here's the distinction:

| Component | What It Is | Analogy |
|---|---|---|
| **ADLS Gen2** | Azure Data Lake Storage Gen2 — a storage account optimized for big data. Stores files (Parquet, CSV, JSON) in containers. | A hard drive with folders |
| **OneLake** | Microsoft Fabric's unified storage layer. All Fabric workspaces share one OneLake. It can mount ADLS Gen2 as a shortcut. | A network drive that connects to your hard drive |
| **Lakehouse** | A Fabric item that combines file storage (OneLake) with SQL analytics. You get tables (Delta Lake) + files + SQL endpoint + notebooks — all in one. | A database built on top of the network drive |

```
┌─────────────────────────────────────────────────────────────────────┐
│ ADLS Gen2 (medbillstoreb29b302f)                                    │
│ Pure file storage — Parquet files in containers                     │
│                                                                     │
│  bronze/                       silver/                  gold/       │
│  ├── claims/                   (written by Functions    (written by │
│  │   └── year=2026/            OLAP pipeline or         Functions   │
│  │       └── month=03/         Fabric notebooks)        or Fabric)  │
│  │           └── day=30/                                            │
│  │               └── claims_20260330.parquet                        │
│  ├── remittances/                                                   │
│  ├── cases/                                                         │
│  └── ...                                                            │
│                                                                     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           │ OneLake shortcut (Fabric mounts ADLS automatically)
                           │
┌──────────────────────────▼──────────────────────────────────────────┐
│ Fabric Lakehouse (medbill_lakehouse)                                │
│ Adds Delta tables + SQL endpoint + notebooks on top of files        │
│                                                                     │
│  Tables/                                                            │
│  ├── bronze/                   ← Delta Lake (from nb_bronze_cdc)    │
│  │   ├── claims                                                     │
│  │   ├── remittances                                                │
│  │   └── ...                                                        │
│  ├── silver/                   ← Delta Lake (from nb_silver)        │
│  │   ├── silver_claims                                              │
│  │   ├── silver_claim_remittance                                    │
│  │   └── ...                                                        │
│  └── gold/                     ← Delta Lake (from nb_gold)          │
│      ├── gold_recovery_by_payer                                     │
│      ├── gold_financial_summary                                     │
│      └── ...                                                        │
│                                                                     │
│  SQL Endpoint:                 ← Query Delta tables with SQL        │
│  Power BI Direct Lake:         ← Read Delta tables with zero copy   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Both ADLS and Lakehouse?

| Question | Answer |
|---|---|
| Can I use just ADLS without Fabric? | **Yes** — the Functions OLAP pipeline writes Parquet directly to ADLS. Power BI can read Parquet from ADLS. |
| Can I use just Fabric without ADLS? | **No** — Fabric needs a storage layer. OneLake IS storage (backed by ADLS under the hood). |
| Why do we have both? | ADLS is the CDC landing zone (ADF writes here). Fabric Lakehouse adds Delta Lake, SQL queries, and Direct Lake for Power BI. |
| Which is cheaper? | ADLS alone (~$1/mo). Fabric adds trial capacity (free 60 days) or paid capacity units. |

---

## Medallion Architecture (Bronze → Silver → Gold)

A 3-layer data quality pattern. Each layer refines the data:

```
Raw CDC events       Cleaned, joined,        Aggregated for BI
(append-only)        deduplicated             (pre-computed metrics)
      │                    │                        │
      ▼                    ▼                        ▼
  ┌────────┐         ┌──────────┐            ┌──────────┐
  │ BRONZE │  ────►  │  SILVER  │  ────────► │   GOLD   │
  │        │         │          │            │          │
  │ Parquet│         │ Delta    │            │ Delta    │
  │ append │         │ MERGE    │            │ OVERWRITE│
  └────────┘         └──────────┘            └──────────┘
```

| Layer | Format | Write Strategy | Contents | Size |
|---|---|---|---|---|
| **Bronze** | Parquet (ADLS) → Delta (Fabric) | Append-only | Raw CDC events with metadata | Grows over time |
| **Silver** | Delta Lake | MERGE (upsert on PK) | Current-state, cleaned, joined | 1 row per entity |
| **Gold** | Delta Lake | Full OVERWRITE | Pre-aggregated metrics | Hundreds of rows |

### Why Three Layers?

```
Bronze only:   "Show me all claims"  → need to deduplicate CDC events manually
Silver only:   "Show me recovery rate by payer"  → need to compute aggregation in every query
Gold:          "Show me recovery rate by payer"  → SELECT * FROM gold_recovery_by_payer (instant)
```

---

## Bronze Layer

### What It Does

Reads raw CDC Parquet files from ADLS and appends them to Delta Lake tables in Fabric.
No transformations — just adds metadata columns.

### Notebook: `nb_bronze_cdc`

**File:** `fabric-notebooks/nb_bronze_cdc.py`

```python
# Configuration
BRONZE_ADLS_PATH = "abfss://bronze@medbillstoreb29b302f.dfs.core.windows.net"
LAKEHOUSE_BRONZE = "Tables/bronze"

# Tables to process (must match ADF CDC pipeline)
CDC_TABLES = [
    {"name": "claims",       "pk": "claim_id"},
    {"name": "claim_lines",  "pk": "line_id"},
    {"name": "remittances",  "pk": "remit_id"},
    {"name": "patients",     "pk": "patient_id"},
    {"name": "cases",        "pk": "case_id"},
    {"name": "disputes",     "pk": "dispute_id"},
    {"name": "deadlines",    "pk": "deadline_id"},
    {"name": "evidence_artifacts", "pk": "artifact_id"},
    {"name": "audit_log",    "pk": "log_id"},
]
```

**What it adds to each row:**

| Column | Value | Purpose |
|---|---|---|
| `_cdc_operation` | 'I' / 'U' / 'D' | Already in CDC Parquet (Insert/Update/Delete) |
| `_cdc_timestamp` | ISO timestamp | When the CDC pipeline extracted this row |
| `_source_table` | Table name | Which OLTP table this came from |
| `_bronze_loaded_ts` | `current_timestamp()` | When this notebook processed it |
| `_source_file` | `input_file_name()` | Which Parquet file it came from |

**Input:** `bronze/{table}/year=YYYY/month=MM/day=DD/*.parquet` (ADLS Gen2)
**Output:** `Tables/bronze/{table}` (Delta Lake in Fabric Lakehouse)

### How to Run

1. Fabric portal → workspace `medbill-test-lakehouse`
2. Open `nb_bronze_cdc`
3. Update storage account name if needed:
   ```python
   BRONZE_ADLS_PATH = "abfss://bronze@medbillstoreb29b302f.dfs.core.windows.net"
   ```
4. Click **Run all**

---

## Silver Layer

### What It Does

Resolves CDC events to current-state rows, cleans data, joins related tables.
The key operation is **CDC resolution** — converting an append-only event log into a
point-in-time snapshot.

### CDC Resolution Logic

```python
# For each table:
# 1. Read all Bronze rows (multiple CDC events per entity)
# 2. Window: partition by PK, order by _cdc_timestamp DESC
# 3. Take row_number = 1 (most recent event per entity)
# 4. Exclude _cdc_operation = 'D' (deleted in source)

# Example: claim_id=1 has 3 Bronze rows:
#   _cdc_timestamp=10:00  _cdc_operation='I'  status='filed'        ← original insert
#   _cdc_timestamp=14:00  _cdc_operation='U'  status='underpaid'    ← updated
#   _cdc_timestamp=16:00  _cdc_operation='U'  status='in_dispute'   ← latest

# After CDC resolution: 1 Silver row with status='in_dispute'
```

### Notebook: `nb_silver_transforms`

**File:** `fabric-notebooks/nb_silver_transforms.py`

**9 Silver Tables:**

| Silver Table | Source | Transform | Key Joins |
|---|---|---|---|
| `silver_claims` | bronze/claims + claim_lines | Denormalize lines into claim | CPT codes, diagnosis aggregated |
| `silver_remittances` | bronze/remittances | Add payer names | JOIN payers |
| `silver_claim_remittance` | silver_claims + silver_remittances | Underpayment calculation | LEFT JOIN on claim_id |
| `silver_fee_schedule` | bronze/fee_schedule | Keep all SCD2 rows (historical + current) | No join — preserve history |
| `silver_providers` | bronze/providers | Current records only (is_current=1) | — |
| `silver_patients` | bronze/patients | Clean demographics | — |
| `silver_disputes` | bronze/disputes | Add claim and payer context | JOIN claims, payers |
| `silver_cases` | bronze/cases | Add dispute count, financials | LEFT JOIN disputes |
| `silver_deadlines` | bronze/deadlines | Add SLA compliance tracking | JOIN cases |

**Write strategy:** `MERGE INTO` (upsert on primary key) — idempotent, safe to re-run.

### How to Run

1. Fabric portal → open `nb_silver_transforms`
2. Click **Run all** (after Bronze notebook completes)

---

## Gold Layer

### What It Does

Computes pre-aggregated business metrics from Silver tables. These are the tables
that Power BI reads via Direct Lake — no further joins or calculations needed.

### Notebook: `nb_gold_aggregations`

**File:** `fabric-notebooks/nb_gold_aggregations.py`

**13 Gold Tables** (8 original + 5 business success metrics):

| # | Gold Table | Silver Source | Key Metrics |
|---|---|---|---|
| 1 | `gold_recovery_by_payer` | claim_remittance | Billed, paid, underpayment, recovery %, denial % per payer |
| 2 | `gold_cpt_analysis` | claim_remittance, claims, fee_schedule | Payment ratio, Medicare/FAIR Health benchmarks per CPT |
| 3 | `gold_payer_scorecard` | claim_remittance | Payment rate, denial rate, risk tier per payer |
| 4 | `gold_financial_summary` | claim_remittance | 10 KPIs as key-value rows |
| 5 | `gold_claims_aging` | claim_remittance | Aging buckets (0-30, 31-60, 61-90, 91-180, 180+) |
| 6 | `gold_case_pipeline` | cases, deadlines | Case count, underpayment, SLA compliance per status |
| 7 | `gold_deadline_compliance` | deadlines | Met/missed/pending/at-risk per deadline type |
| 8 | `gold_underpayment_detection` | disputes | Per-claim QPA analysis, arbitration eligibility |
| 9 | `gold_win_loss_analysis` | disputes, cases | Outcomes by payer, dispute type, ROI |
| 10 | `gold_analyst_productivity` | cases, disputes | Cases per analyst, win rate, resolution time |
| 11 | `gold_time_to_resolution` | cases, disputes | Cycle time by status and priority |
| 12 | `gold_provider_performance` | claim_remittance, disputes | Provider billing, disputes, recovery |
| 13 | `gold_monthly_trends` | claim_remittance, disputes, cases | Monthly volume and financial trends |

**Write strategy:** Full `OVERWRITE` on every run (small tables, simpler than incremental).

### How to Run

1. Fabric portal → open `nb_gold_aggregations`
2. Click **Run all** (after Silver notebook completes)

---

## Two Parallel Paths to Gold

You have **two independent systems** that produce Gold data:

```
Path 1: Fabric Notebooks (for Power BI Direct Lake)
──────────────────────────────────────────────────
  ADF CDC → ADLS Bronze Parquet
    → nb_bronze_cdc (Fabric) → Bronze Delta
    → nb_silver_transforms   → Silver Delta
    → nb_gold_aggregations   → Gold Delta tables
    → Power BI Direct Lake (reads Delta natively)

Path 2: Azure Functions (for AI Agent + SQL queries)
──────────────────────────────────────────────────
  Functions OLAP timer (every 4h) or POST /api/olap/run
    → olap/bronze.py   → ADLS Bronze Parquet
    → olap/silver.py   → ADLS Silver Parquet
    → olap/gold.py     → ADLS Gold Parquet
    → (Power BI can also read these Parquet files)

Path 3: Gold SQL Views (for AI Agent — default, always live)
──────────────────────────────────────────────────
  No pipeline needed. Views query OLTP directly.
    → sql/gold_views.sql → 13 views in Azure SQL
    → AI Agent queries these in real-time
    → Set GOLD_DATA_SOURCE=azure_sql (default)

Path 4: Fabric SQL Endpoint (for AI Agent — isolated from OLTP)
──────────────────────────────────────────────────
  Requires Gold Delta tables in Fabric Lakehouse.
    → AI Agent queries Gold Delta tables via Fabric SQL endpoint
    → Set GOLD_DATA_SOURCE=fabric
    → Set FABRIC_SQL_CONNECTION_STRING=<endpoint>
```

| Path | Latency | Used By | Requires | Env Var |
|---|---|---|---|---|
| Fabric notebooks | 15-30 min (after CDC) | Power BI Direct Lake | Fabric workspace + capacity | — |
| Functions OLAP | 4 hours (timer) | Power BI (Parquet) | Function App running | — |
| Gold SQL views | **Real-time** | **AI Agent (default)** | Nothing — always live | `GOLD_DATA_SOURCE=azure_sql` |
| Fabric SQL endpoint | 15-30 min (after notebooks) | **AI Agent (production)** | Fabric + Gold notebooks run | `GOLD_DATA_SOURCE=fabric` |

---

## ADLS Container Layout

```
Storage Account: medbillstoreb29b302f
│
├── bronze/                              ← CDC Parquet from ADF
│   ├── claims/
│   │   └── year=2026/month=03/day=30/
│   │       └── claims_20260330001457.parquet
│   ├── remittances/
│   ├── cases/
│   ├── disputes/
│   ├── deadlines/
│   ├── patients/
│   ├── claim_lines/
│   ├── evidence_artifacts/
│   ├── audit_log/
│   └── fee-schedules/                   ← CSV uploads (batch fee schedule pipeline)
│       ├── 1_rates_2026.csv             ← waiting to be processed
│       └── archive/2026-03-29/          ← processed files moved here
│
├── silver/                              ← Functions OLAP output (Parquet)
│   ├── claims/current.parquet
│   ├── claim_remittance/current.parquet
│   └── ...
│
├── gold/                                ← Functions OLAP output (Parquet)
│   ├── recovery_by_payer/current.parquet
│   ├── financial_summary/current.parquet
│   └── ...
│
└── documents/                           ← Uploaded PDFs (Blob trigger)
    ├── eob_anthem_2025.pdf
    └── clinical_record_001.pdf
```

---

## Code Reference Summary

| File | Layer | Runtime | Purpose |
|---|---|---|---|
| `fabric-notebooks/nb_bronze_cdc.py` | Bronze | Fabric (PySpark) | CDC Parquet → Delta Lake (append-only) |
| `fabric-notebooks/nb_silver_transforms.py` | Silver | Fabric (PySpark) | CDC resolution + clean + join → Delta (MERGE) |
| `fabric-notebooks/nb_gold_aggregations.py` | Gold | Fabric (PySpark) | Aggregate Silver → Gold Delta (OVERWRITE) |
| `functions/olap/bronze.py` | Bronze | Azure Functions (pandas) | CDC OLTP → ADLS Parquet |
| `functions/olap/silver.py` | Silver | Azure Functions (pandas) | Bronze Parquet → Silver Parquet |
| `functions/olap/gold.py` | Gold | Azure Functions (pandas) | Silver Parquet → Gold Parquet (13 functions) |
| `functions/olap/lake.py` | Shared | Azure Functions | ADLS Gen2 read/write helpers |
| `sql/gold_views.sql` | Gold | Azure SQL | 13 live SQL views (AI Agent) |
| `adf/pipelines/pl_cdc_incremental_copy.json` | CDC | ADF | OLTP → Bronze Parquet (every 15 min) |
| `adf/pipelines/pl_master_orchestrator.json` | All | ADF | Chain: CDC → Bronze NB → Silver NB → Gold NB |
| `scripts/provision.sh:107` | Setup | Bash | Creates ADLS storage account + 4 containers |

---

## Delta Lake vs Parquet

| Feature | Parquet (ADLS) | Delta Lake (Fabric Lakehouse) |
|---|---|---|
| **Format** | Columnar file | Parquet + transaction log (JSON) |
| **ACID** | No | Yes |
| **MERGE/upsert** | No (overwrite only) | Yes |
| **Time travel** | No | Yes (query any past version) |
| **Schema evolution** | No | Yes |
| **Direct Lake** | No (need import) | Yes (zero-copy to Power BI) |
| **SQL queries** | Via external tools | Built-in SQL endpoint in Fabric |
| **Used by** | Functions OLAP, ADF | Fabric notebooks |

**Why we have both:** Functions OLAP writes plain Parquet (doesn't need Fabric). Fabric notebooks write Delta Lake (adds ACID + Direct Lake). Both produce the same Gold metrics — choose based on whether you're using Fabric or not.
