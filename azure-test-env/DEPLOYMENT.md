# Medical Billing Arbitration — Full Cloud Deployment Guide

> **Environment:** Test (`rg-medbill-test`)
> **Tenant:** `yedaaillcgmail.onmicrosoft.com`
> **Last deployed:** March 2026

---

## Overview

This guide covers deploying the complete Medical Billing Arbitration system to Azure. The architecture consists of 7 layers:

```
1. Azure Resources (SQL, ADLS, Event Hubs, Functions, ADF, App Service)
2. OLTP Schema + Seed Data (Azure SQL)
3. Ingestion Functions (Event Hub / Blob / HTTP triggers)
4. ADF Pipelines (CDC, batch loads, master orchestrator)
5. Fabric Lakehouse (Bronze → Silver → Gold notebooks)
6. Durable Functions (Workflow engine — deadline monitoring)
7. Power BI (Direct Lake from Gold tables)
```

---

## Prerequisites

```bash
# Install tools
brew install azure-cli
brew install microsoft/mssql-release/mssql-tools18
brew install azure-functions-core-tools@4

# Login
az login
az account set --subscription <your-subscription-id>
```

---

## Step 1: Provision Azure Resources

```bash
cd azure-test-env/scripts
chmod +x *.sh
./provision.sh
```

Creates (idempotent — safe to re-run):
- Azure SQL Database (free tier) — `medbill-sql-*`
- ADLS Gen2 Storage — `medbillstore*` (Bronze/Silver/Gold containers)
- Event Hubs (4) — claims, remittances, documents, status-changes
- Azure Functions App — `medbill-func-*`
- Azure AI Search (free tier) — `medbill-search-*`
- Document Intelligence (free tier) — `medbill-docintel-*`
- App Service (free F1 tier) — `medbill-api-*`
- Azure Data Factory — `medbill-adf`

Connection details saved to `.env` (git-ignored).

---

## Step 2: Create Schema + Load Sample Data

```bash
./run_sql.sh
```

Creates OLTP tables and loads synthetic data:
- 8 payers, 6 providers, 10 patients
- 30 fee schedule rates (SCD Type 2)
- 10 claims, 8 remittances, 6 cases, 6 disputes
- 14 deadline entries, 10 evidence artifacts
- CDC watermark tracking table + stored procedures

---

## Step 3: Deploy Ingestion Functions

```bash
./deploy_functions.sh
```

Deploys 6 Azure Function triggers:
| Trigger | Type | Source |
|---|---|---|
| Claims | Event Hub | EDI 837 |
| Remittances | Event Hub | EDI 835 |
| Documents | Blob Storage | PDFs via Doc Intelligence |
| Patients | HTTP POST | FHIR R4 webhook |
| Fee Schedules | Timer (daily 6AM) | CSV from Bronze container |
| EOB Processing | Event Hub | Doc Intelligence results |

Test with:
```bash
cd ../functions/sample-events
python simulate.py --all
```

---

## Step 4: Deploy ADF Pipelines

```bash
./deploy_adf.sh
```

Deploys to `medbill-adf`:

| Component | Count | Details |
|---|---|---|
| Linked Services | 2 | ADLS Gen2 (managed identity), Azure SQL |
| Datasets | 4 | CDC source, Bronze Parquet, Bronze CSV, SQL staging |
| Pipelines | 3 | CDC incremental copy, fee schedule batch, provider batch |
| Triggers | 2 | CDC every 15 min, fee schedule daily 6AM (start in STOPPED state) |

**Note:** `pl_master_orchestrator` must be configured manually in ADF portal after Fabric is set up (see Step 5b).

Start triggers when ready:
```bash
az datafactory trigger start --factory-name medbill-adf \
  --resource-group rg-medbill-test --trigger-name trg_cdc_15min
az datafactory trigger start --factory-name medbill-adf \
  --resource-group rg-medbill-test --trigger-name trg_fee_schedule_daily
```

Run CDC pipeline manually:
```bash
az datafactory pipeline create-run --factory-name medbill-adf \
  --resource-group rg-medbill-test --pipeline-name pl_cdc_incremental_copy
```

---

## Step 5: Set Up Fabric Lakehouse

> Detailed guide: [FABRIC_SETUP.md](FABRIC_SETUP.md)

### 5a. One-time Fabric account setup

1. **Create Entra work account:**
   Azure Portal → Entra ID → Users → + New user
   - UPN: `admin@<tenant>.onmicrosoft.com`
   - Role: Global Administrator

2. **Enable Fabric settings** (Admin portal → Tenant settings):
   - "Users can create Fabric items" → Enabled
   - "Users can try Microsoft Fabric paid features" → Enabled

3. **Activate trial:**
   Profile icon (top-right) → "Start Fabric trial"

4. **Grant storage access:**
   ```bash
   ./setup_fabric.sh
   ```

### 5b. Create workspace + Lakehouse + notebooks

1. Fabric Home → **New workspace** → `medbill-test-lakehouse` (Fabric Trial, Small semantic model)
2. Inside workspace → **+ New item** → **Lakehouse** → `medbill_lakehouse`
3. Workspace level → **+ New item** → **Import notebook** → upload 3 `.ipynb` files:
   ```
   fabric-notebooks/nb_bronze_cdc.ipynb
   fabric-notebooks/nb_silver_transforms.ipynb
   fabric-notebooks/nb_gold_aggregations.ipynb
   ```
   To regenerate `.ipynb` from `.py` sources: `python3 scripts/convert_notebooks.py`

4. Open each notebook → **Add data items** → **From OneLake catalog** → select `medbill_lakehouse`

5. In `nb_bronze_cdc`, update storage account:
   ```python
   BRONZE_ADLS_PATH = "abfss://bronze@medbillstoreb29b302f.dfs.core.windows.net"
   ```

6. Run in order: `nb_bronze_cdc` → `nb_silver_transforms` → `nb_gold_aggregations`

### 5c. Connect ADF master orchestrator to Fabric

In Azure Portal → Data Factory → `medbill-adf` → Author & Monitor:
1. Create a **linked service** to Fabric workspace
2. Edit `pl_master_orchestrator` → configure notebook activities to reference Fabric notebooks
3. This chains: CDC copy → Bronze NB → Silver NB → Gold NB on a schedule

---

## Step 6: Deploy Workflow Engine (Durable Functions)

The workflow engine handles automated underpayment detection and NSA deadline monitoring.

Files already exist in `functions/workflow/`:
- `orchestrator.py` — 3 orchestrators (claim_to_dispute, deadline_monitor, case_transition)
- `activities.py` — 14 activity functions
- `deadline_monitor.py` — Timer trigger (every 6 hours) + HTTP triggers

Deploy with the ingestion functions (same Function App):
```bash
./deploy_functions.sh
```

---

## Step 6b: Deploy Gold Views + AI Analyst Agent

### Create Gold views in Azure SQL

```bash
# Run the Gold views SQL against your database
sqlcmd -S medbill-sql-214f9d00.database.windows.net -d medbill_oltp \
  -U medbilladmin -P "$SQL_PASSWORD" -i ../sql/gold_views.sql
```

Creates 8 Gold views that mirror the Parquet-based Gold layer:
`gold_recovery_by_payer`, `gold_cpt_analysis`, `gold_payer_scorecard`,
`gold_financial_summary`, `gold_claims_aging`, `gold_case_pipeline`,
`gold_deadline_compliance`, `gold_underpayment_detection`

### Configure Claude API key

```bash
az functionapp config appsettings set \
  --name medbill-func-8df6df9c \
  --resource-group rg-medbill-test \
  --settings "ANTHROPIC_API_KEY=<your-anthropic-api-key>"
```

Optional: override the default model (defaults to `claude-sonnet-4-20250514`):
```bash
az functionapp config appsettings set \
  --name medbill-func-8df6df9c \
  --resource-group rg-medbill-test \
  --settings "CLAUDE_MODEL=claude-sonnet-4-20250514"
```

### Deploy (included in deploy_functions.sh)

The agent deploys with all other functions:
```bash
./deploy_functions.sh
```

### Test the agent

```bash
# Free-form question
curl -X POST "https://medbill-func-8df6df9c.azurewebsites.net/api/agent/ask?code=<function-key>" \
  -H "Content-Type: application/json" \
  -d '{"question": "Which payer has the highest underpayment rate?"}'

# List all common pre-built analyses (10 available)
curl "https://medbill-func-8df6df9c.azurewebsites.net/api/agent/common?code=<function-key>"

# Run a specific common analysis
curl -X POST "https://medbill-func-8df6df9c.azurewebsites.net/api/agent/common/executive_summary?code=<function-key>"
curl -X POST "https://medbill-func-8df6df9c.azurewebsites.net/api/agent/common/worst_payers?code=<function-key>"
curl -X POST "https://medbill-func-8df6df9c.azurewebsites.net/api/agent/common/arbitration_ready?code=<function-key>"
```

Response includes: `answer` (analysis text), `sql` (generated query), `data` (raw results), `suggested_analyses` (3 recommended follow-ups).

### Agent Endpoints

| Method | Route | Auth | Purpose |
|---|---|---|---|
| GET | `/api/agent/ui` | Anonymous | Web UI — chat interface |
| POST | `/api/agent/ask` | Function key | Free-form natural language question |
| GET | `/api/agent/common` | Function key | List 10 pre-built common analyses |
| POST | `/api/agent/common/{id}` | Function key | Run a pre-built analysis by ID |

### Access the Web UI

Open in browser:
```
https://medbill-func-8df6df9c.azurewebsites.net/api/agent/ui?code=<function-key>
```

The UI auto-detects the API base URL. Pass `?code=<key>` to authenticate API calls.

### Common Analyses (10 pre-built)

| ID | Name |
|---|---|
| `executive_summary` | Financial KPIs overview |
| `worst_payers` | Payers ranked by underpayment + denial rate |
| `arbitration_ready` | Claims eligible for IDR arbitration |
| `cpt_underpayment` | CPT codes vs Medicare/FAIR Health benchmarks |
| `deadline_risk` | At-risk/missed NSA regulatory deadlines |
| `case_pipeline` | Cases by status + SLA compliance |
| `aging_analysis` | Claims by aging bucket |
| `payer_comparison` | Side-by-side payer scorecards with risk tiers |
| `recovery_opportunity` | Total recovery potential estimate |
| `denial_patterns` | Denial rate patterns by payer |

---

## Step 6c: Run Tests & Verify

### Unit tests (local, no Azure needed)

```bash
cd azure-test-env
python3 -m pytest tests/test_agent.py tests/test_gold_views.py -v
```

| Test File | Tests | Coverage |
|---|---|---|
| `tests/test_agent.py` | 35 | SQL safety guards (13), common analyses catalog (5), ask_common validation (4), suggested analyses (3), mocked Claude flow (4), Gold schema (2), UI file (3), new analyses (1) |
| `tests/test_gold_views.py` | 36 | Gold view queries against seed data in SQLite: 8 original views (22) + win/loss (3), analyst productivity (3), time-to-resolution (2), provider performance (2), monthly trends (2), cross-view consistency (3) |
| `tests/test_e2e_pipeline.py` | 32 | Full E2E: OLTP integrity (7), financial consistency (5), Gold view chain (7), agent response structure (3), workflow state machine (7), cross-layer data flow (3) |
| `tests/test_olap_gold.py` | 18 | OLAP Gold functions with mocked Silver: 8 original agg functions (5) + 5 new business metrics (9) + aggregate_all_gold pipeline (2) + fixture validation (2) |

### E2E verification (requires deployed Function App)

```bash
cd scripts
./verify_agent.sh <function-app-name> <function-key>
```

Checks 7 endpoints: health, UI HTML, common analyses list, free-form question, common analysis run, invalid ID handling, bad request handling.

---

## Step 7: Connect Power BI

### Option A: Direct Lake (recommended — requires Fabric)

1. In Fabric, open `medbill_lakehouse`
2. Click **New semantic model** → select all `gold_*` tables
3. Power BI Desktop → **Get Data** → **OneLake data hub** → select semantic model
4. Import DAX measures from `powerbi/dax_measures.dax`
5. Build 5-page report using layout in `powerbi/report_template.json`

### Option B: Azure SQL Direct Query (without Fabric)

1. Power BI Desktop → **Get Data** → **Azure SQL Database**
2. Server: `medbill-sql-214f9d00.database.windows.net`
3. Database: `medbill_oltp`
4. Import the 8 Gold views (or point to ADLS Parquet files)

### Report Pages (5)

| Page | Data Source | Key Visuals |
|---|---|---|
| Executive Summary | financial_summary, recovery_by_payer, claims_aging | KPI cards, recovery bar chart, aging donut |
| Payer Analysis | payer_scorecard | Risk matrix, scatter plot, conditional formatting |
| CPT Code Analysis | cpt_analysis, fee_schedule | Payment ratio heatmap, Medicare comparison |
| Arbitration Pipeline | case_pipeline, underpayment_detection | Funnel chart, SLA gauges, eligibility table |
| Deadline Compliance | deadline_compliance | Stacked bar, at-risk alerts, compliance % |

---

## Current Deployment Status

| Component | Status | Resource Name |
|---|---|---|
| Resource Group | Done | `rg-medbill-test` |
| Azure SQL + schema + data | Done | `medbill-sql-214f9d00` |
| ADLS Gen2 (Bronze/Silver/Gold) | Done | `medbillstoreb29b302f` |
| Event Hubs (4) | Done | `medbill-ehub-*` |
| Ingestion Functions (6) | Done | `medbill-func-8df6df9c` |
| AI Search | Provisioned | `medbill-search-651dafbb` |
| Document Intelligence | Provisioned | `medbill-docintel-f689c7ca` |
| App Service | Provisioned | `medbill-api-686b8e0e` |
| ADF — Linked Services (2) | Done | `ls_medbill_adls`, `ls_medbill_azure_sql` |
| ADF — Datasets (4) | Done | CDC source, Bronze Parquet, Bronze CSV, SQL staging |
| ADF — CDC Pipeline | Done | `pl_cdc_incremental_copy` |
| ADF — Fee Schedule Pipeline | Done | `pl_batch_fee_schedule` |
| ADF — Provider Pipeline | Done | `pl_batch_providers` |
| ADF — Master Orchestrator | Manual | Needs Fabric notebook link in ADF portal |
| ADF — Triggers (2) | Done (stopped) | `trg_cdc_15min`, `trg_fee_schedule_daily` |
| Fabric Workspace | Done | `medbill-test-lakehouse` |
| Fabric Lakehouse | Done | `medbill_lakehouse` |
| Fabric Notebooks (3) | Done | Bronze, Silver, Gold imported |
| ADLS Permissions (Fabric) | Done | Storage Blob Data Contributor |
| ADLS Permissions (ADF) | Done | Managed identity role |
| Durable Functions (Workflow) | Not deployed | Code exists, needs deploy via `deploy_functions.sh` |
| Power BI `.pbix` Report | Not created | DAX measures + template ready |
| Gold SQL Views (13) | Code ready | `sql/gold_views.sql` — run via `run_sql.sh` |
| AI Analyst Agent (Claude) | Code ready | `functions/agent/analyst.py` — deploys with Functions |
| AI Agent Web UI | Code ready | `GET /api/agent/ui` — deploys with Functions |
| Gold SQL Views (13) | Code ready | `sql/gold_views.sql` — 8 original + 5 business success views |
| AI Agent (15 common analyses) | Code ready | `functions/agent/analyst.py` — 15 pre-built analyses |
| Power BI Report (8 pages) | Template ready | 5 original + Financial Forecast + Operational Efficiency + AI Performance |
| Tests (121 total) | All passing | Agent (35) + Gold views (36) + E2E pipeline (32) + OLAP Gold (18) |
| AI Agent E2E Script | Ready | `scripts/verify_agent.sh` — run post-deploy |

---

## Teardown

```bash
cd scripts
./teardown.sh
```

Deletes the entire `rg-medbill-test` resource group and all resources within it.

---

## Cost Estimate

| Service | Tier | Monthly Cost |
|---|---|---|
| Azure SQL | Free tier (32GB) | $0 |
| ADLS Gen2 | Pay-per-use | ~$1 |
| Event Hubs | Basic | ~$11 |
| Functions | Consumption | ~$0 |
| AI Search | Free tier | $0 |
| Doc Intelligence | Free tier | $0 |
| App Service | F1 Free | $0 |
| Data Factory | Pay-per-activity | ~$2-5 |
| Fabric | Trial (60 days) | $0 |
| **Total** | | **~$15-20/month** |
