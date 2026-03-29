# Medical Billing Arbitration — Azure Test Environment

Test environment for the architecture described in `medical_billing_arbitration_future_architecture.md`.

Estimated cost: **~$15-20/month** (mostly Event Hubs Basic tier). Most services use free tiers.

## What's In This Environment

| Layer | Components | Status |
|---|---|---|
| **Ingestion** | 6 Azure Function triggers (Event Hub, Blob, Timer, HTTP) | Deployed |
| **OLTP** | Azure SQL — 13 tables, 3 staging tables, 3 stored procedures | Deployed |
| **OLAP** | 13 Gold views (SQL) + 13 Gold Parquet aggregations | Code ready |
| **Lakehouse** | ADF pipelines (CDC, batch) + Fabric notebooks (Bronze/Silver/Gold) | Deployed |
| **Workflow** | 3 Durable Function orchestrators, 14 activities, NSA state machine | Code ready |
| **AI Agent** | Claude API text-to-SQL analyst, 15 pre-built analyses, web chat UI | Code ready |
| **BI** | 8 Power BI report pages, 44 DAX measures, report template | Template ready |
| **Tests** | 121 passing (agent, Gold views, E2E pipeline, OLAP Gold) + E2E script | Passing |

## Prerequisites

```bash
brew install azure-cli
brew install microsoft/mssql-release/mssql-tools18
brew install azure-functions-core-tools@4

az login
az account set --subscription <your-subscription-id>
```

## Setup

### Step 1: Provision Azure resources

```bash
cd azure-test-env/scripts
chmod +x *.sh
./provision.sh
```

Creates: Azure SQL (free), ADLS Gen2, Event Hubs (4), Azure Functions App, AI Search, Document Intelligence, App Service. Connection details saved to `.env`.

### Step 2: Create schema + load sample data

```bash
./run_sql.sh
```

Creates 13 OLTP tables + 3 staging tables + 3 stored procedures. Loads synthetic seed data: 8 payers, 6 providers, 10 patients, 30 fee schedule rates, 10 claims, 8 remittances, 6 cases, 6 disputes, 14 deadlines.

### Step 3: Create Gold views

```bash
sqlcmd -S <server> -d medbill_oltp -U medbilladmin -P "$SQL_PASSWORD" -i ../sql/gold_views.sql
```

Creates 13 Gold views for the AI agent and Power BI (live queries against OLTP).

### Step 4: Deploy functions (Ingestion + Workflow + AI Agent)

Add your Anthropic API key to `.env`:
```bash
echo "ANTHROPIC_API_KEY=sk-ant-..." >> ../.env
```

Deploy:
```bash
./deploy_functions.sh
```

Deploys 16 Azure Functions: 6 ingestion triggers, 3 workflow orchestrators + 14 activities, AI agent endpoints, OLAP pipeline, health check.

### Step 5: Deploy ADF pipelines

```bash
./deploy_adf.sh
```

Deploys CDC incremental copy, fee schedule batch, provider batch, and master orchestrator pipelines.

### Step 6: Run tests

```bash
cd azure-test-env
python3 -m pytest tests/ -v
```

121 tests covering agent, Gold views, E2E pipeline, and OLAP Gold aggregations.

### Step 7: Open the AI Agent UI

```
https://<function-app>.azurewebsites.net/api/agent/ui?code=<function-key>
```

### Step 8: Connect Power BI

See [DEPLOYMENT.md](DEPLOYMENT.md) Step 7 for Direct Lake or Azure SQL options.

## AI Analyst Agent

Natural language queries against 13 Gold layer views via Claude API.

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/agent/ui` | GET | Web chat UI (anonymous) |
| `/api/agent/ask` | POST | Free-form question → SQL → analysis |
| `/api/agent/common` | GET | List 15 pre-built analyses |
| `/api/agent/common/{id}` | POST | Run a pre-built analysis |

**15 Pre-built Analyses:** Executive Summary, Worst Payers, Arbitration-Ready Claims, CPT Underpayment, Deadline Risk, Case Pipeline, Claims Aging, Payer Comparison, Recovery Opportunity, Denial Patterns, Win/Loss & ROI, Analyst Productivity, Resolution Time, Provider Performance, Monthly Trends.

## Testing

| Test File | Tests | Scope |
|---|---|---|
| `tests/test_agent.py` | 35 | SQL safety, catalog, mocked Claude API, UI |
| `tests/test_gold_views.py` | 36 | 13 Gold views against seed data (SQLite) |
| `tests/test_e2e_pipeline.py` | 32 | OLTP → Gold → Agent → Workflow pipeline |
| `tests/test_olap_gold.py` | 18 | 13 OLAP Gold aggregation functions |
| `scripts/verify_agent.sh` | 7 | Live endpoint checks (post-deploy) |

## Folder Structure

```
azure-test-env/
├── README.md
├── DEPLOYMENT.md                 # Full deployment guide + status
├── ARCHITECTURE.md               # Architecture diagrams + codebase reference
├── FABRIC_SETUP.md               # Fabric Lakehouse setup guide
├── .env                          # Connection details (git-ignored)
├── scripts/
│   ├── provision.sh              # Create all Azure resources
│   ├── run_sql.sh                # Run schema + seed data
│   ├── deploy_functions.sh       # Deploy functions (ingestion + workflow + agent)
│   ├── deploy_adf.sh             # Deploy ADF pipelines
│   ├── setup_fabric.sh           # Configure Fabric permissions
│   ├── verify_agent.sh           # E2E verification of agent endpoints
│   ├── convert_notebooks.py      # .py → .ipynb for Fabric
│   └── teardown.sh               # Delete everything
├── sql/
│   ├── schema.sql                # 13 OLTP tables
│   ├── staging_tables.sql        # 3 staging tables + 3 stored procedures
│   ├── seed_data.sql             # Synthetic sample data
│   └── gold_views.sql            # 13 Gold views (AI agent + Power BI)
├── functions/                    # Azure Functions project
│   ├── function_app.py           # Main entry — 16 triggers/endpoints
│   ├── host.json                 # Functions runtime config
│   ├── local.settings.json       # Local dev settings template
│   ├── requirements.txt          # Python deps (incl. anthropic)
│   ├── agent/                    # AI Analyst Agent
│   │   ├── analyst.py            # Claude API text-to-SQL + 15 common analyses
│   │   └── ui.html               # Web chat UI (dark-themed SPA)
│   ├── workflow/                 # Durable Functions workflow engine
│   │   ├── orchestrator.py       # 3 orchestrators (claim→dispute, deadline, case transition)
│   │   ├── activities.py         # 14 activity functions
│   │   └── deadline_monitor.py   # Timer + HTTP triggers
│   ├── olap/                     # Medallion pipeline (pandas + ADLS Parquet)
│   │   ├── lake.py               # ADLS Gen2 read/write
│   │   ├── bronze.py             # CDC extraction
│   │   ├── silver.py             # Transforms + dedup
│   │   └── gold.py               # 13 Gold aggregation functions
│   ├── ingest/                   # Ingestion modules
│   │   ├── claims.py             # EDI 837 → Azure SQL
│   │   ├── remittances.py        # EDI 835 + EOB → Azure SQL
│   │   ├── documents.py          # Blob → Doc Intelligence → Azure SQL
│   │   ├── patients.py           # FHIR → Azure SQL
│   │   └── fee_schedules.py      # CSV → SCD Type 2 merge
│   ├── parsers/                  # Format parsers (pure logic)
│   ├── validators/               # Validation rules
│   ├── shared/                   # Cloud infrastructure (db, events, audit, dedup, dlq)
│   └── sample-events/
│       └── simulate.py           # Send test data through pipeline
├── adf/                          # ADF pipeline definitions (JSON)
├── fabric-notebooks/             # Fabric Lakehouse notebooks (.py + .ipynb)
├── powerbi/
│   ├── dax_measures.dax          # 44 DAX measures
│   ├── report_template.json      # 8-page report layout (13 data tables)
│   ├── dashboard.html            # HTML preview
│   └── README.md                 # Power BI setup instructions
└── tests/
    ├── test_agent.py             # Agent unit tests (35)
    ├── test_gold_views.py        # Gold view tests (36)
    ├── test_e2e_pipeline.py      # E2E pipeline tests (32)
    └── test_olap_gold.py         # OLAP Gold function tests (18)
```

## Teardown

```bash
cd scripts
./teardown.sh
```

Deletes the entire `rg-medbill-test` resource group and all resources.

## Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) — Step-by-step deployment guide + current status
- [ARCHITECTURE.md](ARCHITECTURE.md) — System architecture diagrams + codebase reference
- [EVENT_HUBS.md](EVENT_HUBS.md) — Azure Event Hubs deep dive (vs Kafka, partitions, tiers, code)
- [FABRIC_SETUP.md](FABRIC_SETUP.md) — Microsoft Fabric Lakehouse setup
- [powerbi/README.md](powerbi/README.md) — Power BI connection guide
