# Medical Billing Arbitration — Azure Test Environment

Test environment for the architecture described in `medical_billing_arbitration_future_architecture.md`.

Estimated cost: **~$15-20/month** (mostly Event Hubs Basic tier). Most services use free tiers.

## Prerequisites

1. **Azure CLI** installed:
   ```bash
   brew install azure-cli
   ```

2. **sqlcmd** installed (for running SQL scripts):
   ```bash
   brew install microsoft/mssql-release/mssql-tools18
   ```

3. **Azure Functions Core Tools** (for local testing and deployment):
   ```bash
   brew install azure-functions-core-tools@4
   ```

4. **Azure account** with active subscription:
   ```bash
   az login
   az account set --subscription <your-subscription-id>
   ```

## Setup (5 steps)

### Step 1: Provision Azure resources

```bash
cd azure-test-env/scripts
chmod +x *.sh
./provision.sh
```

This creates:
- Azure SQL Database (free tier)
- Storage Account with ADLS Gen2 (Bronze/Silver/Gold containers)
- Event Hubs (claims, remittances, documents, status-changes)
- Azure Functions App (consumption/free)
- Azure AI Search (free tier)
- Document Intelligence (free tier)
- App Service for FastAPI (free F1 tier)

Connection details are saved to `.env` (git-ignored).

### Step 2: Create schema and load sample data

```bash
./run_sql.sh
```

This creates all OLTP tables and loads synthetic sample data:
- 8 payers, 6 providers, 10 patients
- 30 fee schedule rates with SCD Type 2 history
- 10 claims, 8 remittances, 6 cases, 6 disputes
- 14 deadline entries, 10 evidence artifacts
- Dead-letter queue and claim alias tables

### Step 3: Deploy ingestion functions

```bash
./deploy_functions.sh
```

This deploys the ingestion layer (migrated from `../ingestion/`) as Azure Functions:
- **Claims** — Event Hub trigger, parses EDI 837
- **Remittances** — Event Hub trigger, parses EDI 835
- **Documents** — Blob trigger, classifies via Doc Intelligence
- **Patients** — HTTP trigger (FHIR webhook)
- **Fee Schedules** — Timer trigger (daily batch from Bronze container)
- **EOB Processing** — Event Hub trigger for Doc Intelligence results
- **Health Check** — HTTP GET at `/api/health`

### Step 4: Test the ingestion pipeline

```bash
cd ../functions/sample-events
pip install azure-eventhub azure-storage-blob python-dotenv
python simulate.py --all
```

This sends the same sample data from `../ingestion/sample_data/` through the cloud pipeline:
- EDI 837 claims → Event Hub → Azure Function → Azure SQL
- EDI 835 remittances → Event Hub → Azure Function → Azure SQL
- FHIR patients → HTTP POST → Azure Function → Azure SQL
- Documents → Blob Storage → Azure Function → Azure SQL
- Fee schedules → Bronze container → Timer trigger → Azure SQL (SCD Type 2)

You can also test individual pipelines:
```bash
python simulate.py --claims
python simulate.py --remittances
python simulate.py --patients
python simulate.py --documents
python simulate.py --fee-schedules
```

### Step 5: Connect Power BI Desktop

1. Open Power BI Desktop (free download from Microsoft)
2. Get Data > Azure SQL Database
3. Server: `<value from .env: SQL_SERVER>`
4. Database: `medbill_oltp`
5. Use Database authentication with credentials from `.env`

## Local Development

You can run the Azure Functions locally before deploying:

```bash
cd functions
pip install -r requirements.txt
# Update local.settings.json with values from .env
func start
```

Then simulate events against `http://localhost:7071`:
```bash
cd sample-events
python simulate.py --patients   # Uses HTTP trigger locally
```

## Teardown

To delete all resources and stop charges:

```bash
cd scripts
./teardown.sh
```

## Architecture: Local → Cloud Migration

```
LOCAL (ingestion/)                    CLOUD (azure-test-env/functions/)
===========================          =======================================
sqlite3.connect() (db.py)     →     pyodbc → Azure SQL (shared/db.py)
event_log table (events.py)   →     Azure Event Hubs producer (shared/events.py)
DLQ table (dlq.py)            →     Azure SQL table (shared/dlq.py)
dedup.py                      →     Same logic, pyodbc (shared/dedup.py)
audit.py                      →     Same logic, pyodbc (shared/audit.py)
parsers/*                     →     Copied unchanged (pure logic)
validators/*                  →     Copied unchanged (pure logic)
ingest/claims.py              →     Same logic, Azure SQL (ingest/claims.py)
ingest/remittances.py         →     Same logic, Azure SQL (ingest/remittances.py)
ingest/documents.py           →     + Real Doc Intelligence (ingest/documents.py)
ingest/patients.py            →     Same logic, Azure SQL (ingest/patients.py)
ingest/fee_schedules.py       →     Same logic, Azure SQL (ingest/fee_schedules.py)
run_ingestion.py (manual)     →     function_app.py (event-driven triggers)
```

## Folder Structure

```
azure-test-env/
├── .env                          # Connection details (auto-generated, git-ignored)
├── .gitignore
├── README.md
├── scripts/
│   ├── provision.sh              # Create all Azure resources
│   ├── run_sql.sh                # Run schema + seed data
│   ├── deploy_functions.sh       # Deploy ingestion functions
│   └── teardown.sh               # Delete everything
├── sql/
│   ├── schema.sql                # OLTP table definitions
│   └── seed_data.sql             # Synthetic sample data
├── functions/                    # Azure Functions project
│   ├── function_app.py           # Main entry — all triggers
│   ├── host.json                 # Functions runtime config
│   ├── local.settings.json       # Local dev settings
│   ├── requirements.txt          # Python dependencies
│   ├── shared/                   # Cloud infrastructure modules
│   │   ├── db.py                 # Azure SQL connection (pyodbc)
│   │   ├── events.py             # Event Hubs producer
│   │   ├── audit.py              # Audit log writer
│   │   ├── dedup.py              # Deduplication logic
│   │   └── dlq.py                # Dead-letter queue
│   ├── parsers/                  # Copied from ingestion/parsers/
│   │   ├── edi_835.py            # EDI 835 parser
│   │   ├── edi_837.py            # EDI 837 parser
│   │   ├── csv_parser.py         # Fee schedule/NPPES parser
│   │   ├── eob_mock.py           # EOB extraction parser
│   │   ├── fhir_patient.py       # FHIR patient normalizer
│   │   └── hl7v2_patient.py      # HL7 v2 patient normalizer
│   ├── validators/               # Copied from ingestion/validators/
│   │   └── validation.py         # All validation rules
│   ├── ingest/                   # Cloud-adapted ingestion modules
│   │   ├── claims.py             # EDI 837 → Azure SQL
│   │   ├── remittances.py        # EDI 835 + EOB → Azure SQL
│   │   ├── documents.py          # Blob → Doc Intelligence → Azure SQL
│   │   ├── patients.py           # FHIR → Azure SQL
│   │   └── fee_schedules.py      # CSV → SCD Type 2 merge → Azure SQL
│   └── sample-events/
│       └── simulate.py           # Send test data through the pipeline
├── fabric-notebooks/             # Medallion pipeline notebooks
│   ├── nb_bronze_cdc.py          # Bronze: CDC Parquet → Delta Lake
│   ├── nb_silver_transforms.py   # Silver: Cleaned + deduplicated
│   ├── nb_gold_aggregations.py   # Gold: Business aggregations for Power BI
│   └── *.ipynb                   # Fabric-importable versions (auto-generated)
├── FABRIC_SETUP.md               # Fabric Lakehouse setup guide
└── sample-data/                  # (future: Parquet files for lakehouse)
```

## What's Included vs. What's Next

| Component | Status |
|---|---|
| Azure SQL (OLTP) + schema + data | Included |
| ADLS Gen2 lakehouse containers | Included (Bronze/Silver/Gold) |
| Event Hubs (4 hubs) | Included + producing events |
| Ingestion Functions (6 triggers) | Included + deployed |
| Event Simulator | Included |
| AI Search | Provisioned (no indexes yet) |
| Document Intelligence | Provisioned + integrated in doc function |
| App Service | Provisioned (FastAPI app next) |
| Power BI | Connect Desktop to SQL |
| Fabric Lakehouse + notebooks | Included — see [FABRIC_SETUP.md](FABRIC_SETUP.md) |
| Fabric permissions automation | Included (`scripts/setup_fabric.sh`) |
| FastAPI case management app | Next step |
| Power BI Direct Lake | Next step (after Gold tables populated) |
