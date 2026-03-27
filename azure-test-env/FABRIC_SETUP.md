# Microsoft Fabric Lakehouse — Setup Guide

> **Workspace:** `medbill-test-lakehouse`
> **Lakehouse:** `medbill_lakehouse`
> **Medallion pipeline:** Bronze (CDC) → Silver (transforms) → Gold (aggregations)
> **Last updated:** March 2026

---

## Prerequisites

1. **Azure resources provisioned** — run `scripts/provision.sh` first (creates ADLS Gen2, SQL, etc.)
2. **Azure CLI** logged in: `az login`
3. **Fabric-capable Azure tenant** — personal Gmail/Outlook tenants need a work account (see Step 1)

## Setup (6 steps)

### Step 1: Create a work account in your Entra tenant

Fabric requires a work/school account — personal accounts (`@gmail.com`, `@outlook.com`) cannot sign in.

1. Azure Portal → **Microsoft Entra ID** → **Users** → **+ New user** → **Create new user**
2. Fill in:
   - User principal name: `admin@<your-tenant>.onmicrosoft.com`
   - Display name: `Admin`
   - Password: auto-generate or set one
3. Under **Assignments**, add role: **Global Administrator**
4. Click **Create**

To find your tenant domain:
```bash
az account show --query tenantDefaultDomain -o tsv
```

### Step 2: Enable Fabric tenant settings

Sign into [app.fabric.microsoft.com](https://app.fabric.microsoft.com) with the work account from Step 1.

In **Settings** (gear icon) → **Admin portal** → **Tenant settings**, enable:

1. **"Users can create Fabric items"** — under Microsoft Fabric section
2. **"Users can try Microsoft Fabric paid features"** — under Help and support section

Both should be set to **Enabled for the entire organization**.

### Step 3: Activate Fabric trial

1. Click your **profile icon** (top-right circle)
2. Click **"Start Fabric trial"** (green button at bottom of dropdown)
3. Accept the 60-day trial

### Step 4: Create workspace and Lakehouse

1. From Fabric Home, click **New workspace**
2. Name: `medbill-test-lakehouse`
3. Workspace type: **Fabric Trial**
4. Semantic model: **Small** (test data is <1GB)
5. Click **Apply**
6. Inside the workspace: **+ New item** → **Lakehouse** → name: `medbill_lakehouse`

### Step 5: Grant storage permissions

The Fabric notebook identity needs access to the ADLS Gen2 storage account.
Run the automation script (or do it manually):

```bash
cd azure-test-env/scripts
./setup_fabric.sh
```

Or manually:
```bash
# Get the admin user's object ID
USER_ID=$(az ad user show --id admin@<tenant>.onmicrosoft.com --query id -o tsv)

# Get storage account name from .resource-names
source ../.resource-names

# Assign Storage Blob Data Contributor
az role assignment create \
  --assignee "$USER_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/rg-medbill-test/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME"
```

> Role assignments take up to 5 minutes to propagate.

### Step 6: Import and run notebooks

1. In the workspace (not inside the Lakehouse), click **+ New item** → **Import notebook**
2. Upload the 3 `.ipynb` files from `fabric-notebooks/`:
   - `nb_bronze_cdc.ipynb`
   - `nb_silver_transforms.ipynb`
   - `nb_gold_aggregations.ipynb`
3. Open each notebook → **Add data items** → **From OneLake catalog** → select `medbill_lakehouse`
4. In `nb_bronze_cdc`, update the storage account:
   ```python
   BRONZE_ADLS_PATH = "abfss://bronze@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net"
   ```
5. Run in order:
   1. `nb_bronze_cdc` (Bronze — raw CDC ingestion)
   2. `nb_silver_transforms` (Silver — deduplicate + current state)
   3. `nb_gold_aggregations` (Gold — business metrics for Power BI)

## Converting .py to .ipynb

Fabric only imports `.ipynb` (Jupyter Notebook) format. To regenerate from the `.py` source files:

```bash
cd azure-test-env/scripts
./setup_fabric.sh --convert-notebooks
```

Or use the standalone converter:
```bash
cd azure-test-env/fabric-notebooks
python3 ../scripts/convert_notebooks.py
```

## Connect Power BI (Direct Lake)

After Gold tables are populated:

1. In Fabric, open `medbill_lakehouse`
2. Click **New semantic model** → select all `gold_*` tables
3. In Power BI Desktop: **Get Data** → **OneLake data hub** → select the semantic model
4. Gold tables appear as native tables — no import/ETL needed (Direct Lake mode)

## Troubleshooting

| Problem | Fix |
|---|---|
| Can't sign into Fabric | Use a work account (`admin@<tenant>.onmicrosoft.com`), not a personal email |
| Fabric Trial greyed out in workspace type | Activate trial first via profile icon → "Start Fabric trial" |
| 403 AccessDenied on ADLS | Run `setup_fabric.sh` to grant Storage Blob Data Contributor role |
| Notebooks uploaded as files | Import at workspace level (+ New item → Import notebook), not via Lakehouse upload |
| "No trials available on this tenant" | Enable "Users can try Microsoft Fabric paid features" in Admin portal → Tenant settings |

## Architecture

```
ADLS Gen2 (medbillstoreb29b302f)          Fabric Lakehouse (medbill_lakehouse)
================================          =====================================
bronze/{table}/year=.../...parquet  →     nb_bronze_cdc     → Tables/bronze/*
                                          nb_silver_transforms → Tables/silver/*
                                          nb_gold_aggregations → Tables/gold/*
                                                                      │
                                                              Power BI Direct Lake
                                                              (no ETL, reads Delta natively)
```

## What's Next

| Component | Status |
|---|---|
| Fabric workspace + Lakehouse | Done |
| Bronze/Silver/Gold notebooks | Done (imported) |
| ADLS → Fabric permissions | Done (setup_fabric.sh) |
| ADF pipeline orchestration | Next (pl_master_orchestrator: CDC → Bronze → Silver → Gold) |
| Power BI Direct Lake semantic model | Next (create after Gold tables are populated) |
| Scheduled refresh | Next (ADF trigger or Fabric pipeline) |
