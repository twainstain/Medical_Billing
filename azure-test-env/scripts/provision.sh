#!/bin/bash
# ============================================================================
# Medical Billing Arbitration — Azure Test Environment Provisioning
# ============================================================================
# Prerequisites:
#   1. Azure CLI installed: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli
#   2. Logged in: az login
#   3. Subscription selected: az account set --subscription <sub-id>
#
# Usage:
#   chmod +x provision.sh
#   ./provision.sh
#
# Idempotent — safe to re-run. Skips resources that already exist.
# Estimated cost: ~$15-20/month (mostly Event Hubs Basic tier)
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration — deterministic names (no random suffixes on re-run)
# ---------------------------------------------------------------------------
RESOURCE_GROUP="rg-medbill-test"
LOCATION="centralus"
SQL_DB_NAME="medbill_oltp"
SQL_ADMIN_USER="medbilladmin"
APP_SERVICE_PLAN="medbill-plan"
ADF_NAME="medbill-adf"
TAGS="project=medical-billing env=test"

# Generate random suffixes only if creating new resources (saved in .names file)
NAMES_FILE="$(dirname "$0")/../.resource-names"

if [ -f "$NAMES_FILE" ]; then
    echo "Loading existing resource names from $NAMES_FILE"
    source "$NAMES_FILE"
else
    SQL_SERVER_NAME="medbill-sql-$(openssl rand -hex 4)"
    STORAGE_ACCOUNT_NAME="medbillstore$(openssl rand -hex 4)"
    FUNCTION_APP_NAME="medbill-func-$(openssl rand -hex 4)"
    EVENT_HUB_NAMESPACE="medbill-ehub-$(openssl rand -hex 4)"
    AI_SEARCH_NAME="medbill-search-$(openssl rand -hex 4)"
    DOC_INTEL_NAME="medbill-docintel-$(openssl rand -hex 4)"
    APP_SERVICE_NAME="medbill-api-$(openssl rand -hex 4)"

    cat > "$NAMES_FILE" << NAMES
SQL_SERVER_NAME=$SQL_SERVER_NAME
STORAGE_ACCOUNT_NAME=$STORAGE_ACCOUNT_NAME
FUNCTION_APP_NAME=$FUNCTION_APP_NAME
EVENT_HUB_NAMESPACE=$EVENT_HUB_NAMESPACE
AI_SEARCH_NAME=$AI_SEARCH_NAME
DOC_INTEL_NAME=$DOC_INTEL_NAME
APP_SERVICE_NAME=$APP_SERVICE_NAME
NAMES
    echo "Generated new resource names → $NAMES_FILE"
fi

# SQL password — prompt so it's not stored in the script
read -sp "Enter SQL admin password (min 8 chars, uppercase+lowercase+number): " SQL_ADMIN_PASSWORD
echo ""

echo "============================================"
echo "  Provisioning Azure Test Environment"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location:       $LOCATION"
echo "============================================"

# ---------------------------------------------------------------------------
# 1. Resource Group
# ---------------------------------------------------------------------------
echo ""
echo "[1/10] Resource Group..."
if az group exists --name "$RESOURCE_GROUP" 2>/dev/null | grep -q true; then
  echo "  Already exists — reusing"
else
  az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags $TAGS \
    --output none
fi
echo "  Done: $RESOURCE_GROUP"

# ---------------------------------------------------------------------------
# 2. Azure SQL Database (Free tier)
# ---------------------------------------------------------------------------
echo ""
echo "[2/10] Azure SQL Server + Database..."
if az sql server show --name "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  SQL Server already exists — skipping"
else
  az sql server create \
    --name "$SQL_SERVER_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --admin-user "$SQL_ADMIN_USER" \
    --admin-password "$SQL_ADMIN_PASSWORD" \
    --output none

  az sql server firewall-rule create \
    --name "AllowAzureServices" \
    --resource-group "$RESOURCE_GROUP" \
    --server "$SQL_SERVER_NAME" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 \
    --output none

  MY_IP=$(curl -s https://api.ipify.org)
  az sql server firewall-rule create \
    --name "AllowMyIP" \
    --resource-group "$RESOURCE_GROUP" \
    --server "$SQL_SERVER_NAME" \
    --start-ip-address "$MY_IP" \
    --end-ip-address "$MY_IP" \
    --output none
fi

# Create database if it doesn't exist
if az sql db show --name "$SQL_DB_NAME" --resource-group "$RESOURCE_GROUP" --server "$SQL_SERVER_NAME" --output none 2>/dev/null; then
  echo "  Database already exists — skipping"
else
  az sql db create \
    --name "$SQL_DB_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --server "$SQL_SERVER_NAME" \
    --edition Free \
    --output none
fi
echo "  Done: $SQL_SERVER_NAME/$SQL_DB_NAME"

# ---------------------------------------------------------------------------
# 3. Storage Account (ADLS Gen2 — for lakehouse Bronze/Silver/Gold)
# ---------------------------------------------------------------------------
echo ""
echo "[3/10] Storage Account (ADLS Gen2)..."
if az storage account show --name "$STORAGE_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Already exists — skipping creation"
else
  az storage account create \
    --name "$STORAGE_ACCOUNT_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --tags $TAGS \
    --output none
fi

STORAGE_KEY=$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[0].value" -o tsv)

for LAYER in bronze silver gold documents; do
  az storage container create \
    --name "$LAYER" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true
done
echo "  Done: $STORAGE_ACCOUNT_NAME (containers: bronze, silver, gold, documents)"

# ---------------------------------------------------------------------------
# 4. Event Hubs Namespace + Hubs (Basic tier — ~$11/mo)
# ---------------------------------------------------------------------------
echo ""
echo "[4/10] Event Hubs..."
if az eventhubs namespace show --name "$EVENT_HUB_NAMESPACE" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Namespace already exists — skipping"
else
  az eventhubs namespace create \
    --name "$EVENT_HUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Basic \
    --tags $TAGS \
    --output none
fi

for HUB in claims remittances documents status-changes; do
  az eventhubs eventhub create \
    --name "$HUB" \
    --resource-group "$RESOURCE_GROUP" \
    --namespace-name "$EVENT_HUB_NAMESPACE" \
    --partition-count 2 \
    --output none 2>/dev/null || true
done
echo "  Done: $EVENT_HUB_NAMESPACE (hubs: claims, remittances, documents, status-changes)"

# ---------------------------------------------------------------------------
# 5. Azure Functions (Consumption plan — free tier)
# ---------------------------------------------------------------------------
echo ""
echo "[5/10] Azure Functions App..."
if az functionapp show --name "$FUNCTION_APP_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Already exists — skipping"
else
  az functionapp create \
    --name "$FUNCTION_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --storage-account "$STORAGE_ACCOUNT_NAME" \
    --consumption-plan-location "$LOCATION" \
    --runtime python \
    --runtime-version 3.11 \
    --functions-version 4 \
    --os-type Linux \
    --tags $TAGS \
    --output none
fi
echo "  Done: $FUNCTION_APP_NAME"

# ---------------------------------------------------------------------------
# 6. Azure AI Search (Free tier — 50 MB, 3 indexes)
# ---------------------------------------------------------------------------
echo ""
echo "[6/10] Azure AI Search (free tier)..."
if az search service show --name "$AI_SEARCH_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Already exists — skipping"
else
  az search service create \
    --name "$AI_SEARCH_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku free \
    --output none
fi
echo "  Done: $AI_SEARCH_NAME"

# ---------------------------------------------------------------------------
# 7. Azure Document Intelligence (Free tier — 500 pages/mo)
# ---------------------------------------------------------------------------
echo ""
echo "[7/10] Azure Document Intelligence (free tier)..."
if az cognitiveservices account show --name "$DOC_INTEL_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Already exists — skipping"
else
  az cognitiveservices account create \
    --name "$DOC_INTEL_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --kind FormRecognizer \
    --sku F0 \
    --tags $TAGS \
    --output none
fi
echo "  Done: $DOC_INTEL_NAME"

# ---------------------------------------------------------------------------
# 8. App Service (Free F1 tier — for FastAPI backend)
# ---------------------------------------------------------------------------
echo ""
echo "[8/10] App Service for FastAPI..."
if az appservice plan show --name "$APP_SERVICE_PLAN" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Plan already exists — skipping"
else
  az appservice plan create \
    --name "$APP_SERVICE_PLAN" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku F1 \
    --is-linux \
    --output none
fi

if az webapp show --name "$APP_SERVICE_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Web app already exists — skipping"
else
  az webapp create \
    --name "$APP_SERVICE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --plan "$APP_SERVICE_PLAN" \
    --runtime "PYTHON:3.11" \
    --output none
fi
echo "  Done: $APP_SERVICE_NAME"

# ---------------------------------------------------------------------------
# 9. Azure Data Factory
# ---------------------------------------------------------------------------
echo ""
echo "[9/10] Azure Data Factory..."
if az datafactory show --name "$ADF_NAME" --resource-group "$RESOURCE_GROUP" --output none 2>/dev/null; then
  echo "  Already exists — skipping"
else
  az datafactory create \
    --name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags $TAGS \
    --output none
fi
echo "  Done: $ADF_NAME"

# ---------------------------------------------------------------------------
# 10. Output connection info
# ---------------------------------------------------------------------------
echo ""
echo "[10/10] Gathering connection details..."

SQL_FQDN=$(az sql server show \
  --name "$SQL_SERVER_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "fullyQualifiedDomainName" -o tsv)

EVENTHUB_CONNSTR=$(az eventhubs namespace authorization-rule keys list \
  --name RootManageSharedAccessKey \
  --namespace-name "$EVENT_HUB_NAMESPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --query "primaryConnectionString" -o tsv)

SEARCH_KEY=$(az search admin-key show \
  --service-name "$AI_SEARCH_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "primaryKey" -o tsv)

DOC_INTEL_KEY=$(az cognitiveservices account keys list \
  --name "$DOC_INTEL_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "key1" -o tsv)

DOC_INTEL_ENDPOINT=$(az cognitiveservices account show \
  --name "$DOC_INTEL_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --query "properties.endpoint" -o tsv)

# Write .env file (DO NOT commit this)
ENV_FILE="$(dirname "$0")/../.env"
cat > "$ENV_FILE" << EOF
# Medical Billing Test Environment — generated $(date -u +%Y-%m-%dT%H:%M:%SZ)
# DO NOT COMMIT THIS FILE

# Azure SQL
SQL_SERVER=$SQL_FQDN
SQL_DATABASE=$SQL_DB_NAME
SQL_USER=$SQL_ADMIN_USER
SQL_PASSWORD=$SQL_ADMIN_PASSWORD
SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=$SQL_FQDN;Database=$SQL_DB_NAME;Uid=$SQL_ADMIN_USER;Pwd=$SQL_ADMIN_PASSWORD;Encrypt=yes;TrustServerCertificate=no;

# Storage (ADLS Gen2)
STORAGE_ACCOUNT=$STORAGE_ACCOUNT_NAME
STORAGE_KEY=$STORAGE_KEY
STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=$STORAGE_ACCOUNT_NAME;AccountKey=$STORAGE_KEY;EndpointSuffix=core.windows.net

# Event Hubs
EVENTHUB_NAMESPACE=$EVENT_HUB_NAMESPACE
EVENTHUB_CONNECTION_STRING=$EVENTHUB_CONNSTR

# Azure AI Search
SEARCH_SERVICE=$AI_SEARCH_NAME
SEARCH_KEY=$SEARCH_KEY

# Document Intelligence
DOC_INTEL_ENDPOINT=$DOC_INTEL_ENDPOINT
DOC_INTEL_KEY=$DOC_INTEL_KEY

# App Service
APP_SERVICE_URL=https://$APP_SERVICE_NAME.azurewebsites.net

# Azure Data Factory
ADF_NAME=$ADF_NAME

# Function App
FUNCTION_APP_NAME=$FUNCTION_APP_NAME

# Resource Group (for teardown)
RESOURCE_GROUP=$RESOURCE_GROUP
EOF

echo ""
echo "============================================"
echo "  Provisioning Complete!"
echo "============================================"
echo ""
echo "  Resource Group:      $RESOURCE_GROUP"
echo "  SQL Server:          $SQL_FQDN"
echo "  SQL Database:        $SQL_DB_NAME"
echo "  Storage Account:     $STORAGE_ACCOUNT_NAME"
echo "  Event Hubs:          $EVENT_HUB_NAMESPACE"
echo "  Functions App:       $FUNCTION_APP_NAME"
echo "  AI Search:           $AI_SEARCH_NAME"
echo "  Document Intel:      $DOC_INTEL_NAME"
echo "  App Service:         https://$APP_SERVICE_NAME.azurewebsites.net"
echo "  Data Factory:        $ADF_NAME"
echo ""
echo "  Connection details saved to: $ENV_FILE"
echo "  IMPORTANT: Do NOT commit .env to git!"
echo ""
echo "  Next steps:"
echo "    1. ./run_sql.sh         — create schema + staging + seed data"
echo "    2. ./deploy_functions.sh — deploy Azure Functions"
echo "    3. ./deploy_adf.sh      — deploy ADF pipelines"
echo ""
