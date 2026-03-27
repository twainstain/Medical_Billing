#!/bin/bash
# ============================================================================
# Deploy Azure Functions (Ingestion Layer)
# ============================================================================
# Prerequisites:
#   1. Azure Functions Core Tools: brew install azure-functions-core-tools@4
#   2. Provisioning complete (provision.sh has been run)
#   3. .env file exists with connection strings
#
# Usage:
#   chmod +x deploy_functions.sh
#   ./deploy_functions.sh
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
FUNC_DIR="$SCRIPT_DIR/../functions"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found. Run provision.sh first."
    exit 1
fi

# Load config from .env
RESOURCE_GROUP=$(grep "^RESOURCE_GROUP=" "$ENV_FILE" | cut -d'=' -f2)
FUNC_APP_NAME=$(grep "^FUNC_APP_NAME=" "$ENV_FILE" | cut -d'=' -f2 2>/dev/null || true)

# If FUNC_APP_NAME not in .env, find it from Azure
if [ -z "$FUNC_APP_NAME" ]; then
    echo "Looking up Function App name in resource group $RESOURCE_GROUP..."
    FUNC_APP_NAME=$(az functionapp list \
        --resource-group "$RESOURCE_GROUP" \
        --query "[0].name" -o tsv)
fi

if [ -z "$FUNC_APP_NAME" ]; then
    echo "Error: No Function App found in $RESOURCE_GROUP"
    exit 1
fi

echo "============================================"
echo "  Deploying Ingestion Functions"
echo "  Function App: $FUNC_APP_NAME"
echo "  Resource Group: $RESOURCE_GROUP"
echo "============================================"

# ---------------------------------------------------------------------------
# 1. Configure app settings (connection strings)
# ---------------------------------------------------------------------------
echo ""
echo "[1/3] Setting app configuration..."

SQL_CONNECTION_STRING=$(grep "^SQL_SERVER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_DATABASE=$(grep "^SQL_DATABASE=" "$ENV_FILE" | cut -d'=' -f2)
SQL_USER=$(grep "^SQL_USER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_PASSWORD=$(grep "^SQL_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2)
EVENTHUB_CONN=$(grep "^EVENTHUB_CONNECTION_STRING=" "$ENV_FILE" | cut -d'=' -f2-)
STORAGE_KEY=$(grep "^STORAGE_KEY=" "$ENV_FILE" | cut -d'=' -f2)
STORAGE_ACCOUNT=$(grep "^STORAGE_ACCOUNT=" "$ENV_FILE" | cut -d'=' -f2)
DOC_INTEL_ENDPOINT=$(grep "^DOC_INTEL_ENDPOINT=" "$ENV_FILE" | cut -d'=' -f2)
DOC_INTEL_KEY=$(grep "^DOC_INTEL_KEY=" "$ENV_FILE" | cut -d'=' -f2)

FULL_SQL_CONN="Driver={ODBC Driver 18 for SQL Server};Server=${SQL_CONNECTION_STRING};Database=${SQL_DATABASE};Uid=${SQL_USER};Pwd=${SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no;"
FULL_STORAGE_CONN="DefaultEndpointsProtocol=https;AccountName=${STORAGE_ACCOUNT};AccountKey=${STORAGE_KEY};EndpointSuffix=core.windows.net"

az functionapp config appsettings set \
    --name "$FUNC_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --settings \
        "SQL_CONNECTION_STRING=$FULL_SQL_CONN" \
        "EVENTHUB_CONNECTION_STRING=$EVENTHUB_CONN" \
        "STORAGE_CONNECTION_STRING=$FULL_STORAGE_CONN" \
        "DOC_INTEL_ENDPOINT=$DOC_INTEL_ENDPOINT" \
        "DOC_INTEL_KEY=$DOC_INTEL_KEY" \
    --output none

echo "  App settings configured"

# ---------------------------------------------------------------------------
# 2. Install ODBC driver on the Function App (Linux)
# ---------------------------------------------------------------------------
echo ""
echo "[2/3] Configuring ODBC driver startup command..."
az functionapp config set \
    --name "$FUNC_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --startup-file "" \
    --output none
echo "  Note: pyodbc uses the pre-installed ODBC driver on Azure Functions Linux"

# ---------------------------------------------------------------------------
# 3. Deploy function code
# ---------------------------------------------------------------------------
echo ""
echo "[3/3] Deploying function code..."
cd "$FUNC_DIR"

# Publish using Azure Functions Core Tools
func azure functionapp publish "$FUNC_APP_NAME" --python

echo ""
echo "============================================"
echo "  Deployment Complete!"
echo "============================================"
echo ""
echo "  Function App: $FUNC_APP_NAME"
echo "  Health check: https://$FUNC_APP_NAME.azurewebsites.net/api/health"
echo ""
echo "  View logs:"
echo "    func azure functionapp logstream $FUNC_APP_NAME"
echo ""
echo "  Test with simulator:"
echo "    cd ../functions/sample-events"
echo "    python simulate.py --all"
echo ""
