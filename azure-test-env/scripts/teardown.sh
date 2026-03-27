#!/bin/bash
# ============================================================================
# Medical Billing Arbitration — Teardown (Delete All Azure Resources)
# ============================================================================
# This deletes the ENTIRE resource group and all resources within it.
# Use this to avoid unexpected charges when you're done testing.
#
# Usage:
#   chmod +x teardown.sh
#   ./teardown.sh
# ============================================================================

set -euo pipefail

# Load resource group name from .env if it exists
ENV_FILE="$(dirname "$0")/../.env"
if [ -f "$ENV_FILE" ]; then
    RESOURCE_GROUP=$(grep "^RESOURCE_GROUP=" "$ENV_FILE" | cut -d'=' -f2)
else
    RESOURCE_GROUP="rg-medbill-test"
fi

echo "============================================"
echo "  TEARDOWN — Delete Test Environment"
echo "============================================"
echo ""
echo "  Resource Group: $RESOURCE_GROUP"
echo ""
echo "  This will PERMANENTLY DELETE:"
echo "    - Azure SQL Server + Database"
echo "    - Storage Account (ADLS Gen2)"
echo "    - Event Hubs Namespace"
echo "    - Azure Functions App"
echo "    - Azure AI Search"
echo "    - Document Intelligence"
echo "    - App Service + Plan"
echo "    - ALL data in these services"
echo ""

read -p "Are you sure? Type 'yes' to confirm: " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Deleting resource group '$RESOURCE_GROUP'..."
az group delete \
  --name "$RESOURCE_GROUP" \
  --yes \
  --no-wait

echo ""
echo "Deletion initiated (runs in background, ~2-5 minutes)."
echo "Check status: az group show --name $RESOURCE_GROUP"
echo ""
echo "You can also delete the local .env file:"
echo "  rm $ENV_FILE"
echo ""
