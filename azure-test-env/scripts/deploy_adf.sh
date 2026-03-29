#!/bin/bash
# ============================================================================
# Deploy ADF Pipelines, Linked Services, and Datasets to Azure Data Factory
# ============================================================================
# Prerequisites:
#   1. Azure CLI installed + logged in
#   2. provision.sh has been run (.env exists)
#   3. ADF resource created in the resource group
#
# Usage:
#   chmod +x deploy_adf.sh
#   ./deploy_adf.sh
# ============================================================================

set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"
ADF_DIR="$(dirname "$0")/../adf"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found. Run provision.sh first."
    exit 1
fi

RESOURCE_GROUP=$(grep "^RESOURCE_GROUP=" "$ENV_FILE" | cut -d'=' -f2)
STORAGE_ACCOUNT=$(grep "^STORAGE_ACCOUNT=" "$ENV_FILE" | cut -d'=' -f2)

# ADF factory name (deterministic from resource group)
ADF_NAME="medbill-adf"

echo "============================================"
echo "  Deploying Azure Data Factory Pipelines"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  ADF Name:       $ADF_NAME"
echo "============================================"

# ---------------------------------------------------------------------------
# 1. Create ADF instance (if it doesn't exist)
# ---------------------------------------------------------------------------
echo ""
echo "[1/5] Creating Data Factory..."
az datafactory create \
  --name "$ADF_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --output none 2>/dev/null || echo "  (already exists)"
echo "  Done: $ADF_NAME"

# ---------------------------------------------------------------------------
# 2. Deploy Linked Services
#    Note: az CLI needs just the .properties block, not the full JSON wrapper
# ---------------------------------------------------------------------------
echo ""
echo "[2/5] Deploying Linked Services..."
for LS_FILE in "$ADF_DIR"/linked-services/*.json; do
  LS_NAME=$(jq -r '.name' "$LS_FILE")
  LS_PROPS=$(jq '.properties' "$LS_FILE")
  echo "  Deploying linked service: $LS_NAME"
  echo "$LS_PROPS" | az datafactory linked-service create \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --linked-service-name "$LS_NAME" \
    --properties @- \
    --output none 2>&1 | grep -v "^$" | sed 's/^/    /' || true
done

# ---------------------------------------------------------------------------
# 3. Deploy Datasets
# ---------------------------------------------------------------------------
echo ""
echo "[3/5] Deploying Datasets..."
for DS_FILE in "$ADF_DIR"/datasets/*.json; do
  DS_NAME=$(jq -r '.name' "$DS_FILE")
  DS_PROPS=$(jq '.properties' "$DS_FILE")
  echo "  Deploying dataset: $DS_NAME"
  echo "$DS_PROPS" | az datafactory dataset create \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --dataset-name "$DS_NAME" \
    --properties @- \
    --output none 2>&1 | grep -v "^$" | sed 's/^/    /' || true
done

# ---------------------------------------------------------------------------
# 4. Deploy Pipelines
# ---------------------------------------------------------------------------
echo ""
echo "[4/5] Deploying Pipelines..."
# Deploy in dependency order: leaf pipelines first, then master orchestrator
for PL_FILE in \
  "$ADF_DIR/pipelines/pl_cdc_incremental_copy.json" \
  "$ADF_DIR/pipelines/pl_batch_fee_schedule.json" \
  "$ADF_DIR/pipelines/pl_batch_providers.json" \
  "$ADF_DIR/pipelines/pl_master_orchestrator.json"; do

  if [ ! -f "$PL_FILE" ]; then
    echo "  Skipping: $PL_FILE (not found)"
    continue
  fi

  PL_NAME=$(jq -r '.name' "$PL_FILE")
  PL_PROPS=$(jq '.properties' "$PL_FILE")
  echo "  Deploying pipeline: $PL_NAME"
  echo "$PL_PROPS" | az datafactory pipeline create \
    --factory-name "$ADF_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --pipeline-name "$PL_NAME" \
    --pipeline @- \
    --output none 2>&1 | grep -v "^$" | sed 's/^/    /' || true
done

# ---------------------------------------------------------------------------
# 5. Create Triggers
# ---------------------------------------------------------------------------
echo ""
echo "[5/5] Creating Triggers..."

# CDC trigger: every 15 minutes
echo "  Creating CDC schedule trigger (every 15 min)..."
az datafactory trigger create \
  --factory-name "$ADF_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --trigger-name "trg_cdc_15min" \
  --properties '{
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Minute",
        "interval": 15,
        "startTime": "2026-01-01T00:00:00Z",
        "timeZone": "UTC"
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "pl_cdc_incremental_copy",
          "type": "PipelineReference"
        }
      }
    ]
  }' --output none 2>/dev/null || echo "    (trigger may already exist)"

# Fee schedule trigger: daily at 6 AM UTC
echo "  Creating fee schedule batch trigger (daily 6AM)..."
az datafactory trigger create \
  --factory-name "$ADF_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --trigger-name "trg_fee_schedule_daily" \
  --properties '{
    "type": "ScheduleTrigger",
    "typeProperties": {
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2026-01-01T06:00:00Z",
        "timeZone": "UTC"
      }
    },
    "pipelines": [
      {
        "pipelineReference": {
          "referenceName": "pl_batch_fee_schedule",
          "type": "PipelineReference"
        }
      }
    ]
  }' --output none 2>/dev/null || echo "    (trigger may already exist)"

echo ""
echo "============================================"
echo "  ADF Deployment Complete!"
echo "============================================"
echo ""
echo "  Factory:    $ADF_NAME"
echo "  Pipelines:  4 (CDC, fee schedule, providers, master)"
echo "  Triggers:   2 (15-min CDC, daily fee schedule)"
echo ""
echo "  NOTE: Triggers are created in STOPPED state."
echo "  To start them:"
echo "    az datafactory trigger start --factory-name $ADF_NAME \\"
echo "      --resource-group $RESOURCE_GROUP --trigger-name trg_cdc_15min"
echo ""
echo "  To run a pipeline manually:"
echo "    az datafactory pipeline create-run --factory-name $ADF_NAME \\"
echo "      --resource-group $RESOURCE_GROUP --pipeline-name pl_master_orchestrator"
echo ""
