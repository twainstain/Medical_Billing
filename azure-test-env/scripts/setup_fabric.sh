#!/bin/bash
# ============================================================================
# Medical Billing Arbitration — Fabric Lakehouse Setup
# ============================================================================
# Prerequisites:
#   1. Azure CLI installed and logged in: az login
#   2. provision.sh already run (creates ADLS Gen2, .resource-names, .env)
#   3. Entra work account created (admin@<tenant>.onmicrosoft.com)
#   4. Fabric trial activated in the portal
#
# Usage:
#   chmod +x setup_fabric.sh
#   ./setup_fabric.sh                    # Grant ADLS permissions + convert notebooks
#   ./setup_fabric.sh --convert-notebooks # Only convert .py to .ipynb
#   ./setup_fabric.sh --permissions-only  # Only grant ADLS permissions
#
# Idempotent — safe to re-run. Role assignments skip if already granted.
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NAMES_FILE="$PROJECT_DIR/.resource-names"
NOTEBOOKS_DIR="$PROJECT_DIR/fabric-notebooks"
RESOURCE_GROUP="rg-medbill-test"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
CONVERT_ONLY=false
PERMISSIONS_ONLY=false

for arg in "$@"; do
    case $arg in
        --convert-notebooks) CONVERT_ONLY=true ;;
        --permissions-only) PERMISSIONS_ONLY=true ;;
    esac
done

# ---------------------------------------------------------------------------
# Helper: Convert .py notebooks to .ipynb
# ---------------------------------------------------------------------------
convert_notebooks() {
    echo ""
    echo "--- Converting .py notebooks to .ipynb ---"

    python3 "$SCRIPT_DIR/convert_notebooks.py"

    echo "Notebooks ready for Fabric import in: $NOTEBOOKS_DIR/"
}

# ---------------------------------------------------------------------------
# Helper: Grant ADLS permissions to Fabric user
# ---------------------------------------------------------------------------
grant_permissions() {
    echo ""
    echo "--- Granting ADLS Gen2 permissions for Fabric ---"

    if [ ! -f "$NAMES_FILE" ]; then
        echo "ERROR: $NAMES_FILE not found. Run provision.sh first."
        exit 1
    fi

    source "$NAMES_FILE"

    # Get tenant domain
    TENANT_DOMAIN=$(az account show --query tenantDefaultDomain -o tsv)
    echo "  Tenant: $TENANT_DOMAIN"

    # Prompt for Fabric user (default: admin@<tenant>)
    DEFAULT_USER="admin@$TENANT_DOMAIN"
    read -p "  Fabric user [$DEFAULT_USER]: " FABRIC_USER
    FABRIC_USER="${FABRIC_USER:-$DEFAULT_USER}"

    # Get user object ID
    USER_OID=$(az ad user show --id "$FABRIC_USER" --query id -o tsv 2>/dev/null || true)
    if [ -z "$USER_OID" ]; then
        echo "ERROR: User '$FABRIC_USER' not found in Entra ID."
        echo "Create the user first: Azure Portal → Entra ID → Users → + New user"
        exit 1
    fi
    echo "  User: $FABRIC_USER ($USER_OID)"

    # Get subscription ID
    SUB_ID=$(az account show --query id -o tsv)
    STORAGE_SCOPE="/subscriptions/$SUB_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT_NAME"

    # Check if role already assigned
    EXISTING=$(az role assignment list \
        --assignee "$USER_OID" \
        --role "Storage Blob Data Contributor" \
        --scope "$STORAGE_SCOPE" \
        --query "[].id" -o tsv 2>/dev/null || true)

    if [ -n "$EXISTING" ]; then
        echo "  Storage Blob Data Contributor already assigned — skipping."
    else
        echo "  Assigning Storage Blob Data Contributor on $STORAGE_ACCOUNT_NAME..."
        az role assignment create \
            --assignee "$USER_OID" \
            --role "Storage Blob Data Contributor" \
            --scope "$STORAGE_SCOPE" \
            --output none
        echo "  Role assigned. May take up to 5 minutes to propagate."
    fi

    # Save Fabric user to .env for reference
    ENV_FILE="$PROJECT_DIR/.env"
    if [ -f "$ENV_FILE" ]; then
        if ! grep -q "FABRIC_USER" "$ENV_FILE"; then
            echo "" >> "$ENV_FILE"
            echo "# Fabric Lakehouse" >> "$ENV_FILE"
            echo "FABRIC_USER=$FABRIC_USER" >> "$ENV_FILE"
            echo "FABRIC_WORKSPACE=medbill-test-lakehouse" >> "$ENV_FILE"
            echo "FABRIC_LAKEHOUSE=medbill_lakehouse" >> "$ENV_FILE"
        fi
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "============================================"
echo "  Fabric Lakehouse Setup"
echo "  Storage: $RESOURCE_GROUP"
echo "============================================"

if [ "$CONVERT_ONLY" = true ]; then
    convert_notebooks
elif [ "$PERMISSIONS_ONLY" = true ]; then
    grant_permissions
else
    grant_permissions
    convert_notebooks
fi

echo ""
echo "============================================"
echo "  Fabric setup complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Sign into https://app.fabric.microsoft.com"
echo "  2. Create workspace: medbill-test-lakehouse (Fabric Trial)"
echo "  3. Create Lakehouse: medbill_lakehouse"
echo "  4. Import notebooks from: $NOTEBOOKS_DIR/*.ipynb"
echo "  5. Attach medbill_lakehouse to each notebook"
echo "  6. Update BRONZE_ADLS_PATH in nb_bronze_cdc with: $STORAGE_ACCOUNT_NAME"
echo "  7. Run notebooks: bronze → silver → gold"
echo ""
echo "See FABRIC_SETUP.md for detailed instructions."
