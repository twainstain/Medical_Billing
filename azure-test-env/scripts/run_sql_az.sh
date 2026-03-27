#!/bin/bash
# ============================================================================
# Run SQL scripts against Azure SQL Database using az sql query
# (No sqlcmd needed — uses Azure CLI directly)
# ============================================================================

set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found. Run provision.sh first."
    exit 1
fi

SQL_SERVER=$(grep "^SQL_SERVER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_DATABASE=$(grep "^SQL_DATABASE=" "$ENV_FILE" | cut -d'=' -f2)
# Extract just the server name (before .database.windows.net)
SQL_SERVER_NAME=$(echo "$SQL_SERVER" | cut -d'.' -f1)
RESOURCE_GROUP=$(grep "^RESOURCE_GROUP=" "$ENV_FILE" | cut -d'=' -f2)

SQL_DIR="$(dirname "$0")/../sql"

echo "============================================"
echo "  Running SQL Scripts (via az sql)"
echo "  Server: $SQL_SERVER"
echo "  Database: $SQL_DATABASE"
echo "============================================"

# Function to run SQL file in batches (split on GO statements)
run_sql_file() {
    local file="$1"
    local label="$2"
    echo ""
    echo "  Running: $label ($file)"

    # Split on GO and run each batch
    # az sql query doesn't support GO, so we split manually
    local batch=""
    local batch_num=0

    while IFS= read -r line || [ -n "$line" ]; do
        # Skip GO statements and PRINT statements
        if echo "$line" | grep -qiE '^[[:space:]]*GO[[:space:]]*$'; then
            if [ -n "$batch" ]; then
                batch_num=$((batch_num + 1))
                echo "    Batch $batch_num..."
                az sql db query \
                    --name "$SQL_DATABASE" \
                    --server "$SQL_SERVER_NAME" \
                    --resource-group "$RESOURCE_GROUP" \
                    --query-text "$batch" \
                    --output none 2>&1 || echo "    (warning: batch $batch_num had errors)"
                batch=""
            fi
            continue
        fi

        # Skip comment-only lines and PRINT
        if echo "$line" | grep -qiE '^[[:space:]]*PRINT'; then
            continue
        fi

        batch="$batch
$line"
    done < "$file"

    # Run remaining batch
    if [ -n "$batch" ]; then
        batch_num=$((batch_num + 1))
        echo "    Batch $batch_num..."
        az sql db query \
            --name "$SQL_DATABASE" \
            --server "$SQL_SERVER_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --query-text "$batch" \
            --output none 2>&1 || echo "    (warning: batch $batch_num had errors)"
    fi

    echo "    Done ($batch_num batches)"
}

echo ""
echo "[1/3] Creating schema..."
run_sql_file "$SQL_DIR/schema.sql" "schema.sql"

echo ""
echo "[2/3] Creating staging tables & CDC watermarks..."
run_sql_file "$SQL_DIR/staging_tables.sql" "staging_tables.sql"

echo ""
echo "[3/3] Loading sample data..."
run_sql_file "$SQL_DIR/seed_data.sql" "seed_data.sql"

echo ""
echo "============================================"
echo "  Database setup complete!"
echo "============================================"
echo ""
echo "  Verify with:"
echo "    az sql db query --name $SQL_DATABASE --server $SQL_SERVER_NAME \\"
echo "      --resource-group $RESOURCE_GROUP --query-text \"SELECT COUNT(*) AS cnt FROM claims\""
echo ""
