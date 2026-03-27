#!/bin/bash
# ============================================================================
# Run SQL scripts against Azure SQL Database
# ============================================================================
# Prerequisites: sqlcmd installed
#   brew install microsoft/mssql-release/mssql-tools18
#
# Usage:
#   chmod +x run_sql.sh
#   ./run_sql.sh
# ============================================================================

set -euo pipefail

ENV_FILE="$(dirname "$0")/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found. Run provision.sh first."
    exit 1
fi

# Load connection details
SQL_SERVER=$(grep "^SQL_SERVER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_DATABASE=$(grep "^SQL_DATABASE=" "$ENV_FILE" | cut -d'=' -f2)
SQL_USER=$(grep "^SQL_USER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_PASSWORD=$(grep "^SQL_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2)

SQL_DIR="$(dirname "$0")/../sql"

echo "============================================"
echo "  Running SQL Scripts"
echo "  Server: $SQL_SERVER"
echo "  Database: $SQL_DATABASE"
echo "============================================"

echo ""
echo "[1/3] Creating schema..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" \
  -i "$SQL_DIR/schema.sql" -C

echo ""
echo "[2/3] Creating staging tables & CDC watermarks..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" \
  -i "$SQL_DIR/staging_tables.sql" -C

echo ""
echo "[3/3] Loading sample data..."
sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" \
  -i "$SQL_DIR/seed_data.sql" -C

echo ""
echo "Done! Database is ready (schema + staging + seed data)."
echo ""
echo "Connect manually:"
echo "  sqlcmd -S $SQL_SERVER -d $SQL_DATABASE -U $SQL_USER -P '<password>' -C"
echo ""
echo "Or use Azure Data Studio / SSMS to connect to:"
echo "  Server: $SQL_SERVER"
echo "  Database: $SQL_DATABASE"
echo ""
