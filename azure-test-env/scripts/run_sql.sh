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

set -uo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

ENV_FILE="$(dirname "$0")/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}Error: .env file not found. Run provision.sh first.${NC}"
    exit 1
fi

# Load connection details
SQL_SERVER=$(grep "^SQL_SERVER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_DATABASE=$(grep "^SQL_DATABASE=" "$ENV_FILE" | cut -d'=' -f2)
SQL_USER=$(grep "^SQL_USER=" "$ENV_FILE" | cut -d'=' -f2)
SQL_PASSWORD=$(grep "^SQL_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2)

SQL_DIR="$(dirname "$0")/../sql"
ERRORS=0

echo "============================================"
echo "  Running SQL Scripts"
echo "  Server: $SQL_SERVER"
echo "  Database: $SQL_DATABASE"
echo "============================================"

run_sql_file() {
    local step="$1"
    local file="$2"
    local desc="$3"

    echo ""
    echo -e "${YELLOW}[$step] $desc...${NC}"

    OUTPUT=$(sqlcmd -S "$SQL_SERVER" -d "$SQL_DATABASE" -U "$SQL_USER" -P "$SQL_PASSWORD" \
        -i "$file" -C -I 2>&1)
    EXIT_CODE=$?

    # Check for errors in output (sqlcmd doesn't always return non-zero)
    if echo "$OUTPUT" | grep -q "^Msg [0-9]"; then
        echo "$OUTPUT" | while IFS= read -r line; do
            if echo "$line" | grep -q "^Msg [0-9]"; then
                echo -e "  ${RED}$line${NC}"
            else
                echo "  $line"
            fi
        done
        ERRORS=$((ERRORS + 1))
        echo -e "  ${RED}ERRORS detected — see above${NC}"
        return 1
    else
        echo "$OUTPUT" | grep -v "^$" | while IFS= read -r line; do
            echo "  $line"
        done
        echo -e "  ${GREEN}OK${NC}"
        return 0
    fi
}

run_sql_file "1/5" "$SQL_DIR/schema.sql"              "Creating schema (13 tables)"
S1=$?
run_sql_file "2/5" "$SQL_DIR/staging_tables.sql"       "Creating staging tables & CDC watermarks"
S2=$?
run_sql_file "3/5" "$SQL_DIR/seed_data.sql"            "Loading base seed data"
S3=$?
run_sql_file "4/5" "$SQL_DIR/seed_data_extended.sql"   "Loading extended seed data (50 claims, 20 cases)"
S4=$?
run_sql_file "5/5" "$SQL_DIR/gold_views.sql"           "Creating Gold views (13 views)"
S5=$?

echo ""
echo "============================================"
echo "  Results"
echo "============================================"

print_status() {
    local step="$1"
    local desc="$2"
    local code="$3"
    if [ "$code" -eq 0 ]; then
        echo -e "  ${GREEN}✓${NC} $step $desc"
    else
        echo -e "  ${RED}✗${NC} $step $desc"
    fi
}

print_status "1/5" "Schema"         "$S1"
print_status "2/5" "Staging"        "$S2"
print_status "3/5" "Seed data"      "$S3"
print_status "4/5" "Extended data"  "$S4"
print_status "5/5" "Gold views"     "$S5"

TOTAL_ERRORS=$((S1 + S2 + S3 + S4 + S5))

echo ""
if [ "$TOTAL_ERRORS" -eq 0 ]; then
    echo -e "${GREEN}All steps completed successfully!${NC}"
else
    echo -e "${RED}$TOTAL_ERRORS step(s) had errors — review output above${NC}"
fi

echo ""
echo "Connect manually:"
echo "  sqlcmd -S $SQL_SERVER -d $SQL_DATABASE -U $SQL_USER -P '<password>' -C"
echo ""
echo "Verify with test script:"
echo "  Open sql/test_oltp.sql in VS Code and run (Cmd+Shift+E)"
echo ""
