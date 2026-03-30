#!/usr/bin/env bash
# run_agent.sh — Start the AI Analyst Agent locally
# Usage: bash scripts/run_agent.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FUNC_DIR="$PROJECT_DIR/functions"
VENV_DIR="$PROJECT_DIR/.venv"

# ---- Preflight checks ----

echo "=== AI Analyst Agent — Local Startup ==="
echo ""

# 1. Check Azure Functions Core Tools
if ! command -v func &>/dev/null; then
    echo "ERROR: Azure Functions Core Tools not found."
    echo "  Install: brew install azure-functions-core-tools@4"
    exit 1
fi

# 2. Find Python 3.9+ (required by Azure Functions Core Tools)
PYTHON=""
for py in python3.11 python3.10 python3.9 python3.12; do
    if command -v "$py" &>/dev/null; then
        PYTHON="$py"
        break
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3.9+ not found. Azure Functions requires Python >= 3.9."
    echo "  Your current python3 is $(python3 --version 2>/dev/null) which is too old."
    echo "  Install: brew install python@3.11"
    exit 1
fi

echo "  Using Python:         $($PYTHON --version)"

# 3. Fix Arm64 worker symlinks (Homebrew ships X64 only)
if [ "$(uname -m)" = "arm64" ]; then
    FUNC_PATH="$(readlink -f "$(which func)" 2>/dev/null || realpath "$(which func)" 2>/dev/null || which func)"
    WORKERS_DIR="$(dirname "$FUNC_PATH")/../workers/python"
    # Fallback: check Homebrew Cellar directly
    if [ ! -d "$WORKERS_DIR" ]; then
        WORKERS_DIR="/usr/local/Cellar/azure-functions-core-tools@4/$(func --version 2>/dev/null)/workers/python"
    fi
    if [ -d "$WORKERS_DIR" ]; then
        for ver_dir in "$WORKERS_DIR"/3.*/OSX; do
            if [ -d "$ver_dir/X64" ] && [ ! -e "$ver_dir/Arm64" ]; then
                echo "  Symlinking Arm64 -> X64 in $(basename "$(dirname "$ver_dir")") worker..."
                ln -s X64 "$ver_dir/Arm64"
            fi
        done
    fi
fi

# 4. Create/activate venv with correct Python
if [ ! -d "$VENV_DIR" ]; then
    echo "  Creating virtualenv at $VENV_DIR ..."
    "$PYTHON" -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

# 5. Check ODBC driver
if ! odbcinst -q -d 2>/dev/null | grep -qi "ODBC Driver 18"; then
    echo "WARNING: ODBC Driver 18 for SQL Server not detected."
    echo "  Install: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
    echo ""
fi

# 6. Load .env file
if [ -f "$PROJECT_DIR/.env" ]; then
    echo "  Loading .env ..."
    while IFS= read -r line || [[ -n "${line:-}" ]]; do
        [[ -z "${line:-}" || "${line:-}" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" != *"="* ]] && continue
        key="${line%%=*}"
        value="${line#*=}"
        export "$key"="$value"
    done < "$PROJECT_DIR/.env"
fi

# 7. Check ANTHROPIC_API_KEY
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "ERROR: ANTHROPIC_API_KEY is not set."
    echo "  Option 1: export ANTHROPIC_API_KEY=sk-ant-..."
    echo "  Option 2: Add ANTHROPIC_API_KEY=sk-ant-... to azure-test-env/.env"
    exit 1
fi

echo "  Functions Core Tools: $(func --version 2>/dev/null)"
echo "  Python (venv):        $(python --version 2>/dev/null)"
echo "  ANTHROPIC_API_KEY:    ...${ANTHROPIC_API_KEY: -6}"
echo "  GOLD_DATA_SOURCE:     ${GOLD_DATA_SOURCE:-azure_sql} (default: azure_sql)"
echo ""

# ---- Install dependencies ----

echo "Installing Python dependencies..."
pip install -q --upgrade pip
pip install -q -r "$FUNC_DIR/requirements.txt"
echo ""

# ---- Start the agent ----

# Skip Azurite requirement — agent uses HTTP triggers only, not Durable Functions
export AzureWebJobsSecretStorageType="files"

# Use real Azure Storage for Durable Functions/WebJobs (avoids Azurite requirement)
if [ -n "${STORAGE_CONNECTION_STRING:-}" ]; then
    export AzureWebJobsStorage="$STORAGE_CONNECTION_STRING"
    echo "  AzureWebJobsStorage:  using STORAGE_CONNECTION_STRING from .env"
fi

echo ""
echo "Starting Azure Functions host..."
echo "  UI:  http://localhost:7071/api/agent/ui"
echo "  API: http://localhost:7071/api/agent/ask"
echo ""

cd "$FUNC_DIR"
func start --functions agent_ask_function agent_common_list_function agent_common_run_function agent_ui_function health_check_function
