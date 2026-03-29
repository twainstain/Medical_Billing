#!/bin/bash
# ============================================================================
# End-to-End Verification — AI Analyst Agent
# ============================================================================
# Tests all agent endpoints against the deployed Function App.
#
# Prerequisites:
#   - Function App deployed (deploy_functions.sh)
#   - Gold views created (gold_views.sql)
#   - ANTHROPIC_API_KEY configured
#
# Usage:
#   ./verify_agent.sh <function-app-name> <function-key>
#   ./verify_agent.sh medbill-func-8df6df9c sk-xxx-yyy
# ============================================================================

set -uo pipefail

FUNC_APP="${1:-}"
FUNC_KEY="${2:-}"

if [ -z "$FUNC_APP" ] || [ -z "$FUNC_KEY" ]; then
    echo "Usage: $0 <function-app-name> <function-key>"
    echo "Example: $0 medbill-func-8df6df9c your-function-key"
    exit 1
fi

BASE_URL="https://${FUNC_APP}.azurewebsites.net/api"
PASS=0
FAIL=0
TOTAL=0

# ---- Helpers ----
check() {
    local name="$1"
    local expected_status="$2"
    local actual_status="$3"
    local body="$4"
    TOTAL=$((TOTAL + 1))

    if [ "$actual_status" = "$expected_status" ]; then
        echo "  PASS  $name (HTTP $actual_status)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $name — expected HTTP $expected_status, got $actual_status"
        echo "        Response: ${body:0:200}"
        FAIL=$((FAIL + 1))
    fi
}

check_json_field() {
    local name="$1"
    local field="$2"
    local body="$3"
    TOTAL=$((TOTAL + 1))

    if echo "$body" | python3 -c "import sys,json; d=json.load(sys.stdin); assert '$field' in d" 2>/dev/null; then
        echo "  PASS  $name — '$field' present in response"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $name — '$field' missing from response"
        FAIL=$((FAIL + 1))
    fi
}

echo "============================================"
echo "  E2E Verification — AI Analyst Agent"
echo "  Function App: $FUNC_APP"
echo "============================================"
echo ""

# ---- Test 1: Health Check ----
echo "[1/7] Health Check..."
RESP=$(curl -s -w "\n%{http_code}" "${BASE_URL}/health")
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "GET /api/health" "200" "$STATUS" "$BODY"
echo ""

# ---- Test 2: Agent UI ----
echo "[2/7] Agent UI..."
RESP=$(curl -s -w "\n%{http_code}" "${BASE_URL}/agent/ui?code=${FUNC_KEY}")
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "GET /api/agent/ui" "200" "$STATUS" "$BODY"

TOTAL=$((TOTAL + 1))
if echo "$BODY" | grep -q "Medical Billing AI Analyst"; then
    echo "  PASS  UI contains expected title"
    PASS=$((PASS + 1))
else
    echo "  FAIL  UI missing expected title"
    FAIL=$((FAIL + 1))
fi
echo ""

# ---- Test 3: Common Analyses List ----
echo "[3/7] Common Analyses List..."
RESP=$(curl -s -w "\n%{http_code}" "${BASE_URL}/agent/common?code=${FUNC_KEY}")
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "GET /api/agent/common" "200" "$STATUS" "$BODY"
check_json_field "Common analyses list" "analyses" "$BODY"

TOTAL=$((TOTAL + 1))
COUNT=$(echo "$BODY" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['analyses']))" 2>/dev/null || echo "0")
if [ "$COUNT" = "10" ]; then
    echo "  PASS  10 common analyses returned"
    PASS=$((PASS + 1))
else
    echo "  FAIL  Expected 10 analyses, got $COUNT"
    FAIL=$((FAIL + 1))
fi
echo ""

# ---- Test 4: Free-form Question ----
echo "[4/7] Free-form Question (may take 10-30s)..."
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{"question": "How many total claims are there and what is the total billed amount?"}' \
    "${BASE_URL}/agent/ask?code=${FUNC_KEY}" \
    --max-time 60)
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "POST /api/agent/ask" "200" "$STATUS" "$BODY"
check_json_field "Ask response has 'answer'" "answer" "$BODY"
check_json_field "Ask response has 'sql'" "sql" "$BODY"
check_json_field "Ask response has 'suggested_analyses'" "suggested_analyses" "$BODY"
echo ""

# ---- Test 5: Common Analysis Run ----
echo "[5/7] Run Common Analysis: executive_summary (may take 10-30s)..."
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    "${BASE_URL}/agent/common/executive_summary?code=${FUNC_KEY}" \
    --max-time 60)
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "POST /api/agent/common/executive_summary" "200" "$STATUS" "$BODY"
check_json_field "Executive summary has 'answer'" "answer" "$BODY"
check_json_field "Executive summary has 'analysis_name'" "analysis_name" "$BODY"
echo ""

# ---- Test 6: Invalid Common Analysis ----
echo "[6/7] Invalid Common Analysis ID..."
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    "${BASE_URL}/agent/common/nonexistent_id?code=${FUNC_KEY}" \
    --max-time 30)
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "POST /api/agent/common/nonexistent_id" "200" "$STATUS" "$BODY"

TOTAL=$((TOTAL + 1))
if echo "$BODY" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'Unknown' in d['answer']" 2>/dev/null; then
    echo "  PASS  Returns 'Unknown analysis ID' message"
    PASS=$((PASS + 1))
else
    echo "  FAIL  Missing error message for invalid ID"
    FAIL=$((FAIL + 1))
fi
echo ""

# ---- Test 7: Bad Request (missing question) ----
echo "[7/7] Bad Request (missing question)..."
RESP=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d '{}' \
    "${BASE_URL}/agent/ask?code=${FUNC_KEY}" \
    --max-time 15)
STATUS=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')
check "POST /api/agent/ask (empty body)" "400" "$STATUS" "$BODY"
echo ""

# ---- Summary ----
echo "============================================"
echo "  Results: $PASS passed, $FAIL failed, $TOTAL total"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
