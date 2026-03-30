# AI Analyst Agent

Natural language analytics over the Medical Billing Gold layer, powered by Claude API.

Ask questions in plain English — the agent generates SQL, executes it against the Gold tables, and returns a human-readable analysis with charts-ready data.

---

## Architecture

```
User Question
    │
    ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
│  Claude API  │────>│  execute_sql │────>│  Gold Layer           │
│  (text→SQL)  │     │  (tool use)  │     │  ┌─ Fabric Lakehouse │
└──────────────┘     └──────────────┘     │  └─ Azure SQL Views  │
    │                       │              └──────────────────────┘
    ▼                       ▼
┌──────────────┐     ┌──────────────┐
│  Claude API  │<────│  SQL Results │
│  (analysis)  │     │  (JSON rows) │
└──────────────┘     └──────────────┘
    │
    ▼
Human-Readable Analysis + Suggested Next Steps
```

### Data Sources

The agent queries the same Gold tables used by Power BI:

| # | Gold Table | Description |
|---|---|---|
| 1 | `gold_recovery_by_payer` | Recovery metrics per payer |
| 2 | `gold_cpt_analysis` | CPT payment ratios + Medicare benchmarks |
| 3 | `gold_payer_scorecard` | Payer risk tier classification |
| 4 | `gold_financial_summary` | Executive KPIs (key-value format) |
| 5 | `gold_claims_aging` | Claims by aging bucket |
| 6 | `gold_case_pipeline` | Case status + SLA compliance |
| 7 | `gold_deadline_compliance` | Deadline met/missed/at-risk by type |
| 8 | `gold_underpayment_detection` | Per-claim arbitration eligibility |

### Two Connection Modes

| Mode | Config | Best For |
|---|---|---|
| **Fabric Lakehouse** | `GOLD_DATA_SOURCE=fabric` | Production — reads Gold Delta tables via SQL analytics endpoint |
| **Azure SQL Views** | `GOLD_DATA_SOURCE=azure_sql` (default) | Development — reads Gold SQL views on OLTP database |

Both modes use identical table names (`gold_*`) and T-SQL syntax, so the agent works with either.

---

## Setup

### 1. Environment Variables

```bash
# Required
ANTHROPIC_API_KEY=sk-ant-...          # Claude API key

# Gold data source (pick one)
GOLD_DATA_SOURCE=fabric               # or "azure_sql" (default)

# For Fabric Lakehouse connection
FABRIC_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=<workspace-guid>.datawarehouse.fabric.microsoft.com;Database=medbill_lakehouse;Authentication=ActiveDirectoryServicePrincipal;UID=<sp-client-id>;PWD=<sp-secret>;Encrypt=yes;TrustServerCertificate=no;"

# For Azure SQL connection (used by default if GOLD_DATA_SOURCE=azure_sql)
SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=medbill-sql-214f9d00.database.windows.net;Database=medbill_oltp;..."

# Optional
CLAUDE_MODEL=claude-sonnet-4-20250514  # Default model (any Claude model works)
```

### 2. Connect to Fabric Lakehouse (Recommended for Production)

1. In **Fabric** -> open `medbill_lakehouse` -> **SQL analytics endpoint**
2. Copy the **Server** value (looks like `xxxxxxxx-xxxx.datawarehouse.fabric.microsoft.com`)
3. Create a **Service Principal** in Azure AD with access to the Fabric workspace
4. Set the connection string:

```bash
FABRIC_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=<server-from-step-2>;Database=medbill_lakehouse;Authentication=ActiveDirectoryServicePrincipal;UID=<sp-client-id>;PWD=<sp-secret>;Encrypt=yes;TrustServerCertificate=no;"
GOLD_DATA_SOURCE=fabric
```

If running on Azure Functions with managed identity:
```bash
FABRIC_SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=<server>;Database=medbill_lakehouse;Authentication=ActiveDirectoryMsi;Encrypt=yes;TrustServerCertificate=no;"
```

### 3. Deploy

The agent deploys as part of the Azure Functions app:

```bash
cd azure-test-env && bash scripts/deploy_functions.sh
```

This deploys all endpoints including the agent API and web UI.

---

## Web UI

The agent includes a built-in chat interface served at:

```
https://<function-app>.azurewebsites.net/api/agent/ui
```

Or locally:
```
http://localhost:7071/api/agent/ui
```

### UI Features

- **Chat interface** — type questions in plain English
- **Sidebar** — 15 pre-built analyses (click to run)
- **SQL toggle** — view the generated SQL query
- **Data table toggle** — view raw result rows
- **Suggested next** — follow-up analyses recommended after each query
- **Conversation history** — multi-turn context (up to 10 messages)
- **Dark theme** — responsive design, works on mobile

### Screenshot Layout

```
┌─────────────────┬──────────────────────────────────────┐
│                 │                                      │
│  Common         │  Medical Billing AI Analyst           │
│  Analyses       │                                      │
│                 │  ┌────────────────────────────────┐  │
│  ▸ Executive    │  │ User: Which payers have the    │  │
│    Summary      │  │ highest denial rates?          │  │
│  ▸ Worst Payers │  └────────────────────────────────┘  │
│  ▸ Arbitration  │                                      │
│    Ready        │  ┌────────────────────────────────┐  │
│  ▸ CPT Under-   │  │ Analyst: Based on the payer    │  │
│    payment      │  │ scorecard, the highest denial  │  │
│  ▸ Deadline     │  │ rates are:                     │  │
│    Risk         │  │ 1. Medicare Part B — 12.5%     │  │
│  ▸ Case         │  │ 2. Anthem Blue Cross — 12.5%   │  │
│    Pipeline     │  │ ...                            │  │
│  ▸ ...          │  │                                │  │
│                 │  │ ▾ Show SQL  ▾ Show Data        │  │
│  Suggested:     │  │                                │  │
│  ▸ Denial       │  │ Suggested next analyses:       │  │
│    Patterns     │  │ 1. Recovery opportunity...     │  │
│  ▸ Recovery     │  └────────────────────────────────┘  │
│                 │                                      │
│                 │  ┌────────────────────────────────┐  │
│                 │  │ Ask a question...          [→] │  │
│                 │  └────────────────────────────────┘  │
└─────────────────┴──────────────────────────────────────┘
```

---

## API Endpoints

### POST /api/agent/ask

Free-form natural language question.

**Request:**
```json
{
  "question": "Which payers have the highest denial rates?",
  "history": []
}
```

**Response:**
```json
{
  "answer": "Based on the payer scorecard, the highest denial rates are...",
  "sql": "SELECT payer_name, denial_rate_pct FROM gold_payer_scorecard ORDER BY denial_rate_pct DESC",
  "row_count": 8,
  "data": [
    {"payer_name": "Medicare Part B", "denial_rate_pct": 12.5},
    ...
  ],
  "model": "claude-sonnet-4-20250514",
  "suggested_analyses": [
    {"id": "denial_patterns", "name": "Denial Pattern Analysis", "description": "..."}
  ]
}
```

### GET /api/agent/common

Returns the catalog of 15 pre-built analyses.

### POST /api/agent/common/{analysis_id}

Run a pre-built analysis. Valid IDs:

| ID | Name |
|---|---|
| `executive_summary` | Executive Summary — financial KPIs |
| `worst_payers` | Worst Performing Payers — ranked by underpayment |
| `arbitration_ready` | Arbitration-Ready Claims — eligible for IDR |
| `cpt_underpayment` | CPT Code Underpayment — vs Medicare/FAIR Health |
| `deadline_risk` | Deadline Risk Report — at-risk NSA deadlines |
| `case_pipeline` | Case Pipeline Status — by status + SLA |
| `aging_analysis` | Claims Aging — by bucket with unpaid amounts |
| `payer_comparison` | Payer Risk Comparison — side-by-side scorecard |
| `recovery_opportunity` | Recovery Opportunity — total potential recovery |
| `denial_patterns` | Denial Pattern Analysis — systematic denials |
| `win_loss` | Win/Loss & ROI — arbitration outcomes |
| `analyst_performance` | Analyst Productivity — cases, win rate, recovery |
| `resolution_time` | Time-to-Resolution — bottleneck identification |
| `provider_analysis` | Provider Performance — disputes and recoveries |
| `monthly_trends` | Monthly Trends — volume and financial trends |

### GET /api/agent/ui

Serves the web chat interface (anonymous access).

---

## Security

- **SELECT-only**: The agent only executes SELECT queries against `gold_*` tables
- **Blocked keywords**: INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, EXEC, TRUNCATE, MERGE
- **Audit logging**: Every agent query is logged to the `audit_log` table with:
  - Question asked
  - SQL generated
  - Model used
  - Timestamp
- **No PII in prompts**: Only aggregated Gold data reaches the Claude API — no raw patient data

---

## Example Queries

```
"Give me an executive summary of all financial metrics"
"Which payers are high risk and should we prioritize for arbitration?"
"Show me claims over 90 days old with the highest underpayment"
"What is our arbitration win rate by payer?"
"Which CPT codes are most underpaid compared to Medicare rates?"
"How many deadlines are at risk this week?"
"Compare analyst productivity — who has the best win rate?"
"What is the total recovery opportunity if we win 60% of cases?"
```

---

## Local Development

```bash
cd azure-test-env/functions

# Set environment
export ANTHROPIC_API_KEY=sk-ant-...
export GOLD_DATA_SOURCE=azure_sql
export SQL_CONNECTION_STRING="..."

# Run locally
func start

# Open UI
open http://localhost:7071/api/agent/ui
```

### Verify Post-Deploy

```bash
bash scripts/verify_agent.sh
```

This runs smoke tests against the deployed agent endpoints.
