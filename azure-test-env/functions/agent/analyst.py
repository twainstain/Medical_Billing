"""AI Data Analyst Agent — natural language queries against the Gold layer.

Uses Claude API to convert questions into SQL, executes against Gold views
in Azure SQL, then generates a human-readable analysis of the results.

Gold views available:
  - gold_recovery_by_payer      — recovery metrics per payer
  - gold_cpt_analysis           — CPT code payment ratios + benchmarks
  - gold_payer_scorecard        — payer risk scoring
  - gold_financial_summary      — overall financial KPIs (metric_name, metric_value)
  - gold_claims_aging           — claim aging buckets
  - gold_case_pipeline          — case status + SLA compliance
  - gold_deadline_compliance    — deadline met/missed/at-risk by type
  - gold_underpayment_detection — per-claim underpayment + arbitration eligibility
"""

import json
import logging
import os
from datetime import datetime, date
from decimal import Decimal

import anthropic

logger = logging.getLogger(__name__)

GOLD_SCHEMA = """
-- View: gold_recovery_by_payer
-- Recovery metrics per insurance payer
-- Columns: payer_id INT, payer_name NVARCHAR, total_claims INT, total_billed DECIMAL,
--          total_paid DECIMAL, total_underpayment DECIMAL, denial_count INT,
--          recovery_rate_pct DECIMAL, denial_rate_pct DECIMAL

-- View: gold_cpt_analysis
-- CPT code payment analysis with Medicare/FAIR Health benchmarks
-- Columns: cpt_code VARCHAR, claim_count INT, total_billed DECIMAL, total_paid DECIMAL,
--          avg_billed DECIMAL, avg_paid DECIMAL, payment_ratio_pct DECIMAL,
--          medicare_rate DECIMAL, fair_health_rate DECIMAL

-- View: gold_payer_scorecard
-- Payer behavior and risk assessment
-- Columns: payer_id INT, payer_name NVARCHAR, payer_type NVARCHAR, total_claims INT,
--          total_billed DECIMAL, total_paid DECIMAL, total_underpayment DECIMAL,
--          avg_underpayment DECIMAL, denial_count INT, payment_rate_pct DECIMAL,
--          denial_rate_pct DECIMAL, risk_tier VARCHAR ('low','medium','high')

-- View: gold_financial_summary
-- Overall financial KPIs as key-value pairs
-- Columns: metric_name VARCHAR, metric_value DECIMAL
-- Metrics: total_claims, total_billed, total_paid, total_underpayment,
--          recovery_rate_pct, paid_claims, denial_count, denial_rate_pct,
--          avg_billed_per_claim, avg_underpayment_per_claim

-- View: gold_claims_aging
-- Claims grouped by aging buckets
-- Columns: aging_bucket VARCHAR ('0-30 days','31-60 days','61-90 days','91-180 days','180+ days'),
--          claim_count INT, total_billed DECIMAL, total_unpaid DECIMAL, pct_of_total DECIMAL

-- View: gold_case_pipeline
-- Arbitration case pipeline by status
-- Columns: status VARCHAR ('open','in_review','negotiation','idr_initiated','idr_submitted','decided','closed'),
--          case_count INT, total_billed DECIMAL, total_underpayment DECIMAL,
--          avg_age_days FLOAT, sla_compliance_pct DECIMAL

-- View: gold_deadline_compliance
-- NSA regulatory deadline compliance by type
-- Columns: deadline_type VARCHAR ('open_negotiation','idr_initiation','entity_selection',
--          'evidence_submission','decision'),
--          total_deadlines INT, met_count INT, missed_count INT, pending_count INT,
--          at_risk_count INT, compliance_pct DECIMAL

-- View: gold_underpayment_detection
-- Per-claim underpayment analysis with arbitration eligibility
-- Columns: claim_id INT, payer_id INT, payer_name NVARCHAR, provider_npi CHAR,
--          date_of_service DATE, billed_amount DECIMAL, paid_amount DECIMAL,
--          qpa_amount DECIMAL, underpayment_amount DECIMAL, underpayment_pct DECIMAL,
--          arbitration_eligible INT (1=yes), dispute_status VARCHAR

-- View: gold_win_loss_analysis
-- Arbitration outcomes by payer, dispute type, and outcome
-- Columns: payer_id INT, payer_name NVARCHAR, dispute_type VARCHAR, outcome VARCHAR,
--          case_count INT, total_billed DECIMAL, total_disputed DECIMAL,
--          total_awarded DECIMAL, win_rate_pct DECIMAL, recovery_roi_pct DECIMAL, avg_award DECIMAL

-- View: gold_analyst_productivity
-- Analyst workload, resolution rate, and recovery metrics
-- Columns: assigned_analyst NVARCHAR, total_cases INT, active_cases INT, resolved_cases INT,
--          won_cases INT, lost_cases INT, settled_cases INT, win_rate_pct DECIMAL,
--          total_recovered DECIMAL, total_disputed DECIMAL, avg_resolution_days FLOAT,
--          critical_cases INT, high_priority_cases INT

-- View: gold_time_to_resolution
-- Cycle time by case status and priority
-- Columns: status VARCHAR, priority VARCHAR, case_count INT, avg_days FLOAT,
--          min_days INT, max_days INT, total_at_stake DECIMAL, total_recovered DECIMAL,
--          win_rate_pct DECIMAL

-- View: gold_provider_performance
-- Provider billing, payment, and dispute metrics
-- Columns: provider_npi CHAR, provider_name NVARCHAR, specialty NVARCHAR,
--          facility_name NVARCHAR, total_claims INT, total_disputes INT,
--          total_billed DECIMAL, total_paid DECIMAL, total_underpayment DECIMAL,
--          payment_rate_pct DECIMAL, dispute_rate_pct DECIMAL, total_recovered DECIMAL

-- View: gold_monthly_trends
-- Volume and financial metrics by month (YYYY-MM)
-- Columns: month VARCHAR, claim_count INT, total_billed DECIMAL, total_paid DECIMAL,
--          total_underpayment DECIMAL, recovery_rate_pct DECIMAL, denial_count INT,
--          new_disputes INT, resolved_cases INT, total_awarded DECIMAL
"""

SYSTEM_PROMPT = f"""You are a medical billing data analyst agent. You answer questions by querying
Gold layer tables for a healthcare billing arbitration company.

The Gold layer is the final stage of a Medallion (Bronze -> Silver -> Gold) pipeline.
Data may come from Azure SQL Gold views or Fabric Lakehouse Gold Delta tables — both
use the same table names and T-SQL syntax.

Your job:
1. Understand the user's question about billing, claims, payers, underpayments, arbitration, or deadlines.
2. Write a SQL query (T-SQL) to answer it using ONLY the Gold tables below.
3. Return the SQL inside a tool call.

Rules:
- ONLY query the gold_* tables listed below. Never query other tables.
- Use TOP 50 to limit results unless the user asks for all data.
- Return valid T-SQL syntax.
- For financial amounts, use ROUND() to 2 decimal places.
- If a question cannot be answered from the available tables, explain why.

Available Gold Tables:
{GOLD_SCHEMA}
"""

ANALYSIS_PROMPT = """You are a medical billing data analyst. Given the user's question and the SQL query results,
provide a clear, concise analysis. Include:
- Direct answer to the question
- Key numbers and percentages
- Notable patterns or outliers
- Actionable insights when relevant (e.g., which payers to prioritize for arbitration)

Keep the response focused and professional. Use dollar amounts and percentages.
Do not include SQL in your response. Do not repeat the raw data — summarize it.

At the end of your analysis, suggest 2-3 follow-up questions the user should ask next.
These should be logical next steps that dig deeper into the findings.
Format them as a numbered list under a "Suggested next analyses:" heading."""


# ---------------------------------------------------------------------------
# Common / pre-built analyses catalog
# ---------------------------------------------------------------------------

COMMON_ANALYSES = [
    {
        "id": "executive_summary",
        "name": "Executive Summary",
        "description": "High-level financial KPIs: total billed, paid, underpayment, recovery rate, denial rate",
        "question": "Give me an executive summary of all financial metrics including total billed, total paid, total underpayment, recovery rate, and denial rate."
    },
    {
        "id": "worst_payers",
        "name": "Worst Performing Payers",
        "description": "Payers ranked by underpayment rate and denial rate — prioritize for arbitration",
        "question": "Which payers have the highest underpayment amounts and denial rates? Rank them by risk and suggest which to prioritize for arbitration."
    },
    {
        "id": "arbitration_ready",
        "name": "Arbitration-Ready Claims",
        "description": "Claims eligible for IDR arbitration (underpayment > $25, billed > QPA)",
        "question": "Show me all claims that are eligible for arbitration. How many are there, what is the total potential recovery, and which payers are involved?"
    },
    {
        "id": "cpt_underpayment",
        "name": "CPT Code Underpayment Analysis",
        "description": "Which CPT codes are most underpaid vs Medicare and FAIR Health benchmarks",
        "question": "Which CPT codes have the lowest payment ratios? Compare them against Medicare and FAIR Health rates to identify the biggest gaps."
    },
    {
        "id": "deadline_risk",
        "name": "Deadline Risk Report",
        "description": "At-risk and missed NSA regulatory deadlines — compliance snapshot",
        "question": "Give me a deadline compliance report. How many deadlines are at risk or missed? Which deadline types have the worst compliance? What cases need immediate attention?"
    },
    {
        "id": "case_pipeline",
        "name": "Case Pipeline Status",
        "description": "Active cases by status, SLA compliance, and aging",
        "question": "Show me the full case pipeline: how many cases are in each status, what is the average age, and what is the SLA compliance rate per status?"
    },
    {
        "id": "aging_analysis",
        "name": "Claims Aging Analysis",
        "description": "Claims by aging bucket with unpaid amounts — identify stale claims",
        "question": "Break down claims by aging bucket. How much is unpaid in each bucket? Which bucket has the most financial exposure?"
    },
    {
        "id": "payer_comparison",
        "name": "Payer Risk Comparison",
        "description": "Side-by-side payer scorecard with risk tiers",
        "question": "Compare all payers side by side: payment rate, denial rate, average underpayment, and risk tier. Which payers are high risk?"
    },
    {
        "id": "recovery_opportunity",
        "name": "Recovery Opportunity Assessment",
        "description": "Total recovery opportunity across all underpaid claims and disputes",
        "question": "What is the total recovery opportunity? Break it down by payer, showing how much we could recover from arbitration-eligible claims vs non-eligible. What is the estimated recovery if we win 60% of arbitration cases?"
    },
    {
        "id": "denial_patterns",
        "name": "Denial Pattern Analysis",
        "description": "Denial rates by payer to identify systematic denial behavior",
        "question": "Analyze denial patterns across payers. Which payers have the highest denial rates? Is there a correlation between denial rate and underpayment amount?"
    },
    {
        "id": "win_loss",
        "name": "Win/Loss & ROI Analysis",
        "description": "Arbitration outcomes by payer and dispute type — win rate, awards, ROI",
        "question": "Show me the win/loss record for arbitration cases. Break down by payer and dispute type. What is the overall win rate, total awarded, and ROI (awarded vs disputed amount)?"
    },
    {
        "id": "analyst_performance",
        "name": "Analyst Productivity Report",
        "description": "Cases per analyst, win rate, resolution time, recovery amount",
        "question": "Compare analyst productivity: how many cases does each analyst have, what is their win rate, average resolution time, and total recovery? Who is the top performer?"
    },
    {
        "id": "resolution_time",
        "name": "Time-to-Resolution Analysis",
        "description": "How long cases take by status and priority — identify bottlenecks",
        "question": "Analyze time-to-resolution across case statuses and priorities. Where are the bottlenecks? Which priority levels take longest? What is the average days by status?"
    },
    {
        "id": "provider_analysis",
        "name": "Provider Performance",
        "description": "Which providers generate the most disputes and recoveries",
        "question": "Analyze provider performance: which providers have the most claims, highest dispute rates, and most recovery? Show specialty breakdown and identify which providers drive the most arbitration value."
    },
    {
        "id": "monthly_trends",
        "name": "Monthly Trend Analysis",
        "description": "Volume, financial, and resolution trends over time",
        "question": "Show me monthly trends: claim volume, total billed, total paid, underpayment, new disputes, and resolved cases. Are things improving month over month? What is the recovery rate trend?"
    },
]


def get_common_analyses() -> list:
    """Return the catalog of common pre-built analyses."""
    return COMMON_ANALYSES


def ask_common(analysis_id: str, conversation_history: list = None) -> dict:
    """Run a pre-built common analysis by ID.

    Args:
        analysis_id: One of the IDs from COMMON_ANALYSES.
        conversation_history: Optional prior messages for multi-turn context.

    Returns: Same format as ask().
    """
    analysis = next((a for a in COMMON_ANALYSES if a["id"] == analysis_id), None)
    if not analysis:
        valid_ids = [a["id"] for a in COMMON_ANALYSES]
        return {
            "answer": f"Unknown analysis ID '{analysis_id}'. Valid IDs: {valid_ids}",
            "sql": None,
            "sql_explanation": None,
            "row_count": 0,
            "data": [],
            "model": None,
            "analysis_id": analysis_id,
            "suggested_analyses": valid_ids,
        }

    result = ask(analysis["question"], conversation_history=conversation_history)
    result["analysis_id"] = analysis_id
    result["analysis_name"] = analysis["name"]
    return result

SQL_TOOL = {
    "name": "execute_sql",
    "description": "Execute a T-SQL query against the Gold layer views to answer the user's question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The T-SQL SELECT query to execute against Gold views."
            },
            "explanation": {
                "type": "string",
                "description": "Brief explanation of what this query does and why."
            }
        },
        "required": ["sql", "explanation"]
    }
}


def _get_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    return anthropic.Anthropic(api_key=api_key)


# ---------------------------------------------------------------------------
# Gold data source configuration
# ---------------------------------------------------------------------------
# GOLD_DATA_SOURCE controls where the agent queries Gold data from:
#   "azure_sql"  — Gold SQL views on OLTP database (default, always live)
#   "fabric"     — Gold Delta tables via Fabric SQL analytics endpoint
#
# For Fabric, set FABRIC_SQL_CONNECTION_STRING in environment:
#   Driver={ODBC Driver 18 for SQL Server};
#   Server=<workspace-guid>.datawarehouse.fabric.microsoft.com;
#   Database=<lakehouse-name>;
#   Authentication=ActiveDirectoryServicePrincipal;
#   UID=<service-principal-id>;
#   PWD=<service-principal-secret>;
#   Encrypt=yes;TrustServerCertificate=no;
#
# Or use managed identity on Azure Functions:
#   Authentication=ActiveDirectoryMsi;
# ---------------------------------------------------------------------------

def _get_gold_connection():
    """Get a database connection to the configured Gold data source."""
    import pyodbc
    data_source = os.environ.get("GOLD_DATA_SOURCE", "azure_sql")

    if data_source == "fabric":
        conn_str = os.environ.get("FABRIC_SQL_CONNECTION_STRING")
        if not conn_str:
            raise ValueError(
                "GOLD_DATA_SOURCE=fabric but FABRIC_SQL_CONNECTION_STRING not set. "
                "Set it to your Fabric Lakehouse SQL analytics endpoint connection string."
            )
        return pyodbc.connect(conn_str, autocommit=True)
    else:
        # Default: Azure SQL with Gold views
        from shared.db import get_connection
        return get_connection()


def _get_gold_data_source() -> str:
    """Return which Gold data source is configured."""
    return os.environ.get("GOLD_DATA_SOURCE", "azure_sql")


def _execute_gold_sql(sql: str) -> list:
    """Execute a read-only SQL query against Gold data. Returns list of row dicts.

    Queries either:
    - Azure SQL Gold views (GOLD_DATA_SOURCE=azure_sql, default)
    - Fabric Lakehouse Gold Delta tables (GOLD_DATA_SOURCE=fabric)

    Both use the same table names (gold_*) and same T-SQL syntax.
    """
    sql_stripped = sql.strip().rstrip(";").strip()
    sql_upper = sql_stripped.upper()

    # Safety: only allow SELECT statements against gold_ views
    if not sql_upper.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed")

    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "EXEC", "TRUNCATE", "MERGE"]
    for keyword in blocked:
        if keyword in sql_upper.split():
            raise ValueError(f"Blocked SQL keyword: {keyword}")

    conn = _get_gold_connection()
    cursor = conn.cursor()
    cursor.execute(sql_stripped)

    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = cursor.fetchall()

    results = []
    for row in rows:
        row_dict = {}
        for col_name, val in zip(columns, row):
            if isinstance(val, Decimal):
                row_dict[col_name] = float(val)
            elif isinstance(val, (datetime, date)):
                row_dict[col_name] = val.isoformat()
            else:
                row_dict[col_name] = val
        results.append(row_dict)

    return results


def ask(question: str, conversation_history: list = None) -> dict:
    """Main entry point: ask a question, get an analysis.

    Args:
        question: Natural language question about billing data.
        conversation_history: Optional prior messages for multi-turn context.

    Returns:
        {
            "answer": str,          # Human-readable analysis
            "sql": str,             # The SQL that was executed
            "sql_explanation": str,  # Why this query was chosen
            "row_count": int,       # Number of result rows
            "data": list,     # Raw query results (max 50 rows)
            "model": str            # Claude model used
        }
    """
    client = _get_client()
    model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")

    messages = []
    if conversation_history:
        messages.extend(conversation_history)
    messages.append({"role": "user", "content": question})

    # Step 1: Generate SQL via tool use
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        tools=[SQL_TOOL],
        messages=messages,
    )

    sql = None
    sql_explanation = None
    text_response = None

    for block in response.content:
        if block.type == "tool_use" and block.name == "execute_sql":
            sql = block.input["sql"]
            sql_explanation = block.input.get("explanation", "")
        elif block.type == "text":
            text_response = block.text

    # If Claude didn't generate SQL, return the text explanation
    if not sql:
        return {
            "answer": text_response or "I couldn't determine how to answer that from the available data.",
            "sql": None,
            "sql_explanation": None,
            "row_count": 0,
            "data": [],
            "model": model,
        }

    # Step 2: Execute the SQL
    try:
        results = _execute_gold_sql(sql)
    except Exception as e:
        logger.error("SQL execution failed: %s | Query: %s", e, sql)
        return {
            "answer": f"I generated a query but it failed to execute: {e}",
            "sql": sql,
            "sql_explanation": sql_explanation,
            "row_count": 0,
            "data": [],
            "model": model,
            "error": str(e),
        }

    # Step 3: Generate human-readable analysis from the results
    results_text = json.dumps(results[:50], indent=2, default=str)

    analysis_response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=ANALYSIS_PROMPT,
        messages=[
            {"role": "user", "content": (
                f"Question: {question}\n\n"
                f"Query explanation: {sql_explanation}\n\n"
                f"Results ({len(results)} rows):\n{results_text}"
            )}
        ],
    )

    answer = analysis_response.content[0].text

    # Step 4: Audit log
    _log_agent_invocation(question, sql, len(results), model)

    # Step 5: Suggest relevant common analyses based on the question
    suggested = _suggest_next_analyses(question, answer)

    return {
        "answer": answer,
        "sql": sql,
        "sql_explanation": sql_explanation,
        "row_count": len(results),
        "data": results[:50],
        "model": model,
        "data_source": _get_gold_data_source(),
        "suggested_analyses": suggested,
    }


def _suggest_next_analyses(question: str, answer: str) -> list:
    """Pick 3 common analyses most relevant as follow-ups to the current question."""
    q_lower = question.lower() + " " + answer.lower()

    # Keyword affinity scoring per analysis
    affinity = {
        "executive_summary": ["summary", "overview", "total", "kpi", "financial"],
        "worst_payers": ["payer", "underpay", "denial", "worst", "risk"],
        "arbitration_ready": ["arbitration", "eligible", "idr", "recover", "dispute"],
        "cpt_underpayment": ["cpt", "code", "procedure", "medicare", "fair health", "benchmark"],
        "deadline_risk": ["deadline", "compliance", "sla", "at risk", "missed", "overdue"],
        "case_pipeline": ["case", "pipeline", "status", "open", "review", "negotiation"],
        "aging_analysis": ["aging", "old", "stale", "bucket", "days"],
        "payer_comparison": ["compare", "payer", "scorecard", "side by side", "tier"],
        "recovery_opportunity": ["recovery", "opportunity", "potential", "win", "estimate"],
        "denial_patterns": ["denial", "pattern", "denied", "reject"],
        "win_loss": ["win", "loss", "outcome", "award", "roi", "decided"],
        "analyst_performance": ["analyst", "productivity", "workload", "staff", "team"],
        "resolution_time": ["resolution", "cycle", "time", "days", "bottleneck", "slow"],
        "provider_analysis": ["provider", "doctor", "npi", "specialty", "facility"],
        "monthly_trends": ["trend", "month", "growth", "over time", "improving"],
    }

    scores = []
    for analysis in COMMON_ANALYSES:
        keywords = affinity.get(analysis["id"], [])
        score = sum(1 for kw in keywords if kw in q_lower)
        # Slightly penalize the analysis that most closely matches the current question
        # to avoid suggesting what was just asked
        if score > 2:
            score -= 1
        scores.append((score, analysis))

    # Sort by score ascending (least overlap = best follow-up), then pick top 3
    # that have at least some relevance but aren't the same topic
    scores.sort(key=lambda x: x[0])
    suggested = []
    for score, analysis in scores:
        if len(suggested) >= 3:
            break
        suggested.append({
            "id": analysis["id"],
            "name": analysis["name"],
            "description": analysis["description"],
        })

    return suggested


def _log_agent_invocation(question: str, sql: str, row_count: int, model: str):
    """Log the agent invocation to audit_log for compliance."""
    try:
        from shared.db import execute_query
        execute_query(
            """INSERT INTO audit_log
               (entity_type, entity_id, action, user_id, old_value, new_value, ai_agent, ai_model_version)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("gold_query", "agent", "ai_generated", "agent_analyst",
             question[:500], json.dumps({"sql": sql, "rows": row_count}),
             "data_analyst", model),
            commit=True,
        )
    except Exception as e:
        logger.warning("Failed to log agent invocation: %s", e)
