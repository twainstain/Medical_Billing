"""Durable Function Activities — individual steps in workflow orchestrations.

Each activity is an atomic unit of work: database queries, status updates,
notifications. They run independently and are retried automatically on failure.
"""

import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Priority routing: assigns analysts based on underpayment severity
ANALYST_ROUTING = {
    "high": "analyst_kpatel",       # $500+ underpayment
    "medium": "analyst_mchen",      # $200-$500
    "low": "analyst_jsmith",        # <$200
}

# NSA regulatory deadlines (days from filed_date)
NSA_DEADLINES = {
    "open_negotiation": 30,     # 30 days for open negotiation
    "idr_initiation": 34,       # 4 business days after negotiation
    "evidence_submission": 44,  # 10 business days after IDR initiation
    "entity_selection": 37,     # 3 business days after IDR initiation
    "idr_decision": 64,         # 30 days after IDR initiation
}

# Valid case status transitions
VALID_TRANSITIONS = {
    "open": ["in_review"],
    "in_review": ["negotiation", "closed"],
    "negotiation": ["idr_initiated", "closed"],
    "idr_initiated": ["idr_submitted", "closed"],
    "idr_submitted": ["decided", "closed"],
    "decided": ["closed"],
}


def _get_db_connection():
    """Get a pyodbc connection to Azure SQL."""
    import pyodbc
    conn_str = os.environ.get("SQL_CONNECTION_STRING")
    return pyodbc.connect(conn_str, autocommit=False)


# ---------------------------------------------------------------------------
# Activity: Detect underpaid claims without existing disputes
# ---------------------------------------------------------------------------

def detect_underpaid_claims(input_data) -> list:
    """Find claims where billed > paid and no dispute exists yet."""
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.claim_id, c.payer_id, c.total_billed,
                   c.date_of_service, c.date_filed,
                   COALESCE(r.total_paid, 0) AS total_paid,
                   COALESCE(r.total_allowed, 0) AS total_allowed
            FROM claims c
            LEFT JOIN (
                SELECT claim_id, SUM(paid_amount) AS total_paid,
                       SUM(allowed_amount) AS total_allowed
                FROM remittances GROUP BY claim_id
            ) r ON c.claim_id = r.claim_id
            WHERE c.total_billed > COALESCE(r.total_paid, 0)
              AND NOT EXISTS (
                  SELECT 1 FROM disputes d WHERE d.claim_id = c.claim_id
              )
        """)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Create dispute + case for an underpaid claim
# ---------------------------------------------------------------------------

def create_dispute_and_case(claim_id: str) -> dict:
    """Create a case, dispute, and initial audit entry for an underpaid claim."""
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()

        # Get claim details
        cursor.execute("""
            SELECT c.claim_id, c.payer_id, c.total_billed, c.date_filed,
                   COALESCE(r.total_paid, 0) AS total_paid,
                   COALESCE(r.total_allowed, 0) AS total_allowed
            FROM claims c
            LEFT JOIN (
                SELECT claim_id, SUM(paid_amount) AS total_paid,
                       SUM(allowed_amount) AS total_allowed
                FROM remittances GROUP BY claim_id
            ) r ON c.claim_id = r.claim_id
            WHERE c.claim_id = ?
        """, (claim_id,))

        row = cursor.fetchone()
        if not row:
            return {"status": "not_found", "claim_id": claim_id}

        columns = [desc[0] for desc in cursor.description]
        claim = dict(zip(columns, row))

        total_billed = float(claim["total_billed"])
        total_paid = float(claim["total_paid"])
        total_allowed = float(claim["total_allowed"])
        underpayment = total_billed - total_paid

        # Priority routing
        if underpayment > 500:
            priority = "high"
        elif underpayment > 200:
            priority = "medium"
        else:
            priority = "low"

        analyst = ANALYST_ROUTING[priority]
        now = datetime.utcnow()
        filed_date = now.strftime("%Y-%m-%d")

        # QPA approximation
        qpa_amount = total_allowed if total_allowed > 0 else round(total_billed * 0.6, 2)

        dispute_type = "underpayment" if total_paid > 0 else "denial"

        # Create case
        cursor.execute("""
            INSERT INTO cases (assigned_analyst, status, priority, created_date, last_activity)
            OUTPUT INSERTED.case_id
            VALUES (?, 'open', ?, SYSUTCDATETIME(), SYSUTCDATETIME())
        """, (analyst, priority))
        case_id = cursor.fetchone()[0]

        # Create dispute
        cursor.execute("""
            INSERT INTO disputes
                (claim_id, case_id, dispute_type, status,
                 billed_amount, paid_amount, requested_amount, qpa_amount,
                 filed_date, open_negotiation_deadline, idr_initiation_deadline)
            OUTPUT INSERTED.dispute_id
            VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?)
        """, (claim_id, case_id, dispute_type,
              total_billed, total_paid, total_billed, qpa_amount,
              filed_date,
              (now + timedelta(days=30)).strftime("%Y-%m-%d"),
              (now + timedelta(days=34)).strftime("%Y-%m-%d")))
        dispute_id = cursor.fetchone()[0]

        # Update claim status
        cursor.execute(
            "UPDATE claims SET status = 'in_dispute', updated_at = SYSUTCDATETIME() WHERE claim_id = ?",
            (claim_id,)
        )

        conn.commit()

        return {
            "status": "created",
            "claim_id": claim_id,
            "case_id": case_id,
            "dispute_id": dispute_id,
            "analyst": analyst,
            "priority": priority,
            "underpayment": underpayment,
            "filed_date": filed_date
        }

    except Exception as e:
        conn.rollback()
        logger.error("Failed to create dispute for %s: %s", claim_id, e)
        return {"status": "error", "claim_id": claim_id, "error": str(e)}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Set regulatory deadlines per NSA timeline
# ---------------------------------------------------------------------------

def set_regulatory_deadlines(input_data: dict) -> dict:
    """Create NSA regulatory deadlines for a dispute.

    Default: open_negotiation (30d), idr_initiation (34d), idr_decision (64d)
    Optional: evidence_submission (44d), entity_selection (37d)
    """
    case_id = input_data["case_id"]
    dispute_id = input_data["dispute_id"]
    filed_date_str = input_data.get("filed_date", datetime.utcnow().strftime("%Y-%m-%d"))
    deadline_types = input_data.get("deadline_types",
                                     ["open_negotiation", "idr_initiation", "idr_decision"])

    filed_date = datetime.strptime(filed_date_str, "%Y-%m-%d")
    conn = _get_db_connection()

    try:
        cursor = conn.cursor()
        created = 0

        for dl_type in deadline_types:
            days = NSA_DEADLINES.get(dl_type)
            if days is None:
                continue

            due_date = (filed_date + timedelta(days=days)).strftime("%Y-%m-%d")

            # Skip if deadline already exists
            cursor.execute(
                "SELECT 1 FROM deadlines WHERE case_id = ? AND dispute_id = ? AND type = ?",
                (case_id, dispute_id, dl_type)
            )
            if cursor.fetchone():
                continue

            cursor.execute("""
                INSERT INTO deadlines (case_id, dispute_id, type, due_date, status)
                VALUES (?, ?, ?, ?, 'pending')
            """, (case_id, dispute_id, dl_type, due_date))
            created += 1

        conn.commit()
        return {"case_id": case_id, "deadlines_created": created}

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Get at-risk deadlines (due within N days)
# ---------------------------------------------------------------------------

def get_at_risk_deadlines(input_data: dict) -> list:
    days_ahead = input_data.get("days_ahead", 5)
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT d.deadline_id, d.case_id, d.dispute_id, d.type,
                   CAST(d.due_date AS VARCHAR) AS due_date,
                   DATEDIFF(DAY, CAST(SYSUTCDATETIME() AS DATE), d.due_date) AS days_remaining,
                   c.assigned_analyst
            FROM deadlines d
            JOIN cases c ON d.case_id = c.case_id
            WHERE d.status = 'pending'
              AND d.due_date BETWEEN CAST(SYSUTCDATETIME() AS DATE)
                                 AND DATEADD(DAY, ?, CAST(SYSUTCDATETIME() AS DATE))
        """, (days_ahead,))
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Get missed deadlines
# ---------------------------------------------------------------------------

def get_missed_deadlines(input_data) -> list:
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT d.deadline_id, d.case_id, d.dispute_id, d.type,
                   CAST(d.due_date AS VARCHAR) AS due_date,
                   c.assigned_analyst
            FROM deadlines d
            JOIN cases c ON d.case_id = c.case_id
            WHERE d.status = 'pending'
              AND d.due_date < CAST(SYSUTCDATETIME() AS DATE)
        """)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Mark deadline as missed
# ---------------------------------------------------------------------------

def mark_deadline_missed(input_data: dict) -> dict:
    deadline_id = input_data["deadline_id"]
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE deadlines SET status = 'missed' WHERE deadline_id = ? AND status = 'pending'",
            (deadline_id,)
        )
        conn.commit()
        return {"deadline_id": deadline_id, "status": "missed"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Complete a deadline
# ---------------------------------------------------------------------------

def complete_deadline(input_data: dict) -> dict:
    case_id = input_data["case_id"]
    deadline_type = input_data["deadline_type"]
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE deadlines
            SET status = 'met', completed_date = SYSUTCDATETIME()
            WHERE case_id = ? AND type = ? AND status IN ('pending', 'alerted')
        """, (case_id, deadline_type))
        conn.commit()
        return {"case_id": case_id, "type": deadline_type, "status": "met"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Close all pending deadlines for a case
# ---------------------------------------------------------------------------

def close_all_deadlines(input_data: dict) -> dict:
    case_id = input_data["case_id"]
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE deadlines
            SET status = 'met', completed_date = SYSUTCDATETIME()
            WHERE case_id = ? AND status IN ('pending', 'alerted')
        """, (case_id,))
        updated = cursor.rowcount
        conn.commit()
        return {"case_id": case_id, "deadlines_closed": updated}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Validate case status transition
# ---------------------------------------------------------------------------

def validate_case_transition(input_data: dict) -> dict:
    case_id = input_data["case_id"]
    target_status = input_data["target_status"]

    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM cases WHERE case_id = ?", (case_id,))
        row = cursor.fetchone()
        if not row:
            return {"valid": False, "reason": f"Case {case_id} not found"}

        current_status = row[0]
        allowed = VALID_TRANSITIONS.get(current_status, [])

        if target_status not in allowed:
            return {
                "valid": False,
                "reason": f"Cannot transition from '{current_status}' to '{target_status}'. "
                          f"Allowed: {allowed}",
                "current_status": current_status
            }

        return {"valid": True, "current_status": current_status}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Execute case status transition
# ---------------------------------------------------------------------------

def execute_case_transition(input_data: dict) -> dict:
    case_id = input_data["case_id"]
    target_status = input_data["target_status"]

    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE cases
            SET status = ?, last_activity = SYSUTCDATETIME()
            WHERE case_id = ?
        """, (target_status, case_id))

        # If closing, set outcome
        if target_status == "closed":
            cursor.execute("""
                UPDATE cases SET closed_date = SYSUTCDATETIME() WHERE case_id = ?
            """, (case_id,))

        # Get dispute_id for this case
        cursor.execute(
            "SELECT TOP 1 dispute_id FROM disputes WHERE case_id = ?", (case_id,)
        )
        dispute_row = cursor.fetchone()
        dispute_id = dispute_row[0] if dispute_row else None

        conn.commit()
        return {"case_id": case_id, "status": target_status, "dispute_id": dispute_id}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Activity: Send deadline alert (notification stub)
# ---------------------------------------------------------------------------

def send_deadline_alert(input_data: dict) -> dict:
    """Send alert for at-risk deadline.

    In production: sends to Azure Communication Services (email),
    Microsoft Teams webhook, or Azure Event Grid for downstream processing.
    """
    logger.warning(
        "DEADLINE ALERT [%s]: Case %s — %s due %s (%d days remaining)",
        input_data.get("severity", "warning"),
        input_data["case_id"],
        input_data["type"],
        input_data["due_date"],
        input_data.get("days_remaining", 0)
    )

    # Update deadline status to 'alerted'
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE deadlines
            SET status = 'alerted', alerted_date = SYSUTCDATETIME()
            WHERE deadline_id = ? AND status = 'pending'
        """, (input_data["deadline_id"],))
        conn.commit()
    finally:
        conn.close()

    return {"deadline_id": input_data["deadline_id"], "alerted": True}


# ---------------------------------------------------------------------------
# Activity: Escalate missed deadlines
# ---------------------------------------------------------------------------

def escalate_missed_deadlines(deadlines: list) -> dict:
    """Escalate critical missed deadlines to supervisor.

    In production: creates high-priority notification via Azure Event Grid,
    updates case priority to 'critical', and logs to compliance audit trail.
    """
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        for dl in deadlines:
            # Escalate case priority
            cursor.execute("""
                UPDATE cases SET priority = 'critical', last_activity = SYSUTCDATETIME()
                WHERE case_id = ?
            """, (dl["case_id"],))

            logger.critical(
                "ESCALATION: Missed %s deadline for case %s (due %s)",
                dl["type"], dl["case_id"], dl["due_date"]
            )
        conn.commit()
    finally:
        conn.close()

    return {"escalated": len(deadlines)}


# ---------------------------------------------------------------------------
# Activity: Send analyst notification
# ---------------------------------------------------------------------------

def send_analyst_notification(input_data: dict) -> dict:
    """Notify assigned analyst of new case.

    In production: Microsoft Teams adaptive card or email via
    Azure Communication Services.
    """
    logger.info(
        "NOTIFICATION: New %s-priority case %s assigned to %s (claim: %s)",
        input_data["priority"], input_data["case_id"],
        input_data["analyst"], input_data["claim_id"]
    )
    return {"notified": input_data["analyst"], "case_id": input_data["case_id"]}


# ---------------------------------------------------------------------------
# Activity: Write audit log entry
# ---------------------------------------------------------------------------

def write_audit_log(input_data: dict) -> dict:
    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO audit_log
                (entity_type, entity_id, action, user_id, old_value, new_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            input_data["entity_type"],
            str(input_data["entity_id"]),
            input_data["action"],
            input_data.get("user_id", "system"),
            input_data.get("old_value"),
            input_data.get("new_value")
        ))
        conn.commit()
        return {"logged": True}
    finally:
        conn.close()
