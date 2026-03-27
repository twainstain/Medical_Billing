"""Durable Function Orchestrators — Workflow Engine.

Implements the Workflow & Event Engine (Architecture Section 9):
  - Claim-to-Dispute pipeline: detects underpayments, creates disputes/cases
  - Case lifecycle: open → review → negotiation → IDR → decision
  - Deadline management: sets NSA regulatory deadlines per dispute

Uses Azure Durable Functions (fan-out/fan-in pattern) for reliable,
resumable orchestration with built-in retry and state persistence.
"""

import azure.functions as func
import azure.durable_functions as df
from datetime import timedelta


# ---------------------------------------------------------------------------
# Orchestrator: Claim-to-Dispute Pipeline
# Triggered when new claims are ingested with underpayment detected
# ---------------------------------------------------------------------------

def claim_to_dispute_orchestrator(context: df.DurableOrchestrationContext):
    """Orchestrate the claim-to-dispute workflow.

    1. Detect underpaid claims (billed > paid)
    2. For each underpaid claim: create dispute + case + deadlines
    3. Assign analyst based on priority routing rules
    4. Send notification to assigned analyst
    """
    input_data = context.get_input()
    claim_ids = input_data.get("claim_ids", [])

    if not claim_ids:
        # Detect all underpaid claims without existing disputes
        underpaid = yield context.call_activity("detect_underpaid_claims", None)
        claim_ids = [c["claim_id"] for c in underpaid]

    if not claim_ids:
        return {"status": "no_underpaid_claims", "cases_created": 0}

    # Fan-out: create dispute+case for each underpaid claim in parallel
    tasks = []
    for claim_id in claim_ids:
        tasks.append(context.call_activity("create_dispute_and_case", claim_id))

    results = yield context.task_all(tasks)

    # Aggregate results
    cases_created = sum(1 for r in results if r.get("status") == "created")
    case_ids = [r["case_id"] for r in results if r.get("case_id")]

    # Fan-out: set regulatory deadlines for each new case
    deadline_tasks = []
    for result in results:
        if result.get("status") == "created":
            deadline_tasks.append(
                context.call_activity("set_regulatory_deadlines", {
                    "case_id": result["case_id"],
                    "dispute_id": result["dispute_id"],
                    "filed_date": result["filed_date"]
                })
            )

    if deadline_tasks:
        yield context.task_all(deadline_tasks)

    # Send notifications
    for result in results:
        if result.get("status") == "created":
            yield context.call_activity("send_analyst_notification", {
                "case_id": result["case_id"],
                "analyst": result["analyst"],
                "priority": result["priority"],
                "claim_id": result["claim_id"]
            })

    return {
        "status": "completed",
        "cases_created": cases_created,
        "case_ids": case_ids
    }


# ---------------------------------------------------------------------------
# Orchestrator: Deadline Monitor
# Runs on a timer to check upcoming/missed deadlines
# ---------------------------------------------------------------------------

def deadline_monitor_orchestrator(context: df.DurableOrchestrationContext):
    """Monitor deadlines and trigger alerts.

    1. Find deadlines due within 5 days (at-risk)
    2. Find missed deadlines
    3. Send alerts for at-risk deadlines
    4. Update status of missed deadlines
    5. Escalate critical missed deadlines
    """
    # Get at-risk and missed deadlines
    at_risk = yield context.call_activity("get_at_risk_deadlines", {"days_ahead": 5})
    missed = yield context.call_activity("get_missed_deadlines", None)

    # Send alerts for at-risk (parallel)
    alert_tasks = []
    for deadline in at_risk:
        alert_tasks.append(
            context.call_activity("send_deadline_alert", {
                "deadline_id": deadline["deadline_id"],
                "case_id": deadline["case_id"],
                "type": deadline["type"],
                "due_date": deadline["due_date"],
                "days_remaining": deadline["days_remaining"],
                "severity": "warning"
            })
        )

    # Mark missed deadlines (parallel)
    missed_tasks = []
    for deadline in missed:
        missed_tasks.append(
            context.call_activity("mark_deadline_missed", {
                "deadline_id": deadline["deadline_id"],
                "case_id": deadline["case_id"]
            })
        )

    if alert_tasks:
        yield context.task_all(alert_tasks)
    if missed_tasks:
        yield context.task_all(missed_tasks)

    # Escalate critical missed deadlines (IDR initiation, decision)
    critical_missed = [
        d for d in missed
        if d.get("type") in ("idr_initiation", "idr_decision", "evidence_submission")
    ]
    if critical_missed:
        yield context.call_activity("escalate_missed_deadlines", critical_missed)

    return {
        "at_risk_count": len(at_risk),
        "missed_count": len(missed),
        "escalated_count": len(critical_missed),
        "alerts_sent": len(alert_tasks)
    }


# ---------------------------------------------------------------------------
# Orchestrator: Case Status Transition
# Handles state machine transitions with validation
# ---------------------------------------------------------------------------

def case_transition_orchestrator(context: df.DurableOrchestrationContext):
    """Manage case status transitions with validation and side effects.

    Valid transitions (NSA workflow):
      open → in_review → negotiation → idr_initiated → idr_submitted → decided → closed

    Each transition triggers:
      - Validation (required evidence, deadlines met)
      - Status update in OLTP
      - Audit log entry
      - Deadline updates
      - Notifications
    """
    input_data = context.get_input()
    case_id = input_data["case_id"]
    target_status = input_data["target_status"]
    user_id = input_data.get("user_id", "system")

    # Validate the transition
    validation = yield context.call_activity("validate_case_transition", {
        "case_id": case_id,
        "target_status": target_status
    })

    if not validation.get("valid"):
        return {
            "status": "rejected",
            "reason": validation.get("reason"),
            "case_id": case_id
        }

    # Execute the transition
    result = yield context.call_activity("execute_case_transition", {
        "case_id": case_id,
        "target_status": target_status,
        "user_id": user_id
    })

    # Side effects based on target status
    if target_status == "negotiation":
        # Mark open_negotiation deadline as met
        yield context.call_activity("complete_deadline", {
            "case_id": case_id,
            "deadline_type": "open_negotiation"
        })

    elif target_status == "idr_initiated":
        yield context.call_activity("complete_deadline", {
            "case_id": case_id,
            "deadline_type": "idr_initiation"
        })
        # Set evidence submission deadline (10 business days)
        yield context.call_activity("set_regulatory_deadlines", {
            "case_id": case_id,
            "dispute_id": result.get("dispute_id"),
            "deadline_types": ["evidence_submission"]
        })

    elif target_status == "decided":
        yield context.call_activity("complete_deadline", {
            "case_id": case_id,
            "deadline_type": "idr_decision"
        })

    elif target_status == "closed":
        # Mark all remaining deadlines as met
        yield context.call_activity("close_all_deadlines", {"case_id": case_id})

    # Audit log
    yield context.call_activity("write_audit_log", {
        "entity_type": "case",
        "entity_id": case_id,
        "action": "status_change",
        "user_id": user_id,
        "old_value": validation.get("current_status"),
        "new_value": target_status
    })

    return {
        "status": "completed",
        "case_id": case_id,
        "new_status": target_status
    }
