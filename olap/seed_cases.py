"""Seed OLTP with disputes, cases, and deadlines from claims data.

Simulates the Workflow & Event Engine (Section 9): when an underpayment
is detected, the system creates a dispute, assigns it to a case, and
sets regulatory deadlines per the No Surprises Act (NSA).

This module bridges the gap between the ingestion layer (which populates
claims/remittances) and the OLAP layer (which needs disputes/cases for
the Case Pipeline and Deadline Compliance dashboards).
"""

import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def seed_disputes_and_cases(oltp_conn: sqlite3.Connection) -> dict:
    """Generate disputes, cases, and deadlines from underpaid claims.

    For each claim where billed > paid, creates:
      - A case (assigned to an analyst, with status/priority)
      - A dispute (with QPA estimate and underpayment calculation)
      - Three deadlines (open negotiation, IDR initiation, IDR decision)

    Returns: {"cases": N, "disputes": N, "deadlines": N}
    """
    claims = oltp_conn.execute("""
        SELECT c.claim_id, c.payer_id, c.total_billed, c.date_of_service,
               c.date_filed,
               COALESCE(r.total_paid, 0) AS total_paid,
               COALESCE(r.total_allowed, 0) AS total_allowed
        FROM claims c
        LEFT JOIN (
            SELECT claim_id, SUM(paid_amount) AS total_paid,
                   SUM(allowed_amount) AS total_allowed
            FROM remittances GROUP BY claim_id
        ) r ON c.claim_id = r.claim_id
        WHERE c.total_billed > COALESCE(r.total_paid, 0)
    """).fetchall()

    if not claims:
        return {"cases": 0, "disputes": 0, "deadlines": 0}

    now = datetime.utcnow()
    case_count = 0
    dispute_count = 0
    deadline_count = 0

    analysts = ["analyst_jsmith", "analyst_mchen", "analyst_kpatel"]

    for i, claim in enumerate(claims):
        claim_id = claim["claim_id"]
        payer_id = claim["payer_id"]
        total_billed = claim["total_billed"]
        total_paid = claim["total_paid"]
        total_allowed = claim["total_allowed"]
        underpayment = total_billed - total_paid

        # Determine priority by underpayment amount
        if underpayment > 500:
            priority = "high"
        elif underpayment > 200:
            priority = "medium"
        else:
            priority = "low"

        # Vary created_date so cases have different ages
        created_offset = (hash(claim_id) % 60) + 5
        created_date = (now - timedelta(days=created_offset)).strftime("%Y-%m-%d")

        # Assign status — distribute across open/in_review/resolved
        bucket = hash(claim_id) % 7
        if bucket <= 1:
            status = "resolved"
            outcome = "provider_award" if bucket == 0 else "payer_award"
            award_amount = round(underpayment * (0.75 if bucket == 0 else 0.30), 2)
        elif bucket <= 3:
            status = "in_review"
            outcome = None
            award_amount = None
        else:
            status = "open"
            outcome = None
            award_amount = None

        analyst = analysts[i % len(analysts)]
        case_id = f"CASE-{claim_id}"

        oltp_conn.execute("""
            INSERT OR IGNORE INTO cases
                (case_id, assigned_analyst, status, priority, created_date,
                 last_activity_date, outcome, award_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (case_id, analyst, status, priority, created_date,
              now.strftime("%Y-%m-%d"), outcome, award_amount))
        case_count += 1

        # Create dispute
        dispute_id = f"DISP-{claim_id}"
        dispute_type = "underpayment" if total_paid > 0 else "denial"
        dispute_status = "resolved" if status == "resolved" else "open"

        # QPA approximation: use allowed_amount if available, else 60% of billed
        qpa_amount = total_allowed if total_allowed > 0 else round(total_billed * 0.6, 2)

        filed_date = created_date
        filed_dt = datetime.strptime(filed_date, "%Y-%m-%d")
        # NSA: 30-day open negotiation, then 4 business days to initiate IDR
        open_neg_deadline = (filed_dt + timedelta(days=30)).strftime("%Y-%m-%d")
        idr_init_deadline = (filed_dt + timedelta(days=34)).strftime("%Y-%m-%d")

        oltp_conn.execute("""
            INSERT OR IGNORE INTO disputes
                (dispute_id, claim_id, case_id, dispute_type, status,
                 billed_amount, paid_amount, requested_amount, qpa_amount,
                 underpayment_amount, filed_date,
                 open_negotiation_deadline, idr_initiation_deadline)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (dispute_id, claim_id, case_id, dispute_type, dispute_status,
              total_billed, total_paid, total_billed, qpa_amount,
              underpayment, filed_date, open_neg_deadline, idr_init_deadline))
        dispute_count += 1

        # Create deadlines
        idr_decision_deadline = (filed_dt + timedelta(days=64)).strftime("%Y-%m-%d")
        deadlines_spec = [
            ("open_negotiation", open_neg_deadline),
            ("idr_initiation", idr_init_deadline),
            ("idr_decision", idr_decision_deadline),
        ]

        for dl_type, due_date in deadlines_spec:
            due_dt = datetime.strptime(due_date, "%Y-%m-%d")
            if status == "resolved":
                dl_status = "met"
                completed = now.strftime("%Y-%m-%d")
            elif due_dt < now:
                dl_status = "missed"
                completed = None
            elif (due_dt - now).days <= 5:
                dl_status = "at_risk"
                completed = None
            else:
                dl_status = "pending"
                completed = None

            oltp_conn.execute("""
                INSERT INTO deadlines
                    (case_id, dispute_id, type, due_date, status, completed_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (case_id, dispute_id, dl_type, due_date, dl_status, completed))
            deadline_count += 1

    oltp_conn.commit()
    summary = {"cases": case_count, "disputes": dispute_count, "deadlines": deadline_count}
    logger.info("Seeded disputes & cases: %s", summary)
    return summary
