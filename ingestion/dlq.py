"""Dead-Letter Queue — captures failed/rejected records for investigation.

In production, this would use Azure Service Bus DLQ.
For testing, we store in a SQLite table with auto-purge after retention period.
"""

import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

DLQ_SCHEMA = """
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    dlq_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT,
    error_category  TEXT NOT NULL,
    error_detail    TEXT NOT NULL,
    payload_ref     TEXT,
    created_at      TEXT NOT NULL,
    resolved_at     TEXT,
    resolved_by     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
);
"""

ERROR_CATEGORIES = {
    "parse_failure": "Malformed source data — do not retry automatically",
    "validation_failure": "Missing required field or invalid value — analyst can fix and resubmit",
    "transient_failure": "Temporary infrastructure error — auto-retry eligible",
    "unmatched_reference": "References unknown entity — analyst matches manually",
}


def init_dlq(conn: sqlite3.Connection):
    conn.executescript(DLQ_SCHEMA)
    conn.commit()


def send_to_dlq(conn: sqlite3.Connection, source: str, entity_type: str,
                 error_category: str, error_detail: str,
                 entity_id: str = None, payload: dict = None):
    """Route a failed record to the dead-letter queue."""
    payload_ref = json.dumps(payload, default=str) if payload else None
    conn.execute("""
        INSERT INTO dead_letter_queue
            (source, entity_type, entity_id, error_category, error_detail,
             payload_ref, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
    """, (source, entity_type, entity_id, error_category, error_detail,
          payload_ref, datetime.utcnow().isoformat()))
    logger.warning("DLQ: %s %s/%s — %s", error_category, entity_type,
                   entity_id or "unknown", error_detail)


def resolve_dlq_entry(conn: sqlite3.Connection, dlq_id: int,
                      resolved_by: str = "system"):
    conn.execute("""
        UPDATE dead_letter_queue
        SET status = 'resolved', resolved_at = ?, resolved_by = ?
        WHERE dlq_id = ?
    """, (datetime.utcnow().isoformat(), resolved_by, dlq_id))


def get_pending_dlq(conn: sqlite3.Connection, source: str = None) -> list:
    if source:
        rows = conn.execute(
            "SELECT * FROM dead_letter_queue WHERE status = 'pending' AND source = ?",
            (source,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM dead_letter_queue WHERE status = 'pending'"
        ).fetchall()
    return [dict(r) for r in rows]
