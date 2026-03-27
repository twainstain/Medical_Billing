"""Audit logging — every ingestion action is recorded."""

import sqlite3
from datetime import datetime


def log_action(conn: sqlite3.Connection, entity_type: str, entity_id: str,
               action: str, user_id: str = "system",
               old_value: str = None, new_value: str = None):
    conn.execute("""
        INSERT INTO audit_log (entity_type, entity_id, action, user_id,
                               timestamp, old_value, new_value)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (entity_type, entity_id, action, user_id,
          datetime.utcnow().isoformat(), old_value, new_value))
