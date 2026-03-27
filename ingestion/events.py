"""Event emission — Pipeline Stage 5.

In production, events are published to Azure Event Hub for downstream consumers
(workflow engine, CDC triggers, notification services).
For testing, events are stored in a SQLite table.
"""

import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_log (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    payload     TEXT,
    emitted_at  TEXT NOT NULL
);
"""


def init_events(conn: sqlite3.Connection):
    conn.executescript(EVENTS_SCHEMA)
    conn.commit()


def emit_event(conn: sqlite3.Connection, event_type: str, entity_type: str,
               entity_id: str, payload: dict = None):
    """Emit a downstream event. Only called on insert/update — never on duplicate skip."""
    conn.execute("""
        INSERT INTO event_log (event_type, entity_type, entity_id, payload, emitted_at)
        VALUES (?, ?, ?, ?, ?)
    """, (event_type, entity_type, entity_id,
          json.dumps(payload, default=str) if payload else None,
          datetime.utcnow().isoformat()))
    logger.info("Event: %s %s/%s", event_type, entity_type, entity_id)


def get_events(conn: sqlite3.Connection, entity_type: str = None,
               event_type: str = None) -> list:
    query = "SELECT * FROM event_log WHERE 1=1"
    params = []
    if entity_type:
        query += " AND entity_type = ?"
        params.append(entity_type)
    if event_type:
        query += " AND event_type = ?"
        params.append(event_type)
    query += " ORDER BY event_id"
    return [dict(r) for r in conn.execute(query, params).fetchall()]
