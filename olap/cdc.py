"""CDC Watermark Tracking — simulates Azure SQL Change Data Capture.

In production: Azure SQL CDC (sys.sp_cdc_enable_table) tracks row-level
changes automatically. Fabric Pipeline reads the change feed incrementally.

Here we simulate CDC with content-hash comparison and watermark tracking,
so the Bronze layer only processes new or changed rows on re-runs.

See: Section 8 of medical_billing_arbitration_future_architecture.md
"""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

WATERMARK_SCHEMA = """
CREATE TABLE IF NOT EXISTS cdc_watermarks (
    source_table    TEXT PRIMARY KEY,
    last_sync_ts    TEXT NOT NULL,
    rows_synced     INTEGER NOT NULL DEFAULT 0,
    total_inserts   INTEGER NOT NULL DEFAULT 0,
    total_updates   INTEGER NOT NULL DEFAULT 0,
    total_deletes   INTEGER NOT NULL DEFAULT 0,
    sync_status     TEXT NOT NULL DEFAULT 'ok'
);

CREATE TABLE IF NOT EXISTS cdc_row_hashes (
    source_table    TEXT NOT NULL,
    row_pk          TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    last_seen_ts    TEXT NOT NULL,
    PRIMARY KEY (source_table, row_pk)
);
"""


def init_cdc(olap_conn: sqlite3.Connection):
    """Create CDC tracking tables in OLAP database."""
    olap_conn.executescript(WATERMARK_SCHEMA)
    olap_conn.commit()


def get_watermark(olap_conn: sqlite3.Connection, table: str):
    """Get the last sync watermark for a table."""
    row = olap_conn.execute(
        "SELECT last_sync_ts, rows_synced, total_inserts, total_updates, "
        "total_deletes, sync_status FROM cdc_watermarks WHERE source_table = ?",
        (table,)
    ).fetchone()
    if row:
        return {
            "last_sync_ts": row[0], "rows_synced": row[1],
            "total_inserts": row[2], "total_updates": row[3],
            "total_deletes": row[4], "sync_status": row[5],
        }
    return None


def update_watermark(olap_conn: sqlite3.Connection, table: str,
                     inserts: int, updates: int, deletes: int,
                     status: str = "ok"):
    """Update sync watermark after a CDC extraction."""
    now = datetime.utcnow().isoformat()
    existing = get_watermark(olap_conn, table)
    total_i = (existing["total_inserts"] if existing else 0) + inserts
    total_u = (existing["total_updates"] if existing else 0) + updates
    total_d = (existing["total_deletes"] if existing else 0) + deletes

    olap_conn.execute("""
        INSERT OR REPLACE INTO cdc_watermarks
            (source_table, last_sync_ts, rows_synced, total_inserts,
             total_updates, total_deletes, sync_status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (table, now, inserts + updates + deletes, total_i, total_u, total_d, status))


def compute_row_hash(payload: dict) -> str:
    """Compute a content hash for change detection."""
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def detect_changes(olap_conn: sqlite3.Connection, table: str, pk: str,
                   rows: list) -> dict:
    """Compare OLTP rows against stored hashes to detect inserts/updates/deletes.

    Simulates Azure SQL CDC change feed. On first run, all rows are inserts.
    On subsequent runs, only changed rows produce events.

    Returns: {"inserts": [payload, ...], "updates": [payload, ...], "deletes": [pk_val, ...]}
    """
    now = datetime.utcnow().isoformat()

    existing = {}
    for r in olap_conn.execute(
        "SELECT row_pk, content_hash FROM cdc_row_hashes WHERE source_table = ?",
        (table,)
    ).fetchall():
        existing[r[0]] = r[1]

    current_pks = set()
    inserts = []
    updates = []

    for row in rows:
        row_pk = str(row[pk])
        current_pks.add(row_pk)
        new_hash = compute_row_hash(row)

        if row_pk not in existing:
            inserts.append(row)
            olap_conn.execute(
                "INSERT INTO cdc_row_hashes (source_table, row_pk, content_hash, last_seen_ts) "
                "VALUES (?, ?, ?, ?)",
                (table, row_pk, new_hash, now)
            )
        elif existing[row_pk] != new_hash:
            updates.append(row)
            olap_conn.execute(
                "UPDATE cdc_row_hashes SET content_hash = ?, last_seen_ts = ? "
                "WHERE source_table = ? AND row_pk = ?",
                (new_hash, now, table, row_pk)
            )

    deleted_pks = set(existing.keys()) - current_pks
    for pk_val in deleted_pks:
        olap_conn.execute(
            "DELETE FROM cdc_row_hashes WHERE source_table = ? AND row_pk = ?",
            (table, pk_val)
        )

    return {"inserts": inserts, "updates": updates, "deletes": list(deleted_pks)}


def get_sync_summary(olap_conn: sqlite3.Connection) -> list:
    """Get CDC sync status for all tracked tables."""
    return olap_conn.execute(
        "SELECT source_table, last_sync_ts, rows_synced, total_inserts, "
        "total_updates, total_deletes, sync_status FROM cdc_watermarks "
        "ORDER BY source_table"
    ).fetchall()
