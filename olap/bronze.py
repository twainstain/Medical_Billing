"""Bronze Layer — Raw CDC events from OLTP.

Simulates Azure SQL CDC → ADLS Gen2 Bronze pipeline.
In production: Fabric Pipeline reads CDC change feed, writes raw Parquet
partitioned by date. Here we use content-hash CDC (see cdc.py) to detect
inserts, updates, and deletes — writing them as append-only events.

Bronze is append-only, schema-on-read. Every row carries:
  - _cdc_operation: 'I' (insert), 'U' (update), 'D' (delete)
  - _cdc_timestamp: when the change was captured
  - _source_table: originating OLTP table

See: Section 8 of medical_billing_arbitration_future_architecture.md
"""

import json
import logging
import sqlite3
from datetime import datetime

from olap.cdc import init_cdc, detect_changes, update_watermark

logger = logging.getLogger(__name__)

CDC_TABLES = {
    "claims": {
        "pk": "claim_id",
        "columns": [
            "claim_id", "patient_id", "provider_npi", "payer_id",
            "date_of_service", "date_filed", "facility_type",
            "total_billed", "status", "source",
        ],
    },
    "claim_lines": {
        "pk": "line_id",
        "columns": [
            "line_id", "claim_id", "cpt_code", "modifier",
            "units", "billed_amount", "diagnosis_codes",
        ],
    },
    "remittances": {
        "pk": "remit_id",
        "columns": [
            "remit_id", "claim_id", "trace_number", "payer_id",
            "era_date", "paid_amount", "allowed_amount",
            "adjustment_reason", "denial_code", "check_number", "source",
        ],
    },
    "patients": {
        "pk": "patient_id",
        "columns": [
            "patient_id", "first_name", "last_name", "dob", "gender",
            "address_line", "city", "state", "zip", "insurance_id",
            "source_system", "last_updated",
        ],
    },
    "providers": {
        "pk": "id",
        "columns": [
            "id", "npi", "name_first", "name_last", "specialty_taxonomy",
            "state", "city", "zip", "status", "deactivation_date",
            "valid_from", "valid_to", "is_current",
        ],
    },
    "payers": {
        "pk": "payer_id",
        "columns": [
            "payer_id", "name", "type", "state",
        ],
    },
    "fee_schedule": {
        "pk": "id",
        "columns": [
            "id", "payer_id", "cpt_code", "modifier", "geo_region",
            "rate", "rate_type", "valid_from", "valid_to", "is_current",
            "source", "loaded_date",
        ],
    },
    "disputes": {
        "pk": "dispute_id",
        "columns": [
            "dispute_id", "claim_id", "claim_line_id", "case_id",
            "dispute_type", "status", "billed_amount", "paid_amount",
            "requested_amount", "qpa_amount", "underpayment_amount",
            "filed_date", "open_negotiation_deadline", "idr_initiation_deadline",
        ],
    },
    "cases": {
        "pk": "case_id",
        "columns": [
            "case_id", "assigned_analyst", "status", "priority",
            "created_date", "last_activity_date", "outcome", "award_amount",
        ],
    },
    "deadlines": {
        "pk": "deadline_id",
        "columns": [
            "deadline_id", "case_id", "dispute_id", "type",
            "due_date", "status", "alerted_date", "completed_date",
        ],
    },
    "evidence_artifacts": {
        "pk": "artifact_id",
        "columns": [
            "artifact_id", "case_id", "type", "blob_url", "extracted_data",
            "classification_conf", "ocr_status", "content_hash",
            "uploaded_date", "uploaded_by",
        ],
    },
    "audit_log": {
        "pk": "log_id",
        "columns": [
            "log_id", "entity_type", "entity_id", "action", "user_id",
            "timestamp", "old_value", "new_value",
        ],
    },
}


BRONZE_SCHEMA_TEMPLATE = """
CREATE TABLE IF NOT EXISTS bronze_{table} (
    _bronze_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    _cdc_operation  TEXT NOT NULL DEFAULT 'I',
    _cdc_timestamp  TEXT NOT NULL,
    _source_table   TEXT NOT NULL DEFAULT '{table}',
    payload         TEXT NOT NULL
);
"""


def init_bronze(olap_conn: sqlite3.Connection):
    """Create bronze_* tables in OLAP database."""
    for table in CDC_TABLES:
        olap_conn.executescript(BRONZE_SCHEMA_TEMPLATE.format(table=table))
    olap_conn.commit()


def extract_bronze(oltp_conn: sqlite3.Connection,
                   olap_conn: sqlite3.Connection) -> dict:
    """CDC extraction: detect changes in OLTP and write to Bronze layer.

    Uses content-hash based change detection (simulates Azure SQL CDC).
    First run: all rows detected as inserts ('I').
    Subsequent runs: only new/changed rows produce events ('I'/'U'/'D').

    Returns: {table_name: {"inserted": N, "updated": N, "deleted": N, "skipped": N}}
    """
    init_bronze(olap_conn)
    init_cdc(olap_conn)
    now = datetime.utcnow().isoformat()
    summary = {}

    for table, config in CDC_TABLES.items():
        pk = config["pk"]
        cols = config["columns"]

        # Read current OLTP state
        try:
            rows_raw = oltp_conn.execute(
                f"SELECT {', '.join(cols)} FROM {table}"
            ).fetchall()
        except sqlite3.OperationalError:
            summary[table] = {"inserted": 0, "updated": 0, "deleted": 0, "skipped": 0}
            continue

        rows = [{col: row[col] for col in cols} for row in rows_raw]

        # Detect inserts, updates, deletes via CDC content-hash comparison
        changes = detect_changes(olap_conn, table, pk, rows)

        for payload in changes["inserts"]:
            olap_conn.execute(
                f"INSERT INTO bronze_{table} "
                f"(_cdc_operation, _cdc_timestamp, _source_table, payload) "
                f"VALUES (?, ?, ?, ?)",
                ("I", now, table, json.dumps(payload, default=str)),
            )

        for payload in changes["updates"]:
            olap_conn.execute(
                f"INSERT INTO bronze_{table} "
                f"(_cdc_operation, _cdc_timestamp, _source_table, payload) "
                f"VALUES (?, ?, ?, ?)",
                ("U", now, table, json.dumps(payload, default=str)),
            )

        for pk_val in changes["deletes"]:
            olap_conn.execute(
                f"INSERT INTO bronze_{table} "
                f"(_cdc_operation, _cdc_timestamp, _source_table, payload) "
                f"VALUES (?, ?, ?, ?)",
                ("D", now, table, json.dumps({pk: pk_val}, default=str)),
            )

        ins = len(changes["inserts"])
        upd = len(changes["updates"])
        dlt = len(changes["deletes"])
        skipped = len(rows) - ins - upd

        update_watermark(olap_conn, table, ins, upd, dlt)
        summary[table] = {"inserted": ins, "updated": upd, "deleted": dlt, "skipped": skipped}

    olap_conn.commit()
    logger.info("Bronze CDC extraction complete: %s", summary)
    return summary
