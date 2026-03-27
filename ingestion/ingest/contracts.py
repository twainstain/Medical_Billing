"""Contract ingestion — manual upload with Doc Intelligence mock."""

import json
import logging
import sqlite3
from datetime import datetime

from ingestion.parsers.eob_mock import parse_contract_extraction
from ingestion.dedup import check_evidence_duplicate
from ingestion.audit import log_action
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_contract(conn: sqlite3.Connection, extraction_json: str,
                    uploaded_by: str = "analyst") -> dict:
    """Ingest a contract extraction. Always requires human review.

    Returns: {stored: bool, duplicate: bool, tables_found: int}
    """
    data = parse_contract_extraction(extraction_json)
    summary = {"stored": False, "duplicate": False, "tables_found": 0}

    # Dedup
    if check_evidence_duplicate(conn, data["content_hash"]):
        summary["duplicate"] = True
        return summary

    tables_count = len(data.get("tables", []))
    summary["tables_found"] = tables_count

    # Store artifact — status always needs_review for contracts
    conn.execute("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_conf,
             ocr_status, content_hash, uploaded_date, uploaded_by)
        VALUES (?, 'contract', ?, ?, 0.0, 'needs_review', ?, ?, ?)
    """, (None, f"contracts-restricted/{data['source_file']}",
          json.dumps({"tables": data["tables"], "payer_id": data["payer_id"],
                       "effective_date": data["effective_date"],
                       "provider_npi": data.get("provider_npi")}),
          data["content_hash"], datetime.utcnow().isoformat(), uploaded_by))

    log_action(conn, "contract", data["content_hash"][:12], "upload",
               user_id=uploaded_by)

    summary["stored"] = True
    emit_event(conn, "contract.upload", "contract", data["content_hash"][:12],
               {"payer_id": data["payer_id"], "effective_date": data["effective_date"]})
    conn.commit()
    return summary
