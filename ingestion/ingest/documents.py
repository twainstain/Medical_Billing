"""General document upload ingestion — classification + evidence storage."""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime

from ingestion.dedup import check_evidence_duplicate
from ingestion.audit import log_action
from ingestion.events import emit_event

logger = logging.getLogger(__name__)

# Mock classification model output
DOCUMENT_TYPES = [
    "EOB", "clinical_record", "contract", "payer_correspondence",
    "prior_auth", "appeal", "arbitration_decision", "provider_correspondence"
]


def classify_document(content: bytes, filename: str) -> dict:
    """Mock document classification (would call Azure Doc Intelligence in prod)."""
    filename_lower = filename.lower()
    if "eob" in filename_lower:
        return {"type": "EOB", "confidence": 0.95}
    if "clinical" in filename_lower or "medical" in filename_lower:
        return {"type": "clinical_record", "confidence": 0.92}
    if "contract" in filename_lower:
        return {"type": "contract", "confidence": 0.91}
    if "denial" in filename_lower or "correspondence" in filename_lower:
        return {"type": "payer_correspondence", "confidence": 0.88}
    if "appeal" in filename_lower:
        return {"type": "appeal", "confidence": 0.90}
    if "decision" in filename_lower or "award" in filename_lower:
        return {"type": "arbitration_decision", "confidence": 0.93}
    if "auth" in filename_lower:
        return {"type": "prior_auth", "confidence": 0.87}
    return {"type": "provider_correspondence", "confidence": 0.60}


def ingest_document(conn: sqlite3.Connection, content: bytes, filename: str,
                    case_id: str = None, uploaded_by: str = "analyst") -> dict:
    """Ingest a document upload.

    Returns: {stored: bool, duplicate: bool, doc_type: str, confidence: float, needs_review: bool}
    """
    content_hash = hashlib.sha256(content).hexdigest()

    if check_evidence_duplicate(conn, content_hash):
        return {"stored": False, "duplicate": True, "doc_type": None,
                "confidence": 0.0, "needs_review": False}

    classification = classify_document(content, filename)
    needs_review = classification["confidence"] < 0.9

    conn.execute("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_conf,
             ocr_status, content_hash, uploaded_date, uploaded_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (case_id, classification["type"],
          f"documents/{filename}", json.dumps({"filename": filename}),
          classification["confidence"],
          "needs_review" if needs_review else "classified",
          content_hash, datetime.utcnow().isoformat(), uploaded_by))

    log_action(conn, "document", content_hash[:12], "upload", user_id=uploaded_by)
    emit_event(conn, "document.upload", "document", content_hash[:12],
               {"doc_type": classification["type"],
                "confidence": classification["confidence"],
                "needs_review": needs_review})
    conn.commit()

    return {"stored": True, "duplicate": False, "doc_type": classification["type"],
            "confidence": classification["confidence"], "needs_review": needs_review}
