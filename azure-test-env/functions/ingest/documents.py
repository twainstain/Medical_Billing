"""Document ingestion — classification + evidence storage (Azure SQL version).

In the cloud version, this can optionally call Azure Document Intelligence
for real OCR/classification instead of the filename-based mock.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from shared.dedup import check_evidence_duplicate
from shared.audit import log_action
from shared.events import emit_event
from shared.db import execute_query, commit

logger = logging.getLogger(__name__)

DOCUMENT_TYPES = [
    "eob", "clinical_record", "contract", "payer_correspondence",
    "prior_auth", "appeal", "arbitration_decision", "provider_correspondence"
]


def classify_document(content: bytes, filename: str) -> dict:
    """Classify a document. Uses Azure Doc Intelligence if configured,
    otherwise falls back to filename-based heuristic."""
    doc_intel_endpoint = os.environ.get("DOC_INTEL_ENDPOINT")
    doc_intel_key = os.environ.get("DOC_INTEL_KEY")

    if doc_intel_endpoint and doc_intel_key and doc_intel_endpoint != "<from .env after provisioning>":
        return _classify_with_doc_intelligence(content, doc_intel_endpoint, doc_intel_key)

    return _classify_by_filename(filename)


def _classify_with_doc_intelligence(content: bytes, endpoint: str, key: str) -> dict:
    """Call Azure Document Intelligence for classification."""
    try:
        from azure.ai.formrecognizer import DocumentAnalysisClient
        from azure.core.credentials import AzureKeyCredential

        client = DocumentAnalysisClient(endpoint, AzureKeyCredential(key))
        poller = client.begin_analyze_document("prebuilt-document", content)
        result = poller.result()

        doc_type = "provider_correspondence"
        confidence = 0.5

        if result.documents:
            doc = result.documents[0]
            doc_type = doc.doc_type or doc_type
            confidence = doc.confidence or confidence

        return {"type": doc_type, "confidence": confidence}
    except Exception as e:
        logger.warning("Doc Intelligence failed, falling back to filename: %s", e)
        return _classify_by_filename("")


def _classify_by_filename(filename: str) -> dict:
    """Fallback: classify based on filename patterns."""
    filename_lower = filename.lower()
    if "eob" in filename_lower:
        return {"type": "eob", "confidence": 0.95}
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


def ingest_document(content: bytes, filename: str, case_id: str = None,
                    uploaded_by: str = "analyst") -> dict:
    """Ingest a document upload.

    Returns: {stored, duplicate, doc_type, confidence, needs_review}
    """
    content_hash = hashlib.sha256(content).hexdigest()

    if check_evidence_duplicate(content_hash):
        return {"stored": False, "duplicate": True, "doc_type": None,
                "confidence": 0.0, "needs_review": False}

    classification = classify_document(content, filename)
    needs_review = classification["confidence"] < 0.9
    now = datetime.now(timezone.utc).isoformat()

    execute_query("""
        INSERT INTO evidence_artifacts
            (case_id, type, blob_url, extracted_data, classification_confidence,
             ocr_status, uploaded_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (case_id, classification["type"],
          f"documents/{filename}", json.dumps({"filename": filename}),
          classification["confidence"],
          "needs_review" if needs_review else "classified", now))

    log_action("document", content_hash[:12], "upload", user_id=uploaded_by)
    emit_event("document.upload", "document", content_hash[:12],
               {"doc_type": classification["type"],
                "confidence": classification["confidence"],
                "needs_review": needs_review})
    commit()

    return {"stored": True, "duplicate": False, "doc_type": classification["type"],
            "confidence": classification["confidence"], "needs_review": needs_review}
