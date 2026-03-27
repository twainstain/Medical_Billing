"""Remittance ingestion — ERA (EDI 835) and EOB (PDF mock)."""

import json
import logging
import sqlite3
from datetime import datetime

from ingestion.parsers.edi_835 import EDI835Parser
from ingestion.parsers.eob_mock import parse_eob_extractions, normalize_eob_extraction
from ingestion.validators.validation import validate_remittance
from ingestion.dedup import check_remittance_duplicate, match_claim_id, check_evidence_duplicate
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_era(conn: sqlite3.Connection, raw_edi: str) -> dict:
    """Ingest ERA/835 remittances.

    Returns: {inserted: int, skipped: int, unmatched: int, errors: int}
    """
    parser = EDI835Parser(raw_edi)
    remittances = parser.parse()
    summary = {"inserted": 0, "skipped": 0, "unmatched": 0, "errors": 0}

    for remit in remittances:
        validation = validate_remittance(remit)
        if not validation.is_valid:
            logger.warning("Remittance validation failed: %s", validation.errors)
            send_to_dlq(conn, "edi_835", "remittance", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=remit.get("trace_number"),
                        payload={"payer_claim_id": remit.get("payer_claim_id")})
            summary["errors"] += 1
            continue

        # Match claim
        canonical_claim_id = match_claim_id(conn, remit["payer_claim_id"])
        if not canonical_claim_id:
            logger.warning("Unmatched claim: payer_claim_id=%s", remit["payer_claim_id"])
            send_to_dlq(conn, "edi_835", "remittance", "unmatched_reference",
                        f"No claim found for payer_claim_id={remit['payer_claim_id']}",
                        entity_id=remit.get("trace_number"),
                        payload={"payer_claim_id": remit["payer_claim_id"],
                                 "paid_amount": remit.get("paid_amount")})
            summary["unmatched"] += 1
            continue

        # Dedup at the claim-payment grain so one ERA file can carry multiple CLPs.
        if check_remittance_duplicate(conn, canonical_claim_id,
                                      remit["trace_number"], remit["payer_id"]):
            logger.info("Duplicate ERA claim=%s trace=%s — skipping",
                        canonical_claim_id, remit["trace_number"])
            summary["skipped"] += 1
            continue

        # Write
        denial_code = EDI835Parser.extract_denial_code(remit["adjustments"])
        conn.execute("""
            INSERT INTO remittances (claim_id, trace_number, payer_id, era_date,
                                     paid_amount, allowed_amount, adjustment_reason,
                                     denial_code, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'edi_835')
        """, (canonical_claim_id, remit["trace_number"], remit["payer_id"],
              datetime.utcnow().isoformat(), remit["paid_amount"],
              remit["total_billed"], json.dumps(remit["adjustments"]),
              denial_code))

        log_action(conn, "remittance", remit["trace_number"], "insert")
        emit_event(conn, "remittance.received", "remittance", canonical_claim_id,
                   {"paid_amount": remit["paid_amount"],
                    "payer_id": remit["payer_id"],
                    "trace_number": remit["trace_number"]})
        summary["inserted"] += 1

    conn.commit()
    return summary


def ingest_eob(conn: sqlite3.Connection, eob_json: str) -> dict:
    """Ingest EOB PDF extractions (mock Doc Intelligence output).

    Returns: {inserted: int, skipped: int, low_confidence: int, unmatched: int}
    """
    extractions = parse_eob_extractions(eob_json)
    summary = {"inserted": 0, "skipped": 0, "low_confidence": 0, "unmatched": 0}

    for eob in extractions:
        # Dedup by content hash
        if check_evidence_duplicate(conn, eob["content_hash"]):
            summary["skipped"] += 1
            continue

        fields = eob["fields"]
        avg_conf = eob["avg_confidence"]

        # Store evidence artifact regardless of confidence
        conn.execute("""
            INSERT INTO evidence_artifacts
                (case_id, type, blob_url, extracted_data, classification_conf,
                 ocr_status, content_hash, uploaded_date)
            VALUES (?, 'EOB', ?, ?, ?, ?, ?, ?)
        """, (None, f"eob-uploads/{eob['source_file']}",
              json.dumps(fields, default=str), avg_conf,
              "completed" if avg_conf >= 0.9 else "needs_review",
              eob["content_hash"], datetime.utcnow().isoformat()))

        if avg_conf < 0.9:
            logger.warning("Low confidence (%.2f) for %s", avg_conf, eob["source_file"])
            summary["low_confidence"] += 1
            log_action(conn, "eob", eob["content_hash"][:12], "low_confidence_review")
        else:
            # Try to create remittance record
            claim_number = fields.get("claim_number", {}).get("value")
            if claim_number:
                canonical_id = match_claim_id(conn, claim_number)
                if canonical_id:
                    paid = float(fields.get("paid_amount", {}).get("value", 0))
                    allowed = float(fields.get("allowed_amount", {}).get("value", 0))
                    conn.execute("""
                        INSERT INTO remittances (claim_id, payer_id, era_date,
                                                 paid_amount, allowed_amount, source)
                        VALUES (?, ?, ?, ?, ?, 'eob_pdf')
                    """, (canonical_id, eob.get("payer_id"),
                          datetime.utcnow().isoformat(), paid, allowed))
                    log_action(conn, "remittance", canonical_id, "insert_from_eob")
                    emit_event(conn, "remittance.received", "remittance", canonical_id,
                               {"source": "eob_pdf", "paid_amount": paid})
                    summary["inserted"] += 1
                else:
                    summary["unmatched"] += 1
            else:
                summary["unmatched"] += 1

    conn.commit()
    return summary
