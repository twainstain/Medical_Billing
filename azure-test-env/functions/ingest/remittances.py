"""Remittance ingestion — ERA (EDI 835) and EOB (Azure SQL version)."""

import json
import logging
from datetime import datetime, timezone

from parsers.edi_835 import EDI835Parser
from parsers.eob_mock import parse_eob_extractions
from validators.validation import validate_remittance
from shared.dedup import check_remittance_duplicate, match_claim_id, check_evidence_duplicate
from shared.audit import log_action
from shared.dlq import send_to_dlq
from shared.events import emit_event
from shared.db import execute_query, commit

logger = logging.getLogger(__name__)


def ingest_era(raw_edi: str) -> dict:
    """Ingest ERA/835 remittances.

    Returns: {inserted, skipped, unmatched, errors}
    """
    parser = EDI835Parser(raw_edi)
    remittances = parser.parse()
    summary = {"inserted": 0, "skipped": 0, "unmatched": 0, "errors": 0}

    for remit in remittances:
        validation = validate_remittance(remit)
        if not validation.is_valid:
            send_to_dlq("edi_835", "remittance", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=remit.get("trace_number"),
                        payload={"payer_claim_id": remit.get("payer_claim_id")})
            summary["errors"] += 1
            continue

        canonical_claim_id = match_claim_id(remit["payer_claim_id"])
        if not canonical_claim_id:
            send_to_dlq("edi_835", "remittance", "unmatched_reference",
                        f"No claim found for payer_claim_id={remit['payer_claim_id']}",
                        entity_id=remit.get("trace_number"),
                        payload={"payer_claim_id": remit["payer_claim_id"],
                                 "paid_amount": remit.get("paid_amount")})
            summary["unmatched"] += 1
            continue

        if check_remittance_duplicate(canonical_claim_id,
                                      remit["trace_number"], remit["payer_id"]):
            summary["skipped"] += 1
            continue

        denial_code = EDI835Parser.extract_denial_code(remit["adjustments"])
        now = datetime.now(timezone.utc).isoformat()

        execute_query("""
            INSERT INTO remittances (claim_id, era_date, paid_amount,
                                     allowed_amount, adjustment_reason,
                                     denial_code, check_number, source_type)
            VALUES ((SELECT claim_id FROM claims WHERE external_claim_id = ?),
                    ?, ?, ?, ?, ?, ?, 'edi_835')
        """, (canonical_claim_id, now, remit["paid_amount"],
              remit["total_billed"], json.dumps(remit["adjustments"]),
              denial_code, remit.get("check_number")))

        log_action("remittance", remit["trace_number"], "insert")
        emit_event("remittance.received", "remittance", canonical_claim_id,
                   {"paid_amount": remit["paid_amount"],
                    "payer_id": remit["payer_id"],
                    "trace_number": remit["trace_number"]})
        summary["inserted"] += 1

    commit()
    return summary


def ingest_eob(eob_json: str) -> dict:
    """Ingest EOB PDF extractions (Document Intelligence output).

    Returns: {inserted, skipped, low_confidence, unmatched}
    """
    extractions = parse_eob_extractions(eob_json)
    summary = {"inserted": 0, "skipped": 0, "low_confidence": 0, "unmatched": 0}

    for eob in extractions:
        if check_evidence_duplicate(eob["content_hash"]):
            summary["skipped"] += 1
            continue

        fields = eob["fields"]
        avg_conf = eob["avg_confidence"]
        now = datetime.now(timezone.utc).isoformat()

        execute_query("""
            INSERT INTO evidence_artifacts
                (case_id, type, blob_url, extracted_data, classification_confidence,
                 ocr_status, uploaded_date)
            VALUES (NULL, 'eob', ?, ?, ?, ?, ?)
        """, (f"eob-uploads/{eob['source_file']}", json.dumps(fields, default=str),
              avg_conf, "completed" if avg_conf >= 0.9 else "needs_review", now))

        if avg_conf < 0.9:
            summary["low_confidence"] += 1
        else:
            claim_number = fields.get("claim_number", {}).get("value")
            if claim_number:
                canonical_id = match_claim_id(claim_number)
                if canonical_id:
                    paid = float(fields.get("paid_amount", {}).get("value", 0))
                    allowed = float(fields.get("allowed_amount", {}).get("value", 0))
                    execute_query("""
                        INSERT INTO remittances (claim_id, era_date, paid_amount,
                                                 allowed_amount, source_type)
                        VALUES ((SELECT claim_id FROM claims WHERE external_claim_id = ?),
                                ?, ?, ?, 'eob_pdf')
                    """, (canonical_id, now, paid, allowed))
                    log_action("remittance", canonical_id, "insert_from_eob")
                    emit_event("remittance.received", "remittance", canonical_id,
                               {"source": "eob_pdf", "paid_amount": paid})
                    summary["inserted"] += 1
                else:
                    summary["unmatched"] += 1
            else:
                summary["unmatched"] += 1

    commit()
    return summary
