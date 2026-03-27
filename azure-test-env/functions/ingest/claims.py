"""Claims ingestion — EDI 837 (Azure SQL version)."""

import json
import logging
from datetime import datetime, timezone

from parsers.edi_837 import EDI837Parser
from validators.validation import validate_claim
from shared.dedup import resolve_claim_duplicate
from shared.audit import log_action
from shared.dlq import send_to_dlq
from shared.events import emit_event
from shared.db import execute_query, commit

logger = logging.getLogger(__name__)


def ingest_claims(raw_edi: str) -> dict:
    """Parse and ingest claims from an EDI 837 file.

    Returns summary: {inserted, updated, voided, skipped, errors}
    """
    parser = EDI837Parser(raw_edi)
    result = parser.parse()
    summary = {"inserted": 0, "updated": 0, "voided": 0, "skipped": 0, "errors": 0}

    for claim in result["claims"]:
        validation = validate_claim(claim)
        if not validation.is_valid:
            logger.warning("Claim %s validation failed: %s",
                           claim.get("claim_id"), validation.errors)
            send_to_dlq("edi_837", "claim", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=claim.get("claim_id"),
                        payload={"claim_id": claim.get("claim_id"),
                                 "total_billed": claim.get("total_billed")})
            summary["errors"] += 1
            continue

        action = resolve_claim_duplicate(claim["claim_id"],
                                         claim.get("frequency_code", "1"))

        if action == "skip":
            summary["skipped"] += 1
            continue

        if action == "insert":
            _insert_claim(claim)
            summary["inserted"] += 1
        elif action == "update":
            _update_claim(claim)
            summary["updated"] += 1
        elif action == "void":
            _void_claim(claim["claim_id"])
            summary["voided"] += 1

        log_action("claim", claim["claim_id"], action)
        emit_event(f"claim.{action}", "claim", claim["claim_id"],
                   {"total_billed": claim["total_billed"],
                    "payer_id": claim.get("payer_id")})

    commit()
    return summary


def _insert_claim(claim: dict):
    now = datetime.now(timezone.utc).isoformat()
    execute_query("""
        INSERT INTO claims (external_claim_id, patient_id, provider_npi, payer_id,
                            date_of_service, date_filed, place_of_service,
                            total_billed, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'filed')
    """, (claim["claim_id"], claim.get("patient_id"), claim.get("provider_npi"),
          claim.get("payer_id"), claim["date_of_service"], now,
          claim.get("facility_type"), claim["total_billed"]))

    for line in claim["lines"]:
        execute_query("""
            INSERT INTO claim_lines (claim_id, line_number, cpt_code, modifier,
                                     units, billed_amount, diagnosis_codes)
            VALUES ((SELECT claim_id FROM claims WHERE external_claim_id = ?),
                    ?, ?, ?, ?, ?, ?)
        """, (claim["claim_id"], line.get("line_number", 1),
              line["cpt_code"], line.get("modifier"),
              line.get("units", 1), line["billed_amount"],
              json.dumps(claim.get("diagnosis_codes", []))))


def _update_claim(claim: dict):
    now = datetime.now(timezone.utc).isoformat()
    execute_query("""
        UPDATE claims SET total_billed = ?, status = 'corrected', updated_at = ?
        WHERE external_claim_id = ?
    """, (claim["total_billed"], now, claim["claim_id"]))

    # Get the internal claim_id
    row = execute_query(
        "SELECT claim_id FROM claims WHERE external_claim_id = ?",
        (claim["claim_id"],)
    ).fetchone()
    if row:
        internal_id = row[0]
        execute_query("DELETE FROM claim_lines WHERE claim_id = ?", (internal_id,))
        for line in claim["lines"]:
            execute_query("""
                INSERT INTO claim_lines (claim_id, line_number, cpt_code, modifier,
                                         units, billed_amount, diagnosis_codes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (internal_id, line.get("line_number", 1),
                  line["cpt_code"], line.get("modifier"),
                  line.get("units", 1), line["billed_amount"],
                  json.dumps(claim.get("diagnosis_codes", []))))


def _void_claim(claim_id: str):
    execute_query(
        "UPDATE claims SET status = 'voided' WHERE external_claim_id = ?",
        (claim_id,)
    )
