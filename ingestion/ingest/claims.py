"""Claims ingestion — EDI 837."""

import json
import logging
import sqlite3
from datetime import datetime

from ingestion.parsers.edi_837 import EDI837Parser
from ingestion.validators.validation import validate_claim
from ingestion.dedup import resolve_claim_duplicate
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_claims(conn: sqlite3.Connection, raw_edi: str) -> dict:
    """Parse and ingest claims from an EDI 837 file.

    Returns summary: {inserted: int, updated: int, voided: int, skipped: int, errors: int}
    """
    parser = EDI837Parser(raw_edi)
    result = parser.parse()
    summary = {"inserted": 0, "updated": 0, "voided": 0, "skipped": 0, "errors": 0}

    for claim in result["claims"]:
        validation = validate_claim(claim)
        if not validation.is_valid:
            logger.warning("Claim %s validation failed: %s", claim.get("claim_id"), validation.errors)
            send_to_dlq(conn, "edi_837", "claim", "validation_failure",
                        "; ".join(validation.errors), entity_id=claim.get("claim_id"),
                        payload={"claim_id": claim.get("claim_id"),
                                 "total_billed": claim.get("total_billed")})
            summary["errors"] += 1
            continue

        if validation.warnings:
            logger.info("Claim %s warnings: %s", claim["claim_id"], validation.warnings)

        action = resolve_claim_duplicate(conn, claim["claim_id"],
                                         claim.get("frequency_code", "1"))

        if action == "skip":
            logger.info("Claim %s duplicate — skipping", claim["claim_id"])
            summary["skipped"] += 1
            continue

        if action == "insert":
            _insert_claim(conn, claim)
            summary["inserted"] += 1
        elif action == "update":
            _update_claim(conn, claim)
            summary["updated"] += 1
        elif action == "void":
            _void_claim(conn, claim["claim_id"])
            summary["voided"] += 1

        log_action(conn, "claim", claim["claim_id"], action)
        emit_event(conn, f"claim.{action}", "claim", claim["claim_id"],
                   {"total_billed": claim["total_billed"],
                    "payer_id": claim.get("payer_id")})

    conn.commit()
    return summary


def _insert_claim(conn: sqlite3.Connection, claim: dict):
    conn.execute("""
        INSERT INTO claims (claim_id, patient_id, provider_npi, payer_id,
                            date_of_service, date_filed, facility_type,
                            total_billed, status, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'received', 'edi_837')
    """, (claim["claim_id"], claim.get("patient_id"), claim.get("provider_npi"),
          claim.get("payer_id"), claim["date_of_service"],
          datetime.utcnow().isoformat(), claim.get("facility_type"),
          claim["total_billed"]))

    for line in claim["lines"]:
        conn.execute("""
            INSERT INTO claim_lines (claim_id, cpt_code, modifier, units,
                                     billed_amount, diagnosis_codes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (claim["claim_id"], line["cpt_code"], line.get("modifier"),
              line.get("units", 1), line["billed_amount"],
              json.dumps(claim.get("diagnosis_codes", []))))


def _update_claim(conn: sqlite3.Connection, claim: dict):
    conn.execute("""
        UPDATE claims SET total_billed = ?, status = 'corrected',
                          date_filed = ?
        WHERE claim_id = ?
    """, (claim["total_billed"], datetime.utcnow().isoformat(), claim["claim_id"]))

    # Replace lines
    conn.execute("DELETE FROM claim_lines WHERE claim_id = ?", (claim["claim_id"],))
    for line in claim["lines"]:
        conn.execute("""
            INSERT INTO claim_lines (claim_id, cpt_code, modifier, units,
                                     billed_amount, diagnosis_codes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (claim["claim_id"], line["cpt_code"], line.get("modifier"),
              line.get("units", 1), line["billed_amount"],
              json.dumps(claim.get("diagnosis_codes", []))))


def _void_claim(conn: sqlite3.Connection, claim_id: str):
    conn.execute("UPDATE claims SET status = 'voided' WHERE claim_id = ?", (claim_id,))
