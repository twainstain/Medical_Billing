"""Historical claims backfill ingestion."""

import json
import logging
import sqlite3
from datetime import datetime

from ingestion.parsers.csv_parser import parse_backfill_csv
from ingestion.validators.validation import validate_claim
from ingestion.dedup import check_backfill_duplicate
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.crosswalk import apply_crosswalk

logger = logging.getLogger(__name__)


def ingest_backfill(conn: sqlite3.Connection, csv_content: str,
                    source_system: str = None) -> dict:
    """Ingest historical claims from CSV backfill.

    All records inserted with status='historical' and source='backfill'
    so workflow engine ignores them. Applies code crosswalk if mappings exist.

    Returns: {inserted: int, skipped: int, errors: int, crosswalked: int}
    """
    claims = parse_backfill_csv(csv_content)
    summary = {"inserted": 0, "skipped": 0, "errors": 0, "crosswalked": 0}

    for claim in claims:
        # Apply code crosswalk for legacy codes
        original_lines = [l["cpt_code"] for l in claim.get("lines", [])]
        apply_crosswalk(conn, claim, source_system)
        mapped_lines = [l["cpt_code"] for l in claim.get("lines", [])]
        if original_lines != mapped_lines:
            summary["crosswalked"] += 1

        validation = validate_claim(claim)
        if not validation.is_valid:
            logger.warning("Backfill claim %s invalid: %s",
                          claim.get("claim_id"), validation.errors)
            send_to_dlq(conn, "backfill_csv", "claim", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=claim.get("claim_id"))
            summary["errors"] += 1
            continue

        # Dedup: composite key
        if check_backfill_duplicate(conn, claim["claim_id"],
                                    claim["date_of_service"],
                                    claim.get("provider_npi", ""),
                                    claim["total_billed"]):
            summary["skipped"] += 1
            continue

        # Insert with historical status
        conn.execute("""
            INSERT INTO claims (claim_id, patient_id, provider_npi, payer_id,
                                date_of_service, date_filed, total_billed,
                                status, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'historical', 'backfill')
        """, (claim["claim_id"], claim.get("patient_id"),
              claim.get("provider_npi"), claim.get("payer_id"),
              claim["date_of_service"], datetime.utcnow().isoformat(),
              claim["total_billed"]))

        for line in claim["lines"]:
            conn.execute("""
                INSERT INTO claim_lines (claim_id, cpt_code, modifier, units,
                                         billed_amount, diagnosis_codes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (claim["claim_id"], line["cpt_code"], line.get("modifier"),
                  line.get("units", 1), line["billed_amount"],
                  json.dumps(claim.get("diagnosis_codes", []))))

        log_action(conn, "claim", claim["claim_id"], "backfill_insert")
        summary["inserted"] += 1

    conn.commit()
    return summary
