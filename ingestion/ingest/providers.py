"""Provider data ingestion — NPPES (SCD Type 2 MERGE)."""

import logging
import sqlite3
from datetime import datetime
from typing import Set, Optional

from ingestion.parsers.csv_parser import parse_nppes_csv
from ingestion.validators.validation import validate_provider
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_providers(conn: sqlite3.Connection, csv_content: str,
                     known_npis: Set[str] = None,
                     state_whitelist: Set[str] = None) -> dict:
    """Load NPPES CSV, filter, stage, and SCD Type 2 merge.

    Returns: {staged: int, closed: int, inserted: int, unchanged: int, errors: int}
    """
    providers = parse_nppes_csv(csv_content, known_npis, state_whitelist)
    summary = {"staged": 0, "closed": 0, "inserted": 0, "unchanged": 0, "errors": 0}

    # Clear staging
    conn.execute("DELETE FROM stg_providers")

    for prov in providers:
        validation = validate_provider(prov)
        if not validation.is_valid:
            logger.warning("Provider %s invalid: %s", prov.get("npi"), validation.errors)
            send_to_dlq(conn, "nppes_csv", "provider", "validation_failure",
                        "; ".join(validation.errors), entity_id=prov.get("npi"))
            summary["errors"] += 1
            continue

        conn.execute("""
            INSERT INTO stg_providers (npi, name_first, name_last, specialty_taxonomy,
                                       state, city, zip, status, deactivation_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (prov["npi"], prov["name_first"], prov["name_last"],
              prov["specialty_taxonomy"], prov["state"], prov["city"],
              prov["zip"], prov["status"], prov.get("deactivation_date")))
        summary["staged"] += 1

    # SCD Type 2 MERGE
    merge_result = _scd2_merge_providers(conn)
    summary.update(merge_result)

    log_action(conn, "providers", "NPPES_BATCH", "scd2_merge",
               old_value=f"closed:{summary['closed']}",
               new_value=f"inserted:{summary['inserted']}")

    if summary["inserted"] > 0 or summary["closed"] > 0:
        emit_event(conn, "provider.merge", "provider", "NPPES_BATCH",
                   {"inserted": summary["inserted"], "closed": summary["closed"]})

    conn.commit()
    return summary


def _scd2_merge_providers(conn: sqlite3.Connection) -> dict:
    result = {"closed": 0, "inserted": 0, "unchanged": 0}
    now = datetime.utcnow().isoformat()

    # Step 1: Close rows where tracked attributes changed
    changed = conn.execute("""
        SELECT p.id
        FROM providers p
        INNER JOIN stg_providers stg ON p.npi = stg.npi
        WHERE p.is_current = 1
          AND (p.specialty_taxonomy <> stg.specialty_taxonomy
               OR p.status <> stg.status
               OR p.state <> stg.state)
    """).fetchall()

    for row in changed:
        conn.execute("""
            UPDATE providers SET valid_to = ?, is_current = 0
            WHERE id = ?
        """, (now, row["id"]))
        result["closed"] += 1

    # Step 2: Insert new/changed
    to_insert = conn.execute("""
        SELECT stg.*
        FROM stg_providers stg
        WHERE NOT EXISTS (
            SELECT 1 FROM providers p
            WHERE p.npi = stg.npi
              AND p.is_current = 1
              AND p.specialty_taxonomy = stg.specialty_taxonomy
              AND p.status = stg.status
              AND p.state = stg.state
        )
    """).fetchall()

    for row in to_insert:
        conn.execute("""
            INSERT INTO providers (npi, name_first, name_last, specialty_taxonomy,
                                   state, city, zip, status, deactivation_date,
                                   valid_from, valid_to, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1)
        """, (row["npi"], row["name_first"], row["name_last"],
              row["specialty_taxonomy"], row["state"], row["city"],
              row["zip"], row["status"], row["deactivation_date"], now))
        result["inserted"] += 1

    total_staged = conn.execute("SELECT COUNT(*) as c FROM stg_providers").fetchone()["c"]
    result["unchanged"] = total_staged - result["inserted"]

    return result
