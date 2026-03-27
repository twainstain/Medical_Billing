"""Fee schedule ingestion — Medicare, FAIR Health, Payer (SCD Type 2 MERGE)."""

import logging
import sqlite3
from datetime import datetime, timedelta

from ingestion.parsers.csv_parser import parse_fee_schedule_csv
from ingestion.validators.validation import validate_fee_schedule_row
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_fee_schedule(conn: sqlite3.Connection, csv_content: str,
                        payer_id: str, source_label: str = None) -> dict:
    """Load fee schedule CSV into staging, then run SCD Type 2 merge.

    Returns: {staged: int, closed: int, inserted: int, unchanged: int, errors: int}
    """
    rows = parse_fee_schedule_csv(csv_content)
    summary = {"staged": 0, "closed": 0, "inserted": 0, "unchanged": 0, "errors": 0}

    # Clear staging
    conn.execute("DELETE FROM stg_fee_schedule")

    # Validate and load to staging
    for row in rows:
        validation = validate_fee_schedule_row(row)
        if not validation.is_valid:
            logger.warning("Fee schedule row invalid: %s", validation.errors)
            send_to_dlq(conn, "csv_fee_schedule", "fee_schedule", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=f"{payer_id}/{row.get('cpt_code')}",
                        payload=row)
            summary["errors"] += 1
            continue

        conn.execute("""
            INSERT INTO stg_fee_schedule (cpt_code, modifier, geo_region,
                                          rate, rate_type, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["cpt_code"], row.get("modifier", ""), row["geo_region"],
              row["rate"], row["rate_type"], row["effective_date"]))
        summary["staged"] += 1

    # SCD Type 2 MERGE
    merge_result = _scd2_merge(conn, payer_id)
    summary["closed"] = merge_result["closed"]
    summary["inserted"] = merge_result["inserted"]
    summary["unchanged"] = merge_result["unchanged"]

    source = source_label or f"{payer_id}_BATCH"
    log_action(conn, "fee_schedule", source, "scd2_merge",
               old_value=f"closed:{summary['closed']}",
               new_value=f"inserted:{summary['inserted']}")

    if summary["inserted"] > 0 or summary["closed"] > 0:
        emit_event(conn, "fee_schedule.merge", "fee_schedule", source,
                   {"payer_id": payer_id, "inserted": summary["inserted"],
                    "closed": summary["closed"]})

    conn.commit()
    return summary


def _scd2_merge(conn: sqlite3.Connection, payer_id: str) -> dict:
    """Execute SCD Type 2 merge from staging into fee_schedule."""
    result = {"closed": 0, "inserted": 0, "unchanged": 0}

    # Step 1: Find rows to close (current rows where rate changed)
    changed_rows = conn.execute("""
        SELECT fs.id, stg.effective_date
        FROM fee_schedule fs
        INNER JOIN stg_fee_schedule stg
            ON  fs.cpt_code   = stg.cpt_code
            AND fs.modifier   = COALESCE(stg.modifier, '')
            AND fs.geo_region = stg.geo_region
            AND fs.rate_type  = stg.rate_type
        WHERE fs.payer_id = ?
          AND fs.is_current = 1
          AND fs.rate <> stg.rate
    """, (payer_id,)).fetchall()

    for row in changed_rows:
        eff_date = row["effective_date"]
        # Close: set valid_to to day before new effective date
        close_date = (datetime.strptime(eff_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        conn.execute("""
            UPDATE fee_schedule SET valid_to = ?, is_current = 0
            WHERE id = ?
        """, (close_date, row["id"]))
        result["closed"] += 1

    # Step 2: Insert new/changed rows (where no current row with same rate exists)
    to_insert = conn.execute("""
        SELECT stg.cpt_code, COALESCE(stg.modifier, '') as modifier,
               stg.geo_region, stg.rate, stg.rate_type, stg.effective_date
        FROM stg_fee_schedule stg
        WHERE NOT EXISTS (
            SELECT 1 FROM fee_schedule fs
            WHERE fs.payer_id   = ?
              AND fs.cpt_code   = stg.cpt_code
              AND fs.modifier   = COALESCE(stg.modifier, '')
              AND fs.geo_region = stg.geo_region
              AND fs.rate_type  = stg.rate_type
              AND fs.is_current = 1
              AND fs.rate       = stg.rate
        )
    """, (payer_id,)).fetchall()

    now = datetime.utcnow().isoformat()
    for row in to_insert:
        conn.execute("""
            INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region,
                                      rate, rate_type, valid_from, valid_to,
                                      is_current, source, loaded_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, ?, ?)
        """, (payer_id, row["cpt_code"], row["modifier"], row["geo_region"],
              row["rate"], row["rate_type"], row["effective_date"],
              f"{payer_id}_{row['rate_type']}", now))
        result["inserted"] += 1

    # Count unchanged
    total_staged = conn.execute("SELECT COUNT(*) as c FROM stg_fee_schedule").fetchone()["c"]
    result["unchanged"] = total_staged - result["inserted"]

    return result
