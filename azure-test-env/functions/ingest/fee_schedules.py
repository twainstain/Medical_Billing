"""Fee schedule ingestion — SCD Type 2 MERGE (Azure SQL version)."""

import logging
from datetime import datetime, timedelta, timezone

from parsers.csv_parser import parse_fee_schedule_csv
from validators.validation import validate_fee_schedule_row
from shared.audit import log_action
from shared.dlq import send_to_dlq
from shared.events import emit_event
from shared.db import execute_query, fetchone, fetchall, commit

logger = logging.getLogger(__name__)


def ingest_fee_schedule(csv_content: str, payer_id: int,
                        source_label: str = None) -> dict:
    """Load fee schedule CSV, validate, and run SCD Type 2 merge.

    Returns: {staged, closed, inserted, unchanged, errors}
    """
    rows = parse_fee_schedule_csv(csv_content)
    summary = {"staged": 0, "closed": 0, "inserted": 0, "unchanged": 0, "errors": 0}

    # Validate rows
    valid_rows = []
    for row in rows:
        validation = validate_fee_schedule_row(row)
        if not validation.is_valid:
            send_to_dlq("csv_fee_schedule", "fee_schedule", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=f"{payer_id}/{row.get('cpt_code')}",
                        payload=row)
            summary["errors"] += 1
            continue
        valid_rows.append(row)
        summary["staged"] += 1

    # SCD Type 2 MERGE directly (no staging table needed in Azure SQL)
    merge_result = _scd2_merge(valid_rows, payer_id)
    summary["closed"] = merge_result["closed"]
    summary["inserted"] = merge_result["inserted"]
    summary["unchanged"] = merge_result["unchanged"]

    source = source_label or f"payer_{payer_id}_batch"
    log_action("fee_schedule", source, "scd2_merge",
               old_value=f"closed:{summary['closed']}",
               new_value=f"inserted:{summary['inserted']}")

    if summary["inserted"] > 0 or summary["closed"] > 0:
        emit_event("fee_schedule.merge", "fee_schedule", source,
                   {"payer_id": payer_id, "inserted": summary["inserted"],
                    "closed": summary["closed"]})

    commit()
    return summary


def _scd2_merge(rows: list, payer_id: int) -> dict:
    """Execute SCD Type 2 merge from validated rows into fee_schedule."""
    result = {"closed": 0, "inserted": 0, "unchanged": 0}
    now = datetime.now(timezone.utc).isoformat()

    for row in rows:
        modifier = row.get("modifier", "") or ""

        # Check if current row exists with same rate
        existing = fetchone("""
            SELECT id, rate FROM fee_schedule
            WHERE payer_id = ? AND cpt_code = ? AND modifier = ?
              AND geo_region = ? AND rate_type = ? AND is_current = 1
        """, (payer_id, row["cpt_code"], modifier,
              row["geo_region"], row["rate_type"]))

        if existing is None:
            # New row — insert
            execute_query("""
                INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region,
                                          rate, rate_type, valid_from, valid_to,
                                          is_current, source, loaded_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, ?, ?)
            """, (payer_id, row["cpt_code"], modifier, row["geo_region"],
                  row["rate"], row["rate_type"], row["effective_date"],
                  f"payer_{payer_id}_{row['rate_type']}", now))
            result["inserted"] += 1

        elif abs(existing["rate"] - row["rate"]) > 0.001:
            # Rate changed — close old, insert new
            close_date = (datetime.strptime(row["effective_date"], "%Y-%m-%d")
                          - timedelta(days=1)).strftime("%Y-%m-%d")
            execute_query("""
                UPDATE fee_schedule SET valid_to = ?, is_current = 0
                WHERE id = ?
            """, (close_date, existing["id"]))
            result["closed"] += 1

            execute_query("""
                INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region,
                                          rate, rate_type, valid_from, valid_to,
                                          is_current, source, loaded_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, ?, ?)
            """, (payer_id, row["cpt_code"], modifier, row["geo_region"],
                  row["rate"], row["rate_type"], row["effective_date"],
                  f"payer_{payer_id}_{row['rate_type']}", now))
            result["inserted"] += 1
        else:
            result["unchanged"] += 1

    return result
