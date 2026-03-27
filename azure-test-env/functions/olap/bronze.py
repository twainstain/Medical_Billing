"""Bronze Layer — CDC extraction from Azure SQL OLTP → ADLS Gen2 Parquet.

Reads rows changed since last watermark from each OLTP table,
writes append-only Parquet files to the Bronze container with CDC metadata.
Uses pandas + pyodbc (runs on Azure Functions consumption plan).

Replaces: olap/cdc.py + olap/bronze.py (local SQLite version)
Azure equivalent of: ADF pl_cdc_incremental_copy + Fabric nb_bronze_cdc
"""

import json
import logging
from datetime import datetime

import pandas as pd

from shared.db import get_connection, fetchone, execute
from olap.lake import write_parquet, parquet_path_dated

logger = logging.getLogger(__name__)

# Tables to track with their primary key and watermark column
CDC_TABLES = {
    "claims":              {"pk": "claim_id",    "watermark": "updated_at"},
    "claim_lines":         {"pk": "line_id",     "watermark": "created_at"},
    "remittances":         {"pk": "remit_id",    "watermark": "created_at"},
    "patients":            {"pk": "patient_id",  "watermark": "updated_at"},
    "providers":           {"pk": "npi",         "watermark": "created_at"},
    "payers":              {"pk": "payer_id",    "watermark": "created_at"},
    "fee_schedule":        {"pk": "id",          "watermark": "loaded_date"},
    "cases":               {"pk": "case_id",     "watermark": "last_activity"},
    "disputes":            {"pk": "dispute_id",  "watermark": "updated_at"},
    "deadlines":           {"pk": "deadline_id", "watermark": "created_at"},
    "evidence_artifacts":  {"pk": "artifact_id", "watermark": "uploaded_date"},
    "audit_log":           {"pk": "log_id",      "watermark": "timestamp"},
}


def _get_watermark(table: str) -> str:
    """Get last sync timestamp for a table from cdc_watermarks."""
    row = fetchone(
        "SELECT last_sync_ts FROM cdc_watermarks WHERE source_table = ?",
        (table,)
    )
    if row:
        return str(row["last_sync_ts"])
    return "1900-01-01"


def _update_watermark(table: str, new_ts: str, rows_synced: int):
    """Update watermark after successful extraction."""
    execute("""
        MERGE cdc_watermarks AS tgt
        USING (SELECT ? AS source_table) AS src
        ON tgt.source_table = src.source_table
        WHEN MATCHED THEN
            UPDATE SET
                last_sync_ts  = ?,
                rows_synced   = ?,
                total_inserts = tgt.total_inserts + ?,
                updated_at    = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (source_table, last_sync_ts, rows_synced, total_inserts, updated_at)
            VALUES (?, ?, ?, ?, SYSUTCDATETIME());
    """, (table, new_ts, rows_synced, rows_synced, table, new_ts, rows_synced, rows_synced))


def extract_table(table: str, config: dict) -> dict:
    """Extract changed rows from one OLTP table to Bronze Parquet.

    Returns: {"table": str, "rows": int, "status": str}
    """
    watermark_col = config["watermark"]
    last_watermark = _get_watermark(table)
    now = datetime.utcnow().isoformat()

    try:
        conn = get_connection()
        query = f"""
            SELECT *, '{now}' AS _cdc_timestamp, 'I' AS _cdc_operation,
                   '{table}' AS _source_table
            FROM {table}
            WHERE {watermark_col} > ?
        """
        df = pd.read_sql(query, conn, params=[last_watermark])

        if df.empty:
            return {"table": table, "rows": 0, "status": "no_changes"}

        # Write to Bronze container as date-partitioned Parquet
        path = parquet_path_dated(table)
        write_parquet("bronze", path, df)

        # Find max watermark from extracted data
        max_ts = str(df[watermark_col].max())
        _update_watermark(table, max_ts, len(df))

        return {"table": table, "rows": len(df), "status": "success"}

    except Exception as e:
        logger.error("Bronze extraction failed for %s: %s", table, e)
        return {"table": table, "rows": 0, "status": f"error: {e}"}


def extract_all_bronze() -> dict:
    """Run CDC extraction for all OLTP tables → Bronze Parquet.

    Returns: {table_name: {"rows": N, "status": str}}
    """
    summary = {}
    for table, config in CDC_TABLES.items():
        result = extract_table(table, config)
        summary[table] = result
        logger.info("Bronze %s: %d rows (%s)", table, result["rows"], result["status"])

    total = sum(r["rows"] for r in summary.values())
    success = sum(1 for r in summary.values() if r["status"] in ("success", "no_changes"))
    logger.info("Bronze extraction complete: %d tables, %d total rows", success, total)
    return summary
