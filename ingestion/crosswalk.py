"""Code crosswalk — maps legacy codes to standard codes during backfill ingestion.

The code_crosswalk table (already in the schema) stores:
  legacy_code, standard_code, source_system

This module loads crosswalk data and applies mappings to claims.
"""

import csv
import io
import logging
import sqlite3
from typing import List

logger = logging.getLogger(__name__)


def load_crosswalk(conn: sqlite3.Connection, csv_content: str) -> dict:
    """Load crosswalk mappings from CSV into the code_crosswalk table.

    Returns: {loaded: int, skipped: int}
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    summary = {"loaded": 0, "skipped": 0}
    for row in reader:
        legacy = row["legacy_code"].strip()
        standard = row["standard_code"].strip()
        source = row.get("source_system", "").strip()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO code_crosswalk (legacy_code, standard_code, source_system)
                VALUES (?, ?, ?)
            """, (legacy, standard, source))
            summary["loaded"] += 1
        except Exception:
            summary["skipped"] += 1
    conn.commit()
    return summary


def apply_crosswalk(conn: sqlite3.Connection, claim: dict,
                    source_system: str = None) -> dict:
    """Apply code crosswalk to a claim's CPT codes and payer_id.

    Mutates and returns the claim dict. Unmapped codes are left as-is.
    """
    for line in claim.get("lines", []):
        mapped = _lookup(conn, line["cpt_code"], source_system)
        if mapped:
            logger.info("Crosswalk: CPT %s → %s", line["cpt_code"], mapped)
            line["cpt_code"] = mapped

    if claim.get("payer_id"):
        mapped_payer = _lookup(conn, claim["payer_id"], source_system)
        if mapped_payer:
            logger.info("Crosswalk: Payer %s → %s", claim["payer_id"], mapped_payer)
            claim["payer_id"] = mapped_payer

    return claim


def _lookup(conn: sqlite3.Connection, legacy_code: str,
            source_system: str = None) -> str:
    """Look up a legacy code in the crosswalk table."""
    if source_system:
        row = conn.execute(
            "SELECT standard_code FROM code_crosswalk WHERE legacy_code = ? AND source_system = ?",
            (legacy_code, source_system)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT standard_code FROM code_crosswalk WHERE legacy_code = ?",
            (legacy_code,)
        ).fetchone()
    return row["standard_code"] if row else None
