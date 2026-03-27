"""Deduplication and idempotency logic for all ingestion sources."""

import sqlite3
from typing import Optional

FREQ_CODE_MAP = {"1": "original", "7": "replacement", "8": "void"}


def resolve_claim_duplicate(conn: sqlite3.Connection, claim_id: str,
                            frequency_code: str) -> str:
    """Returns: 'insert', 'update', 'void', or 'skip'."""
    cursor = conn.execute("SELECT status FROM claims WHERE claim_id = ?", (claim_id,))
    row = cursor.fetchone()
    existing_status = row["status"] if row else None

    if existing_status is None:
        return "insert"

    action = FREQ_CODE_MAP.get(frequency_code, "original")
    if action == "replacement":
        return "update"
    if action == "void":
        return "void"
    return "skip"


def check_remittance_duplicate(conn: sqlite3.Connection, claim_id: str,
                               trace_number: str, payer_id: str) -> bool:
    """Returns True if this remittance already exists."""
    cursor = conn.execute(
        """
        SELECT remit_id FROM remittances
        WHERE claim_id = ? AND trace_number = ? AND payer_id = ?
        """,
        (claim_id, trace_number, payer_id)
    )
    return cursor.fetchone() is not None


def check_evidence_duplicate(conn: sqlite3.Connection, content_hash: str) -> bool:
    """Returns True if this document already exists."""
    cursor = conn.execute(
        "SELECT artifact_id FROM evidence_artifacts WHERE content_hash = ?",
        (content_hash,)
    )
    return cursor.fetchone() is not None


def match_claim_id(conn: sqlite3.Connection, payer_claim_id: str) -> Optional[str]:
    """Resolve payer's claim ID to our canonical claim_id."""
    # Direct match
    cursor = conn.execute("SELECT claim_id FROM claims WHERE claim_id = ?",
                          (payer_claim_id,))
    row = cursor.fetchone()
    if row:
        return row["claim_id"]

    # Normalized match
    normalized = payer_claim_id.lstrip("0").upper().strip()
    cursor = conn.execute(
        "SELECT claim_id FROM claims WHERE UPPER(TRIM(claim_id)) = ?",
        (normalized,)
    )
    row = cursor.fetchone()
    if row:
        return row["claim_id"]

    # Alias table
    cursor = conn.execute(
        "SELECT canonical_claim_id FROM claim_id_alias WHERE payer_claim_id = ?",
        (payer_claim_id,)
    )
    row = cursor.fetchone()
    if row:
        return row["canonical_claim_id"]

    return None


def check_backfill_duplicate(conn: sqlite3.Connection, claim_id: str,
                             date_of_service: str, provider_npi: str,
                             total_billed: float) -> bool:
    """Returns True if this historical claim already exists."""
    cursor = conn.execute("""
        SELECT claim_id FROM claims
        WHERE claim_id = ? AND date_of_service = ?
          AND provider_npi = ? AND total_billed = ?
    """, (claim_id, date_of_service, provider_npi, total_billed))
    return cursor.fetchone() is not None
