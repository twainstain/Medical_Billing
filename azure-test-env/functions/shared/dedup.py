"""Deduplication and idempotency — Azure SQL version.

Same logic as local dedup.py but uses pyodbc via shared.db.
"""

from typing import Optional
from shared.db import fetchone

FREQ_CODE_MAP = {"1": "original", "7": "replacement", "8": "void"}


def resolve_claim_duplicate(claim_id: str, frequency_code: str) -> str:
    """Returns: 'insert', 'update', 'void', or 'skip'."""
    row = fetchone("SELECT status FROM claims WHERE claim_id = ?", (claim_id,))

    if row is None:
        return "insert"

    action = FREQ_CODE_MAP.get(frequency_code, "original")
    if action == "replacement":
        return "update"
    if action == "void":
        return "void"
    return "skip"


def check_remittance_duplicate(claim_id: str, trace_number: str,
                                payer_id: str) -> bool:
    """Returns True if this remittance already exists."""
    row = fetchone("""
        SELECT remit_id FROM remittances
        WHERE claim_id = ? AND trace_number = ? AND payer_id = ?
    """, (claim_id, trace_number, payer_id))
    return row is not None


def check_evidence_duplicate(content_hash: str) -> bool:
    """Returns True if this document already exists."""
    row = fetchone(
        "SELECT artifact_id FROM evidence_artifacts WHERE content_hash = ?",
        (content_hash,)
    )
    return row is not None


def match_claim_id(payer_claim_id: str) -> Optional[str]:
    """Resolve payer's claim ID to our canonical claim_id."""
    # Direct match
    row = fetchone("SELECT claim_id FROM claims WHERE claim_id = ?",
                   (payer_claim_id,))
    if row:
        return row["claim_id"]

    # Normalized match
    normalized = payer_claim_id.lstrip("0").upper().strip()
    row = fetchone(
        "SELECT claim_id FROM claims WHERE UPPER(LTRIM(RTRIM(claim_id))) = ?",
        (normalized,)
    )
    if row:
        return row["claim_id"]

    # Alias table
    row = fetchone(
        "SELECT canonical_claim_id FROM claim_id_alias WHERE payer_claim_id = ?",
        (payer_claim_id,)
    )
    if row:
        return row["canonical_claim_id"]

    return None
