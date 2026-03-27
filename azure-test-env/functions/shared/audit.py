"""Audit logging — writes to Azure SQL audit_log table."""

import logging
from datetime import datetime, timezone
from shared.db import execute_query, commit

logger = logging.getLogger(__name__)


def log_action(entity_type: str, entity_id: str, action: str,
               user_id: str = "system", old_value: str = None,
               new_value: str = None):
    """Record an ingestion action in the audit_log table."""
    execute_query("""
        INSERT INTO audit_log (entity_type, entity_id, action, user_id,
                               timestamp, old_value, new_value)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (entity_type, entity_id, action, user_id,
          datetime.now(timezone.utc).isoformat(), old_value, new_value))
