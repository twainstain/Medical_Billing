"""Dead-Letter Queue — Azure SQL version.

Records failed/rejected records in the dead_letter_queue table.
In a future iteration, this could also publish to Azure Service Bus DLQ.
"""

import json
import logging
from datetime import datetime, timezone
from shared.db import execute_query

logger = logging.getLogger(__name__)


def send_to_dlq(source: str, entity_type: str, error_category: str,
                error_detail: str, entity_id: str = None,
                payload: dict = None):
    """Route a failed record to the dead-letter queue."""
    payload_ref = json.dumps(payload, default=str) if payload else None
    execute_query("""
        INSERT INTO dead_letter_queue
            (source, entity_type, entity_id, error_category, error_detail,
             payload_ref, created_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
    """, (source, entity_type, entity_id, error_category, error_detail,
          payload_ref, datetime.now(timezone.utc).isoformat()))
    logger.warning("DLQ: %s %s/%s — %s", error_category, entity_type,
                   entity_id or "unknown", error_detail)
