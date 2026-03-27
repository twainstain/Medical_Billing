"""Event emission — Azure Event Hubs producer.

Cloud replacement for the local SQLite event_log.
Publishes events to Azure Event Hubs for downstream consumers
(workflow engine, CDC triggers, notification services).
"""

import json
import logging
import os
from datetime import datetime, timezone

from azure.eventhub import EventHubProducerClient, EventData

logger = logging.getLogger(__name__)

_producers = {}


def _get_producer(hub_name: str) -> EventHubProducerClient:
    """Get or create an Event Hub producer for the given hub."""
    if hub_name not in _producers:
        conn_str = os.environ["EVENTHUB_CONNECTION_STRING"]
        _producers[hub_name] = EventHubProducerClient.from_connection_string(
            conn_str, eventhub_name=hub_name
        )
    return _producers[hub_name]


# Map entity types to Event Hub names
ENTITY_HUB_MAP = {
    "claim": "claims",
    "remittance": "remittances",
    "document": "documents",
    "patient": "status-changes",
    "provider": "status-changes",
    "fee_schedule": "status-changes",
    "case": "status-changes",
    "dispute": "status-changes",
}


def emit_event(event_type: str, entity_type: str, entity_id: str,
               payload: dict = None):
    """Publish an event to the appropriate Event Hub.

    Args:
        event_type: e.g. "claim.insert", "remittance.received"
        entity_type: e.g. "claim", "remittance", "document"
        entity_id: The entity's primary key
        payload: Optional dict of additional event data
    """
    hub_name = ENTITY_HUB_MAP.get(entity_type, "status-changes")

    event_body = {
        "event_type": event_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "payload": payload,
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer = _get_producer(hub_name)
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event_body, default=str)))
        producer.send_batch(batch)
        logger.info("Event sent to %s: %s %s/%s", hub_name, event_type,
                     entity_type, entity_id)
    except Exception as e:
        # Log but don't fail the ingestion — events are best-effort
        logger.error("Failed to send event to %s: %s", hub_name, e)


def close_producers():
    """Close all Event Hub producers (call on shutdown)."""
    for name, producer in _producers.items():
        try:
            producer.close()
        except Exception:
            pass
    _producers.clear()
