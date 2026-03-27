"""Deadline Monitor — Timer-triggered Azure Function.

Runs every 6 hours to check for at-risk and missed deadlines.
Starts the deadline_monitor_orchestrator Durable Function.

Schedule: 0 0 */6 * * * (every 6 hours)

In production, this replaces the manual deadline checking in
olap/seed_cases.py with automated, reliable monitoring.
"""

import azure.functions as func
import azure.durable_functions as df
import logging

logger = logging.getLogger(__name__)


async def deadline_monitor_timer(timer: func.TimerRequest,
                                  starter: str) -> None:
    """Timer trigger that starts the deadline monitor orchestration."""
    if timer.past_due:
        logger.warning("Deadline monitor timer is past due")

    client = df.DurableOrchestrationClient(starter)

    instance_id = await client.start_new(
        "deadline_monitor_orchestrator",
        instance_id=None,
        client_input={}
    )

    logger.info("Started deadline monitor orchestration: %s", instance_id)


async def claim_dispute_http_trigger(req: func.HttpRequest,
                                      starter: str) -> func.HttpResponse:
    """HTTP trigger to manually start the claim-to-dispute pipeline.

    POST /api/workflow/create-disputes
    Body (optional): {"claim_ids": ["CLM-001", "CLM-002"]}
    """
    client = df.DurableOrchestrationClient(starter)

    try:
        body = req.get_json()
    except ValueError:
        body = {}

    instance_id = await client.start_new(
        "claim_to_dispute_orchestrator",
        instance_id=None,
        client_input=body
    )

    logger.info("Started claim-to-dispute orchestration: %s", instance_id)
    return client.create_check_status_response(req, instance_id)


async def case_transition_http_trigger(req: func.HttpRequest,
                                        starter: str) -> func.HttpResponse:
    """HTTP trigger to transition a case status.

    POST /api/workflow/transition-case
    Body: {"case_id": 1, "target_status": "in_review", "user_id": "analyst_jsmith"}
    """
    client = df.DurableOrchestrationClient(starter)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            '{"error": "Request body must be JSON with case_id and target_status"}',
            status_code=400, mimetype="application/json"
        )

    if "case_id" not in body or "target_status" not in body:
        return func.HttpResponse(
            '{"error": "Missing required fields: case_id, target_status"}',
            status_code=400, mimetype="application/json"
        )

    instance_id = await client.start_new(
        "case_transition_orchestrator",
        instance_id=None,
        client_input=body
    )

    logger.info("Started case transition: case %s → %s",
                body["case_id"], body["target_status"])
    return client.create_check_status_response(req, instance_id)
