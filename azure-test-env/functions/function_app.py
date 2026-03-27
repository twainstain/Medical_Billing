"""
Medical Billing Arbitration — Azure Functions App (Ingestion + Workflow + OLAP)

Migrated from local SQLite-based ingestion to Azure cloud services.
Functions 1-7: ingestion triggers (Event Hub, Blob, Timer, HTTP)
Functions 8-10: workflow orchestration via Durable Functions
Functions 11-12: OLAP Medallion pipeline (Bronze→Silver→Gold via pandas + ADLS Parquet)

Architecture reference: medical_billing_arbitration_future_architecture.md § 5, 9
"""

import json
import logging
import azure.functions as func
import azure.durable_functions as df

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Register Durable Functions (orchestrators + activities)
# ---------------------------------------------------------------------------
workflow_bp = df.Blueprint()


# --- Orchestrators ---

@workflow_bp.orchestration_trigger(context_name="context")
def claim_to_dispute_orchestrator(context: df.DurableOrchestrationContext):
    from workflow.orchestrator import claim_to_dispute_orchestrator as impl
    return (yield from impl(context))


@workflow_bp.orchestration_trigger(context_name="context")
def deadline_monitor_orchestrator(context: df.DurableOrchestrationContext):
    from workflow.orchestrator import deadline_monitor_orchestrator as impl
    return (yield from impl(context))


@workflow_bp.orchestration_trigger(context_name="context")
def case_transition_orchestrator(context: df.DurableOrchestrationContext):
    from workflow.orchestrator import case_transition_orchestrator as impl
    return (yield from impl(context))


# --- Activities ---

@workflow_bp.activity_trigger(input_name="input_data")
def detect_underpaid_claims(input_data):
    from workflow.activities import detect_underpaid_claims as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="claim_id")
def create_dispute_and_case(claim_id: str):
    from workflow.activities import create_dispute_and_case as impl
    return impl(claim_id)


@workflow_bp.activity_trigger(input_name="input_data")
def set_regulatory_deadlines(input_data: dict):
    from workflow.activities import set_regulatory_deadlines as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def get_at_risk_deadlines(input_data: dict):
    from workflow.activities import get_at_risk_deadlines as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def get_missed_deadlines(input_data):
    from workflow.activities import get_missed_deadlines as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def mark_deadline_missed(input_data: dict):
    from workflow.activities import mark_deadline_missed as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def complete_deadline(input_data: dict):
    from workflow.activities import complete_deadline as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def close_all_deadlines(input_data: dict):
    from workflow.activities import close_all_deadlines as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def validate_case_transition(input_data: dict):
    from workflow.activities import validate_case_transition as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def execute_case_transition(input_data: dict):
    from workflow.activities import execute_case_transition as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def send_deadline_alert(input_data: dict):
    from workflow.activities import send_deadline_alert as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="deadlines")
def escalate_missed_deadlines(deadlines: list):
    from workflow.activities import escalate_missed_deadlines as impl
    return impl(deadlines)


@workflow_bp.activity_trigger(input_name="input_data")
def send_analyst_notification(input_data: dict):
    from workflow.activities import send_analyst_notification as impl
    return impl(input_data)


@workflow_bp.activity_trigger(input_name="input_data")
def write_audit_log(input_data: dict):
    from workflow.activities import write_audit_log as impl
    return impl(input_data)


app.register_functions(workflow_bp)


# =============================================================================
# 1. CLAIMS INGESTION — Event Hub trigger
#    Receives EDI 837 files pushed to the "claims" Event Hub
# =============================================================================
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="claims",
    connection="EVENTHUB_CONNECTION_STRING",
    consumer_group="$Default"
)
def ingest_claims_function(event: func.EventHubEvent):
    """Process EDI 837 claims from Event Hub."""
    raw_edi = event.get_body().decode("utf-8")
    logger.info("Claims function triggered, body length: %d", len(raw_edi))

    try:
        from ingest.claims import ingest_claims
        result = ingest_claims(raw_edi)
        logger.info("Claims ingestion complete: %s", json.dumps(result))
    except Exception as e:
        logger.error("Claims ingestion failed: %s", e, exc_info=True)
        raise


# =============================================================================
# 2. REMITTANCES INGESTION — Event Hub trigger
#    Receives EDI 835 files pushed to the "remittances" Event Hub
# =============================================================================
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="remittances",
    connection="EVENTHUB_CONNECTION_STRING",
    consumer_group="$Default"
)
def ingest_remittances_function(event: func.EventHubEvent):
    """Process EDI 835 remittances from Event Hub."""
    raw_edi = event.get_body().decode("utf-8")
    logger.info("Remittances function triggered, body length: %d", len(raw_edi))

    try:
        from ingest.remittances import ingest_era
        result = ingest_era(raw_edi)
        logger.info("ERA ingestion complete: %s", json.dumps(result))
    except Exception as e:
        logger.error("ERA ingestion failed: %s", e, exc_info=True)
        raise


# =============================================================================
# 3. DOCUMENT PROCESSING — Blob trigger
#    Fires when a document is uploaded to the "documents" container
# =============================================================================
@app.blob_trigger(
    arg_name="blob",
    path="documents/{name}",
    connection="STORAGE_CONNECTION_STRING"
)
def ingest_document_function(blob: func.InputStream):
    """Process uploaded documents (PDF, images) — classify and store."""
    filename = blob.name or "unknown"
    content = blob.read()
    logger.info("Document function triggered: %s (%d bytes)", filename, len(content))

    try:
        from ingest.documents import ingest_document
        result = ingest_document(content, filename)
        logger.info("Document ingestion complete: %s", json.dumps(result))
    except Exception as e:
        logger.error("Document ingestion failed: %s", e, exc_info=True)
        raise


# =============================================================================
# 4. PATIENT DATA — HTTP trigger (FHIR webhook)
#    Receives FHIR Patient resources via POST
# =============================================================================
@app.route(
    route="ingest/patients",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def ingest_patients_function(req: func.HttpRequest) -> func.HttpResponse:
    """Ingest patients from FHIR webhook (POST JSON)."""
    logger.info("Patient ingestion HTTP trigger")

    try:
        fhir_json = req.get_body().decode("utf-8")
        from ingest.patients import ingest_patients
        result = ingest_patients(fhir_json)
        return func.HttpResponse(
            json.dumps(result), status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error("Patient ingestion failed: %s", e, exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(e)}), status_code=500,
            mimetype="application/json"
        )


# =============================================================================
# 5. FEE SCHEDULE BATCH — Timer trigger
#    Runs daily to check for new fee schedule files in blob storage
# =============================================================================
@app.timer_trigger(
    schedule="0 0 6 * * *",  # 6 AM UTC daily
    arg_name="timer",
    run_on_startup=False
)
def ingest_fee_schedule_function(timer: func.TimerRequest):
    """Check for new fee schedule CSVs in the 'bronze' container and ingest."""
    logger.info("Fee schedule timer triggered (daily batch)")

    try:
        from azure.storage.blob import BlobServiceClient
        import os

        storage_conn = os.environ.get("STORAGE_CONNECTION_STRING", "")
        if not storage_conn or storage_conn.startswith("<"):
            logger.info("Storage not configured — skipping fee schedule batch")
            return

        blob_service = BlobServiceClient.from_connection_string(storage_conn)
        container = blob_service.get_container_client("bronze")

        from ingest.fee_schedules import ingest_fee_schedule

        # Process any CSV files in bronze/fee-schedules/
        for blob in container.list_blobs(name_starts_with="fee-schedules/"):
            if not blob.name.endswith(".csv"):
                continue

            logger.info("Processing fee schedule: %s", blob.name)
            blob_client = container.get_blob_client(blob.name)
            csv_content = blob_client.download_blob().readall().decode("utf-8")

            # Extract payer_id from filename convention: fee-schedules/<payer_id>_rates.csv
            filename = blob.name.split("/")[-1]
            payer_id = int(filename.split("_")[0]) if filename[0].isdigit() else 1

            result = ingest_fee_schedule(csv_content, payer_id, source_label=blob.name)
            logger.info("Fee schedule %s: %s", blob.name, json.dumps(result))

            # Move processed file to bronze/fee-schedules/processed/
            dest_blob = container.get_blob_client(f"fee-schedules/processed/{filename}")
            dest_blob.start_copy_from_url(blob_client.url)
            blob_client.delete_blob()

    except Exception as e:
        logger.error("Fee schedule batch failed: %s", e, exc_info=True)
        raise


# =============================================================================
# 6. EOB PROCESSING — Event Hub trigger
#    Receives Document Intelligence extraction results for EOB PDFs
# =============================================================================
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="documents",
    connection="EVENTHUB_CONNECTION_STRING",
    consumer_group="$Default"
)
def ingest_eob_function(event: func.EventHubEvent):
    """Process EOB extraction results from Document Intelligence pipeline."""
    body = event.get_body().decode("utf-8")
    logger.info("EOB function triggered, body length: %d", len(body))

    try:
        from ingest.remittances import ingest_eob
        result = ingest_eob(body)
        logger.info("EOB ingestion complete: %s", json.dumps(result))
    except Exception as e:
        logger.error("EOB ingestion failed: %s", e, exc_info=True)
        raise


# =============================================================================
# 7. HEALTH CHECK — HTTP trigger
#    Quick connectivity test for SQL + Event Hubs
# =============================================================================
@app.route(
    route="health",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check — verifies SQL connectivity."""
    checks = {"sql": "unknown", "status": "unhealthy"}
    try:
        from shared.db import fetchone
        row = fetchone("SELECT 1 AS ok")
        checks["sql"] = "connected" if row and row["ok"] == 1 else "failed"
        checks["status"] = "healthy"
    except Exception as e:
        checks["sql"] = f"error: {e}"

    status = 200 if checks["status"] == "healthy" else 503
    return func.HttpResponse(
        json.dumps(checks), status_code=status,
        mimetype="application/json"
    )


# =============================================================================
# 8. WORKFLOW: Create Disputes — HTTP trigger
#    Starts the claim-to-dispute Durable Function orchestration
# =============================================================================
@app.route(
    route="workflow/create-disputes",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
@app.durable_client_input(client_name="starter")
async def create_disputes_function(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    """Start claim-to-dispute pipeline."""
    from workflow.deadline_monitor import claim_dispute_http_trigger
    return await claim_dispute_http_trigger(req, starter)


# =============================================================================
# 9. WORKFLOW: Case Transition — HTTP trigger
#    Transitions a case to a new status via the Durable Function orchestration
# =============================================================================
@app.route(
    route="workflow/transition-case",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
@app.durable_client_input(client_name="starter")
async def transition_case_function(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    """Transition a case status (NSA workflow state machine)."""
    from workflow.deadline_monitor import case_transition_http_trigger
    return await case_transition_http_trigger(req, starter)


# =============================================================================
# 10. WORKFLOW: Deadline Monitor — Timer trigger (every 6 hours)
#     Checks at-risk/missed deadlines and sends alerts
# =============================================================================
@app.timer_trigger(
    schedule="0 0 */6 * * *",  # every 6 hours
    arg_name="timer",
    run_on_startup=False
)
@app.durable_client_input(client_name="starter")
async def deadline_monitor_function(timer: func.TimerRequest, starter: str):
    """Timer-triggered deadline monitoring."""
    from workflow.deadline_monitor import deadline_monitor_timer
    await deadline_monitor_timer(timer, starter)


# =============================================================================
# 11. OLAP PIPELINE — Timer trigger (every 4 hours)
#     Bronze CDC extraction → Silver transforms → Gold aggregations
#     Uses pandas + ADLS Gen2 Parquet (Medallion architecture)
# =============================================================================
@app.timer_trigger(
    schedule="0 0 */4 * * *",  # every 4 hours
    arg_name="timer",
    run_on_startup=False
)
def olap_pipeline_function(timer: func.TimerRequest):
    """Run full Medallion pipeline: Bronze → Silver → Gold."""
    logger.info("OLAP pipeline timer triggered")

    results = {}
    try:
        from olap.bronze import extract_all_bronze
        results["bronze"] = extract_all_bronze()
        logger.info("Bronze extraction complete")
    except Exception as e:
        logger.error("Bronze extraction failed: %s", e, exc_info=True)
        results["bronze"] = f"ERROR: {e}"

    try:
        from olap.silver import transform_all_silver
        results["silver"] = transform_all_silver()
        logger.info("Silver transforms complete")
    except Exception as e:
        logger.error("Silver transforms failed: %s", e, exc_info=True)
        results["silver"] = f"ERROR: {e}"

    try:
        from olap.gold import aggregate_all_gold
        results["gold"] = aggregate_all_gold()
        logger.info("Gold aggregations complete")
    except Exception as e:
        logger.error("Gold aggregations failed: %s", e, exc_info=True)
        results["gold"] = f"ERROR: {e}"

    logger.info("OLAP pipeline complete: %s", json.dumps(results, default=str))


# =============================================================================
# 12. OLAP PIPELINE — HTTP trigger (manual run)
#     Same Bronze → Silver → Gold pipeline, triggered on demand
# =============================================================================
@app.route(
    route="olap/run",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def olap_run_function(req: func.HttpRequest) -> func.HttpResponse:
    """Manually trigger the full OLAP pipeline or a single layer."""
    body = {}
    try:
        body = req.get_json()
    except ValueError:
        pass

    layer = body.get("layer", "all")  # "bronze", "silver", "gold", or "all"
    results = {}

    try:
        if layer in ("bronze", "all"):
            from olap.bronze import extract_all_bronze
            results["bronze"] = extract_all_bronze()

        if layer in ("silver", "all"):
            from olap.silver import transform_all_silver
            results["silver"] = transform_all_silver()

        if layer in ("gold", "all"):
            from olap.gold import aggregate_all_gold
            results["gold"] = aggregate_all_gold()

        return func.HttpResponse(
            json.dumps({"status": "success", "results": results}, default=str),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error("OLAP manual run failed: %s", e, exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
