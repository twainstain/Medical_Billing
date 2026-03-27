"""Patient data ingestion — FHIR R4 (Azure SQL version)."""

import json
import logging
from datetime import datetime, timezone

from parsers.fhir_patient import normalize_fhir_patient
from validators.validation import validate_patient
from shared.audit import log_action
from shared.dlq import send_to_dlq
from shared.events import emit_event
from shared.db import execute_query, fetchone, commit

logger = logging.getLogger(__name__)


def ingest_patients(fhir_json: str) -> dict:
    """Ingest patients from FHIR Patient resources (JSON array or single).

    Returns: {inserted, updated, skipped, errors}
    """
    resources = json.loads(fhir_json)
    if isinstance(resources, dict):
        resources = [resources]

    summary = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

    for fhir_resource in resources:
        if fhir_resource.get("resourceType") != "Patient":
            continue

        patient = normalize_fhir_patient(fhir_resource)
        validation = validate_patient(patient)
        if not validation.is_valid:
            send_to_dlq("fhir", "patient", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=patient.get("patient_id"))
            summary["errors"] += 1
            continue

        _upsert_patient(patient, summary)

    commit()
    return summary


def _upsert_patient(patient: dict, summary: dict):
    """SCD Type 1 upsert — overwrite demographics."""
    existing = fetchone(
        "SELECT patient_id FROM patients WHERE patient_id = ?",
        (patient["patient_id"],)
    )
    # Note: the Azure SQL schema uses INT IDENTITY for patient_id, but the
    # FHIR patient_id is a string (MRN). We store MRN in the insurance_id
    # field and use an auto-generated ID. For the test env, we match on
    # insurance_id for upsert.
    now = datetime.now(timezone.utc).isoformat()

    if existing:
        execute_query("""
            UPDATE patients
            SET first_name = ?, last_name = ?, date_of_birth = ?, gender = ?,
                address_line1 = ?, city = ?, state = ?, zip_code = ?,
                insurance_id = ?, updated_at = ?
            WHERE patient_id = ?
        """, (patient["first_name"], patient["last_name"], patient["dob"],
              patient["gender"], patient["address_line"], patient["city"],
              patient["state"], patient["zip"], patient["insurance_id"],
              now, existing["patient_id"]))
        summary["updated"] += 1
        log_action("patient", str(existing["patient_id"]), "update")
        emit_event("patient.update", "patient", str(existing["patient_id"]))
    else:
        execute_query("""
            INSERT INTO patients (first_name, last_name, date_of_birth, gender,
                                  address_line1, city, state, zip_code,
                                  insurance_id, payer_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """, (patient["first_name"], patient["last_name"], patient["dob"],
              patient["gender"], patient["address_line"], patient["city"],
              patient["state"], patient["zip"], patient["insurance_id"]))
        summary["inserted"] += 1
        log_action("patient", patient["patient_id"], "insert")
        emit_event("patient.insert", "patient", patient["patient_id"])
