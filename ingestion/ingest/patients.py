"""Patient data ingestion — FHIR R4 and HL7 v2."""

import json
import logging
import sqlite3

from ingestion.parsers.fhir_patient import normalize_fhir_patient
from ingestion.parsers.hl7v2_patient import normalize_hl7v2_patient
from ingestion.validators.validation import validate_patient
from ingestion.audit import log_action
from ingestion.dlq import send_to_dlq
from ingestion.events import emit_event

logger = logging.getLogger(__name__)


def ingest_patients(conn: sqlite3.Connection, fhir_json: str) -> dict:
    """Ingest patients from FHIR Patient resources (JSON array).

    Returns: {inserted: int, updated: int, skipped: int, errors: int}
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
            logger.warning("Patient validation failed: %s", validation.errors)
            send_to_dlq(conn, "fhir", "patient", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=patient.get("patient_id"))
            summary["errors"] += 1
            continue

        # Upsert (SCD Type 1)
        cursor = conn.execute("SELECT patient_id FROM patients WHERE patient_id = ?",
                              (patient["patient_id"],))
        exists = cursor.fetchone() is not None

        if exists:
            conn.execute("""
                UPDATE patients
                SET first_name = ?, last_name = ?, dob = ?, gender = ?,
                    address_line = ?, city = ?, state = ?, zip = ?,
                    insurance_id = ?, source_system = ?, last_updated = ?
                WHERE patient_id = ?
            """, (patient["first_name"], patient["last_name"], patient["dob"],
                  patient["gender"], patient["address_line"], patient["city"],
                  patient["state"], patient["zip"], patient["insurance_id"],
                  patient["source_system"], patient["last_updated"],
                  patient["patient_id"]))
            summary["updated"] += 1
            log_action(conn, "patient", patient["patient_id"], "update")
            emit_event(conn, "patient.update", "patient", patient["patient_id"])
        else:
            conn.execute("""
                INSERT INTO patients (patient_id, first_name, last_name, dob, gender,
                                      address_line, city, state, zip, insurance_id,
                                      source_system, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (patient["patient_id"], patient["first_name"], patient["last_name"],
                  patient["dob"], patient["gender"], patient["address_line"],
                  patient["city"], patient["state"], patient["zip"],
                  patient["insurance_id"], patient["source_system"],
                  patient["last_updated"]))
            summary["inserted"] += 1
            log_action(conn, "patient", patient["patient_id"], "insert")
            emit_event(conn, "patient.insert", "patient", patient["patient_id"])

    conn.commit()
    return summary


def _upsert_patient(conn: sqlite3.Connection, patient: dict, summary: dict):
    """Shared upsert logic for both FHIR and HL7 v2."""
    cursor = conn.execute("SELECT patient_id FROM patients WHERE patient_id = ?",
                          (patient["patient_id"],))
    exists = cursor.fetchone() is not None

    if exists:
        conn.execute("""
            UPDATE patients
            SET first_name = ?, last_name = ?, dob = ?, gender = ?,
                address_line = ?, city = ?, state = ?, zip = ?,
                insurance_id = ?, source_system = ?, last_updated = ?
            WHERE patient_id = ?
        """, (patient["first_name"], patient["last_name"], patient["dob"],
              patient["gender"], patient["address_line"], patient["city"],
              patient["state"], patient["zip"], patient["insurance_id"],
              patient["source_system"], patient["last_updated"],
              patient["patient_id"]))
        summary["updated"] += 1
        log_action(conn, "patient", patient["patient_id"], "update")
        emit_event(conn, "patient.update", "patient", patient["patient_id"])
    else:
        conn.execute("""
            INSERT INTO patients (patient_id, first_name, last_name, dob, gender,
                                  address_line, city, state, zip, insurance_id,
                                  source_system, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (patient["patient_id"], patient["first_name"], patient["last_name"],
              patient["dob"], patient["gender"], patient["address_line"],
              patient["city"], patient["state"], patient["zip"],
              patient["insurance_id"], patient["source_system"],
              patient["last_updated"]))
        summary["inserted"] += 1
        log_action(conn, "patient", patient["patient_id"], "insert")
        emit_event(conn, "patient.insert", "patient", patient["patient_id"])


def ingest_hl7v2_patients(conn: sqlite3.Connection, raw_hl7: str) -> dict:
    """Ingest patients from HL7 v2 ADT messages (multi-message file).

    Messages are separated by blank lines or double-MSH.
    Returns: {inserted: int, updated: int, skipped: int, errors: int}
    """
    summary = {"inserted": 0, "updated": 0, "skipped": 0, "errors": 0}

    # Split multi-message file: each message starts with MSH
    messages = _split_hl7_messages(raw_hl7)

    for raw_msg in messages:
        patient = normalize_hl7v2_patient(raw_msg)
        if patient is None:
            logger.warning("HL7 v2 message could not be parsed")
            send_to_dlq(conn, "hl7v2", "patient", "parse_failure",
                        "Could not parse HL7 v2 ADT message")
            summary["errors"] += 1
            continue

        validation = validate_patient(patient)
        if not validation.is_valid:
            logger.warning("HL7 v2 patient validation failed: %s", validation.errors)
            send_to_dlq(conn, "hl7v2", "patient", "validation_failure",
                        "; ".join(validation.errors),
                        entity_id=patient.get("patient_id"))
            summary["errors"] += 1
            continue

        _upsert_patient(conn, patient, summary)

    conn.commit()
    return summary


def _split_hl7_messages(raw: str) -> list:
    """Split a multi-message HL7 file into individual messages."""
    # Normalize line endings
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    messages = []
    current = []
    for line in raw.split("\n"):
        if line.startswith("MSH") and current:
            messages.append("\n".join(current))
            current = []
        if line.strip():
            current.append(line)
    if current:
        messages.append("\n".join(current))
    return messages
