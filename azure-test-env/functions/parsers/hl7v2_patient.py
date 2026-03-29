"""HL7 v2 ADT Message Parser — Patient demographics.

Parses HL7 v2.x ADT messages (A01 admit, A04 register, A08 update)
and maps them to the same normalized patient schema used by the FHIR parser.

Sample input (HL7 v2 ADT^A01):

    MSH|^~\\&|EHR_EPIC|FACILITY_A|MEDBILL|ARBSYS|20250615120000||ADT^A01|MSG0001|P|2.5
    PID|1|INS200001|INS200001^^^MRN||Roberts^Michael^James||19780520|M|||456 Oak Ave^^San Francisco^CA^94102
    IN1|1|BCBS-HMO-99876|||BCBS_CA

Key: PID-3=MRN, PID-5=Name(Last^First^Middle), PID-7=DOB, PID-8=Gender, PID-11=Address

In production, you'd use the python-hl7 library for robust parsing.
This implementation handles the pipe-delimited format directly.
"""

from datetime import datetime
from typing import Optional


def parse_hl7v2_message(raw_message: str) -> dict:
    """Parse a raw HL7 v2 message into segments."""
    # Normalize line endings: HL7 v2 uses \r, but files may use \r\n or \n
    normalized = raw_message.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.strip().split("\n")

    segments = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        fields = line.split("|")
        seg_id = fields[0]
        if seg_id not in segments:
            segments[seg_id] = []
        segments[seg_id].append(fields)
    return segments


def normalize_hl7v2_patient(raw_message: str) -> Optional[dict]:
    """Extract canonical patient fields from an HL7 v2 ADT message.

    Supports ADT^A01 (admit), ADT^A04 (register), ADT^A08 (update).
    Returns the same schema as normalize_fhir_patient().
    """
    segments = parse_hl7v2_message(raw_message)

    # Validate: must have MSH and PID
    if "MSH" not in segments or "PID" not in segments:
        return None

    msh = segments["MSH"][0]
    pid = segments["PID"][0]

    # MSH-9: Message Type (e.g., ADT^A01) — at index 8 because MSH-1 is the field separator
    msg_type = msh[8] if len(msh) > 8 else ""
    if "ADT" not in msg_type:
        return None

    # PID-3: Patient ID (MRN) — component separator is ^
    patient_id = _extract_component(pid, 3, 0)

    # PID-5: Patient Name (last^first^middle)
    last_name = _extract_component(pid, 5, 0)
    first_name = _extract_component(pid, 5, 1)
    middle_name = _extract_component(pid, 5, 2)
    if middle_name:
        first_name = f"{first_name} {middle_name}"

    # PID-7: Date of Birth (YYYYMMDD → YYYY-MM-DD)
    dob_raw = _extract_field(pid, 7)
    dob = _format_date(dob_raw)

    # PID-8: Gender (M/F/O/U → male/female/other/unknown)
    gender_raw = _extract_field(pid, 8)
    gender = _map_gender(gender_raw)

    # PID-11: Address (street^city^state^zip^country)
    address_line = _extract_component(pid, 11, 0)
    city = _extract_component(pid, 11, 2)
    state = _extract_component(pid, 11, 3)
    zip_code = _extract_component(pid, 11, 4)

    # IN1 segment: Insurance info (if present)
    insurance_id = None
    if "IN1" in segments:
        in1 = segments["IN1"][0]
        insurance_id = _extract_field(in1, 2)

    return {
        "patient_id": patient_id,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "gender": gender,
        "address_line": address_line,
        "city": city,
        "state": state,
        "zip": zip_code,
        "insurance_id": insurance_id,
        "source_system": "hl7v2",
        "last_updated": datetime.utcnow().isoformat(),
    }


def _extract_field(segment: list, index: int) -> str:
    """Extract a field from a segment by index."""
    if index < len(segment):
        return segment[index].split("^")[0] if segment[index] else ""
    return ""


def _extract_component(segment: list, field_index: int, component_index: int) -> str:
    """Extract a component from a field (^ separated)."""
    if field_index < len(segment) and segment[field_index]:
        components = segment[field_index].split("^")
        if component_index < len(components):
            return components[component_index].strip()
    return ""


def _format_date(raw: str) -> Optional[str]:
    """Convert YYYYMMDD to YYYY-MM-DD."""
    raw = raw.strip()
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return None


def _map_gender(code: str) -> str:
    mapping = {"M": "male", "F": "female", "O": "other", "U": "unknown"}
    return mapping.get(code.upper(), "unknown")
