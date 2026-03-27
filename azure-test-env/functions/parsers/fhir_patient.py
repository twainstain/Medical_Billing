"""FHIR R4 Patient resource normalizer."""

from datetime import datetime


def normalize_fhir_patient(fhir_resource: dict) -> dict:
    """Extract canonical patient fields from a FHIR Patient resource."""
    names = fhir_resource.get("name", [{}])
    official_name = next(
        (n for n in names if n.get("use") == "official"),
        names[0] if names else {}
    )

    identifiers = fhir_resource.get("identifier", [])
    mrn = next(
        (i["value"] for i in identifiers
         if i.get("type", {}).get("coding", [{}])[0].get("code") == "MR"),
        fhir_resource.get("id")
    )

    addresses = fhir_resource.get("address", [])
    home_addr = next(
        (a for a in addresses if a.get("use") == "home"),
        addresses[0] if addresses else {}
    )

    insurance_id = None
    for ext in fhir_resource.get("extension", []):
        if "insurance" in ext.get("url", "").lower():
            insurance_id = ext.get("valueString")

    return {
        "patient_id": mrn,
        "first_name": " ".join(official_name.get("given", [])),
        "last_name": official_name.get("family", ""),
        "dob": fhir_resource.get("birthDate"),
        "gender": fhir_resource.get("gender"),
        "address_line": " ".join(home_addr.get("line", [])),
        "city": home_addr.get("city"),
        "state": home_addr.get("state"),
        "zip": home_addr.get("postalCode"),
        "insurance_id": insurance_id,
        "source_system": "fhir",
        "last_updated": datetime.utcnow().isoformat(),
    }
