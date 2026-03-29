"""Mock Doc Intelligence extraction for EOB PDFs and contracts.

In production, these would call Azure Doc Intelligence APIs.
For testing, they read pre-extracted JSON files.

Sample EOB extraction input::

    {
        "source_file": "eob_cigna_clm1005.pdf",
        "payer_id": "CIGNA",
        "fields": {
            "ClaimNumber": {"value": "CLM-1005", "confidence": 0.95},
            "PaidAmount": {"value": "180.00", "confidence": 0.92},
            "AllowedAmount": {"value": "200.00", "confidence": 0.91},
            "PatientResp": {"value": "40.00", "confidence": 0.88},
            "AdjustmentReason": {"value": "CO-45", "confidence": 0.85}
        },
        "avg_confidence": 0.917
    }

Field names vary by payer. FIELD_MAPPINGS normalizes them:
    "ClaimNumber" / "Claim_No" / "ClaimID"  →  "claim_number"
    "PaidAmount" / "PaymentAmount"           →  "paid_amount"
    "AllowedAmount" / "Allowed"              →  "allowed_amount"

Sample contract extraction input::

    {
        "source_file": "contract_aetna_2025.pdf",
        "payer_id": "AETNA",
        "provider_npi": "1234567890",
        "effective_date": "2025-01-01",
        "tables": [
            [
                {"col_0": "CPT Code", "col_1": "Rate"},
                {"col_0": "99213", "col_1": "$130.00"},
                {"col_0": "99214", "col_1": "$190.00"}
            ]
        ]
    }
"""

import hashlib
import json
from typing import List, Dict, Union


# Standard field name mappings (payer-specific → canonical)
FIELD_MAPPINGS = {
    "claim_number": ["ClaimNumber", "Claim_No", "ClaimID", "claim_number"],
    "paid_amount": ["PaidAmount", "PaymentAmount", "AmountPaid", "paid_amount"],
    "allowed_amount": ["AllowedAmount", "Allowed", "allowed_amount"],
    "patient_responsibility": ["PatientResp", "PatientAmount", "patient_resp"],
    "check_number": ["CheckNumber", "Check_No", "EFTNumber", "check_number"],
    "service_date": ["ServiceDate", "DOS", "DateOfService", "service_date"],
    "adjustment_reason": ["AdjustmentReason", "RemarkCode", "DenialReason"],
}


def normalize_eob_extraction(raw_fields: dict) -> dict:
    """Map payer-specific field names to canonical schema."""
    normalized = {}
    for canonical_name, possible_keys in FIELD_MAPPINGS.items():
        for key in possible_keys:
            if key in raw_fields and raw_fields[key].get("value") is not None:
                normalized[canonical_name] = {
                    "value": raw_fields[key]["value"],
                    "confidence": raw_fields[key].get("confidence", 0.0),
                }
                break
    return normalized


def compute_content_hash(content: Union[bytes, str]) -> str:
    """SHA-256 hash of file content for dedup."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def parse_eob_extractions(json_content: str) -> List[dict]:
    """Parse a JSON file of mock Doc Intelligence EOB extractions."""
    raw_list = json.loads(json_content)
    results = []
    for item in raw_list:
        normalized = normalize_eob_extraction(item["fields"])
        results.append({
            "source_file": item["source_file"],
            "payer_id": item.get("payer_id"),
            "fields": normalized,
            "avg_confidence": item.get("avg_confidence", 0.0),
            "content_hash": compute_content_hash(item["source_file"]),
        })
    return results


def parse_contract_extraction(json_content: str) -> dict:
    """Parse a JSON file of mock Doc Intelligence contract extraction."""
    data = json.loads(json_content)
    return {
        "source_file": data["source_file"],
        "payer_id": data["payer_id"],
        "provider_npi": data.get("provider_npi"),
        "effective_date": data["effective_date"],
        "tables": data.get("tables", []),
        "content_hash": compute_content_hash(data["source_file"]),
    }
