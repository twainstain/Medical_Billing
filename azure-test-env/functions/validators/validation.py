"""Validation framework for all ingestion sources."""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def validate_claim(claim: dict) -> ValidationResult:
    errors, warnings = [], []
    if not claim.get("claim_id"):
        errors.append("Missing claim_id")
    if not claim.get("date_of_service"):
        errors.append("Missing date_of_service")
    if not claim.get("total_billed") or claim["total_billed"] <= 0:
        errors.append(f"Invalid total_billed: {claim.get('total_billed')}")
    if not claim.get("lines"):
        errors.append("No service lines")
    for i, line in enumerate(claim.get("lines", [])):
        if not line.get("cpt_code"):
            errors.append(f"Line {i}: missing cpt_code")
    if not claim.get("diagnosis_codes"):
        warnings.append("No diagnosis codes")
    if not claim.get("provider_npi"):
        warnings.append("Missing provider NPI")
    if not claim.get("payer_id"):
        warnings.append("Missing payer_id")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_remittance(remit: dict) -> ValidationResult:
    errors, warnings = [], []
    if not remit.get("trace_number"):
        errors.append("Missing trace_number")
    if remit.get("paid_amount") is None:
        errors.append("Missing paid_amount")
    if not remit.get("payer_claim_id"):
        errors.append("Missing payer_claim_id")
    if not remit.get("adjustments"):
        warnings.append("No adjustment reason codes")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_patient(patient: dict) -> ValidationResult:
    errors, warnings = [], []
    if not patient.get("patient_id"):
        errors.append("Missing patient_id")
    if not patient.get("dob"):
        errors.append("Missing dob")
    if not patient.get("last_name"):
        warnings.append("Missing last_name")
    if not patient.get("insurance_id"):
        warnings.append("Missing insurance_id")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_fee_schedule_row(row: dict) -> ValidationResult:
    errors, warnings = [], []
    if not row.get("cpt_code"):
        errors.append("Missing cpt_code")
    if not row.get("geo_region"):
        errors.append("Missing geo_region")
    if row.get("rate") is None or row["rate"] < 0:
        errors.append(f"Invalid rate: {row.get('rate')}")
    if not row.get("effective_date"):
        errors.append("Missing effective_date")
    if not row.get("rate_type"):
        errors.append("Missing rate_type")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)


def validate_provider(provider: dict) -> ValidationResult:
    errors, warnings = [], []
    if not provider.get("npi"):
        errors.append("Missing npi")
    if not provider.get("specialty_taxonomy"):
        warnings.append("Missing specialty_taxonomy")
    if not provider.get("state"):
        warnings.append("Missing state")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)
