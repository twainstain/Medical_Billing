"""CSV parsers for fee schedules, NPPES providers, and backfill claims."""

import csv
import io
from typing import List, Dict, Set, Optional


def parse_fee_schedule_csv(content: str) -> List[dict]:
    """Parse a fee schedule CSV (Medicare, FAIR Health, or payer)."""
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        rows.append({
            "cpt_code": row["cpt_code"].strip(),
            "modifier": row.get("modifier", "").strip(),
            "geo_region": row["geo_region"].strip(),
            "rate": float(row["rate"]),
            "rate_type": row["rate_type"].strip(),
            "effective_date": row["effective_date"].strip(),
        })
    return rows


def parse_nppes_csv(content: str, known_npis: Set[str] = None,
                    state_whitelist: Set[str] = None) -> List[dict]:
    """Parse NPPES CSV, optionally filtering to known NPIs or state whitelist."""
    reader = csv.DictReader(io.StringIO(content))
    providers = []
    for row in reader:
        npi = row.get("NPI", "").strip()
        entity_type = row.get("Entity Type Code", "")
        state = row.get("Provider Business Practice Location Address State Name", "").strip()

        # Filter: only individuals (type 1)
        if entity_type != "1":
            continue
        # Filter by known NPIs or state whitelist
        if known_npis is not None and state_whitelist is not None:
            if npi not in known_npis and state not in state_whitelist:
                continue

        deact_reason = row.get("NPI Deactivation Reason Code", "").strip()
        deact_date = row.get("NPI Deactivation Date", "").strip() or None
        status = "deactivated" if deact_reason else "active"

        providers.append({
            "npi": npi,
            "name_first": row.get("Provider First Name", "").strip(),
            "name_last": row.get("Provider Last Name (Legal Name)", "").strip(),
            "specialty_taxonomy": row.get("Healthcare Provider Taxonomy Code_1", "").strip(),
            "state": state,
            "city": row.get("Provider Business Practice Location Address City Name", "").strip(),
            "zip": row.get("Provider Business Practice Location Address Postal Code", "")[:5].strip(),
            "status": status,
            "deactivation_date": deact_date,
        })
    return providers


def parse_backfill_csv(content: str) -> List[dict]:
    """Parse historical claims backfill CSV."""
    reader = csv.DictReader(io.StringIO(content))
    claims = []
    for row in reader:
        cpt_list = row.get("cpt_codes", "").split(";")
        diag_list = row.get("diagnosis_codes", "").split(";") if row.get("diagnosis_codes") else []
        lines = []
        for cpt in cpt_list:
            cpt = cpt.strip()
            if cpt:
                lines.append({"cpt_code": cpt, "billed_amount": 0.0, "units": 1, "modifier": None})

        # Distribute billed amount evenly across lines if needed
        total = float(row.get("total_billed", 0))
        if lines:
            per_line = round(total / len(lines), 2)
            for line in lines:
                line["billed_amount"] = per_line

        claims.append({
            "claim_id": row["claim_id"].strip(),
            "patient_id": row.get("patient_id", "").strip() or None,
            "provider_npi": row.get("provider_npi", "").strip() or None,
            "payer_id": row.get("payer_id", "").strip() or None,
            "date_of_service": row.get("date_of_service", "").strip(),
            "total_billed": total,
            "diagnosis_codes": diag_list,
            "lines": lines,
            "frequency_code": "1",
        })
    return claims
