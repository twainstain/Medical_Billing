"""Silver Layer — Cleaned, conformed, joined data (Bronze Parquet → Silver Parquet).

Reads Bronze Parquet from ADLS Gen2, resolves CDC to current state,
applies cleaning/joining/enrichment, writes Silver Parquet back to ADLS.
Uses pandas (runs on Azure Functions consumption plan).

Replaces: olap/silver.py (local SQLite version)
Azure equivalent of: Fabric nb_silver_transforms
"""

import json
import logging
from datetime import datetime, date

import pandas as pd
import numpy as np

from olap.lake import read_parquet_folder, write_parquet, parquet_path

logger = logging.getLogger(__name__)


def _resolve_cdc(df: pd.DataFrame, pk: str) -> pd.DataFrame:
    """Resolve CDC events to current state — keep latest non-delete per PK."""
    if df.empty:
        return df
    if "_cdc_timestamp" in df.columns:
        df = df.sort_values("_cdc_timestamp", ascending=False)
    df = df.drop_duplicates(subset=[pk], keep="first")
    if "_cdc_operation" in df.columns:
        df = df[df["_cdc_operation"] != "D"]
    # Drop CDC metadata columns
    drop_cols = [c for c in df.columns if c.startswith("_cdc_") or c.startswith("_source_")]
    return df.drop(columns=drop_cols, errors="ignore")


def _read_bronze(table: str, pk: str) -> pd.DataFrame:
    """Read all Bronze Parquet for a table and resolve to current state."""
    df = read_parquet_folder("bronze", f"{table}/")
    return _resolve_cdc(df, pk)


def _write_silver(table: str, df: pd.DataFrame):
    """Write Silver table as single Parquet file."""
    df = df.copy()
    df["_silver_ts"] = datetime.utcnow().isoformat()
    write_parquet("silver", parquet_path(table), df)
    return len(df)


def transform_patients() -> int:
    df = _read_bronze("patients", "patient_id")
    if df.empty:
        return 0
    silver = df[["patient_id", "first_name", "last_name", "date_of_birth",
                  "gender", "state", "zip_code", "insurance_id"]].copy()
    silver.columns = ["patient_id", "first_name", "last_name", "dob",
                      "gender", "state", "zip", "insurance_id"]
    return _write_silver("patients", silver)


def transform_providers() -> int:
    df = _read_bronze("providers", "npi")
    if df.empty:
        return 0
    silver = df[["npi", "name", "specialty", "state", "is_active"]].copy()
    return _write_silver("providers", silver)


def transform_claims() -> int:
    claims = _read_bronze("claims", "claim_id")
    lines = _read_bronze("claim_lines", "line_id")
    if claims.empty:
        return 0

    # Aggregate lines per claim
    if not lines.empty:
        lines_agg = (lines
                     .groupby("claim_id")
                     .agg(
                         line_count=("line_id", "count"),
                         cpt_codes=("cpt_code", lambda x: json.dumps(list(x.unique()))),
                         total_units=("units", "sum")
                     )
                     .reset_index())
        silver = claims.merge(lines_agg, on="claim_id", how="left")
    else:
        silver = claims.copy()
        silver["line_count"] = 0
        silver["cpt_codes"] = "[]"
        silver["total_units"] = 0

    silver["line_count"] = silver["line_count"].fillna(0).astype(int)
    silver["total_units"] = silver["total_units"].fillna(0).astype(int)

    cols = ["claim_id", "external_claim_id", "patient_id", "provider_npi", "payer_id",
            "date_of_service", "date_filed", "total_billed", "status",
            "line_count", "cpt_codes", "total_units"]
    silver = silver[[c for c in cols if c in silver.columns]]
    return _write_silver("claims", silver)


def transform_remittances() -> int:
    remit = _read_bronze("remittances", "remit_id")
    payers = _read_bronze("payers", "payer_id")
    if remit.empty:
        return 0

    if not payers.empty:
        payer_names = payers[["payer_id", "name"]].rename(columns={"name": "payer_name"})
        silver = remit.merge(payer_names, on="payer_id", how="left")
    else:
        silver = remit.copy()
        silver["payer_name"] = None

    cols = ["remit_id", "claim_id", "payer_id", "payer_name", "era_date",
            "paid_amount", "allowed_amount", "adjustment_reason",
            "denial_code", "check_number", "source_type"]
    silver = silver[[c for c in cols if c in silver.columns]]
    return _write_silver("remittances", silver)


def transform_claim_remittance() -> int:
    """Join Silver claims with Silver remittances for underpayment analysis."""
    claims = read_parquet_folder("silver", "claims/")
    remit = read_parquet_folder("silver", "remittances/")
    payers = _read_bronze("payers", "payer_id")

    if claims.empty:
        return 0

    if not remit.empty:
        remit_agg = (remit
                     .groupby("claim_id")
                     .agg(
                         total_paid=("paid_amount", "sum"),
                         total_allowed=("allowed_amount", "sum"),
                         remittance_count=("remit_id", "count"),
                         has_denial=("denial_code", lambda x: int(x.notna().any() and (x != "").any()))
                     )
                     .reset_index())
        silver = claims.merge(remit_agg, on="claim_id", how="left")
    else:
        silver = claims.copy()
        silver["total_paid"] = 0
        silver["total_allowed"] = 0
        silver["remittance_count"] = 0
        silver["has_denial"] = 0

    silver["total_paid"] = silver["total_paid"].fillna(0)
    silver["total_allowed"] = silver["total_allowed"].fillna(0)
    silver["remittance_count"] = silver["remittance_count"].fillna(0).astype(int)
    silver["has_denial"] = silver["has_denial"].fillna(0).astype(int)
    silver["underpayment"] = silver["total_billed"] - silver["total_paid"]

    # Add payer names
    if not payers.empty:
        payer_names = payers[["payer_id", "name"]].rename(columns={"name": "payer_name"})
        silver = silver.merge(payer_names, on="payer_id", how="left")
    else:
        silver["payer_name"] = None

    silver = silver.rename(columns={"status": "claim_status"})
    cols = ["claim_id", "patient_id", "provider_npi", "payer_id", "payer_name",
            "date_of_service", "total_billed", "total_paid", "total_allowed",
            "underpayment", "cpt_codes", "line_count", "remittance_count",
            "has_denial", "claim_status"]
    silver = silver[[c for c in cols if c in silver.columns]]
    return _write_silver("claim_remittance", silver)


def transform_fee_schedule() -> int:
    df = _read_bronze("fee_schedule", "id")
    if df.empty:
        return 0
    cols = ["id", "payer_id", "cpt_code", "modifier", "geo_region",
            "rate", "rate_type", "valid_from", "valid_to", "is_current", "source"]
    silver = df[[c for c in cols if c in df.columns]]
    return _write_silver("fee_schedule", silver)


def transform_disputes() -> int:
    disputes = _read_bronze("disputes", "dispute_id")
    if disputes.empty:
        return 0

    claims = _read_bronze("claims", "claim_id")
    payers = _read_bronze("payers", "payer_id")

    # Enrich with claim context
    if not claims.empty:
        claim_ctx = claims[["claim_id", "payer_id", "provider_npi", "date_of_service"]].copy()
        claim_ctx.columns = ["claim_id", "c_payer_id", "c_provider_npi", "c_dos"]
        disputes = disputes.merge(claim_ctx, on="claim_id", how="left")
        disputes["payer_id"] = disputes.get("payer_id", disputes["c_payer_id"])

    # Get CPT codes from silver claims
    silver_claims = read_parquet_folder("silver", "claims/")
    if not silver_claims.empty and "cpt_codes" in silver_claims.columns:
        cpt_lookup = silver_claims[["claim_id", "cpt_codes"]]
        disputes = disputes.merge(cpt_lookup, on="claim_id", how="left", suffixes=("", "_cl"))

    # Add payer names
    if not payers.empty:
        payer_names = payers[["payer_id", "name"]].rename(columns={"name": "payer_name"})
        if "payer_id" in disputes.columns:
            disputes = disputes.merge(payer_names, on="payer_id", how="left")

    cols = ["dispute_id", "claim_id", "case_id", "payer_id", "payer_name",
            "dispute_type", "status", "billed_amount", "paid_amount",
            "qpa_amount", "underpayment_amount", "filed_date",
            "c_provider_npi", "c_dos", "cpt_codes"]
    available = [c for c in cols if c in disputes.columns]
    silver = disputes[available].copy()
    if "c_provider_npi" in silver.columns:
        silver = silver.rename(columns={"c_provider_npi": "provider_npi", "c_dos": "date_of_service"})
    return _write_silver("disputes", silver)


def transform_cases() -> int:
    cases = _read_bronze("cases", "case_id")
    if cases.empty:
        return 0

    # Aggregate from silver disputes
    silver_disputes = read_parquet_folder("silver", "disputes/")
    if not silver_disputes.empty:
        disp_agg = (silver_disputes
                    .groupby("case_id")
                    .agg(
                        dispute_count=("dispute_id", "count"),
                        total_billed=("billed_amount", "sum"),
                        total_underpayment=("underpayment_amount", "sum")
                    )
                    .reset_index()
                    .fillna(0))
        silver = cases.merge(disp_agg, on="case_id", how="left")
    else:
        silver = cases.copy()
        silver["dispute_count"] = 0
        silver["total_billed"] = 0
        silver["total_underpayment"] = 0

    silver["dispute_count"] = silver["dispute_count"].fillna(0).astype(int)
    silver["total_billed"] = silver["total_billed"].fillna(0)
    silver["total_underpayment"] = silver["total_underpayment"].fillna(0)

    # Calculate age in days
    if "created_date" in silver.columns:
        silver["created_date"] = pd.to_datetime(silver["created_date"], errors="coerce")
        silver["age_days"] = (pd.Timestamp.utcnow() - silver["created_date"]).dt.days.fillna(0).astype(int)
    else:
        silver["age_days"] = 0

    cols = ["case_id", "assigned_analyst", "status", "priority", "created_date",
            "last_activity", "outcome", "award_amount",
            "dispute_count", "total_billed", "total_underpayment", "age_days"]
    silver = silver[[c for c in cols if c in silver.columns]]
    return _write_silver("cases", silver)


def transform_deadlines() -> int:
    dl = _read_bronze("deadlines", "deadline_id")
    if dl.empty:
        return 0

    dl["due_date"] = pd.to_datetime(dl["due_date"], errors="coerce")
    now = pd.Timestamp.utcnow()

    dl["days_remaining"] = np.where(
        dl["status"].isin(["met", "missed"]),
        None,
        (dl["due_date"] - now).dt.days
    )
    dl["is_at_risk"] = np.where(
        pd.to_numeric(dl["days_remaining"], errors="coerce") <= 5,
        1, 0
    )

    cols = ["deadline_id", "case_id", "dispute_id", "type", "due_date",
            "status", "completed_date", "days_remaining", "is_at_risk"]
    silver = dl[[c for c in cols if c in dl.columns]]
    return _write_silver("deadlines", silver)


def transform_all_silver() -> dict:
    """Run all Silver transforms. Returns {table: row_count}."""
    transforms = [
        ("patients", transform_patients),
        ("providers", transform_providers),
        ("claims", transform_claims),
        ("remittances", transform_remittances),
        ("claim_remittance", transform_claim_remittance),
        ("fee_schedule", transform_fee_schedule),
        ("disputes", transform_disputes),
        ("cases", transform_cases),
        ("deadlines", transform_deadlines),
    ]

    summary = {}
    for name, fn in transforms:
        try:
            count = fn()
            summary[name] = count
            logger.info("Silver %s: %d rows", name, count)
        except Exception as e:
            summary[name] = f"ERROR: {e}"
            logger.error("Silver %s failed: %s", name, e, exc_info=True)

    logger.info("Silver transform complete: %s", summary)
    return summary
