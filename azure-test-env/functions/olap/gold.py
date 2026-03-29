"""Gold Layer — Business-ready aggregations for Power BI (Silver Parquet → Gold Parquet).

Reads Silver Parquet from ADLS Gen2, computes business metrics,
writes Gold Parquet back to ADLS. Uses pandas.

Gold tables:
  - recovery_by_payer: recovery metrics per payer
  - cpt_analysis: billed vs. paid per CPT code
  - payer_scorecard: payer behavior/risk assessment
  - financial_summary: overall financial KPIs
  - claims_aging: claim aging buckets
  - case_pipeline: case status with SLA compliance
  - deadline_compliance: deadline met/missed/at-risk by type
  - underpayment_detection: per-claim QPA analysis for arbitration eligibility

Replaces: olap/gold.py (local SQLite version)
Azure equivalent of: Fabric nb_gold_aggregations
"""

import json
import logging
from datetime import datetime

import pandas as pd
import numpy as np

from olap.lake import read_parquet_folder, write_parquet, parquet_path

logger = logging.getLogger(__name__)


def _read_silver(table: str) -> pd.DataFrame:
    return read_parquet_folder("silver", f"{table}/")


def _write_gold(table: str, df: pd.DataFrame) -> int:
    df = df.copy()
    df["_gold_ts"] = datetime.utcnow().isoformat()
    write_parquet("gold", parquet_path(table), df)
    return len(df)


def agg_recovery_by_payer() -> int:
    cr = _read_silver("claim_remittance")
    if cr.empty:
        return 0

    cr = cr[cr["payer_id"].notna()]
    gold = (cr
            .groupby("payer_id")
            .agg(
                payer_name=("payer_name", "first"),
                total_claims=("claim_id", "count"),
                total_billed=("total_billed", "sum"),
                total_paid=("total_paid", "sum"),
                total_underpayment=("underpayment", "sum"),
                denial_count=("has_denial", "sum"),
            )
            .reset_index())

    gold["recovery_rate_pct"] = np.where(
        gold["total_billed"] > 0,
        (gold["total_paid"] / gold["total_billed"] * 100).round(2),
        0
    )
    gold["avg_payment_pct"] = np.where(
        gold["total_claims"] > 0,
        (gold["total_paid"] / gold["total_billed"] * 100).round(2),
        0
    )
    gold["denial_rate_pct"] = np.where(
        gold["total_claims"] > 0,
        (gold["denial_count"] / gold["total_claims"] * 100).round(2),
        0
    )
    return _write_gold("recovery_by_payer", gold)


def agg_cpt_analysis() -> int:
    cr = _read_silver("claim_remittance")
    claims = _read_silver("claims")
    fs = _read_silver("fee_schedule")
    if cr.empty or claims.empty:
        return 0

    # Explode CPT codes
    claims_cpt = claims[["claim_id", "cpt_codes"]].copy()
    claims_cpt["cpt_list"] = claims_cpt["cpt_codes"].apply(
        lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else []
    )
    exploded = claims_cpt.explode("cpt_list").rename(columns={"cpt_list": "cpt_code"})
    exploded = exploded[exploded["cpt_code"].notna()]

    # Count CPTs per claim for proportional allocation
    cpt_counts = exploded.groupby("claim_id").size().reset_index(name="n_codes")
    merged = exploded.merge(cr[["claim_id", "total_billed", "total_paid"]], on="claim_id")
    merged = merged.merge(cpt_counts, on="claim_id")
    merged["per_code_billed"] = merged["total_billed"] / merged["n_codes"]
    merged["per_code_paid"] = merged["total_paid"] / merged["n_codes"]

    gold = (merged
            .groupby("cpt_code")
            .agg(
                claim_count=("claim_id", "count"),
                total_billed=("per_code_billed", "sum"),
                total_paid=("per_code_paid", "sum"),
                avg_billed=("per_code_billed", "mean"),
                avg_paid=("per_code_paid", "mean"),
            )
            .reset_index()
            .round(2))

    gold["payment_ratio_pct"] = np.where(
        gold["total_billed"] > 0,
        (gold["total_paid"] / gold["total_billed"] * 100).round(2),
        0
    )

    # Join fee schedule rates
    if not fs.empty:
        for label, payer_id in [("medicare_rate", "MEDICARE"), ("fair_health_rate", "FAIR_HEALTH")]:
            rates = fs[(fs["payer_id"].astype(str) == payer_id) & (fs["is_current"] == 1)]
            if not rates.empty:
                rate_map = rates.drop_duplicates("cpt_code")[["cpt_code", "rate"]].rename(
                    columns={"rate": label}
                )
                gold = gold.merge(rate_map, on="cpt_code", how="left")
            else:
                gold[label] = None
    else:
        gold["medicare_rate"] = None
        gold["fair_health_rate"] = None

    return _write_gold("cpt_analysis", gold)


def agg_payer_scorecard() -> int:
    cr = _read_silver("claim_remittance")
    if cr.empty:
        return 0

    cr = cr[cr["payer_id"].notna()]
    gold = (cr
            .groupby("payer_id")
            .agg(
                payer_name=("payer_name", "first"),
                total_claims=("claim_id", "count"),
                total_paid=("total_paid", "sum"),
                total_billed=("total_billed", "sum"),
                denial_count=("has_denial", "sum"),
                avg_underpayment=("underpayment", "mean"),
                total_underpayment=("underpayment", "sum"),
            )
            .reset_index()
            .round(2))

    gold["payment_rate_pct"] = np.where(
        gold["total_billed"] > 0,
        (gold["total_paid"] / gold["total_billed"] * 100).round(2),
        0
    )
    gold["denial_rate_pct"] = np.where(
        gold["total_claims"] > 0,
        (gold["denial_count"] / gold["total_claims"] * 100).round(2),
        0
    )

    # Risk tier
    gold["risk_tier"] = np.where(
        (gold["payment_rate_pct"] >= 80) & (gold["denial_rate_pct"] <= 10), "low",
        np.where(
            (gold["payment_rate_pct"] >= 50) | (gold["denial_rate_pct"] <= 30), "medium",
            "high"
        )
    )
    return _write_gold("payer_scorecard", gold)


def agg_financial_summary() -> int:
    cr = _read_silver("claim_remittance")
    if cr.empty:
        return 0

    total_claims = len(cr)
    total_billed = cr["total_billed"].sum()
    total_paid = cr["total_paid"].sum()
    total_underpayment = cr["underpayment"].sum()
    paid_claims = (cr["total_paid"] > 0).sum()
    denial_count = cr["has_denial"].sum()

    metrics = pd.DataFrame([
        {"metric_name": "total_claims", "metric_value": float(total_claims)},
        {"metric_name": "total_billed", "metric_value": float(total_billed)},
        {"metric_name": "total_paid", "metric_value": float(total_paid)},
        {"metric_name": "total_underpayment", "metric_value": float(total_underpayment)},
        {"metric_name": "recovery_rate_pct",
         "metric_value": round(total_paid / total_billed * 100, 2) if total_billed > 0 else 0},
        {"metric_name": "paid_claims", "metric_value": float(paid_claims)},
        {"metric_name": "denial_count", "metric_value": float(denial_count)},
        {"metric_name": "denial_rate_pct",
         "metric_value": round(denial_count / total_claims * 100, 2) if total_claims > 0 else 0},
        {"metric_name": "avg_billed_per_claim",
         "metric_value": round(total_billed / total_claims, 2) if total_claims > 0 else 0},
        {"metric_name": "avg_underpayment_per_claim",
         "metric_value": round(total_underpayment / total_claims, 2) if total_claims > 0 else 0},
    ])
    return _write_gold("financial_summary", metrics)


def agg_claims_aging() -> int:
    cr = _read_silver("claim_remittance")
    if cr.empty:
        return 0

    cr["date_of_service"] = pd.to_datetime(cr["date_of_service"], errors="coerce")
    cr = cr[cr["date_of_service"].notna()]
    cr["age_days"] = (pd.Timestamp.now() - cr["date_of_service"]).dt.days

    cr["aging_bucket"] = pd.cut(
        cr["age_days"],
        bins=[-1, 30, 60, 90, 180, 999999],
        labels=["0-30 days", "31-60 days", "61-90 days", "91-180 days", "180+ days"]
    )

    total = len(cr)
    gold = (cr
            .groupby("aging_bucket", observed=True)
            .agg(
                claim_count=("claim_id", "count"),
                total_billed=("total_billed", "sum"),
                total_unpaid=("underpayment", "sum"),
            )
            .reset_index())

    gold["pct_of_total"] = (gold["claim_count"] / max(total, 1) * 100).round(2)
    return _write_gold("claims_aging", gold)


def agg_case_pipeline() -> int:
    cases = _read_silver("cases")
    deadlines = _read_silver("deadlines")
    if cases.empty:
        return 0

    gold = (cases
            .groupby("status")
            .agg(
                case_count=("case_id", "count"),
                total_billed=("total_billed", "sum"),
                total_underpayment=("total_underpayment", "sum"),
                avg_age_days=("age_days", "mean"),
            )
            .reset_index()
            .round(1))
    gold["total_billed"] = gold["total_billed"].fillna(0)
    gold["total_underpayment"] = gold["total_underpayment"].fillna(0)

    # SLA compliance per status
    if not deadlines.empty and not cases.empty:
        dl_with_status = deadlines.merge(
            cases[["case_id", "status"]].rename(columns={"status": "case_status"}),
            on="case_id", how="inner"
        )
        sla = (dl_with_status
               .groupby("case_status")
               .agg(
                   total_dl=("deadline_id", "count"),
                   met_dl=("status", lambda x: (x == "met").sum())
               )
               .reset_index())
        sla["sla_compliance_pct"] = np.where(
            sla["total_dl"] > 0,
            (sla["met_dl"] / sla["total_dl"] * 100).round(2),
            0
        )
        gold = gold.merge(
            sla[["case_status", "sla_compliance_pct"]].rename(columns={"case_status": "status"}),
            on="status", how="left"
        )
    else:
        gold["sla_compliance_pct"] = 0

    gold["sla_compliance_pct"] = gold["sla_compliance_pct"].fillna(0)
    return _write_gold("case_pipeline", gold)


def agg_deadline_compliance() -> int:
    dl = _read_silver("deadlines")
    if dl.empty:
        return 0

    gold = (dl
            .groupby("type")
            .agg(
                total_deadlines=("deadline_id", "count"),
                met_count=("status", lambda x: (x == "met").sum()),
                missed_count=("status", lambda x: (x == "missed").sum()),
                pending_count=("status", lambda x: (x == "pending").sum()),
                at_risk_count=("is_at_risk", lambda x: (x == 1).sum()),
            )
            .reset_index()
            .rename(columns={"type": "deadline_type"}))

    gold["compliance_pct"] = np.where(
        gold["total_deadlines"] > 0,
        (gold["met_count"] / gold["total_deadlines"] * 100).round(2),
        0
    )
    return _write_gold("deadline_compliance", gold)


def agg_underpayment_detection() -> int:
    disputes = _read_silver("disputes")
    if disputes.empty:
        return 0

    if "underpayment_amount" not in disputes.columns:
        return 0

    disputes["underpayment_amount"] = pd.to_numeric(disputes["underpayment_amount"], errors="coerce")
    gold = disputes[disputes["underpayment_amount"] > 0].copy()
    if gold.empty:
        return 0

    gold["billed_amount"] = pd.to_numeric(gold["billed_amount"], errors="coerce").fillna(0)
    gold["paid_amount"] = pd.to_numeric(gold["paid_amount"], errors="coerce").fillna(0)
    gold["qpa_amount"] = pd.to_numeric(gold["qpa_amount"], errors="coerce").fillna(0)

    gold["underpayment_pct"] = np.where(
        gold["billed_amount"] > 0,
        (gold["underpayment_amount"] / gold["billed_amount"] * 100).round(2),
        0
    )
    gold["arbitration_eligible"] = np.where(
        (gold["underpayment_amount"] > 25) & (gold["billed_amount"] > gold["qpa_amount"]),
        1, 0
    )
    gold = gold.rename(columns={"status": "dispute_status"})

    cols = ["claim_id", "payer_id", "payer_name", "provider_npi",
            "date_of_service", "billed_amount", "paid_amount", "qpa_amount",
            "underpayment_amount", "underpayment_pct", "arbitration_eligible",
            "dispute_status"]
    gold = gold[[c for c in cols if c in gold.columns]]
    return _write_gold("underpayment_detection", gold)


def agg_win_loss_analysis() -> int:
    disputes = _read_silver("disputes")
    cases = _read_silver("cases")
    if disputes.empty or cases.empty:
        return 0

    merged = disputes.merge(cases[["case_id", "status", "outcome", "award_amount", "closed_date"]],
                            on="case_id", how="inner", suffixes=("_d", "_c"))
    closed = merged[merged["status_c"].isin(["decided", "closed"])]
    if closed.empty:
        return 0

    gold = (closed
            .groupby(["payer_id", "payer_name", "dispute_type", "outcome"])
            .agg(
                case_count=("case_id", "count"),
                total_billed=("billed_amount", "sum"),
                total_disputed=("underpayment_amount", "sum"),
                total_awarded=("award_amount", "sum"),
                avg_award=("award_amount", "mean"),
            )
            .reset_index()
            .round(2))

    gold["total_awarded"] = gold["total_awarded"].fillna(0)
    gold["win_rate_pct"] = np.where(
        gold["case_count"] > 0,
        (gold.groupby(["payer_id", "dispute_type"])["outcome"]
         .transform(lambda x: (x == "won").sum() / len(x) * 100)).round(2),
        0
    )
    gold["recovery_roi_pct"] = np.where(
        gold["total_disputed"] > 0,
        (gold["total_awarded"] / gold["total_disputed"] * 100).round(2),
        0
    )
    return _write_gold("win_loss_analysis", gold)


def agg_analyst_productivity() -> int:
    cases = _read_silver("cases")
    disputes = _read_silver("disputes")
    if cases.empty:
        return 0

    cases["created_date"] = pd.to_datetime(cases["created_date"], errors="coerce")
    cases["closed_date"] = pd.to_datetime(cases["closed_date"], errors="coerce")
    cases["resolution_days"] = (cases["closed_date"] - cases["created_date"]).dt.days

    case_dispute = cases.merge(
        disputes.groupby("case_id")["underpayment_amount"].sum().reset_index()
        .rename(columns={"underpayment_amount": "total_disputed"}),
        on="case_id", how="left"
    ) if not disputes.empty else cases.assign(total_disputed=0)

    gold = (case_dispute
            .groupby("assigned_analyst")
            .agg(
                total_cases=("case_id", "count"),
                active_cases=("status", lambda x: (~x.isin(["closed", "decided"])).sum()),
                resolved_cases=("status", lambda x: x.isin(["closed", "decided"]).sum()),
                won_cases=("outcome", lambda x: (x == "won").sum()),
                lost_cases=("outcome", lambda x: (x == "lost").sum()),
                settled_cases=("outcome", lambda x: (x == "settled").sum()),
                total_recovered=("award_amount", "sum"),
                total_disputed=("total_disputed", "sum"),
                avg_resolution_days=("resolution_days", "mean"),
                critical_cases=("priority", lambda x: (x == "critical").sum()),
                high_priority_cases=("priority", lambda x: (x == "high").sum()),
            )
            .reset_index()
            .round(1))

    gold["total_recovered"] = gold["total_recovered"].fillna(0)
    gold["win_rate_pct"] = np.where(
        gold["resolved_cases"] > 0,
        (gold["won_cases"] / gold["resolved_cases"] * 100).round(2),
        0
    )
    return _write_gold("analyst_productivity", gold)


def agg_time_to_resolution() -> int:
    cases = _read_silver("cases")
    disputes = _read_silver("disputes")
    if cases.empty:
        return 0

    cases["created_date"] = pd.to_datetime(cases["created_date"], errors="coerce")
    cases["closed_date"] = pd.to_datetime(cases["closed_date"], errors="coerce")
    now = pd.Timestamp.now()
    cases["elapsed_days"] = (
        cases["closed_date"].fillna(now) - cases["created_date"]
    ).dt.days

    case_disp = cases.merge(
        disputes.groupby("case_id")["underpayment_amount"].sum().reset_index()
        .rename(columns={"underpayment_amount": "total_at_stake"}),
        on="case_id", how="left"
    ) if not disputes.empty else cases.assign(total_at_stake=0)

    gold = (case_disp
            .groupby(["status", "priority"])
            .agg(
                case_count=("case_id", "count"),
                avg_days=("elapsed_days", "mean"),
                min_days=("elapsed_days", "min"),
                max_days=("elapsed_days", "max"),
                total_at_stake=("total_at_stake", "sum"),
                total_recovered=("award_amount", "sum"),
                won_count=("outcome", lambda x: (x == "won").sum()),
            )
            .reset_index()
            .round(1))

    gold["total_recovered"] = gold["total_recovered"].fillna(0)
    gold["total_at_stake"] = gold["total_at_stake"].fillna(0)
    gold["win_rate_pct"] = np.where(
        gold["case_count"] > 0,
        (gold["won_count"] / gold["case_count"] * 100).round(2),
        0
    )
    gold = gold.drop(columns=["won_count"])
    return _write_gold("time_to_resolution", gold)


def agg_provider_performance() -> int:
    claims = _read_silver("claims")
    disputes = _read_silver("disputes")
    cases = _read_silver("cases")
    cr = _read_silver("claim_remittance")
    if claims.empty:
        return 0

    # Provider-level claim metrics
    prov = (cr
            .groupby(["provider_npi", "provider_name"])
            .agg(
                total_claims=("claim_id", "nunique"),
                total_billed=("total_billed", "sum"),
                total_paid=("total_paid", "sum"),
            )
            .reset_index()) if not cr.empty else pd.DataFrame()

    if prov.empty:
        return 0

    prov["total_underpayment"] = prov["total_billed"] - prov["total_paid"]
    prov["payment_rate_pct"] = np.where(
        prov["total_billed"] > 0,
        (prov["total_paid"] / prov["total_billed"] * 100).round(2),
        0
    )

    # Dispute counts per provider
    if not disputes.empty:
        disp_with_prov = (disputes[["dispute_id", "claim_id"]]
                          .merge(claims[["claim_id", "provider_npi"]], on="claim_id"))
        disp_counts = (disp_with_prov
                       .groupby("provider_npi")
                       .agg(total_disputes=("dispute_id", "nunique"))
                       .reset_index())
        prov = prov.merge(disp_counts, on="provider_npi", how="left")
    else:
        prov["total_disputes"] = 0

    # Recovery per provider
    if not disputes.empty and not cases.empty:
        recovery = (disputes[["claim_id", "case_id"]]
                    .merge(cases[["case_id", "award_amount"]], on="case_id")
                    .merge(claims[["claim_id", "provider_npi"]], on="claim_id")
                    .groupby("provider_npi")
                    .agg(total_recovered=("award_amount", "sum"))
                    .reset_index())
        prov = prov.merge(recovery, on="provider_npi", how="left")
    else:
        prov["total_recovered"] = 0

    prov["total_disputes"] = prov["total_disputes"].fillna(0).astype(int)
    prov["total_recovered"] = prov["total_recovered"].fillna(0)
    prov["dispute_rate_pct"] = np.where(
        prov["total_claims"] > 0,
        (prov["total_disputes"] / prov["total_claims"] * 100).round(2),
        0
    )
    return _write_gold("provider_performance", prov.round(2))


def agg_monthly_trends() -> int:
    cr = _read_silver("claim_remittance")
    disputes = _read_silver("disputes")
    cases = _read_silver("cases")
    if cr.empty:
        return 0

    cr["date_of_service"] = pd.to_datetime(cr["date_of_service"], errors="coerce")
    cr = cr[cr["date_of_service"].notna()]
    cr["month"] = cr["date_of_service"].dt.strftime("%Y-%m")

    gold = (cr
            .groupby("month")
            .agg(
                claim_count=("claim_id", "count"),
                total_billed=("total_billed", "sum"),
                total_paid=("total_paid", "sum"),
                denial_count=("has_denial", "sum"),
            )
            .reset_index()
            .round(2))

    gold["total_underpayment"] = gold["total_billed"] - gold["total_paid"]
    gold["recovery_rate_pct"] = np.where(
        gold["total_billed"] > 0,
        (gold["total_paid"] / gold["total_billed"] * 100).round(2),
        0
    )

    # Add dispute and resolution counts per month
    if not disputes.empty:
        disputes["filed_date"] = pd.to_datetime(disputes["filed_date"], errors="coerce")
        disp_monthly = (disputes[disputes["filed_date"].notna()]
                        .assign(month=lambda x: x["filed_date"].dt.strftime("%Y-%m"))
                        .groupby("month")
                        .agg(new_disputes=("dispute_id", "count"))
                        .reset_index())
        gold = gold.merge(disp_monthly, on="month", how="left")
    else:
        gold["new_disputes"] = 0

    if not cases.empty:
        cases["closed_date"] = pd.to_datetime(cases["closed_date"], errors="coerce")
        resolved = (cases[cases["closed_date"].notna()]
                    .assign(month=lambda x: x["closed_date"].dt.strftime("%Y-%m"))
                    .groupby("month")
                    .agg(
                        resolved_cases=("case_id", "count"),
                        total_awarded=("award_amount", "sum"),
                    )
                    .reset_index())
        gold = gold.merge(resolved, on="month", how="left")
    else:
        gold["resolved_cases"] = 0
        gold["total_awarded"] = 0

    gold = gold.fillna(0)
    return _write_gold("monthly_trends", gold)


def aggregate_all_gold() -> dict:
    """Run all Gold aggregations. Returns {table: row_count}."""
    aggregations = [
        ("recovery_by_payer", agg_recovery_by_payer),
        ("cpt_analysis", agg_cpt_analysis),
        ("payer_scorecard", agg_payer_scorecard),
        ("financial_summary", agg_financial_summary),
        ("claims_aging", agg_claims_aging),
        ("case_pipeline", agg_case_pipeline),
        ("deadline_compliance", agg_deadline_compliance),
        ("underpayment_detection", agg_underpayment_detection),
        ("win_loss_analysis", agg_win_loss_analysis),
        ("analyst_productivity", agg_analyst_productivity),
        ("time_to_resolution", agg_time_to_resolution),
        ("provider_performance", agg_provider_performance),
        ("monthly_trends", agg_monthly_trends),
    ]

    summary = {}
    for name, fn in aggregations:
        try:
            count = fn()
            summary[name] = count
            logger.info("Gold %s: %d rows", name, count)
        except Exception as e:
            summary[name] = f"ERROR: {e}"
            logger.error("Gold %s failed: %s", name, e, exc_info=True)

    logger.info("Gold aggregation complete: %s", summary)
    return summary
