"""Gold Layer — Business-ready aggregations for Power BI.

Simulates Fabric Notebook aggregations: Silver → Gold.
In production: materialized views / Delta tables refreshed by Fabric Pipelines,
consumed by Power BI via Direct Lake mode.

Gold tables:
  - gold_recovery_by_payer:       recovery metrics per payer
  - gold_cpt_analysis:            billed vs. paid per CPT code
  - gold_payer_scorecard:         payer behavior summary
  - gold_financial_summary:       overall financial metrics
  - gold_claims_aging:            claim aging buckets
  - gold_case_pipeline:           case status breakdown with SLA compliance
  - gold_deadline_compliance:     deadline met/missed/at-risk by type
  - gold_underpayment_detection:  per-claim QPA analysis for arbitration eligibility
"""

import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)

GOLD_SCHEMA = """
-- Recovery metrics by payer
CREATE TABLE IF NOT EXISTS gold_recovery_by_payer (
    payer_id            TEXT,
    payer_name          TEXT,
    total_claims        INTEGER,
    total_billed        REAL,
    total_paid          REAL,
    total_underpayment  REAL,
    recovery_rate_pct   REAL,
    avg_payment_pct     REAL,
    denial_count        INTEGER,
    denial_rate_pct     REAL,
    _gold_ts            TEXT NOT NULL,
    PRIMARY KEY (payer_id)
);

-- CPT code analysis
CREATE TABLE IF NOT EXISTS gold_cpt_analysis (
    cpt_code            TEXT,
    claim_count         INTEGER,
    total_billed        REAL,
    total_paid          REAL,
    avg_billed          REAL,
    avg_paid            REAL,
    payment_ratio_pct   REAL,
    medicare_rate       REAL,
    fair_health_rate    REAL,
    _gold_ts            TEXT NOT NULL,
    PRIMARY KEY (cpt_code)
);

-- Payer scorecard (behavioral analysis)
CREATE TABLE IF NOT EXISTS gold_payer_scorecard (
    payer_id            TEXT,
    payer_name          TEXT,
    payer_type          TEXT,
    total_claims        INTEGER,
    avg_days_to_pay     REAL,
    payment_rate_pct    REAL,
    denial_rate_pct     REAL,
    avg_underpayment    REAL,
    total_underpayment  REAL,
    risk_tier           TEXT,
    _gold_ts            TEXT NOT NULL,
    PRIMARY KEY (payer_id)
);

-- Overall financial summary
CREATE TABLE IF NOT EXISTS gold_financial_summary (
    metric_name         TEXT PRIMARY KEY,
    metric_value        REAL,
    metric_detail       TEXT,
    _gold_ts            TEXT NOT NULL
);

-- Claims aging analysis
CREATE TABLE IF NOT EXISTS gold_claims_aging (
    aging_bucket        TEXT,
    claim_count         INTEGER,
    total_billed        REAL,
    total_unpaid        REAL,
    pct_of_total        REAL,
    _gold_ts            TEXT NOT NULL,
    PRIMARY KEY (aging_bucket)
);

-- Case pipeline (Section 12.2 — Case Pipeline dashboard)
CREATE TABLE IF NOT EXISTS gold_case_pipeline (
    status              TEXT PRIMARY KEY,
    case_count          INTEGER,
    total_billed        REAL,
    total_underpayment  REAL,
    avg_age_days        REAL,
    sla_compliance_pct  REAL,
    _gold_ts            TEXT NOT NULL
);

-- Deadline compliance (Section 12.2 — Deadline Compliance dashboard)
CREATE TABLE IF NOT EXISTS gold_deadline_compliance (
    deadline_type       TEXT PRIMARY KEY,
    total_deadlines     INTEGER,
    met_count           INTEGER,
    missed_count        INTEGER,
    pending_count       INTEGER,
    at_risk_count       INTEGER,
    compliance_pct      REAL,
    _gold_ts            TEXT NOT NULL
);

-- Underpayment detection (Section 12.2 — Recovery Overview detail)
CREATE TABLE IF NOT EXISTS gold_underpayment_detection (
    claim_id            TEXT PRIMARY KEY,
    payer_id            TEXT,
    payer_name          TEXT,
    provider_npi        TEXT,
    date_of_service     TEXT,
    billed_amount       REAL,
    paid_amount         REAL,
    qpa_amount          REAL,
    underpayment_amount REAL,
    underpayment_pct    REAL,
    arbitration_eligible INTEGER DEFAULT 0,
    dispute_status      TEXT,
    _gold_ts            TEXT NOT NULL
);
"""


def init_gold(olap_conn: sqlite3.Connection):
    olap_conn.executescript(GOLD_SCHEMA)
    olap_conn.commit()


def aggregate_gold(olap_conn: sqlite3.Connection) -> dict:
    """Aggregate Silver → Gold.

    Reads silver_* tables, computes business metrics, writes to gold_* tables.
    Idempotent — full refresh each run.

    Returns: {table: row_count}
    """
    init_gold(olap_conn)
    now = datetime.utcnow().isoformat()
    summary = {}

    summary["gold_recovery_by_payer"] = _agg_recovery_by_payer(olap_conn, now)
    summary["gold_cpt_analysis"] = _agg_cpt_analysis(olap_conn, now)
    summary["gold_payer_scorecard"] = _agg_payer_scorecard(olap_conn, now)
    summary["gold_financial_summary"] = _agg_financial_summary(olap_conn, now)
    summary["gold_claims_aging"] = _agg_claims_aging(olap_conn, now)
    summary["gold_case_pipeline"] = _agg_case_pipeline(olap_conn, now)
    summary["gold_deadline_compliance"] = _agg_deadline_compliance(olap_conn, now)
    summary["gold_underpayment_detection"] = _agg_underpayment_detection(olap_conn, now)

    olap_conn.commit()
    logger.info("Gold aggregation complete: %s", summary)
    return summary


def _agg_recovery_by_payer(conn, now):
    conn.execute("DELETE FROM gold_recovery_by_payer")
    conn.execute("""
        INSERT INTO gold_recovery_by_payer
            (payer_id, payer_name, total_claims, total_billed, total_paid,
             total_underpayment, recovery_rate_pct, avg_payment_pct,
             denial_count, denial_rate_pct, _gold_ts)
        SELECT
            payer_id,
            MAX(payer_name),
            COUNT(*) AS total_claims,
            SUM(total_billed) AS total_billed,
            SUM(total_paid) AS total_paid,
            SUM(underpayment) AS total_underpayment,
            CASE WHEN SUM(total_billed) > 0
                THEN ROUND(SUM(total_paid) / SUM(total_billed) * 100, 2)
                ELSE 0 END AS recovery_rate_pct,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(AVG(
                    CASE WHEN total_billed > 0
                        THEN total_paid / total_billed * 100
                        ELSE 0 END
                ), 2)
                ELSE 0 END AS avg_payment_pct,
            SUM(has_denial) AS denial_count,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(SUM(has_denial) * 100.0 / COUNT(*), 2)
                ELSE 0 END AS denial_rate_pct,
            ?
        FROM silver_claim_remittance
        WHERE payer_id IS NOT NULL
        GROUP BY payer_id
    """, (now,))
    return conn.execute("SELECT COUNT(*) FROM gold_recovery_by_payer").fetchone()[0]


def _agg_cpt_analysis(conn, now):
    conn.execute("DELETE FROM gold_cpt_analysis")

    # Parse CPT codes from silver_claims (stored as JSON arrays)
    rows = conn.execute("""
        SELECT cr.claim_id, cr.total_billed, cr.total_paid, c.cpt_codes
        FROM silver_claim_remittance cr
        JOIN silver_claims c ON cr.claim_id = c.claim_id
    """).fetchall()

    cpt_stats = {}
    for row in rows:
        cpt_codes = json.loads(row[3]) if row[3] else []
        n_codes = len(cpt_codes) if cpt_codes else 1
        per_code_billed = (row[1] or 0) / n_codes
        per_code_paid = (row[2] or 0) / n_codes

        for cpt in cpt_codes:
            if cpt not in cpt_stats:
                cpt_stats[cpt] = {"count": 0, "billed": 0, "paid": 0}
            cpt_stats[cpt]["count"] += 1
            cpt_stats[cpt]["billed"] += per_code_billed
            cpt_stats[cpt]["paid"] += per_code_paid

    for cpt, stats in cpt_stats.items():
        # Look up Medicare and FAIR Health rates
        medicare_rate = _get_rate(conn, "MEDICARE", cpt)
        fair_health_rate = _get_rate(conn, "FAIR_HEALTH", cpt)

        avg_billed = stats["billed"] / stats["count"] if stats["count"] else 0
        avg_paid = stats["paid"] / stats["count"] if stats["count"] else 0
        payment_ratio = (stats["paid"] / stats["billed"] * 100) if stats["billed"] > 0 else 0

        conn.execute("""
            INSERT OR REPLACE INTO gold_cpt_analysis
                (cpt_code, claim_count, total_billed, total_paid, avg_billed,
                 avg_paid, payment_ratio_pct, medicare_rate, fair_health_rate, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (cpt, stats["count"], round(stats["billed"], 2),
              round(stats["paid"], 2), round(avg_billed, 2),
              round(avg_paid, 2), round(payment_ratio, 2),
              medicare_rate, fair_health_rate, now))

    return len(cpt_stats)


def _get_rate(conn, payer_id, cpt_code):
    """Look up current rate from silver_fee_schedule."""
    row = conn.execute("""
        SELECT rate FROM silver_fee_schedule
        WHERE payer_id = ? AND cpt_code = ? AND is_current = 1
        LIMIT 1
    """, (payer_id, cpt_code)).fetchone()
    return row[0] if row else None


def _agg_payer_scorecard(conn, now):
    conn.execute("DELETE FROM gold_payer_scorecard")

    # Get payer info from bronze
    payers_raw = conn.execute("SELECT payload FROM bronze_payers").fetchall()
    payer_info = {}
    for r in payers_raw:
        p = json.loads(r[0])
        payer_info[p["payer_id"]] = p

    rows = conn.execute("""
        SELECT
            payer_id,
            payer_name,
            COUNT(*) AS total_claims,
            CASE WHEN SUM(total_billed) > 0
                THEN ROUND(SUM(total_paid) / SUM(total_billed) * 100, 2)
                ELSE 0 END AS payment_rate_pct,
            CASE WHEN COUNT(*) > 0
                THEN ROUND(SUM(has_denial) * 100.0 / COUNT(*), 2)
                ELSE 0 END AS denial_rate_pct,
            ROUND(AVG(underpayment), 2) AS avg_underpayment,
            SUM(underpayment) AS total_underpayment
        FROM silver_claim_remittance
        WHERE payer_id IS NOT NULL
        GROUP BY payer_id
    """).fetchall()

    count = 0
    for row in rows:
        payer_id = row[0]
        info = payer_info.get(payer_id, {})
        payment_rate = row[3]
        denial_rate = row[4]

        # Risk tier: based on payment rate and denial rate
        if payment_rate >= 80 and denial_rate <= 10:
            risk_tier = "low"
        elif payment_rate >= 50 or denial_rate <= 30:
            risk_tier = "medium"
        else:
            risk_tier = "high"

        conn.execute("""
            INSERT OR REPLACE INTO gold_payer_scorecard
                (payer_id, payer_name, payer_type, total_claims, avg_days_to_pay,
                 payment_rate_pct, denial_rate_pct, avg_underpayment,
                 total_underpayment, risk_tier, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (payer_id, row[1], info.get("type"),
              row[2], None,  # avg_days_to_pay not available without dates
              payment_rate, denial_rate, row[5], row[6], risk_tier, now))
        count += 1
    return count


def _agg_financial_summary(conn, now):
    conn.execute("DELETE FROM gold_financial_summary")

    row = conn.execute("""
        SELECT
            COUNT(*) AS total_claims,
            SUM(total_billed) AS total_billed,
            SUM(total_paid) AS total_paid,
            SUM(underpayment) AS total_underpayment,
            SUM(CASE WHEN total_paid > 0 THEN 1 ELSE 0 END) AS paid_claims,
            SUM(has_denial) AS denial_count
        FROM silver_claim_remittance
    """).fetchone()

    metrics = [
        ("total_claims", row[0], None),
        ("total_billed", row[1], None),
        ("total_paid", row[2], None),
        ("total_underpayment", row[3], None),
        ("recovery_rate_pct",
         round(row[2] / row[1] * 100, 2) if row[1] and row[1] > 0 else 0,
         None),
        ("paid_claims", row[4], None),
        ("denial_count", row[5], None),
        ("denial_rate_pct",
         round(row[5] / row[0] * 100, 2) if row[0] and row[0] > 0 else 0,
         None),
        ("avg_billed_per_claim",
         round(row[1] / row[0], 2) if row[0] and row[0] > 0 else 0,
         None),
        ("avg_underpayment_per_claim",
         round(row[3] / row[0], 2) if row[0] and row[0] > 0 else 0,
         None),
    ]

    for name, value, detail in metrics:
        conn.execute("""
            INSERT OR REPLACE INTO gold_financial_summary
                (metric_name, metric_value, metric_detail, _gold_ts)
            VALUES (?, ?, ?, ?)
        """, (name, value, detail, now))

    return len(metrics)


def _agg_claims_aging(conn, now):
    """Bucket claims by age (days since date_of_service)."""
    conn.execute("DELETE FROM gold_claims_aging")

    # Use julianday for date math
    rows = conn.execute("""
        SELECT
            CASE
                WHEN julianday('now') - julianday(date_of_service) <= 30 THEN '0-30 days'
                WHEN julianday('now') - julianday(date_of_service) <= 60 THEN '31-60 days'
                WHEN julianday('now') - julianday(date_of_service) <= 90 THEN '61-90 days'
                WHEN julianday('now') - julianday(date_of_service) <= 180 THEN '91-180 days'
                ELSE '180+ days'
            END AS aging_bucket,
            COUNT(*) AS claim_count,
            SUM(total_billed) AS total_billed,
            SUM(underpayment) AS total_unpaid
        FROM silver_claim_remittance
        WHERE date_of_service IS NOT NULL
        GROUP BY aging_bucket
    """).fetchall()

    total_claims = sum(r[1] for r in rows) if rows else 1

    count = 0
    for row in rows:
        conn.execute("""
            INSERT OR REPLACE INTO gold_claims_aging
                (aging_bucket, claim_count, total_billed, total_unpaid, pct_of_total, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row[0], row[1], row[2], row[3],
              round(row[1] / total_claims * 100, 2), now))
        count += 1
    return count


def _agg_case_pipeline(conn, now):
    """Case status breakdown with SLA compliance (Section 12.2 — Case Pipeline)."""
    conn.execute("DELETE FROM gold_case_pipeline")

    # Check if silver_cases has data
    has_cases = conn.execute(
        "SELECT COUNT(*) FROM silver_cases"
    ).fetchone()[0]
    if not has_cases:
        return 0

    rows = conn.execute("""
        SELECT
            c.status,
            COUNT(*) AS case_count,
            COALESCE(SUM(c.total_billed), 0) AS total_billed,
            COALESCE(SUM(c.total_underpayment), 0) AS total_underpayment,
            ROUND(AVG(c.age_days), 1) AS avg_age_days
        FROM silver_cases c
        GROUP BY c.status
    """).fetchall()

    count = 0
    for row in rows:
        status = row[0]

        # SLA compliance: % of deadlines met for cases in this status
        sla_row = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN d.status = 'met' THEN 1 ELSE 0 END) AS met
            FROM silver_deadlines d
            JOIN silver_cases c ON d.case_id = c.case_id
            WHERE c.status = ?
        """, (status,)).fetchone()

        sla_pct = 0.0
        if sla_row and sla_row[0] > 0:
            sla_pct = round(sla_row[1] / sla_row[0] * 100, 2)

        conn.execute("""
            INSERT OR REPLACE INTO gold_case_pipeline
                (status, case_count, total_billed, total_underpayment,
                 avg_age_days, sla_compliance_pct, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (status, row[1], row[2], row[3], row[4], sla_pct, now))
        count += 1
    return count


def _agg_deadline_compliance(conn, now):
    """Deadline met/missed/at-risk by type (Section 12.2 — Deadline Compliance)."""
    conn.execute("DELETE FROM gold_deadline_compliance")

    has_deadlines = conn.execute(
        "SELECT COUNT(*) FROM silver_deadlines"
    ).fetchone()[0]
    if not has_deadlines:
        return 0

    rows = conn.execute("""
        SELECT
            type,
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'met' THEN 1 ELSE 0 END) AS met,
            SUM(CASE WHEN status = 'missed' THEN 1 ELSE 0 END) AS missed,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN is_at_risk = 1 THEN 1 ELSE 0 END) AS at_risk
        FROM silver_deadlines
        GROUP BY type
    """).fetchall()

    count = 0
    for row in rows:
        total = row[1]
        met = row[2]
        compliance = round(met / total * 100, 2) if total > 0 else 0.0

        conn.execute("""
            INSERT OR REPLACE INTO gold_deadline_compliance
                (deadline_type, total_deadlines, met_count, missed_count,
                 pending_count, at_risk_count, compliance_pct, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (row[0], total, met, row[3], row[4], row[5], compliance, now))
        count += 1
    return count


def _agg_underpayment_detection(conn, now):
    """Per-claim underpayment analysis with QPA and arbitration eligibility."""
    conn.execute("DELETE FROM gold_underpayment_detection")

    has_disputes = conn.execute(
        "SELECT COUNT(*) FROM silver_disputes"
    ).fetchone()[0]
    if not has_disputes:
        return 0

    rows = conn.execute("""
        SELECT
            d.claim_id, d.payer_id, d.payer_name, d.provider_npi,
            d.date_of_service, d.billed_amount, d.paid_amount,
            d.qpa_amount, d.underpayment_amount, d.status
        FROM silver_disputes d
        WHERE d.underpayment_amount > 0
    """).fetchall()

    count = 0
    for row in rows:
        billed = row[5] or 0
        paid = row[6] or 0
        qpa = row[7] or 0
        underpayment = row[8] or 0
        underpay_pct = round(underpayment / billed * 100, 2) if billed > 0 else 0

        # Arbitration eligible: underpayment > $25 and billed exceeds QPA
        # (NSA threshold for IDR eligibility)
        arbitration_eligible = 1 if (underpayment > 25 and billed > qpa) else 0

        conn.execute("""
            INSERT OR REPLACE INTO gold_underpayment_detection
                (claim_id, payer_id, payer_name, provider_npi,
                 date_of_service, billed_amount, paid_amount, qpa_amount,
                 underpayment_amount, underpayment_pct, arbitration_eligible,
                 dispute_status, _gold_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row[0], row[1], row[2], row[3], row[4],
              billed, paid, qpa, underpayment, underpay_pct,
              arbitration_eligible, row[9], now))
        count += 1
    return count
