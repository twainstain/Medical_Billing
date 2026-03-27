"""Sample Analytics Report — consumes Gold layer.

Simulates a Power BI dashboard reading from Gold via Direct Lake.
Produces formatted text output covering the key reports from Section 12.2:
  1. Financial Summary
  2. Recovery by Payer
  3. Payer Scorecards
  4. CPT Code Analysis
  5. Claims Aging
  6. Case Pipeline
  7. Deadline Compliance
  8. Underpayment Detection
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


def generate_report(olap_conn: sqlite3.Connection) -> str:
    """Generate a formatted analytics report from Gold layer tables.

    Returns: multi-line string report.
    """
    lines = []
    _header(lines, "MEDICAL BILLING ARBITRATION — ANALYTICS REPORT")
    lines.append("Data Source: Gold Layer (OLAP Lakehouse)")
    lines.append("")

    _report_financial_summary(olap_conn, lines)
    _report_recovery_by_payer(olap_conn, lines)
    _report_payer_scorecards(olap_conn, lines)
    _report_cpt_analysis(olap_conn, lines)
    _report_claims_aging(olap_conn, lines)
    _report_case_pipeline(olap_conn, lines)
    _report_deadline_compliance(olap_conn, lines)
    _report_underpayment_detection(olap_conn, lines)

    _header(lines, "END OF REPORT")
    report_text = "\n".join(lines)
    logger.info("Report generated: %d lines", len(lines))
    return report_text


def _header(lines, title):
    lines.append("=" * 72)
    lines.append(f"  {title}")
    lines.append("=" * 72)


def _subheader(lines, title):
    lines.append("")
    lines.append(f"--- {title} ---")
    lines.append("")


def _report_financial_summary(conn, lines):
    _subheader(lines, "1. FINANCIAL SUMMARY")

    rows = conn.execute(
        "SELECT metric_name, metric_value FROM gold_financial_summary"
    ).fetchall()

    metrics = {r[0]: r[1] for r in rows}

    lines.append(f"  Total Claims:           {_int(metrics.get('total_claims'))}")
    lines.append(f"  Total Billed:           ${_money(metrics.get('total_billed'))}")
    lines.append(f"  Total Paid:             ${_money(metrics.get('total_paid'))}")
    lines.append(f"  Total Underpayment:     ${_money(metrics.get('total_underpayment'))}")
    lines.append(f"  Recovery Rate:          {_pct(metrics.get('recovery_rate_pct'))}")
    lines.append(f"  Denial Rate:            {_pct(metrics.get('denial_rate_pct'))}")
    lines.append(f"  Avg Billed/Claim:       ${_money(metrics.get('avg_billed_per_claim'))}")
    lines.append(f"  Avg Underpayment/Claim: ${_money(metrics.get('avg_underpayment_per_claim'))}")


def _report_recovery_by_payer(conn, lines):
    _subheader(lines, "2. RECOVERY BY PAYER")

    rows = conn.execute("""
        SELECT payer_id, payer_name, total_claims, total_billed,
               total_paid, total_underpayment, recovery_rate_pct, denial_rate_pct
        FROM gold_recovery_by_payer
        ORDER BY total_underpayment DESC
    """).fetchall()

    if not rows:
        lines.append("  No data available.")
        return

    lines.append(f"  {'Payer':<20} {'Claims':>6} {'Billed':>12} {'Paid':>12} "
                 f"{'Underpaid':>12} {'Recovery':>9} {'Denials':>8}")
    lines.append(f"  {'-'*20} {'-'*6} {'-'*12} {'-'*12} {'-'*12} {'-'*9} {'-'*8}")

    for r in rows:
        lines.append(
            f"  {(r[1] or r[0]):<20} {r[2]:>6} "
            f"${_money(r[3]):>11} ${_money(r[4]):>11} "
            f"${_money(r[5]):>11} {_pct(r[6]):>9} {_pct(r[7]):>8}"
        )


def _report_payer_scorecards(conn, lines):
    _subheader(lines, "3. PAYER SCORECARDS")

    rows = conn.execute("""
        SELECT payer_id, payer_name, payer_type, total_claims,
               payment_rate_pct, denial_rate_pct, avg_underpayment,
               total_underpayment, risk_tier
        FROM gold_payer_scorecard
        ORDER BY risk_tier DESC, total_underpayment DESC
    """).fetchall()

    if not rows:
        lines.append("  No data available.")
        return

    for r in rows:
        lines.append(f"  Payer: {r[1] or r[0]} ({r[2] or 'N/A'})")
        lines.append(f"    Claims: {r[3]}  |  Payment Rate: {_pct(r[4])}  |  "
                     f"Denial Rate: {_pct(r[5])}")
        lines.append(f"    Avg Underpayment: ${_money(r[6])}  |  "
                     f"Total Underpayment: ${_money(r[7])}")
        lines.append(f"    Risk Tier: {(r[8] or 'N/A').upper()}")
        lines.append("")


def _report_cpt_analysis(conn, lines):
    _subheader(lines, "4. CPT CODE ANALYSIS")

    rows = conn.execute("""
        SELECT cpt_code, claim_count, avg_billed, avg_paid,
               payment_ratio_pct, medicare_rate, fair_health_rate
        FROM gold_cpt_analysis
        ORDER BY claim_count DESC
    """).fetchall()

    if not rows:
        lines.append("  No data available.")
        return

    lines.append(f"  {'CPT':<8} {'Claims':>6} {'Avg Billed':>11} {'Avg Paid':>11} "
                 f"{'Pay Ratio':>10} {'Medicare':>10} {'FairHlth':>10}")
    lines.append(f"  {'-'*8} {'-'*6} {'-'*11} {'-'*11} {'-'*10} {'-'*10} {'-'*10}")

    for r in rows:
        medicare = f"${_money(r[5])}" if r[5] else "N/A"
        fair_health = f"${_money(r[6])}" if r[6] else "N/A"
        lines.append(
            f"  {r[0]:<8} {r[1]:>6} ${_money(r[2]):>10} ${_money(r[3]):>10} "
            f"{_pct(r[4]):>10} {medicare:>10} {fair_health:>10}"
        )


def _report_claims_aging(conn, lines):
    _subheader(lines, "5. CLAIMS AGING ANALYSIS")

    rows = conn.execute("""
        SELECT aging_bucket, claim_count, total_billed, total_unpaid, pct_of_total
        FROM gold_claims_aging
        ORDER BY
            CASE aging_bucket
                WHEN '0-30 days' THEN 1
                WHEN '31-60 days' THEN 2
                WHEN '61-90 days' THEN 3
                WHEN '91-180 days' THEN 4
                ELSE 5
            END
    """).fetchall()

    if not rows:
        lines.append("  No data available.")
        return

    lines.append(f"  {'Bucket':<15} {'Claims':>7} {'Total Billed':>13} "
                 f"{'Unpaid':>13} {'% of Total':>11}")
    lines.append(f"  {'-'*15} {'-'*7} {'-'*13} {'-'*13} {'-'*11}")

    for r in rows:
        lines.append(
            f"  {r[0]:<15} {r[1]:>7} ${_money(r[2]):>12} "
            f"${_money(r[3]):>12} {_pct(r[4]):>11}"
        )


def _report_case_pipeline(conn, lines):
    _subheader(lines, "6. CASE PIPELINE")

    try:
        rows = conn.execute("""
            SELECT status, case_count, total_underpayment, avg_age_days, sla_compliance_pct
            FROM gold_case_pipeline ORDER BY case_count DESC
        """).fetchall()
    except Exception:
        lines.append("  No case pipeline data available.")
        return

    if not rows:
        lines.append("  No case pipeline data available.")
        return

    lines.append(f"  {'Status':<15} {'Cases':>7} {'Underpayment':>13} "
                 f"{'Avg Age':>10} {'SLA':>9}")
    lines.append(f"  {'-'*15} {'-'*7} {'-'*13} {'-'*10} {'-'*9}")

    for r in rows:
        lines.append(
            f"  {(r[0] or 'N/A'):<15} {r[1]:>7} ${_money(r[2]):>12} "
            f"{(r[3] or 0):>7.0f} days {_pct(r[4]):>9}"
        )


def _report_deadline_compliance(conn, lines):
    _subheader(lines, "7. DEADLINE COMPLIANCE")

    try:
        rows = conn.execute("""
            SELECT deadline_type, total_deadlines, met_count, missed_count,
                   at_risk_count, compliance_pct
            FROM gold_deadline_compliance ORDER BY deadline_type
        """).fetchall()
    except Exception:
        lines.append("  No deadline data available.")
        return

    if not rows:
        lines.append("  No deadline data available.")
        return

    lines.append(f"  {'Type':<25} {'Total':>6} {'Met':>6} {'Missed':>7} "
                 f"{'At Risk':>8} {'Compliance':>11}")
    lines.append(f"  {'-'*25} {'-'*6} {'-'*6} {'-'*7} {'-'*8} {'-'*11}")

    for r in rows:
        dl_type = (r[0] or "").replace("_", " ").title()
        lines.append(
            f"  {dl_type:<25} {r[1]:>6} {r[2]:>6} {r[3]:>7} "
            f"{r[4]:>8} {_pct(r[5]):>11}"
        )


def _report_underpayment_detection(conn, lines):
    _subheader(lines, "8. UNDERPAYMENT DETECTION — TOP ARBITRATION CANDIDATES")

    try:
        rows = conn.execute("""
            SELECT claim_id, payer_name, billed_amount, paid_amount,
                   qpa_amount, underpayment_amount, underpayment_pct,
                   arbitration_eligible, dispute_status
            FROM gold_underpayment_detection
            WHERE arbitration_eligible = 1
            ORDER BY underpayment_amount DESC
            LIMIT 10
        """).fetchall()
    except Exception:
        lines.append("  No underpayment data available.")
        return

    if not rows:
        lines.append("  No arbitration-eligible claims found.")
        return

    lines.append(f"  {'Claim ID':<15} {'Payer':<18} {'Billed':>10} {'Paid':>10} "
                 f"{'QPA':>10} {'Underpay':>10} {'Status':<10}")
    lines.append(f"  {'-'*15} {'-'*18} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    for r in rows:
        lines.append(
            f"  {(r[0] or 'N/A'):<15} {(r[1] or 'N/A'):<18} "
            f"${_money(r[2]):>9} ${_money(r[3]):>9} "
            f"${_money(r[4]):>9} ${_money(r[5]):>9} {(r[8] or 'N/A'):<10}"
        )


def _money(val):
    if val is None:
        return "0.00"
    return f"{val:,.2f}"


def _pct(val):
    if val is None:
        return "0.00%"
    return f"{val:.2f}%"


def _int(val):
    if val is None:
        return "0"
    return f"{int(val):,}"
