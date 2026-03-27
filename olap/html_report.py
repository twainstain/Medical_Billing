"""HTML Analytics Report — Arbitration Recovery Dashboard.

Generates a self-contained HTML dashboard from the Gold layer.
When served via serve_dashboard.py, the dashboard fetches live data
from /api/data every 30 seconds (simulating Power BI Direct Lake).
When opened as a static file, it uses embedded seed data from the
last generation run.

Corresponds to Section 12.2 dashboards:
  - Recovery Overview (KPI cards + recovery by payer)
  - Case Pipeline (status breakdown with SLA)
  - Payer Scorecards (risk tiers)
  - CPT Code Analysis (billed vs paid vs Medicare/FAIR Health)
  - Deadline Compliance (met/missed/at-risk)
  - Claims Aging (aging buckets)
  - Underpayment Detection (arbitration-eligible claims)
"""

import json
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def generate_html_report(olap_conn: sqlite3.Connection) -> str:
    """Generate a live HTML dashboard that queries Gold layer via API.

    Embeds a JSON snapshot as fallback for static file opening.
    """
    seed_data = _query_all_gold(olap_conn)
    seed_json = json.dumps(seed_data)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    html = _TEMPLATE.replace("__SEED_DATA__", seed_json).replace("__GENERATED__", now)

    logger.info("HTML report generated: %d bytes", len(html))
    return html


def _query_all_gold(conn: sqlite3.Connection) -> dict:
    """Query all Gold layer tables into a JSON-serializable dict."""
    data = {}

    rows = conn.execute(
        "SELECT metric_name, metric_value FROM gold_financial_summary"
    ).fetchall()
    data["financial"] = {r[0]: r[1] for r in rows}

    rows = conn.execute("""
        SELECT payer_id, payer_name, total_claims, total_billed,
               total_paid, total_underpayment, recovery_rate_pct, denial_rate_pct
        FROM gold_recovery_by_payer ORDER BY total_underpayment DESC
    """).fetchall()
    data["recovery"] = [_row_to_dict(r) for r in rows]

    rows = conn.execute("""
        SELECT payer_id, payer_name, payer_type, total_claims,
               payment_rate_pct, denial_rate_pct, avg_underpayment,
               total_underpayment, risk_tier
        FROM gold_payer_scorecard ORDER BY risk_tier DESC, total_underpayment DESC
    """).fetchall()
    data["scorecards"] = [_row_to_dict(r) for r in rows]

    rows = conn.execute("""
        SELECT cpt_code, claim_count, avg_billed, avg_paid,
               payment_ratio_pct, medicare_rate, fair_health_rate
        FROM gold_cpt_analysis ORDER BY claim_count DESC
    """).fetchall()
    data["cpt"] = [_row_to_dict(r) for r in rows]

    rows = conn.execute("""
        SELECT aging_bucket, claim_count, total_billed, total_unpaid, pct_of_total
        FROM gold_claims_aging
        ORDER BY CASE aging_bucket
            WHEN '0-30 days' THEN 1 WHEN '31-60 days' THEN 2
            WHEN '61-90 days' THEN 3 WHEN '91-180 days' THEN 4 ELSE 5 END
    """).fetchall()
    data["aging"] = [_row_to_dict(r) for r in rows]

    try:
        rows = conn.execute("""
            SELECT status, case_count, total_underpayment,
                   avg_age_days, sla_compliance_pct
            FROM gold_case_pipeline ORDER BY case_count DESC
        """).fetchall()
        data["pipeline"] = [_row_to_dict(r) for r in rows]
    except Exception:
        data["pipeline"] = []

    try:
        rows = conn.execute("""
            SELECT deadline_type, total_deadlines, met_count, missed_count,
                   at_risk_count, compliance_pct
            FROM gold_deadline_compliance ORDER BY deadline_type
        """).fetchall()
        data["deadlines"] = [_row_to_dict(r) for r in rows]
    except Exception:
        data["deadlines"] = []

    try:
        rows = conn.execute("""
            SELECT claim_id, payer_name, billed_amount, paid_amount,
                   qpa_amount, underpayment_amount, underpayment_pct,
                   arbitration_eligible, dispute_status
            FROM gold_underpayment_detection
            ORDER BY underpayment_amount DESC LIMIT 20
        """).fetchall()
        data["underpayments"] = [_row_to_dict(r) for r in rows]
    except Exception:
        data["underpayments"] = []

    return data


def _row_to_dict(row):
    """Convert a sqlite3.Row to a plain dict."""
    if hasattr(row, "keys"):
        return {k: row[k] for k in row.keys()}
    # Fallback for tuple rows with description
    return dict(row) if isinstance(row, dict) else list(row)


# ---------------------------------------------------------------------------
# Full HTML template — JavaScript renders all tables from JSON.
# ---------------------------------------------------------------------------
_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Arbitration Recovery Dashboard</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #f0f2f5; color: #323130; }
.header { background: linear-gradient(135deg, #0078d4, #005a9e); color: white; padding: 24px 32px; display: flex; align-items: center; justify-content: space-between; }
.header h1 { font-size: 24px; font-weight: 600; }
.header .subtitle { font-size: 13px; opacity: 0.85; margin-top: 4px; }
.header .live-badge { display: inline-flex; align-items: center; gap: 6px; background: rgba(255,255,255,0.15); padding: 6px 14px; border-radius: 20px; font-size: 12px; }
.header .live-dot { width: 8px; height: 8px; border-radius: 50%; background: #6fcf97; animation: pulse 2s infinite; }
.header .static-dot { width: 8px; height: 8px; border-radius: 50%; background: #ffaa44; }
@keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
.container { max-width: 1400px; margin: 0 auto; padding: 24px; }
.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.kpi-card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); transition: transform 0.15s; }
.kpi-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.12); }
.kpi-card .label { font-size: 12px; color: #605e5c; text-transform: uppercase; letter-spacing: 0.5px; }
.kpi-card .value { font-size: 28px; font-weight: 700; color: #0078d4; margin: 8px 0 4px; }
.kpi-card .detail { font-size: 12px; color: #8a8886; }
.kpi-card.warn .value { color: #d83b01; }
.kpi-card.ok .value { color: #107c10; }
.section { background: white; border-radius: 8px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.section h2 { font-size: 16px; font-weight: 600; color: #323130; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #0078d4; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { background: #faf9f8; color: #605e5c; font-weight: 600; text-align: left; padding: 10px 12px; border-bottom: 2px solid #edebe9; }
td { padding: 10px 12px; border-bottom: 1px solid #edebe9; }
tr:hover td { background: #f3f2f1; }
.right { text-align: right; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge-high { background: #fde7e9; color: #a4262c; }
.badge-medium { background: #fff4ce; color: #835c00; }
.badge-low { background: #dff6dd; color: #107c10; }
.badge-met { background: #dff6dd; color: #107c10; }
.badge-missed { background: #fde7e9; color: #a4262c; }
.badge-pending { background: #f3f2f1; color: #605e5c; }
.badge-at_risk, .badge-at-risk { background: #fff4ce; color: #835c00; }
.badge-open { background: #deecf9; color: #004578; }
.badge-in_review, .badge-in-review { background: #fff4ce; color: #835c00; }
.badge-resolved { background: #dff6dd; color: #107c10; }
.bar-container { background: #edebe9; border-radius: 4px; height: 8px; width: 100%; min-width: 80px; display: inline-block; vertical-align: middle; margin-right: 6px; }
.bar-fill { height: 8px; border-radius: 4px; transition: width 0.5s ease; }
.bar-fill.good { background: #107c10; }
.bar-fill.warn { background: #d83b01; }
.bar-fill.mid { background: #ffaa44; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
@media (max-width: 900px) { .grid-2 { grid-template-columns: 1fr; } }
.footer { text-align: center; padding: 16px; font-size: 11px; color: #8a8886; }
.eligible { color: #107c10; font-weight: 600; }
.not-eligible { color: #8a8886; }
.refresh-info { font-size: 11px; color: rgba(255,255,255,0.7); margin-top: 2px; }
.loading { text-align: center; padding: 40px; color: #8a8886; }
</style>
</head>
<body>

<div class="header">
    <div>
        <h1>Arbitration Recovery Dashboard</h1>
        <div class="subtitle">Medical Billing Arbitration Analytics &mdash; Gold Layer (OLAP Lakehouse)</div>
        <div class="refresh-info" id="refresh-info">Loading...</div>
    </div>
    <div class="live-badge" id="live-badge">
        <span class="static-dot" id="status-dot"></span>
        <span id="status-text">Connecting...</span>
    </div>
</div>

<div class="container">

<!-- KPI Cards -->
<div class="kpi-row" id="kpi-row">
    <div class="loading">Loading dashboard data...</div>
</div>

<!-- Recovery by Payer -->
<div class="section">
    <h2>Recovery by Payer</h2>
    <table>
        <thead>
            <tr>
                <th>Payer</th>
                <th class="right">Claims</th>
                <th class="right">Billed</th>
                <th class="right">Paid</th>
                <th class="right">Underpayment</th>
                <th class="right">Recovery Rate</th>
                <th class="right">Denial Rate</th>
            </tr>
        </thead>
        <tbody id="tbl-recovery"></tbody>
    </table>
</div>

<div class="grid-2">

<!-- Case Pipeline -->
<div class="section">
    <h2>Case Pipeline</h2>
    <table>
        <thead>
            <tr>
                <th>Status</th>
                <th class="right">Cases</th>
                <th class="right">Underpayment</th>
                <th class="right">Avg Age</th>
                <th>SLA Compliance</th>
            </tr>
        </thead>
        <tbody id="tbl-pipeline"></tbody>
    </table>
</div>

<!-- Deadline Compliance -->
<div class="section">
    <h2>Deadline Compliance</h2>
    <table>
        <thead>
            <tr>
                <th>Deadline Type</th>
                <th class="right">Total</th>
                <th class="right">Met</th>
                <th class="right">Missed</th>
                <th class="right">At Risk</th>
                <th>Compliance</th>
            </tr>
        </thead>
        <tbody id="tbl-deadlines"></tbody>
    </table>
</div>

</div>

<!-- Payer Scorecards -->
<div class="section">
    <h2>Payer Scorecards</h2>
    <table>
        <thead>
            <tr>
                <th>Payer</th>
                <th>Type</th>
                <th class="right">Claims</th>
                <th class="right">Payment Rate</th>
                <th class="right">Denial Rate</th>
                <th class="right">Avg Underpayment</th>
                <th class="right">Total Underpayment</th>
                <th>Risk Tier</th>
            </tr>
        </thead>
        <tbody id="tbl-scorecards"></tbody>
    </table>
</div>

<!-- CPT Code Analysis -->
<div class="section">
    <h2>CPT Code Analysis</h2>
    <table>
        <thead>
            <tr>
                <th>CPT Code</th>
                <th class="right">Claims</th>
                <th class="right">Avg Billed</th>
                <th class="right">Avg Paid</th>
                <th class="right">Pay Ratio</th>
                <th class="right">Medicare Rate</th>
                <th class="right">FAIR Health</th>
            </tr>
        </thead>
        <tbody id="tbl-cpt"></tbody>
    </table>
</div>

<!-- Claims Aging -->
<div class="section">
    <h2>Claims Aging Analysis</h2>
    <table>
        <thead>
            <tr>
                <th>Aging Bucket</th>
                <th class="right">Claims</th>
                <th class="right">Total Billed</th>
                <th class="right">Unpaid</th>
                <th>% of Total</th>
            </tr>
        </thead>
        <tbody id="tbl-aging"></tbody>
    </table>
</div>

<!-- Underpayment Detection -->
<div class="section">
    <h2>Underpayment Detection &mdash; Arbitration Candidates</h2>
    <table>
        <thead>
            <tr>
                <th>Claim ID</th>
                <th>Payer</th>
                <th class="right">Billed</th>
                <th class="right">Paid</th>
                <th class="right">QPA</th>
                <th class="right">Underpayment</th>
                <th class="right">Underpay %</th>
                <th>IDR Eligible</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody id="tbl-underpayments"></tbody>
    </table>
</div>

</div>

<div class="footer">
    Medical Billing Arbitration System &mdash; OLAP Lakehouse (Medallion Architecture: Bronze &rarr; Silver &rarr; Gold)
    &mdash; Simulates Power BI Direct Lake Mode
</div>

<!-- Embedded seed data for static file fallback -->
<script id="seed-data" type="application/json">__SEED_DATA__</script>

<script>
(function() {
    "use strict";

    const REFRESH_INTERVAL = 30000; // 30 seconds
    const API_URL = "/api/data";
    let isLive = false;
    let lastRefresh = null;

    // --- Formatters ---
    function money(v) { return v == null ? "0.00" : v.toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2}); }
    function pct(v) { return v == null ? "0.00%" : v.toFixed(2) + "%"; }
    function intFmt(v) { return v == null ? "0" : Math.round(v).toLocaleString("en-US"); }
    function barCls(v) { return v >= 70 ? "good" : v >= 50 ? "mid" : "warn"; }
    function badgeCls(s) { return "badge badge-" + (s || "unknown").replace(/\s+/g, "-"); }

    // --- KPI Cards ---
    function renderKPIs(f) {
        var recoveryOk = (f.recovery_rate_pct || 0) >= 70;
        var denialWarn = (f.denial_rate_pct || 0) > 15;
        document.getElementById("kpi-row").innerHTML =
            kpiCard("", "Total Claims", intFmt(f.total_claims), "All claims in system") +
            kpiCard("", "Total Billed", "$" + money(f.total_billed), "Gross charges") +
            kpiCard("ok", "Total Paid", "$" + money(f.total_paid), "Collected revenue") +
            kpiCard("warn", "Total Underpayment", "$" + money(f.total_underpayment), "Recovery opportunity") +
            kpiCard(recoveryOk ? "ok" : "warn", "Recovery Rate", pct(f.recovery_rate_pct), "Paid / Billed") +
            kpiCard(denialWarn ? "warn" : "", "Denial Rate", pct(f.denial_rate_pct), "Claims with denials");
    }
    function kpiCard(cls, label, value, detail) {
        return '<div class="kpi-card ' + cls + '">' +
            '<div class="label">' + label + '</div>' +
            '<div class="value">' + value + '</div>' +
            '<div class="detail">' + detail + '</div></div>';
    }

    // --- Recovery by Payer ---
    function renderRecovery(rows) {
        if (!rows || !rows.length) { byId("tbl-recovery").innerHTML = '<tr><td colspan="7">No data available</td></tr>'; return; }
        byId("tbl-recovery").innerHTML = rows.map(function(r) {
            var rate = r.recovery_rate_pct || r[6] || 0;
            return '<tr><td><strong>' + (r.payer_name || r.payer_id || r[1] || r[0]) + '</strong></td>' +
                '<td class="right">' + (r.total_claims || r[2]) + '</td>' +
                '<td class="right">$' + money(r.total_billed || r[3]) + '</td>' +
                '<td class="right">$' + money(r.total_paid || r[4]) + '</td>' +
                '<td class="right">$' + money(r.total_underpayment || r[5]) + '</td>' +
                '<td class="right"><div class="bar-container"><div class="bar-fill ' + barCls(rate) + '" style="width:' + Math.min(rate,100).toFixed(0) + '%"></div></div>' + pct(rate) + '</td>' +
                '<td class="right">' + pct(r.denial_rate_pct || r[7]) + '</td></tr>';
        }).join("");
    }

    // --- Case Pipeline ---
    function renderPipeline(rows) {
        if (!rows || !rows.length) { byId("tbl-pipeline").innerHTML = '<tr><td colspan="5">No case data available</td></tr>'; return; }
        byId("tbl-pipeline").innerHTML = rows.map(function(r) {
            var status = r.status || r[0] || "unknown";
            var sla = r.sla_compliance_pct || r[4] || 0;
            return '<tr><td><span class="' + badgeCls(status) + '">' + status + '</span></td>' +
                '<td class="right">' + (r.case_count || r[1]) + '</td>' +
                '<td class="right">$' + money(r.total_underpayment || r[2]) + '</td>' +
                '<td class="right">' + ((r.avg_age_days || r[3] || 0).toFixed(0)) + ' days</td>' +
                '<td><div class="bar-container"><div class="bar-fill ' + barCls(sla) + '" style="width:' + Math.min(sla,100).toFixed(0) + '%"></div></div>' + pct(sla) + '</td></tr>';
        }).join("");
    }

    // --- Deadline Compliance ---
    function renderDeadlines(rows) {
        if (!rows || !rows.length) { byId("tbl-deadlines").innerHTML = '<tr><td colspan="6">No deadline data available</td></tr>'; return; }
        byId("tbl-deadlines").innerHTML = rows.map(function(r) {
            var dlType = (r.deadline_type || r[0] || "").replace(/_/g, " ").replace(/\b\w/g, function(c){ return c.toUpperCase(); });
            var comp = r.compliance_pct || r[5] || 0;
            return '<tr><td>' + dlType + '</td>' +
                '<td class="right">' + (r.total_deadlines || r[1]) + '</td>' +
                '<td class="right"><span class="badge badge-met">' + (r.met_count || r[2]) + '</span></td>' +
                '<td class="right"><span class="badge badge-missed">' + (r.missed_count || r[3]) + '</span></td>' +
                '<td class="right"><span class="badge badge-at-risk">' + (r.at_risk_count || r[4]) + '</span></td>' +
                '<td><div class="bar-container"><div class="bar-fill ' + barCls(comp) + '" style="width:' + Math.min(comp,100).toFixed(0) + '%"></div></div>' + pct(comp) + '</td></tr>';
        }).join("");
    }

    // --- Payer Scorecards ---
    function renderScorecards(rows) {
        if (!rows || !rows.length) { byId("tbl-scorecards").innerHTML = '<tr><td colspan="8">No data available</td></tr>'; return; }
        byId("tbl-scorecards").innerHTML = rows.map(function(r) {
            var tier = r.risk_tier || r[8] || "unknown";
            return '<tr><td><strong>' + (r.payer_name || r.payer_id || r[1] || r[0]) + '</strong></td>' +
                '<td>' + (r.payer_type || r[2] || "N/A") + '</td>' +
                '<td class="right">' + (r.total_claims || r[3]) + '</td>' +
                '<td class="right">' + pct(r.payment_rate_pct || r[4]) + '</td>' +
                '<td class="right">' + pct(r.denial_rate_pct || r[5]) + '</td>' +
                '<td class="right">$' + money(r.avg_underpayment || r[6]) + '</td>' +
                '<td class="right">$' + money(r.total_underpayment || r[7]) + '</td>' +
                '<td><span class="' + badgeCls(tier) + '">' + tier.toUpperCase() + '</span></td></tr>';
        }).join("");
    }

    // --- CPT Code Analysis ---
    function renderCPT(rows) {
        if (!rows || !rows.length) { byId("tbl-cpt").innerHTML = '<tr><td colspan="7">No data available</td></tr>'; return; }
        byId("tbl-cpt").innerHTML = rows.map(function(r) {
            var medicare = (r.medicare_rate || r[5]) ? ("$" + money(r.medicare_rate || r[5])) : "N/A";
            var fair = (r.fair_health_rate || r[6]) ? ("$" + money(r.fair_health_rate || r[6])) : "N/A";
            return '<tr><td><strong>' + (r.cpt_code || r[0]) + '</strong></td>' +
                '<td class="right">' + (r.claim_count || r[1]) + '</td>' +
                '<td class="right">$' + money(r.avg_billed || r[2]) + '</td>' +
                '<td class="right">$' + money(r.avg_paid || r[3]) + '</td>' +
                '<td class="right">' + pct(r.payment_ratio_pct || r[4]) + '</td>' +
                '<td class="right">' + medicare + '</td>' +
                '<td class="right">' + fair + '</td></tr>';
        }).join("");
    }

    // --- Claims Aging ---
    function renderAging(rows) {
        if (!rows || !rows.length) { byId("tbl-aging").innerHTML = '<tr><td colspan="5">No data available</td></tr>'; return; }
        byId("tbl-aging").innerHTML = rows.map(function(r) {
            var p = r.pct_of_total || r[4] || 0;
            return '<tr><td>' + (r.aging_bucket || r[0]) + '</td>' +
                '<td class="right">' + (r.claim_count || r[1]) + '</td>' +
                '<td class="right">$' + money(r.total_billed || r[2]) + '</td>' +
                '<td class="right">$' + money(r.total_unpaid || r[3]) + '</td>' +
                '<td><div class="bar-container"><div class="bar-fill mid" style="width:' + Math.min(p,100).toFixed(0) + '%"></div></div>' + pct(p) + '</td></tr>';
        }).join("");
    }

    // --- Underpayment Detection ---
    function renderUnderpayments(rows) {
        if (!rows || !rows.length) { byId("tbl-underpayments").innerHTML = '<tr><td colspan="9">No underpayment data available</td></tr>'; return; }
        byId("tbl-underpayments").innerHTML = rows.map(function(r) {
            var eligible = r.arbitration_eligible || r[7];
            var eligStr = eligible ? '<span class="eligible">YES</span>' : '<span class="not-eligible">No</span>';
            var status = r.dispute_status || r[8] || "unknown";
            return '<tr><td><strong>' + (r.claim_id || r[0]) + '</strong></td>' +
                '<td>' + (r.payer_name || r[1] || "N/A") + '</td>' +
                '<td class="right">$' + money(r.billed_amount || r[2]) + '</td>' +
                '<td class="right">$' + money(r.paid_amount || r[3]) + '</td>' +
                '<td class="right">$' + money(r.qpa_amount || r[4]) + '</td>' +
                '<td class="right">$' + money(r.underpayment_amount || r[5]) + '</td>' +
                '<td class="right">' + pct(r.underpayment_pct || r[6]) + '</td>' +
                '<td>' + eligStr + '</td>' +
                '<td><span class="' + badgeCls(status) + '">' + status + '</span></td></tr>';
        }).join("");
    }

    // --- Render all ---
    function renderAll(data) {
        renderKPIs(data.financial || {});
        renderRecovery(data.recovery);
        renderPipeline(data.pipeline);
        renderDeadlines(data.deadlines);
        renderScorecards(data.scorecards);
        renderCPT(data.cpt);
        renderAging(data.aging);
        renderUnderpayments(data.underpayments);
    }

    function updateStatus(live, msg) {
        isLive = live;
        var dot = document.getElementById("status-dot");
        var text = document.getElementById("status-text");
        var info = document.getElementById("refresh-info");
        if (live) {
            dot.className = "live-dot";
            text.textContent = "LIVE";
            info.textContent = "Connected to Gold Layer \u2014 auto-refresh every 30s \u2014 " + msg;
        } else {
            dot.className = "static-dot";
            text.textContent = "STATIC";
            info.textContent = msg;
        }
    }

    function fetchAndRender() {
        fetch(API_URL)
            .then(function(resp) { return resp.json(); })
            .then(function(data) {
                renderAll(data);
                lastRefresh = new Date();
                updateStatus(true, "Last refresh: " + lastRefresh.toLocaleTimeString());
            })
            .catch(function() {
                if (!lastRefresh) {
                    // First load failed — use seed data
                    loadSeedData();
                }
                // If already rendered, keep stale data and show status
                updateStatus(false, lastRefresh
                    ? "API unavailable \u2014 showing data from " + lastRefresh.toLocaleTimeString()
                    : "Static snapshot from __GENERATED__");
            });
    }

    function loadSeedData() {
        try {
            var el = document.getElementById("seed-data");
            var data = JSON.parse(el.textContent);
            renderAll(data);
            updateStatus(false, "Static snapshot from __GENERATED__ \u2014 start serve_dashboard.py for live data");
        } catch(e) {
            updateStatus(false, "No data available \u2014 run run_olap.py then serve_dashboard.py");
        }
    }

    function byId(id) { return document.getElementById(id); }

    // --- Init ---
    fetchAndRender();
    setInterval(fetchAndRender, REFRESH_INTERVAL);
})();
</script>

</body>
</html>"""
