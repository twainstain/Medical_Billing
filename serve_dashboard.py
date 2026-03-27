#!/usr/bin/env python3
"""
Live Arbitration Dashboard — serves Gold layer data via HTTP.

Replaces the static HTML file with a live dashboard that queries
the OLAP Gold layer on every page load, simulating Power BI Direct Lake.

Usage:
    python serve_dashboard.py          # default port 8050
    python serve_dashboard.py 9090     # custom port
"""

import json
import os
import sys
import sqlite3
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OLAP_PATH = os.path.join(BASE_DIR, "olap.db")
DASHBOARD_HTML_PATH = os.path.join(BASE_DIR, "arbitration_dashboard.html")


def get_olap_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(OLAP_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def query_gold_data() -> dict:
    """Query all Gold layer tables and return as a single JSON-serializable dict."""
    conn = get_olap_connection()
    try:
        data = {}

        # Financial Summary
        rows = conn.execute(
            "SELECT metric_name, metric_value FROM gold_financial_summary"
        ).fetchall()
        data["financial"] = {r["metric_name"]: r["metric_value"] for r in rows}

        # Recovery by Payer
        rows = conn.execute("""
            SELECT payer_id, payer_name, total_claims, total_billed,
                   total_paid, total_underpayment, recovery_rate_pct, denial_rate_pct
            FROM gold_recovery_by_payer ORDER BY total_underpayment DESC
        """).fetchall()
        data["recovery"] = [dict(r) for r in rows]

        # Payer Scorecards
        rows = conn.execute("""
            SELECT payer_id, payer_name, payer_type, total_claims,
                   payment_rate_pct, denial_rate_pct, avg_underpayment,
                   total_underpayment, risk_tier
            FROM gold_payer_scorecard ORDER BY risk_tier DESC, total_underpayment DESC
        """).fetchall()
        data["scorecards"] = [dict(r) for r in rows]

        # CPT Analysis
        rows = conn.execute("""
            SELECT cpt_code, claim_count, avg_billed, avg_paid,
                   payment_ratio_pct, medicare_rate, fair_health_rate
            FROM gold_cpt_analysis ORDER BY claim_count DESC
        """).fetchall()
        data["cpt"] = [dict(r) for r in rows]

        # Claims Aging
        rows = conn.execute("""
            SELECT aging_bucket, claim_count, total_billed, total_unpaid, pct_of_total
            FROM gold_claims_aging
            ORDER BY CASE aging_bucket
                WHEN '0-30 days' THEN 1 WHEN '31-60 days' THEN 2
                WHEN '61-90 days' THEN 3 WHEN '91-180 days' THEN 4 ELSE 5 END
        """).fetchall()
        data["aging"] = [dict(r) for r in rows]

        # Case Pipeline
        try:
            rows = conn.execute("""
                SELECT status, case_count, total_underpayment,
                       avg_age_days, sla_compliance_pct
                FROM gold_case_pipeline ORDER BY case_count DESC
            """).fetchall()
            data["pipeline"] = [dict(r) for r in rows]
        except Exception:
            data["pipeline"] = []

        # Deadline Compliance
        try:
            rows = conn.execute("""
                SELECT deadline_type, total_deadlines, met_count, missed_count,
                       at_risk_count, compliance_pct
                FROM gold_deadline_compliance ORDER BY deadline_type
            """).fetchall()
            data["deadlines"] = [dict(r) for r in rows]
        except Exception:
            data["deadlines"] = []

        # Underpayment Detection
        try:
            rows = conn.execute("""
                SELECT claim_id, payer_name, billed_amount, paid_amount,
                       qpa_amount, underpayment_amount, underpayment_pct,
                       arbitration_eligible, dispute_status
                FROM gold_underpayment_detection
                ORDER BY underpayment_amount DESC LIMIT 20
            """).fetchall()
            data["underpayments"] = [dict(r) for r in rows]
        except Exception:
            data["underpayments"] = []

        return data
    finally:
        conn.close()


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/api/data":
            self._serve_json()
        elif path in ("/", "/index.html"):
            self._serve_dashboard()
        else:
            self.send_error(404)

    def _serve_json(self):
        try:
            data = query_gold_data()
            payload = json.dumps(data)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(payload.encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _serve_dashboard(self):
        try:
            with open(DASHBOARD_HTML_PATH, "r") as f:
                html = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(html.encode())
        except FileNotFoundError:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Dashboard HTML not found. Run run_olap.py first.")

    def log_message(self, format, *args):
        logger.info("HTTP %s", format % args)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8050

    if not os.path.exists(OLAP_PATH):
        logger.error("OLAP database not found at %s. Run run_olap.py first.", OLAP_PATH)
        sys.exit(1)

    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info("=" * 60)
    logger.info("Arbitration Dashboard — Live (Gold Layer)")
    logger.info("=" * 60)
    logger.info("  URL:      http://localhost:%d", port)
    logger.info("  API:      http://localhost:%d/api/data", port)
    logger.info("  Database: %s", OLAP_PATH)
    logger.info("  Auto-refresh: every 30 seconds")
    logger.info("=" * 60)
    logger.info("Press Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
