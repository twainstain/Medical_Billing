#!/usr/bin/env python3
"""
Run the complete OLAP pipeline: Seed Cases → Bronze (CDC) → Silver → Gold → Reports.

Reads from the populated OLTP database (oltp.db), seeds disputes/cases/deadlines,
then produces an OLAP database (olap.db) with Medallion architecture layers,
a text analytics report, and an HTML dashboard.

Usage:
    python run_olap.py
"""

import os
import sys
import logging
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from olap.seed_cases import seed_disputes_and_cases
from olap.bronze import extract_bronze
from olap.silver import transform_silver
from olap.gold import aggregate_gold
from olap.report import generate_report
from olap.html_report import generate_html_report
from olap.cdc import get_sync_summary

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OLTP_PATH = os.path.join(BASE_DIR, "oltp.db")
OLAP_PATH = os.path.join(BASE_DIR, "olap.db")
HTML_REPORT_PATH = os.path.join(BASE_DIR, "arbitration_dashboard.html")


def get_olap_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def main():
    logger.info("=" * 70)
    logger.info("MEDICAL BILLING ARBITRATION — OLAP PIPELINE")
    logger.info("=" * 70)

    if not os.path.exists(OLTP_PATH):
        logger.error("OLTP database not found at %s. Run run_ingestion.py first.", OLTP_PATH)
        sys.exit(1)

    # Open OLTP (read-write for seeding) and OLAP (read-write)
    oltp_conn = sqlite3.connect(OLTP_PATH)
    oltp_conn.row_factory = sqlite3.Row

    # Stage 0: Seed disputes, cases, deadlines from claim underpayments
    logger.info("\n--- Stage 0: Seed Disputes & Cases (Workflow Engine Simulation)")
    seed_summary = seed_disputes_and_cases(oltp_conn)
    logger.info("  Cases: %d  |  Disputes: %d  |  Deadlines: %d",
                seed_summary["cases"], seed_summary["disputes"], seed_summary["deadlines"])

    # Fresh OLAP database
    if os.path.exists(OLAP_PATH):
        os.remove(OLAP_PATH)
    olap_conn = get_olap_connection(OLAP_PATH)

    # Stage 1: Bronze (CDC extraction)
    logger.info("\n--- Stage 1: Bronze Layer (CDC → Raw)")
    bronze_summary = extract_bronze(oltp_conn, olap_conn)
    for table, stats in bronze_summary.items():
        logger.info("  bronze_%-20s %d inserted, %d updated, %d deleted, %d skipped",
                     table, stats["inserted"], stats["updated"],
                     stats["deleted"], stats["skipped"])

    # CDC Watermark Summary
    logger.info("\n  CDC Watermarks:")
    for row in get_sync_summary(olap_conn):
        logger.info("    %-20s synced=%d  (I=%d U=%d D=%d)  last=%s",
                     row[0], row[2], row[3], row[4], row[5], row[1][:19])

    # Stage 2: Silver (cleaned, joined)
    logger.info("\n--- Stage 2: Silver Layer (Clean → Conform → Join)")
    silver_summary = transform_silver(olap_conn)
    for table, count in silver_summary.items():
        logger.info("  %-35s %d rows", table, count)

    # Stage 3: Gold (aggregations)
    logger.info("\n--- Stage 3: Gold Layer (Aggregate → Enrich)")
    gold_summary = aggregate_gold(olap_conn)
    for table, count in gold_summary.items():
        logger.info("  %-35s %d rows", table, count)

    # Stage 4: Text Report
    logger.info("\n--- Stage 4: Text Analytics Report (Gold → Console)")
    report = generate_report(olap_conn)
    print("\n" + report)

    # Stage 5: HTML Dashboard Report
    logger.info("\n--- Stage 5: HTML Dashboard (Gold → Power BI Simulation)")
    html = generate_html_report(olap_conn)
    with open(HTML_REPORT_PATH, "w") as f:
        f.write(html)
    logger.info("  HTML dashboard saved: %s", os.path.abspath(HTML_REPORT_PATH))

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("OLAP DATABASE SUMMARY")
    logger.info("=" * 70)

    for prefix in ("bronze_", "silver_", "gold_"):
        tables = olap_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"{prefix}%",)
        ).fetchall()
        for t in tables:
            name = t[0]
            count = olap_conn.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
            logger.info("  %-35s %d rows", name, count)

    # CDC tracking tables
    for tbl in ("cdc_watermarks", "cdc_row_hashes"):
        count = olap_conn.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
        logger.info("  %-35s %d rows", tbl, count)

    logger.info("\n" + "=" * 70)
    logger.info("OLAP database ready at: %s", os.path.abspath(OLAP_PATH))
    logger.info("HTML report ready at:   %s", os.path.abspath(HTML_REPORT_PATH))
    logger.info("=" * 70)

    oltp_conn.close()
    olap_conn.close()


if __name__ == "__main__":
    main()
