"""Tests for OLAP Lakehouse — CDC, Bronze, Silver, Gold layers + Reports.

Runs against an in-memory OLTP database populated by the ingestion pipeline,
seeds disputes/cases/deadlines, then validates each medallion layer.
"""

import json
import os
import sys
import sqlite3
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.db import reset_db
from ingestion.ingest.patients import ingest_patients, ingest_hl7v2_patients
from ingestion.ingest.providers import ingest_providers
from ingestion.ingest.fee_schedules import ingest_fee_schedule
from ingestion.ingest.claims import ingest_claims
from ingestion.ingest.remittances import ingest_era

from olap.cdc import init_cdc, detect_changes, get_watermark, get_sync_summary
from olap.seed_cases import seed_disputes_and_cases
from olap.bronze import init_bronze, extract_bronze, CDC_TABLES
from olap.silver import init_silver, transform_silver
from olap.gold import init_gold, aggregate_gold
from olap.report import generate_report
from olap.html_report import generate_html_report

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "ingestion", "sample_data")


def _read(filename):
    with open(os.path.join(SAMPLE_DIR, filename)) as f:
        return f.read()


def _build_oltp():
    """Build a populated in-memory OLTP database with seeded cases."""
    conn = reset_db(":memory:")
    ingest_patients(conn, _read("fhir_patients.json"))
    ingest_hl7v2_patients(conn, _read("adt_patients.hl7"))
    ingest_providers(conn, _read("nppes_providers.csv"))
    ingest_fee_schedule(conn, _read("medicare_fee_schedule_2025.csv"), "MEDICARE", "CMS_PFS_2025")
    ingest_fee_schedule(conn, _read("fair_health_rates.csv"), "FAIR_HEALTH", "FAIR_HEALTH_2025")
    ingest_fee_schedule(conn, _read("payer_fee_schedule_aetna.csv"), "AETNA", "AETNA_CONTRACTED_2025")
    ingest_claims(conn, _read("claims_837.edi"))
    ingest_claims(conn, _read("claims_837_replacement.edi"))
    ingest_era(conn, _read("era_835.edi"))
    ingest_era(conn, _read("era_835_second.edi"))
    # Seed disputes, cases, deadlines from underpaid claims
    seed_disputes_and_cases(conn)
    return conn


def _build_olap(oltp_conn):
    """Build OLAP database from OLTP."""
    olap_conn = sqlite3.connect(":memory:")
    olap_conn.execute("PRAGMA journal_mode=WAL")
    olap_conn.row_factory = sqlite3.Row
    return olap_conn


class TestCDCModule(unittest.TestCase):
    """Tests for the CDC watermark and change detection module."""

    def test_detect_changes_first_run_all_inserts(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_cdc(conn)
        rows = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]
        changes = detect_changes(conn, "test_table", "id", rows)
        self.assertEqual(len(changes["inserts"]), 2)
        self.assertEqual(len(changes["updates"]), 0)
        self.assertEqual(len(changes["deletes"]), 0)
        conn.close()

    def test_detect_changes_no_changes(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_cdc(conn)
        rows = [{"id": "1", "name": "Alice"}]
        detect_changes(conn, "test_table", "id", rows)
        conn.commit()
        # Second run with same data
        changes = detect_changes(conn, "test_table", "id", rows)
        self.assertEqual(len(changes["inserts"]), 0)
        self.assertEqual(len(changes["updates"]), 0)
        self.assertEqual(len(changes["deletes"]), 0)
        conn.close()

    def test_detect_changes_update(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_cdc(conn)
        rows = [{"id": "1", "name": "Alice"}]
        detect_changes(conn, "test_table", "id", rows)
        conn.commit()
        # Update name
        rows_updated = [{"id": "1", "name": "Alice Smith"}]
        changes = detect_changes(conn, "test_table", "id", rows_updated)
        self.assertEqual(len(changes["inserts"]), 0)
        self.assertEqual(len(changes["updates"]), 1)
        self.assertEqual(changes["updates"][0]["name"], "Alice Smith")
        conn.close()

    def test_detect_changes_delete(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_cdc(conn)
        rows = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        detect_changes(conn, "test_table", "id", rows)
        conn.commit()
        # Remove Bob
        rows_after = [{"id": "1", "name": "Alice"}]
        changes = detect_changes(conn, "test_table", "id", rows_after)
        self.assertEqual(len(changes["deletes"]), 1)
        self.assertIn("2", changes["deletes"])
        conn.close()

    def test_detect_changes_insert_update_delete_combined(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_cdc(conn)
        rows = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        detect_changes(conn, "test_table", "id", rows)
        conn.commit()
        # Alice updated, Bob deleted, Charlie inserted
        rows_new = [{"id": "1", "name": "Alice Updated"}, {"id": "3", "name": "Charlie"}]
        changes = detect_changes(conn, "test_table", "id", rows_new)
        self.assertEqual(len(changes["inserts"]), 1)
        self.assertEqual(len(changes["updates"]), 1)
        self.assertEqual(len(changes["deletes"]), 1)
        conn.close()


class TestSeedCases(unittest.TestCase):
    """Tests for the case seeding module."""

    def test_seed_creates_data(self):
        conn = reset_db(":memory:")
        ingest_claims(conn, _read("claims_837.edi"))
        ingest_era(conn, _read("era_835.edi"))
        result = seed_disputes_and_cases(conn)
        self.assertGreater(result["cases"], 0)
        self.assertGreater(result["disputes"], 0)
        self.assertGreater(result["deadlines"], 0)

    def test_seed_is_idempotent(self):
        conn = reset_db(":memory:")
        ingest_claims(conn, _read("claims_837.edi"))
        ingest_era(conn, _read("era_835.edi"))
        r1 = seed_disputes_and_cases(conn)
        r2 = seed_disputes_and_cases(conn)
        # INSERT OR IGNORE means second run should still report counts
        # but total rows in DB shouldn't double
        cases_count = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        self.assertEqual(cases_count, r1["cases"])

    def test_seed_creates_deadlines_per_case(self):
        conn = reset_db(":memory:")
        ingest_claims(conn, _read("claims_837.edi"))
        ingest_era(conn, _read("era_835.edi"))
        seed_disputes_and_cases(conn)
        # Each case should have 3 deadlines
        cases = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        deadlines = conn.execute("SELECT COUNT(*) FROM deadlines").fetchone()[0]
        self.assertEqual(deadlines, cases * 3)


class TestBronzeLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.oltp = _build_oltp()
        cls.olap = _build_olap(cls.oltp)
        cls.summary = extract_bronze(cls.oltp, cls.olap)

    def test_all_cdc_tables_extracted(self):
        required_tables = ["claims", "claim_lines", "remittances", "patients",
                           "providers", "payers", "fee_schedule"]
        for table in required_tables:
            self.assertIn(table, self.summary)
            self.assertGreater(self.summary[table]["inserted"], 0,
                               f"bronze_{table} should have inserted rows")

    def test_new_cdc_tables_extracted(self):
        """Disputes, cases, deadlines should be extracted."""
        for table in ["disputes", "cases", "deadlines"]:
            self.assertIn(table, self.summary)
            self.assertGreater(self.summary[table]["inserted"], 0,
                               f"bronze_{table} should have inserted rows")

    def test_bronze_claims_count(self):
        oltp_count = self.oltp.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        bronze_count = self.olap.execute("SELECT COUNT(*) FROM bronze_claims").fetchone()[0]
        self.assertEqual(oltp_count, bronze_count)

    def test_bronze_payload_is_valid_json(self):
        rows = self.olap.execute("SELECT payload FROM bronze_claims").fetchall()
        for row in rows:
            data = json.loads(row[0])
            self.assertIn("claim_id", data)
            self.assertIn("total_billed", data)

    def test_bronze_cdc_metadata(self):
        row = self.olap.execute(
            "SELECT _cdc_operation, _cdc_timestamp, _source_table FROM bronze_claims LIMIT 1"
        ).fetchone()
        self.assertEqual(row[0], "I")
        self.assertIsNotNone(row[1])
        self.assertEqual(row[2], "claims")

    def test_bronze_idempotent(self):
        """Running extract again should skip all already-extracted rows."""
        summary2 = extract_bronze(self.oltp, self.olap)
        for table in CDC_TABLES:
            self.assertEqual(summary2[table]["inserted"], 0,
                             f"bronze_{table} should not re-insert")
            if self.summary[table]["inserted"] > 0:
                self.assertGreater(summary2[table]["skipped"], 0)

    def test_bronze_patients_match_oltp(self):
        oltp_count = self.oltp.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        bronze_count = self.olap.execute("SELECT COUNT(*) FROM bronze_patients").fetchone()[0]
        self.assertEqual(oltp_count, bronze_count)

    def test_bronze_fee_schedule_match_oltp(self):
        oltp_count = self.oltp.execute("SELECT COUNT(*) FROM fee_schedule").fetchone()[0]
        bronze_count = self.olap.execute("SELECT COUNT(*) FROM bronze_fee_schedule").fetchone()[0]
        self.assertEqual(oltp_count, bronze_count)

    def test_bronze_providers_match_oltp(self):
        oltp_count = self.oltp.execute("SELECT COUNT(*) FROM providers").fetchone()[0]
        bronze_count = self.olap.execute("SELECT COUNT(*) FROM bronze_providers").fetchone()[0]
        self.assertEqual(oltp_count, bronze_count)

    def test_cdc_watermarks_populated(self):
        """CDC watermarks should be set for all extracted tables."""
        watermarks = get_sync_summary(self.olap)
        self.assertGreater(len(watermarks), 0)
        for w in watermarks:
            self.assertIsNotNone(w[1])  # last_sync_ts


class TestSilverLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.oltp = _build_oltp()
        cls.olap = _build_olap(cls.oltp)
        extract_bronze(cls.oltp, cls.olap)
        cls.summary = transform_silver(cls.olap)

    def test_silver_patients_populated(self):
        self.assertGreater(self.summary["silver_patients"], 0)

    def test_silver_providers_current_only(self):
        count = self.summary["silver_providers"]
        oltp_current = self.oltp.execute(
            "SELECT COUNT(*) FROM providers WHERE is_current = 1"
        ).fetchone()[0]
        self.assertEqual(count, oltp_current)

    def test_silver_claims_denormalized(self):
        row = self.olap.execute("""
            SELECT claim_id, line_count, cpt_codes
            FROM silver_claims WHERE line_count > 0 LIMIT 1
        """).fetchone()
        self.assertIsNotNone(row)
        cpt_codes = json.loads(row[2])
        self.assertIsInstance(cpt_codes, list)
        self.assertGreater(len(cpt_codes), 0)

    def test_silver_claim_remittance_join(self):
        claims_count = self.olap.execute("SELECT COUNT(*) FROM silver_claims").fetchone()[0]
        joined_count = self.olap.execute("SELECT COUNT(*) FROM silver_claim_remittance").fetchone()[0]
        self.assertEqual(claims_count, joined_count)

    def test_silver_underpayment_calculation(self):
        rows = self.olap.execute("""
            SELECT total_billed, total_paid, underpayment
            FROM silver_claim_remittance
        """).fetchall()
        for r in rows:
            self.assertAlmostEqual(r[2], r[0] - r[1], places=2)

    def test_silver_fee_schedule_has_rates(self):
        count = self.summary["silver_fee_schedule"]
        self.assertGreater(count, 0)
        row = self.olap.execute(
            "SELECT rate FROM silver_fee_schedule WHERE is_current = 1 LIMIT 1"
        ).fetchone()
        self.assertGreater(row[0], 0)

    def test_silver_remittances_enriched(self):
        row = self.olap.execute("""
            SELECT payer_name FROM silver_remittances
            WHERE payer_name IS NOT NULL LIMIT 1
        """).fetchone()
        self.assertIsNotNone(row)

    def test_silver_claim_remittance_payer_name(self):
        rows = self.olap.execute("""
            SELECT payer_id, payer_name FROM silver_claim_remittance
            WHERE payer_id IS NOT NULL
        """).fetchall()
        for r in rows:
            self.assertIsNotNone(r[1], f"payer_name should be set for payer_id={r[0]}")

    def test_silver_disputes_populated(self):
        self.assertGreater(self.summary["silver_disputes"], 0)

    def test_silver_disputes_enriched(self):
        """Disputes should have payer_name from claim context."""
        row = self.olap.execute("""
            SELECT payer_name FROM silver_disputes
            WHERE payer_name IS NOT NULL LIMIT 1
        """).fetchone()
        self.assertIsNotNone(row)

    def test_silver_cases_populated(self):
        self.assertGreater(self.summary["silver_cases"], 0)

    def test_silver_cases_have_dispute_counts(self):
        row = self.olap.execute("""
            SELECT dispute_count, total_billed, total_underpayment
            FROM silver_cases WHERE dispute_count > 0 LIMIT 1
        """).fetchone()
        self.assertIsNotNone(row)
        self.assertGreater(row[0], 0)

    def test_silver_cases_have_age(self):
        row = self.olap.execute("""
            SELECT age_days FROM silver_cases WHERE age_days > 0 LIMIT 1
        """).fetchone()
        self.assertIsNotNone(row)

    def test_silver_deadlines_populated(self):
        self.assertGreater(self.summary["silver_deadlines"], 0)

    def test_silver_deadlines_have_types(self):
        types = self.olap.execute(
            "SELECT DISTINCT type FROM silver_deadlines"
        ).fetchall()
        type_set = {r[0] for r in types}
        self.assertIn("open_negotiation", type_set)
        self.assertIn("idr_initiation", type_set)
        self.assertIn("idr_decision", type_set)

    def test_silver_idempotent(self):
        summary2 = transform_silver(self.olap)
        for table, count in self.summary.items():
            self.assertEqual(summary2[table], count)


class TestGoldLayer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.oltp = _build_oltp()
        cls.olap = _build_olap(cls.oltp)
        extract_bronze(cls.oltp, cls.olap)
        transform_silver(cls.olap)
        cls.summary = aggregate_gold(cls.olap)

    def test_gold_recovery_by_payer(self):
        count = self.summary["gold_recovery_by_payer"]
        self.assertGreater(count, 0)

    def test_gold_recovery_rate_bounded(self):
        rows = self.olap.execute(
            "SELECT recovery_rate_pct FROM gold_recovery_by_payer"
        ).fetchall()
        for r in rows:
            self.assertGreaterEqual(r[0], 0)
            self.assertLessEqual(r[0], 100)

    def test_gold_cpt_analysis(self):
        count = self.summary["gold_cpt_analysis"]
        self.assertGreater(count, 0)

    def test_gold_cpt_has_medicare_rates(self):
        row = self.olap.execute("""
            SELECT COUNT(*) FROM gold_cpt_analysis WHERE medicare_rate IS NOT NULL
        """).fetchone()
        self.assertGreater(row[0], 0)

    def test_gold_payer_scorecard(self):
        count = self.summary["gold_payer_scorecard"]
        self.assertGreater(count, 0)

    def test_gold_payer_risk_tiers(self):
        rows = self.olap.execute(
            "SELECT risk_tier FROM gold_payer_scorecard"
        ).fetchall()
        valid_tiers = {"low", "medium", "high"}
        for r in rows:
            self.assertIn(r[0], valid_tiers)

    def test_gold_financial_summary(self):
        count = self.summary["gold_financial_summary"]
        self.assertEqual(count, 10)

    def test_gold_financial_metrics_present(self):
        rows = self.olap.execute(
            "SELECT metric_name FROM gold_financial_summary"
        ).fetchall()
        names = {r[0] for r in rows}
        for expected in ["total_claims", "total_billed", "total_paid",
                         "recovery_rate_pct", "denial_rate_pct"]:
            self.assertIn(expected, names)

    def test_gold_claims_aging(self):
        count = self.summary["gold_claims_aging"]
        self.assertGreater(count, 0)

    def test_gold_claims_aging_pct_sums_to_100(self):
        total = self.olap.execute(
            "SELECT SUM(pct_of_total) FROM gold_claims_aging"
        ).fetchone()[0]
        self.assertAlmostEqual(total, 100.0, places=1)

    def test_gold_case_pipeline(self):
        count = self.summary["gold_case_pipeline"]
        self.assertGreater(count, 0)

    def test_gold_case_pipeline_statuses(self):
        rows = self.olap.execute(
            "SELECT status FROM gold_case_pipeline"
        ).fetchall()
        statuses = {r[0] for r in rows}
        # Should have at least some of the expected statuses
        self.assertTrue(len(statuses) > 0)

    def test_gold_case_pipeline_sla_bounded(self):
        rows = self.olap.execute(
            "SELECT sla_compliance_pct FROM gold_case_pipeline"
        ).fetchall()
        for r in rows:
            self.assertGreaterEqual(r[0], 0)
            self.assertLessEqual(r[0], 100)

    def test_gold_deadline_compliance(self):
        count = self.summary["gold_deadline_compliance"]
        self.assertGreater(count, 0)

    def test_gold_deadline_compliance_types(self):
        rows = self.olap.execute(
            "SELECT deadline_type FROM gold_deadline_compliance"
        ).fetchall()
        types = {r[0] for r in rows}
        self.assertIn("open_negotiation", types)
        self.assertIn("idr_initiation", types)

    def test_gold_deadline_compliance_bounded(self):
        rows = self.olap.execute(
            "SELECT compliance_pct FROM gold_deadline_compliance"
        ).fetchall()
        for r in rows:
            self.assertGreaterEqual(r[0], 0)
            self.assertLessEqual(r[0], 100)

    def test_gold_underpayment_detection(self):
        count = self.summary["gold_underpayment_detection"]
        self.assertGreater(count, 0)

    def test_gold_underpayment_has_qpa(self):
        row = self.olap.execute("""
            SELECT COUNT(*) FROM gold_underpayment_detection WHERE qpa_amount > 0
        """).fetchone()
        self.assertGreater(row[0], 0)

    def test_gold_underpayment_arbitration_eligible(self):
        """At least some claims should be arbitration-eligible."""
        row = self.olap.execute("""
            SELECT COUNT(*) FROM gold_underpayment_detection WHERE arbitration_eligible = 1
        """).fetchone()
        self.assertGreater(row[0], 0)

    def test_gold_idempotent(self):
        summary2 = aggregate_gold(self.olap)
        for table, count in self.summary.items():
            self.assertEqual(summary2[table], count)

    def test_gold_total_billed_consistent(self):
        fs_total = self.olap.execute(
            "SELECT metric_value FROM gold_financial_summary WHERE metric_name='total_billed'"
        ).fetchone()[0]
        payer_total = self.olap.execute(
            "SELECT SUM(total_billed) FROM gold_recovery_by_payer"
        ).fetchone()[0]
        self.assertGreaterEqual(fs_total, payer_total)


class TestReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        oltp = _build_oltp()
        cls.olap = _build_olap(oltp)
        extract_bronze(oltp, cls.olap)
        transform_silver(cls.olap)
        aggregate_gold(cls.olap)
        cls.report = generate_report(cls.olap)

    def test_report_not_empty(self):
        self.assertGreater(len(self.report), 100)

    def test_report_has_original_sections(self):
        self.assertIn("FINANCIAL SUMMARY", self.report)
        self.assertIn("RECOVERY BY PAYER", self.report)
        self.assertIn("PAYER SCORECARDS", self.report)
        self.assertIn("CPT CODE ANALYSIS", self.report)
        self.assertIn("CLAIMS AGING", self.report)

    def test_report_has_new_sections(self):
        self.assertIn("CASE PIPELINE", self.report)
        self.assertIn("DEADLINE COMPLIANCE", self.report)
        self.assertIn("UNDERPAYMENT DETECTION", self.report)

    def test_report_has_dollar_amounts(self):
        self.assertIn("$", self.report)

    def test_report_has_percentages(self):
        self.assertIn("%", self.report)

    def test_report_has_payer_names(self):
        self.assertIn("Aetna", self.report)

    def test_report_has_cpt_codes(self):
        self.assertIn("99213", self.report)


class TestHTMLReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        oltp = _build_oltp()
        cls.olap = _build_olap(oltp)
        extract_bronze(oltp, cls.olap)
        transform_silver(cls.olap)
        aggregate_gold(cls.olap)
        cls.html = generate_html_report(cls.olap)

    def test_html_not_empty(self):
        self.assertGreater(len(self.html), 500)

    def test_html_is_valid_structure(self):
        self.assertIn("<!DOCTYPE html>", self.html)
        self.assertIn("</html>", self.html)

    def test_html_has_title(self):
        self.assertIn("Arbitration Recovery Dashboard", self.html)

    def test_html_has_kpi_cards(self):
        self.assertIn("Total Claims", self.html)
        self.assertIn("Total Billed", self.html)
        self.assertIn("Recovery Rate", self.html)

    def test_html_has_tables(self):
        self.assertIn("Recovery by Payer", self.html)
        self.assertIn("Payer Scorecards", self.html)
        self.assertIn("CPT Code Analysis", self.html)
        self.assertIn("Claims Aging", self.html)

    def test_html_has_new_sections(self):
        self.assertIn("Case Pipeline", self.html)
        self.assertIn("Deadline Compliance", self.html)
        self.assertIn("Underpayment Detection", self.html)

    def test_html_has_dollar_amounts(self):
        self.assertIn("$", self.html)

    def test_html_has_badges(self):
        self.assertIn("badge", self.html)

    def test_html_has_css(self):
        self.assertIn("<style>", self.html)


class TestEndToEndOLAP(unittest.TestCase):
    """Full pipeline: Seed → OLTP → Bronze → Silver → Gold → Reports."""

    def test_full_pipeline(self):
        oltp = _build_oltp()
        olap = _build_olap(oltp)

        # Bronze
        bronze = extract_bronze(oltp, olap)
        total_inserted = sum(s["inserted"] for s in bronze.values())
        self.assertGreater(total_inserted, 0)

        # Silver
        silver = transform_silver(olap)
        self.assertGreater(silver["silver_claims"], 0)
        self.assertGreater(silver["silver_claim_remittance"], 0)
        self.assertGreater(silver["silver_disputes"], 0)
        self.assertGreater(silver["silver_cases"], 0)
        self.assertGreater(silver["silver_deadlines"], 0)

        # Gold
        gold = aggregate_gold(olap)
        self.assertGreater(gold["gold_recovery_by_payer"], 0)
        self.assertGreater(gold["gold_financial_summary"], 0)
        self.assertGreater(gold["gold_case_pipeline"], 0)
        self.assertGreater(gold["gold_deadline_compliance"], 0)
        self.assertGreater(gold["gold_underpayment_detection"], 0)

        # Text Report
        report = generate_report(olap)
        self.assertIn("ANALYTICS REPORT", report)
        self.assertIn("CASE PIPELINE", report)
        self.assertIn("END OF REPORT", report)

        # HTML Report
        html = generate_html_report(olap)
        self.assertIn("Arbitration Recovery Dashboard", html)
        self.assertIn("</html>", html)

        oltp.close()
        olap.close()


if __name__ == "__main__":
    unittest.main()
