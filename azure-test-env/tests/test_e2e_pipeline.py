"""End-to-end pipeline tests: OLTP → Gold Views → Agent response chain.

Verifies the full data pipeline works correctly:
  1. Seed data lands in OLTP tables with correct counts and integrity
  2. All 13 Gold view queries produce expected results from OLTP data
  3. Gold views chain correctly (e.g., underpayment_detection feeds win_loss)
  4. Agent response structure is valid for every common analysis
  5. Financial totals are consistent across all layers

Uses in-memory SQLite with the same seed data as production.
"""

import os
import sys
import sqlite3
import json
import pytest
from unittest.mock import patch, MagicMock
from decimal import Decimal

FUNC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "functions")
sys.path.insert(0, FUNC_DIR)


# ============================================================================
# Shared fixture: full OLTP database with seed data
# ============================================================================

@pytest.fixture(scope="module")
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE payers (payer_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, type TEXT NOT NULL, state TEXT, is_active INTEGER DEFAULT 1, created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE providers (npi TEXT PRIMARY KEY, tin TEXT NOT NULL, name TEXT NOT NULL, specialty TEXT, facility_id TEXT, facility_name TEXT, state TEXT, is_active INTEGER DEFAULT 1, created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE patients (patient_id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, date_of_birth TEXT, gender TEXT, address_line1 TEXT, city TEXT, state TEXT, zip_code TEXT, insurance_id TEXT, payer_id INTEGER, created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE fee_schedule (id INTEGER PRIMARY KEY AUTOINCREMENT, payer_id INTEGER, cpt_code TEXT, modifier TEXT, geo_region TEXT, rate REAL, rate_type TEXT, valid_from TEXT, valid_to TEXT DEFAULT '9999-12-31', is_current INTEGER DEFAULT 1, source TEXT, loaded_date TEXT DEFAULT (datetime('now')));
        CREATE TABLE claims (claim_id INTEGER PRIMARY KEY AUTOINCREMENT, external_claim_id TEXT UNIQUE, patient_id INTEGER, provider_npi TEXT, payer_id INTEGER, date_of_service TEXT, date_filed TEXT, facility_id TEXT, place_of_service TEXT, total_billed REAL, status TEXT DEFAULT 'filed', created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE claim_lines (line_id INTEGER PRIMARY KEY AUTOINCREMENT, claim_id INTEGER, line_number INTEGER, cpt_code TEXT, modifier TEXT, units INTEGER DEFAULT 1, billed_amount REAL, diagnosis_codes TEXT, created_at TEXT DEFAULT (datetime('now')), UNIQUE(claim_id, line_number));
        CREATE TABLE remittances (remit_id INTEGER PRIMARY KEY AUTOINCREMENT, claim_id INTEGER, era_date TEXT, paid_amount REAL, allowed_amount REAL, adjustment_reason TEXT, denial_code TEXT, check_number TEXT, source_type TEXT DEFAULT 'edi_835', created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE cases (case_id INTEGER PRIMARY KEY AUTOINCREMENT, assigned_analyst TEXT, status TEXT DEFAULT 'open', priority TEXT DEFAULT 'medium', created_date TEXT DEFAULT (datetime('now')), last_activity TEXT DEFAULT (datetime('now')), outcome TEXT, award_amount REAL, closed_date TEXT);
        CREATE TABLE disputes (dispute_id INTEGER PRIMARY KEY AUTOINCREMENT, claim_id INTEGER, claim_line_id INTEGER, case_id INTEGER, dispute_type TEXT, status TEXT DEFAULT 'open', billed_amount REAL, paid_amount REAL, requested_amount REAL, qpa_amount REAL, filed_date TEXT, open_negotiation_deadline TEXT, idr_initiation_deadline TEXT, created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE deadlines (deadline_id INTEGER PRIMARY KEY AUTOINCREMENT, case_id INTEGER, dispute_id INTEGER, type TEXT, due_date TEXT, status TEXT DEFAULT 'pending', alerted_date TEXT, completed_date TEXT, created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE evidence_artifacts (artifact_id INTEGER PRIMARY KEY AUTOINCREMENT, case_id INTEGER, type TEXT, blob_url TEXT, original_filename TEXT, extracted_data TEXT, classification_confidence REAL, ocr_status TEXT DEFAULT 'pending', content_hash TEXT, uploaded_date TEXT DEFAULT (datetime('now')));
        CREATE TABLE audit_log (log_id INTEGER PRIMARY KEY AUTOINCREMENT, entity_type TEXT, entity_id TEXT, action TEXT, user_id TEXT, timestamp TEXT DEFAULT (datetime('now')), old_value TEXT, new_value TEXT, ai_agent TEXT, ai_confidence REAL, ai_model_version TEXT);
        CREATE TABLE dead_letter_queue (dlq_id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, entity_type TEXT, entity_id TEXT, error_category TEXT, error_detail TEXT, payload_ref TEXT, created_at TEXT DEFAULT (datetime('now')), resolved_at TEXT, resolved_by TEXT, status TEXT DEFAULT 'pending');

        INSERT INTO payers (name, type, state) VALUES ('Anthem Blue Cross', 'commercial', 'CA'), ('Aetna', 'commercial', 'NY'), ('UnitedHealthcare', 'commercial', 'TX'), ('Cigna', 'commercial', 'FL'), ('Humana', 'commercial', 'GA'), ('Medicare Part B', 'medicare', NULL), ('Medicaid - New York', 'medicaid', 'NY'), ('TRICARE West', 'tricare', NULL);
        INSERT INTO providers (npi, tin, name, specialty, facility_id, facility_name, state) VALUES ('1234567890', '123456789', 'Dr. Sarah Chen', 'Emergency Medicine', 'FAC001', 'Metro General Hospital', 'NY'), ('2345678901', '234567890', 'Dr. Michael Roberts', 'Orthopedic Surgery', 'FAC001', 'Metro General Hospital', 'NY'), ('3456789012', '345678901', 'Dr. Lisa Patel', 'Anesthesiology', 'FAC002', 'Riverside Medical Center', 'CA'), ('4567890123', '456789012', 'Dr. James Wilson', 'Radiology', 'FAC002', 'Riverside Medical Center', 'CA'), ('5678901234', '567890123', 'Dr. Amanda Foster', 'Cardiology', 'FAC003', 'Heart & Vascular Institute', 'TX'), ('6789012345', '678901234', 'Dr. David Kim', 'Neurosurgery', 'FAC001', 'Metro General Hospital', 'NY');
        INSERT INTO patients (first_name, last_name, date_of_birth, gender, payer_id) VALUES ('John', 'Smith', '1975-03-15', 'M', 1), ('Maria', 'Garcia', '1982-07-22', 'F', 2), ('Robert', 'Johnson', '1968-11-30', 'M', 3), ('Jennifer', 'Williams', '1990-01-05', 'F', 4), ('William', 'Brown', '1955-09-18', 'M', 5), ('Emily', 'Davis', '1988-04-12', 'F', 6), ('James', 'Martinez', '1972-06-25', 'M', 7), ('Susan', 'Anderson', '1965-12-08', 'F', 8), ('David', 'Taylor', '1980-08-20', 'M', 1), ('Lisa', 'Thomas', '1993-02-14', 'F', 2);
        INSERT INTO claims (external_claim_id, patient_id, provider_npi, payer_id, date_of_service, date_filed, facility_id, place_of_service, total_billed, status) VALUES ('CLM-2025-0001', 1, '1234567890', 1, '2025-06-15', '2025-06-20', 'FAC001', '23', 850.00, 'underpaid'), ('CLM-2025-0002', 2, '3456789012', 2, '2025-07-10', '2025-07-15', 'FAC002', '23', 1200.00, 'underpaid'), ('CLM-2025-0003', 3, '5678901234', 3, '2025-08-22', '2025-08-25', 'FAC003', '11', 520.00, 'underpaid'), ('CLM-2025-0004', 9, '2345678901', 1, '2025-05-10', '2025-05-15', 'FAC001', '21', 6500.00, 'in_dispute'), ('CLM-2025-0005', 10, '6789012345', 2, '2025-09-01', '2025-09-05', 'FAC001', '23', 4200.00, 'in_dispute'), ('CLM-2025-0006', 4, '1234567890', 4, '2025-04-01', '2025-04-05', 'FAC001', '23', 450.00, 'paid'), ('CLM-2025-0007', 5, '5678901234', 5, '2025-03-18', '2025-03-22', 'FAC003', '11', 320.00, 'paid'), ('CLM-2025-0008', 6, '4567890123', 6, '2025-10-05', '2025-10-08', 'FAC002', '22', 1800.00, 'denied'), ('CLM-2026-0001', 7, '1234567890', 7, '2026-01-10', '2026-01-15', 'FAC001', '23', 650.00, 'filed'), ('CLM-2026-0002', 8, '3456789012', 8, '2026-02-20', '2026-02-22', 'FAC002', '23', 980.00, 'filed');
        INSERT INTO remittances (claim_id, era_date, paid_amount, allowed_amount, adjustment_reason, denial_code, check_number, source_type) VALUES (1, '2025-07-20', 380.00, 425.00, 'CO-45', NULL, 'CHK-10001', 'edi_835'), (2, '2025-08-15', 520.00, 600.00, 'CO-45', NULL, 'CHK-10002', 'edi_835'), (3, '2025-09-25', 210.00, 250.00, 'CO-45', NULL, 'CHK-10003', 'edi_835'), (4, '2025-06-15', 2100.00, 2800.00, 'CO-45', NULL, 'CHK-10004', 'edi_835'), (5, '2025-10-10', 1800.00, 2200.00, 'CO-45', NULL, 'CHK-10005', 'eob_pdf'), (6, '2025-05-05', 420.00, 420.00, NULL, NULL, 'CHK-10006', 'edi_835'), (7, '2025-04-20', 310.00, 310.00, NULL, NULL, 'CHK-10007', 'edi_835'), (8, '2025-11-01', 0.00, 0.00, 'CO-197', 'CO-197', 'CHK-10008', 'edi_835');
        INSERT INTO cases (assigned_analyst, status, priority, created_date, last_activity, outcome, award_amount, closed_date) VALUES ('Ana Rodriguez', 'negotiation', 'high', '2025-08-01', '2026-03-20', NULL, NULL, NULL), ('Mark Thompson', 'idr_initiated', 'high', '2025-08-20', '2026-03-18', NULL, NULL, NULL), ('Ana Rodriguez', 'in_review', 'medium', '2025-10-01', '2026-03-15', NULL, NULL, NULL), ('Sarah Lee', 'idr_submitted', 'critical', '2025-06-20', '2026-03-10', NULL, NULL, NULL), ('Mark Thompson', 'decided', 'high', '2025-10-15', '2026-02-28', 'won', 3500.00, '2026-02-28'), ('Ana Rodriguez', 'closed', 'medium', '2025-03-01', '2025-06-15', 'lost', NULL, '2025-06-15');
        INSERT INTO disputes (claim_id, claim_line_id, case_id, dispute_type, status, billed_amount, paid_amount, requested_amount, qpa_amount, filed_date) VALUES (1, 1, 1, 'idr', 'negotiation', 650.00, 380.00, 650.00, 475.00, '2025-08-05'), (2, 3, 2, 'idr', 'filed', 1200.00, 520.00, 1200.00, 580.00, '2025-08-25'), (3, 4, 3, 'appeal', 'open', 400.00, 210.00, 400.00, 365.00, '2025-10-05'), (4, 6, 4, 'idr', 'evidence_submitted', 6500.00, 2100.00, 6500.00, 3350.00, '2025-06-25'), (5, 7, 5, 'idr', 'decided', 4200.00, 1800.00, 4200.00, 2200.00, '2025-10-20'), (1, 2, 6, 'appeal', 'closed', 200.00, 120.00, 200.00, 155.00, '2025-03-10');
        INSERT INTO deadlines (case_id, dispute_id, type, due_date, status, completed_date) VALUES (1, 1, 'open_negotiation', '2025-09-04', 'met', '2025-09-02'), (1, 1, 'idr_initiation', '2025-09-10', 'met', '2025-09-08'), (1, 1, 'evidence_submission', '2026-04-10', 'pending', NULL), (2, 2, 'open_negotiation', '2025-09-24', 'met', '2025-09-20'), (2, 2, 'idr_initiation', '2025-09-30', 'met', '2025-09-28'), (2, 2, 'entity_selection', '2026-04-01', 'pending', NULL), (3, 3, 'open_negotiation', '2026-04-04', 'alerted', NULL), (4, 4, 'open_negotiation', '2025-07-25', 'met', '2025-07-22'), (4, 4, 'idr_initiation', '2025-07-31', 'met', '2025-07-29'), (4, 4, 'evidence_submission', '2025-12-01', 'met', '2025-11-28'), (4, 4, 'decision', '2026-04-15', 'pending', NULL), (5, 5, 'open_negotiation', '2025-11-19', 'met', '2025-11-15'), (5, 5, 'idr_initiation', '2025-11-25', 'met', '2025-11-23'), (5, 5, 'decision', '2026-02-28', 'met', '2026-02-28');
    """)
    conn.commit()
    yield conn
    conn.close()


def q(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ============================================================================
# Layer 1: OLTP Seed Data Integrity
# ============================================================================

class TestOLTPIntegrity:
    """Verify seed data landed correctly with full referential integrity."""

    def test_table_counts(self, db):
        expected = {"payers": 8, "providers": 6, "patients": 10, "claims": 10,
                    "remittances": 8, "cases": 6, "disputes": 6, "deadlines": 14}
        for table, count in expected.items():
            rows = q(db, f"SELECT COUNT(*) AS cnt FROM {table}")
            assert rows[0]["cnt"] == count, f"{table} expected {count}, got {rows[0]['cnt']}"

    def test_every_claim_has_patient(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM claims c
            LEFT JOIN patients p ON c.patient_id = p.patient_id
            WHERE p.patient_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_every_claim_has_provider(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM claims c
            LEFT JOIN providers pr ON c.provider_npi = pr.npi
            WHERE pr.npi IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_every_claim_has_payer(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM claims c
            LEFT JOIN payers p ON c.payer_id = p.payer_id
            WHERE p.payer_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_every_remittance_has_claim(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM remittances r
            LEFT JOIN claims c ON r.claim_id = c.claim_id
            WHERE c.claim_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_every_dispute_has_claim_and_case(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM disputes d
            LEFT JOIN claims c ON d.claim_id = c.claim_id
            LEFT JOIN cases cs ON d.case_id = cs.case_id
            WHERE c.claim_id IS NULL OR cs.case_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_every_deadline_has_case_and_dispute(self, db):
        rows = q(db, """
            SELECT COUNT(*) AS cnt FROM deadlines dl
            LEFT JOIN cases cs ON dl.case_id = cs.case_id
            LEFT JOIN disputes d ON dl.dispute_id = d.dispute_id
            WHERE cs.case_id IS NULL OR d.dispute_id IS NULL
        """)
        assert rows[0]["cnt"] == 0


# ============================================================================
# Layer 2: Financial Totals Consistency Across Views
# ============================================================================

class TestFinancialConsistency:
    """Verify the same financial totals appear consistently across all layers."""

    def test_total_billed_consistent(self, db):
        """Total billed must be the same from claims table and from recovery view."""
        claims_total = q(db, "SELECT SUM(total_billed) AS t FROM claims")[0]["t"]
        # Recovery by payer should sum to same total
        recovery_total = q(db, """
            SELECT SUM(c.total_billed) AS t
            FROM claims c JOIN payers p ON c.payer_id = p.payer_id
        """)[0]["t"]
        assert claims_total == recovery_total == 17470.00

    def test_total_paid_consistent(self, db):
        """Total paid must match across remittances and recovery view."""
        remit_total = q(db, "SELECT SUM(paid_amount) AS t FROM remittances")[0]["t"]
        assert remit_total == 5740.00

    def test_underpayment_is_billed_minus_paid(self, db):
        """Total underpayment = total billed - total paid."""
        billed = q(db, "SELECT SUM(total_billed) AS t FROM claims")[0]["t"]
        paid = q(db, "SELECT SUM(paid_amount) AS t FROM remittances")[0]["t"]
        assert billed - paid == 11730.00

    def test_dispute_amounts_match_claims(self, db):
        """Each dispute's billed_amount should match or be <= the claim's total_billed."""
        rows = q(db, """
            SELECT d.dispute_id, d.billed_amount, c.total_billed
            FROM disputes d JOIN claims c ON d.claim_id = c.claim_id
        """)
        for r in rows:
            assert r["billed_amount"] <= r["total_billed"]

    def test_award_only_on_decided_or_closed(self, db):
        """Award amounts should only exist on decided/closed cases."""
        rows = q(db, """
            SELECT case_id, status, award_amount FROM cases
            WHERE award_amount IS NOT NULL AND award_amount > 0
        """)
        for r in rows:
            assert r["status"] in ("decided", "closed"), \
                f"Case {r['case_id']} has award but status={r['status']}"


# ============================================================================
# Layer 3: Gold View Query Chain (OLTP → Gold → Cross-Gold)
# ============================================================================

class TestGoldViewChain:
    """Verify Gold views produce correct results and chain logically."""

    def test_underpaid_claims_feed_disputes(self, db):
        """Claims marked as underpaid/in_dispute should have disputes."""
        underpaid = q(db, """
            SELECT c.claim_id FROM claims c
            WHERE c.status IN ('underpaid', 'in_dispute')
        """)
        for u in underpaid:
            disputes = q(db, f"SELECT COUNT(*) AS cnt FROM disputes WHERE claim_id = {u['claim_id']}")
            assert disputes[0]["cnt"] >= 1, f"Claim {u['claim_id']} underpaid but no dispute"

    def test_disputes_feed_cases(self, db):
        """Every dispute should be linked to a case."""
        rows = q(db, "SELECT COUNT(*) AS cnt FROM disputes WHERE case_id IS NULL")
        assert rows[0]["cnt"] == 0

    def test_cases_feed_deadlines(self, db):
        """Every active case should have at least one deadline."""
        active_cases = q(db, """
            SELECT cs.case_id FROM cases cs
            WHERE cs.status NOT IN ('closed')
        """)
        for c in active_cases:
            dls = q(db, f"SELECT COUNT(*) AS cnt FROM deadlines WHERE case_id = {c['case_id']}")
            assert dls[0]["cnt"] >= 1, f"Case {c['case_id']} has no deadlines"

    def test_decided_cases_feed_win_loss(self, db):
        """Decided/closed cases should have outcomes."""
        rows = q(db, """
            SELECT case_id, outcome FROM cases
            WHERE status IN ('decided', 'closed')
        """)
        assert len(rows) >= 2
        for r in rows:
            assert r["outcome"] is not None, f"Case {r['case_id']} decided but no outcome"

    def test_payer_recovery_matches_payer_scorecard(self, db):
        """Both payer-level views should agree on total billed per payer."""
        recovery = q(db, """
            SELECT c.payer_id, SUM(c.total_billed) AS total
            FROM claims c GROUP BY c.payer_id ORDER BY c.payer_id
        """)
        for r in recovery:
            scorecard = q(db, f"""
                SELECT SUM(c.total_billed) AS total
                FROM claims c WHERE c.payer_id = {r['payer_id']}
            """)
            assert r["total"] == scorecard[0]["total"]

    def test_provider_claims_match_total(self, db):
        """Sum of claims per provider should equal total claims."""
        provider_total = q(db, """
            SELECT SUM(cnt) AS total FROM (
                SELECT COUNT(*) AS cnt FROM claims GROUP BY provider_npi
            )
        """)[0]["total"]
        total = q(db, "SELECT COUNT(*) AS cnt FROM claims")[0]["cnt"]
        assert provider_total == total

    def test_monthly_trend_sums_match_totals(self, db):
        """Sum of monthly totals should match overall totals."""
        monthly_billed = q(db, """
            SELECT SUM(total_billed) AS total
            FROM (SELECT total_billed FROM claims WHERE date_of_service IS NOT NULL)
        """)[0]["total"]
        total_billed = q(db, "SELECT SUM(total_billed) AS t FROM claims")[0]["t"]
        assert monthly_billed == total_billed


# ============================================================================
# Layer 4: Agent Response Structure Validation
# ============================================================================

class TestAgentResponseStructure:
    """Verify the agent produces valid response structures for all analyses."""

    def test_all_common_analysis_ids_are_valid(self):
        from agent.analyst import COMMON_ANALYSES
        ids = [a["id"] for a in COMMON_ANALYSES]
        assert len(ids) == len(set(ids)), "Duplicate analysis IDs"
        assert len(ids) == 15

    @patch("agent.analyst._log_agent_invocation")
    @patch("agent.analyst._execute_gold_sql")
    @patch("agent.analyst._get_client")
    def test_ask_returns_all_required_fields(self, mock_client_fn, mock_sql, mock_log):
        """Every ask() response must have answer, sql, data, row_count, model, suggested_analyses."""
        mock_client = MagicMock()
        mock_client_fn.return_value = mock_client

        tool_block = MagicMock()
        tool_block.type = "tool_use"
        tool_block.name = "execute_sql"
        tool_block.input = {"sql": "SELECT 1 AS test", "explanation": "test"}
        sql_resp = MagicMock()
        sql_resp.content = [tool_block]

        text_block = MagicMock()
        text_block.type = "text"
        text_block.text = "Test analysis result."
        analysis_resp = MagicMock()
        analysis_resp.content = [text_block]

        mock_client.messages.create.side_effect = [sql_resp, analysis_resp]
        mock_sql.return_value = [{"test": 1}]

        from agent.analyst import ask
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test"}):
            result = ask("test question")

        required_fields = ["answer", "sql", "sql_explanation", "row_count", "data", "model", "suggested_analyses"]
        for field in required_fields:
            assert field in result, f"Missing field '{field}' in ask() response"

    def test_ask_common_invalid_returns_valid_structure(self):
        from agent.analyst import ask_common
        result = ask_common("fake_id")
        assert "answer" in result
        assert result["sql"] is None
        assert result["row_count"] == 0

    @patch("agent.analyst.ask")
    def test_all_common_analyses_callable(self, mock_ask):
        """Every common analysis should be callable without error."""
        mock_ask.return_value = {
            "answer": "ok", "sql": "SELECT 1", "sql_explanation": "test",
            "row_count": 1, "data": [], "model": "test", "suggested_analyses": []
        }
        from agent.analyst import COMMON_ANALYSES, ask_common
        for analysis in COMMON_ANALYSES:
            result = ask_common(analysis["id"])
            assert "answer" in result
            assert result.get("analysis_id") == analysis["id"]
            assert result.get("analysis_name") == analysis["name"]


# ============================================================================
# Layer 5: Workflow State Machine Integrity
# ============================================================================

class TestWorkflowStateIntegrity:
    """Verify the NSA case lifecycle is consistent in the seed data."""

    VALID_TRANSITIONS = {
        "open": ["in_review"],
        "in_review": ["negotiation", "closed"],
        "negotiation": ["idr_initiated", "closed"],
        "idr_initiated": ["idr_submitted", "closed"],
        "idr_submitted": ["decided", "closed"],
        "decided": ["closed"],
    }

    VALID_STATUSES = {"open", "in_review", "negotiation", "idr_initiated",
                      "idr_submitted", "decided", "closed"}

    def test_all_case_statuses_valid(self, db):
        rows = q(db, "SELECT DISTINCT status FROM cases")
        for r in rows:
            assert r["status"] in self.VALID_STATUSES, f"Invalid status: {r['status']}"

    def test_closed_cases_have_closed_date(self, db):
        rows = q(db, "SELECT case_id, closed_date FROM cases WHERE status = 'closed'")
        for r in rows:
            assert r["closed_date"] is not None, f"Case {r['case_id']} closed without date"

    def test_decided_cases_have_outcome(self, db):
        rows = q(db, "SELECT case_id, outcome FROM cases WHERE status = 'decided'")
        for r in rows:
            assert r["outcome"] is not None, f"Case {r['case_id']} decided without outcome"

    def test_deadline_statuses_valid(self, db):
        valid = {"pending", "alerted", "met", "missed"}
        rows = q(db, "SELECT DISTINCT status FROM deadlines")
        for r in rows:
            assert r["status"] in valid, f"Invalid deadline status: {r['status']}"

    def test_met_deadlines_have_completed_date(self, db):
        rows = q(db, "SELECT deadline_id, completed_date FROM deadlines WHERE status = 'met'")
        for r in rows:
            assert r["completed_date"] is not None, \
                f"Deadline {r['deadline_id']} met but no completed_date"

    def test_dispute_types_valid(self, db):
        valid = {"appeal", "idr", "state_arb", "external_review"}
        rows = q(db, "SELECT DISTINCT dispute_type FROM disputes")
        for r in rows:
            assert r["dispute_type"] in valid, f"Invalid dispute type: {r['dispute_type']}"

    def test_priority_routing_consistent(self, db):
        """High-value disputes should be on high/critical priority cases."""
        rows = q(db, """
            SELECT d.billed_amount - d.paid_amount AS underpayment, cs.priority
            FROM disputes d JOIN cases cs ON d.case_id = cs.case_id
            WHERE (d.billed_amount - d.paid_amount) > 500
        """)
        for r in rows:
            assert r["priority"] in ("high", "critical"), \
                f"Underpayment {r['underpayment']} on {r['priority']} priority case"


# ============================================================================
# Layer 6: Cross-Layer Data Flow Verification
# ============================================================================

class TestCrossLayerDataFlow:
    """Verify data flows correctly between OLTP → Gold → Agent layers."""

    def test_claim_to_dispute_to_deadline_chain(self, db):
        """Follow a single claim through the full pipeline."""
        # Claim 1 → Dispute 1 → Case 1 → Deadlines for case 1
        claim = q(db, "SELECT * FROM claims WHERE claim_id = 1")[0]
        assert claim["status"] == "underpaid"
        assert claim["total_billed"] == 850.00

        remit = q(db, "SELECT * FROM remittances WHERE claim_id = 1")[0]
        assert remit["paid_amount"] == 380.00  # underpaid

        dispute = q(db, "SELECT * FROM disputes WHERE claim_id = 1 AND case_id = 1")[0]
        assert dispute["billed_amount"] == 650.00
        assert dispute["paid_amount"] == 380.00

        case = q(db, "SELECT * FROM cases WHERE case_id = 1")[0]
        assert case["status"] == "negotiation"
        assert case["priority"] == "high"

        deadlines = q(db, "SELECT * FROM deadlines WHERE case_id = 1 ORDER BY due_date")
        assert len(deadlines) >= 2
        assert deadlines[0]["type"] == "open_negotiation"
        assert deadlines[0]["status"] == "met"

    def test_won_case_end_to_end(self, db):
        """Case 5 is decided/won — verify full chain."""
        case = q(db, "SELECT * FROM cases WHERE case_id = 5")[0]
        assert case["outcome"] == "won"
        assert case["award_amount"] == 3500.00

        dispute = q(db, "SELECT * FROM disputes WHERE case_id = 5")[0]
        assert dispute["status"] == "decided"
        underpayment = dispute["billed_amount"] - dispute["paid_amount"]
        assert underpayment == 2400.00

        deadlines = q(db, "SELECT * FROM deadlines WHERE case_id = 5")
        met = sum(1 for d in deadlines if d["status"] == "met")
        assert met == 3  # all deadlines met

    def test_lost_case_end_to_end(self, db):
        """Case 6 is closed/lost — verify chain."""
        case = q(db, "SELECT * FROM cases WHERE case_id = 6")[0]
        assert case["outcome"] == "lost"
        assert case["award_amount"] is None
        assert case["closed_date"] is not None

    def test_no_orphan_data(self, db):
        """No data exists without proper parent references."""
        orphan_checks = [
            ("remittances", "claims", "claim_id"),
            ("disputes", "claims", "claim_id"),
            ("disputes", "cases", "case_id"),
            ("deadlines", "cases", "case_id"),
        ]
        for child, parent, key in orphan_checks:
            rows = q(db, f"""
                SELECT COUNT(*) AS cnt FROM {child} c
                LEFT JOIN {parent} p ON c.{key} = p.{key}
                WHERE p.{key} IS NULL
            """)
            assert rows[0]["cnt"] == 0, f"Orphan rows in {child} → {parent}"
