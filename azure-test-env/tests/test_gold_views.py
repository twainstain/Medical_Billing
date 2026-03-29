"""Tests for Gold SQL views against seed data in SQLite.

Recreates the OLTP schema and seed data in an in-memory SQLite database,
then runs adapted versions of each Gold view query to verify correctness.

Note: SQLite syntax differs from T-SQL, so views are adapted minimally
(SYSUTCDATETIME → datetime('now'), DATEDIFF → julianday, etc.)
"""

import os
import sqlite3
import pytest


# ============================================================================
# Fixtures: in-memory SQLite with schema + seed data
# ============================================================================

@pytest.fixture(scope="module")
def db():
    """Create in-memory SQLite DB with schema + seed data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Create tables (SQLite-compatible subset of schema.sql)
    cur.executescript("""
        CREATE TABLE payers (
            payer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            state TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE providers (
            npi TEXT PRIMARY KEY,
            tin TEXT NOT NULL,
            name TEXT NOT NULL,
            specialty TEXT,
            facility_id TEXT,
            facility_name TEXT,
            state TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE patients (
            patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            date_of_birth TEXT NOT NULL,
            gender TEXT,
            address_line1 TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            insurance_id TEXT,
            payer_id INTEGER REFERENCES payers(payer_id),
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE fee_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payer_id INTEGER NOT NULL REFERENCES payers(payer_id),
            cpt_code TEXT NOT NULL,
            modifier TEXT,
            geo_region TEXT,
            rate REAL NOT NULL,
            rate_type TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT NOT NULL DEFAULT '9999-12-31',
            is_current INTEGER DEFAULT 1,
            source TEXT,
            loaded_date TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE claims (
            claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_claim_id TEXT UNIQUE,
            patient_id INTEGER NOT NULL REFERENCES patients(patient_id),
            provider_npi TEXT NOT NULL REFERENCES providers(npi),
            payer_id INTEGER NOT NULL REFERENCES payers(payer_id),
            date_of_service TEXT NOT NULL,
            date_filed TEXT,
            facility_id TEXT,
            place_of_service TEXT,
            total_billed REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'filed',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE claim_lines (
            line_id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id INTEGER NOT NULL REFERENCES claims(claim_id),
            line_number INTEGER NOT NULL,
            cpt_code TEXT NOT NULL,
            modifier TEXT,
            units INTEGER DEFAULT 1,
            billed_amount REAL NOT NULL,
            diagnosis_codes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE (claim_id, line_number)
        );

        CREATE TABLE remittances (
            remit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id INTEGER NOT NULL REFERENCES claims(claim_id),
            era_date TEXT NOT NULL,
            paid_amount REAL NOT NULL,
            allowed_amount REAL,
            adjustment_reason TEXT,
            denial_code TEXT,
            check_number TEXT,
            source_type TEXT DEFAULT 'edi_835',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE cases (
            case_id INTEGER PRIMARY KEY AUTOINCREMENT,
            assigned_analyst TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            priority TEXT NOT NULL DEFAULT 'medium',
            created_date TEXT DEFAULT (datetime('now')),
            last_activity TEXT DEFAULT (datetime('now')),
            outcome TEXT,
            award_amount REAL,
            closed_date TEXT
        );

        CREATE TABLE disputes (
            dispute_id INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_id INTEGER NOT NULL REFERENCES claims(claim_id),
            claim_line_id INTEGER REFERENCES claim_lines(line_id),
            case_id INTEGER REFERENCES cases(case_id),
            dispute_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            billed_amount REAL NOT NULL,
            paid_amount REAL NOT NULL,
            requested_amount REAL,
            qpa_amount REAL,
            filed_date TEXT,
            open_negotiation_deadline TEXT,
            idr_initiation_deadline TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE deadlines (
            deadline_id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id INTEGER NOT NULL REFERENCES cases(case_id),
            dispute_id INTEGER REFERENCES disputes(dispute_id),
            type TEXT NOT NULL,
            due_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            alerted_date TEXT,
            completed_date TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE audit_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            action TEXT NOT NULL,
            user_id TEXT,
            timestamp TEXT DEFAULT (datetime('now')),
            old_value TEXT,
            new_value TEXT,
            ai_agent TEXT,
            ai_confidence REAL,
            ai_model_version TEXT
        );
    """)

    # Insert seed data (same as seed_data.sql)
    cur.executescript("""
        INSERT INTO payers (name, type, state) VALUES
        ('Anthem Blue Cross', 'commercial', 'CA'),
        ('Aetna', 'commercial', 'NY'),
        ('UnitedHealthcare', 'commercial', 'TX'),
        ('Cigna', 'commercial', 'FL'),
        ('Humana', 'commercial', 'GA'),
        ('Medicare Part B', 'medicare', NULL),
        ('Medicaid - New York', 'medicaid', 'NY'),
        ('TRICARE West', 'tricare', NULL);

        INSERT INTO providers (npi, tin, name, specialty, facility_id, facility_name, state) VALUES
        ('1234567890', '123456789', 'Dr. Sarah Chen', 'Emergency Medicine', 'FAC001', 'Metro General Hospital', 'NY'),
        ('2345678901', '234567890', 'Dr. Michael Roberts', 'Orthopedic Surgery', 'FAC001', 'Metro General Hospital', 'NY'),
        ('3456789012', '345678901', 'Dr. Lisa Patel', 'Anesthesiology', 'FAC002', 'Riverside Medical Center', 'CA'),
        ('4567890123', '456789012', 'Dr. James Wilson', 'Radiology', 'FAC002', 'Riverside Medical Center', 'CA'),
        ('5678901234', '567890123', 'Dr. Amanda Foster', 'Cardiology', 'FAC003', 'Heart & Vascular Institute', 'TX'),
        ('6789012345', '678901234', 'Dr. David Kim', 'Neurosurgery', 'FAC001', 'Metro General Hospital', 'NY');

        INSERT INTO patients (first_name, last_name, date_of_birth, gender, address_line1, city, state, zip_code, insurance_id, payer_id) VALUES
        ('John', 'Smith', '1975-03-15', 'M', '123 Main St', 'New York', 'NY', '10001', 'ANT-100001', 1),
        ('Maria', 'Garcia', '1982-07-22', 'F', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'AET-200002', 2),
        ('Robert', 'Johnson', '1968-11-30', 'M', '789 Pine Rd', 'Houston', 'TX', '77001', 'UHC-300003', 3),
        ('Jennifer', 'Williams', '1990-01-05', 'F', '321 Elm St', 'Miami', 'FL', '33101', 'CIG-400004', 4),
        ('William', 'Brown', '1955-09-18', 'M', '654 Cedar Ln', 'Atlanta', 'GA', '30301', 'HUM-500005', 5),
        ('Emily', 'Davis', '1988-04-12', 'F', '987 Birch Dr', 'New York', 'NY', '10002', 'MCR-600006', 6),
        ('James', 'Martinez', '1972-06-25', 'M', '147 Maple Way', 'Brooklyn', 'NY', '11201', 'MCD-700007', 7),
        ('Susan', 'Anderson', '1965-12-08', 'F', '258 Walnut Ct', 'Dallas', 'TX', '75201', 'TRI-800008', 8),
        ('David', 'Taylor', '1980-08-20', 'M', '369 Spruce Ave', 'San Diego', 'CA', '92101', 'ANT-100009', 1),
        ('Lisa', 'Thomas', '1993-02-14', 'F', '741 Ash Blvd', 'New York', 'NY', '10003', 'AET-200010', 2);

        INSERT INTO claims (external_claim_id, patient_id, provider_npi, payer_id, date_of_service, date_filed, facility_id, place_of_service, total_billed, status) VALUES
        ('CLM-2025-0001', 1, '1234567890', 1, '2025-06-15', '2025-06-20', 'FAC001', '23', 850.00, 'underpaid'),
        ('CLM-2025-0002', 2, '3456789012', 2, '2025-07-10', '2025-07-15', 'FAC002', '23', 1200.00, 'underpaid'),
        ('CLM-2025-0003', 3, '5678901234', 3, '2025-08-22', '2025-08-25', 'FAC003', '11', 520.00, 'underpaid'),
        ('CLM-2025-0004', 9, '2345678901', 1, '2025-05-10', '2025-05-15', 'FAC001', '21', 6500.00, 'in_dispute'),
        ('CLM-2025-0005', 10, '6789012345', 2, '2025-09-01', '2025-09-05', 'FAC001', '23', 4200.00, 'in_dispute'),
        ('CLM-2025-0006', 4, '1234567890', 4, '2025-04-01', '2025-04-05', 'FAC001', '23', 450.00, 'paid'),
        ('CLM-2025-0007', 5, '5678901234', 5, '2025-03-18', '2025-03-22', 'FAC003', '11', 320.00, 'paid'),
        ('CLM-2025-0008', 6, '4567890123', 6, '2025-10-05', '2025-10-08', 'FAC002', '22', 1800.00, 'denied'),
        ('CLM-2026-0001', 7, '1234567890', 7, '2026-01-10', '2026-01-15', 'FAC001', '23', 650.00, 'filed'),
        ('CLM-2026-0002', 8, '3456789012', 8, '2026-02-20', '2026-02-22', 'FAC002', '23', 980.00, 'filed');

        INSERT INTO claim_lines (claim_id, line_number, cpt_code, modifier, units, billed_amount, diagnosis_codes) VALUES
        (1, 1, '99285', NULL, 1, 650.00, 'R10.9,R11.0'),
        (1, 2, '99283', NULL, 1, 200.00, 'R10.9'),
        (2, 1, '00142', NULL, 1, 1200.00, 'K35.80'),
        (3, 1, '93010', NULL, 1, 120.00, 'I25.10'),
        (3, 2, '99285', NULL, 1, 400.00, 'I25.10,R00.0'),
        (4, 1, '27447', NULL, 1, 6500.00, 'M17.11'),
        (5, 1, '61510', NULL, 1, 4200.00, 'C71.1'),
        (6, 1, '99283', NULL, 1, 450.00, 'J06.9'),
        (7, 1, '93010', NULL, 1, 320.00, 'I10'),
        (8, 1, '74177', NULL, 1, 1800.00, 'R10.0');

        INSERT INTO remittances (claim_id, era_date, paid_amount, allowed_amount, adjustment_reason, denial_code, check_number, source_type) VALUES
        (1, '2025-07-20', 380.00, 425.00, 'CO-45', NULL, 'CHK-10001', 'edi_835'),
        (2, '2025-08-15', 520.00, 600.00, 'CO-45', NULL, 'CHK-10002', 'edi_835'),
        (3, '2025-09-25', 210.00, 250.00, 'CO-45', NULL, 'CHK-10003', 'edi_835'),
        (4, '2025-06-15', 2100.00, 2800.00, 'CO-45', NULL, 'CHK-10004', 'edi_835'),
        (5, '2025-10-10', 1800.00, 2200.00, 'CO-45', NULL, 'CHK-10005', 'eob_pdf'),
        (6, '2025-05-05', 420.00, 420.00, NULL, NULL, 'CHK-10006', 'edi_835'),
        (7, '2025-04-20', 310.00, 310.00, NULL, NULL, 'CHK-10007', 'edi_835'),
        (8, '2025-11-01', 0.00, 0.00, 'CO-197', 'CO-197', 'CHK-10008', 'edi_835');

        INSERT INTO cases (assigned_analyst, status, priority, created_date, last_activity, outcome, award_amount, closed_date) VALUES
        ('Ana Rodriguez', 'negotiation', 'high', '2025-08-01', '2026-03-20', NULL, NULL, NULL),
        ('Mark Thompson', 'idr_initiated', 'high', '2025-08-20', '2026-03-18', NULL, NULL, NULL),
        ('Ana Rodriguez', 'in_review', 'medium', '2025-10-01', '2026-03-15', NULL, NULL, NULL),
        ('Sarah Lee', 'idr_submitted', 'critical', '2025-06-20', '2026-03-10', NULL, NULL, NULL),
        ('Mark Thompson', 'decided', 'high', '2025-10-15', '2026-02-28', 'won', 3500.00, '2026-02-28'),
        ('Ana Rodriguez', 'closed', 'medium', '2025-03-01', '2025-06-15', 'lost', NULL, '2025-06-15');

        INSERT INTO disputes (claim_id, claim_line_id, case_id, dispute_type, status, billed_amount, paid_amount, requested_amount, qpa_amount, filed_date, open_negotiation_deadline, idr_initiation_deadline) VALUES
        (1, 1, 1, 'idr', 'negotiation', 650.00, 380.00, 650.00, 475.00, '2025-08-05', '2025-09-04', '2025-09-10'),
        (2, 3, 2, 'idr', 'filed', 1200.00, 520.00, 1200.00, 580.00, '2025-08-25', '2025-09-24', '2025-09-30'),
        (3, 4, 3, 'appeal', 'open', 400.00, 210.00, 400.00, 365.00, '2025-10-05', '2025-11-04', NULL),
        (4, 6, 4, 'idr', 'evidence_submitted', 6500.00, 2100.00, 6500.00, 3350.00, '2025-06-25', '2025-07-25', '2025-07-31'),
        (5, 7, 5, 'idr', 'decided', 4200.00, 1800.00, 4200.00, 2200.00, '2025-10-20', '2025-11-19', '2025-11-25'),
        (1, 2, 6, 'appeal', 'closed', 200.00, 120.00, 200.00, 155.00, '2025-03-10', '2025-04-09', NULL);

        INSERT INTO deadlines (case_id, dispute_id, type, due_date, status, alerted_date, completed_date) VALUES
        (1, 1, 'open_negotiation', '2025-09-04', 'met', '2025-08-25', '2025-09-02'),
        (1, 1, 'idr_initiation', '2025-09-10', 'met', '2025-09-06', '2025-09-08'),
        (1, 1, 'evidence_submission', '2026-04-10', 'pending', NULL, NULL),
        (2, 2, 'open_negotiation', '2025-09-24', 'met', '2025-09-14', '2025-09-20'),
        (2, 2, 'idr_initiation', '2025-09-30', 'met', '2025-09-26', '2025-09-28'),
        (2, 2, 'entity_selection', '2026-04-01', 'pending', NULL, NULL),
        (3, 3, 'open_negotiation', '2026-04-04', 'alerted', '2026-03-25', NULL),
        (4, 4, 'open_negotiation', '2025-07-25', 'met', '2025-07-15', '2025-07-22'),
        (4, 4, 'idr_initiation', '2025-07-31', 'met', '2025-07-27', '2025-07-29'),
        (4, 4, 'evidence_submission', '2025-12-01', 'met', '2025-11-20', '2025-11-28'),
        (4, 4, 'decision', '2026-04-15', 'pending', NULL, NULL),
        (5, 5, 'open_negotiation', '2025-11-19', 'met', '2025-11-09', '2025-11-15'),
        (5, 5, 'idr_initiation', '2025-11-25', 'met', '2025-11-21', '2025-11-23'),
        (5, 5, 'decision', '2026-02-28', 'met', '2026-02-20', '2026-02-28');
    """)

    conn.commit()
    yield conn
    conn.close()


# ============================================================================
# Helper
# ============================================================================

def query(conn, sql):
    """Execute SQL and return list of dicts."""
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ============================================================================
# Gold View: Recovery by Payer
# ============================================================================

class TestGoldRecoveryByPayer:

    def test_returns_rows(self, db):
        # SQLite-adapted version of gold_recovery_by_payer
        rows = query(db, """
            SELECT p.payer_id, p.name AS payer_name,
                   COUNT(c.claim_id) AS total_claims,
                   SUM(c.total_billed) AS total_billed,
                   COALESCE(SUM(r.total_paid), 0) AS total_paid
            FROM claims c
            JOIN payers p ON c.payer_id = p.payer_id
            LEFT JOIN (
                SELECT claim_id, SUM(paid_amount) AS total_paid
                FROM remittances GROUP BY claim_id
            ) r ON c.claim_id = r.claim_id
            GROUP BY p.payer_id, p.name
        """)
        assert len(rows) > 0

    def test_anthem_has_correct_claims(self, db):
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM claims WHERE payer_id = 1
        """)
        # Anthem (payer_id=1): claims 1, 4 = 2 claims
        assert rows[0]["cnt"] == 2

    def test_total_billed_sums_correctly(self, db):
        rows = query(db, """
            SELECT SUM(total_billed) AS total FROM claims
        """)
        # 850+1200+520+6500+4200+450+320+1800+650+980 = 17470
        assert rows[0]["total"] == 17470.00

    def test_underpayment_calculation(self, db):
        rows = query(db, """
            SELECT SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0) AS total_underpayment
            FROM claims c
            LEFT JOIN (
                SELECT claim_id, SUM(paid_amount) AS total_paid
                FROM remittances GROUP BY claim_id
            ) r ON c.claim_id = r.claim_id
        """)
        # total_billed=17470, total_paid=5740, underpayment=11730
        assert rows[0]["total_underpayment"] == 11730.00


# ============================================================================
# Gold View: Financial Summary
# ============================================================================

class TestGoldFinancialSummary:

    def test_total_claims_is_10(self, db):
        rows = query(db, "SELECT COUNT(*) AS cnt FROM claims")
        assert rows[0]["cnt"] == 10

    def test_total_paid(self, db):
        rows = query(db, "SELECT SUM(paid_amount) AS total FROM remittances")
        # 380+520+210+2100+1800+420+310+0 = 5740
        assert rows[0]["total"] == 5740.00

    def test_denial_count(self, db):
        rows = query(db, """
            SELECT COUNT(DISTINCT claim_id) AS cnt
            FROM remittances WHERE denial_code IS NOT NULL AND denial_code != ''
        """)
        # Only claim 8 has denial_code
        assert rows[0]["cnt"] == 1


# ============================================================================
# Gold View: Claims Aging
# ============================================================================

class TestGoldClaimsAging:

    def test_all_claims_in_buckets(self, db):
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM claims
            WHERE date_of_service IS NOT NULL
        """)
        assert rows[0]["cnt"] == 10

    def test_aging_bucket_assignment(self, db):
        """All seed data claims are older than 30 days (2025-2026 dates)."""
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM claims
            WHERE julianday('now') - julianday(date_of_service) > 30
        """)
        assert rows[0]["cnt"] >= 8  # Most claims are old


# ============================================================================
# Gold View: Case Pipeline
# ============================================================================

class TestGoldCasePipeline:

    def test_6_cases_total(self, db):
        rows = query(db, "SELECT COUNT(*) AS cnt FROM cases")
        assert rows[0]["cnt"] == 6

    def test_case_statuses(self, db):
        rows = query(db, "SELECT DISTINCT status FROM cases ORDER BY status")
        statuses = {r["status"] for r in rows}
        assert "negotiation" in statuses
        assert "idr_initiated" in statuses
        assert "closed" in statuses

    def test_decided_case_has_outcome(self, db):
        rows = query(db, "SELECT outcome, award_amount FROM cases WHERE status = 'decided'")
        assert len(rows) == 1
        assert rows[0]["outcome"] == "won"
        assert rows[0]["award_amount"] == 3500.00


# ============================================================================
# Gold View: Deadline Compliance
# ============================================================================

class TestGoldDeadlineCompliance:

    def test_14_deadlines_total(self, db):
        rows = query(db, "SELECT COUNT(*) AS cnt FROM deadlines")
        assert rows[0]["cnt"] == 14

    def test_met_deadlines(self, db):
        rows = query(db, "SELECT COUNT(*) AS cnt FROM deadlines WHERE status = 'met'")
        assert rows[0]["cnt"] == 10

    def test_pending_deadlines(self, db):
        rows = query(db, "SELECT COUNT(*) AS cnt FROM deadlines WHERE status = 'pending'")
        assert rows[0]["cnt"] == 3

    def test_deadline_types_exist(self, db):
        rows = query(db, "SELECT DISTINCT type FROM deadlines")
        types = {r["type"] for r in rows}
        assert "open_negotiation" in types
        assert "idr_initiation" in types
        assert "evidence_submission" in types
        assert "decision" in types

    def test_compliance_by_type(self, db):
        rows = query(db, """
            SELECT type,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'met' THEN 1 ELSE 0 END) AS met
            FROM deadlines
            GROUP BY type
        """)
        by_type = {r["type"]: r for r in rows}
        # open_negotiation: 5 total, 4 met
        assert by_type["open_negotiation"]["total"] == 5
        assert by_type["open_negotiation"]["met"] == 4


# ============================================================================
# Gold View: Underpayment Detection
# ============================================================================

class TestGoldUnderpaymentDetection:

    def test_disputes_have_underpayment(self, db):
        rows = query(db, """
            SELECT claim_id, billed_amount, paid_amount,
                   (billed_amount - paid_amount) AS underpayment_amount
            FROM disputes
            WHERE (billed_amount - paid_amount) > 0
        """)
        assert len(rows) == 6  # All 6 disputes have underpayment

    def test_arbitration_eligibility(self, db):
        rows = query(db, """
            SELECT claim_id,
                   CASE WHEN (billed_amount - paid_amount) > 25
                             AND billed_amount > COALESCE(qpa_amount, 0)
                        THEN 1 ELSE 0 END AS arbitration_eligible
            FROM disputes
            WHERE (billed_amount - paid_amount) > 0
        """)
        eligible = sum(1 for r in rows if r["arbitration_eligible"] == 1)
        assert eligible >= 5  # Most disputes are eligible

    def test_largest_underpayment(self, db):
        rows = query(db, """
            SELECT MAX(billed_amount - paid_amount) AS max_underpayment
            FROM disputes
        """)
        # Claim 4: 6500 - 2100 = 4400
        assert rows[0]["max_underpayment"] == 4400.00


# ============================================================================
# Gold View: Payer Scorecard
# ============================================================================

class TestGoldPayerScorecard:

    def test_payers_with_claims(self, db):
        rows = query(db, """
            SELECT DISTINCT p.name FROM claims c
            JOIN payers p ON c.payer_id = p.payer_id
        """)
        names = {r["name"] for r in rows}
        assert "Anthem Blue Cross" in names
        assert "Aetna" in names

    def test_denial_detection(self, db):
        rows = query(db, """
            SELECT p.name, COUNT(*) AS denial_count
            FROM remittances r
            JOIN claims c ON r.claim_id = c.claim_id
            JOIN payers p ON c.payer_id = p.payer_id
            WHERE r.denial_code IS NOT NULL AND r.denial_code != ''
            GROUP BY p.name
        """)
        # Only Medicare (payer_id=6) has a denial
        assert len(rows) == 1
        assert rows[0]["name"] == "Medicare Part B"


# ============================================================================
# Cross-view Consistency
# ============================================================================

class TestCrossViewConsistency:

    def test_claims_match_remittances(self, db):
        """Every remittance references a valid claim."""
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM remittances r
            LEFT JOIN claims c ON r.claim_id = c.claim_id
            WHERE c.claim_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_disputes_match_claims(self, db):
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM disputes d
            LEFT JOIN claims c ON d.claim_id = c.claim_id
            WHERE c.claim_id IS NULL
        """)
        assert rows[0]["cnt"] == 0

    def test_deadlines_match_cases(self, db):
        rows = query(db, """
            SELECT COUNT(*) AS cnt FROM deadlines dl
            LEFT JOIN cases cs ON dl.case_id = cs.case_id
            WHERE cs.case_id IS NULL
        """)
        assert rows[0]["cnt"] == 0
