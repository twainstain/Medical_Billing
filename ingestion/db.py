"""
OLTP Database — SQLite mock for Azure SQL.

Creates all operational tables matching the design doc schema:
claims, claim_lines, remittances, patients, providers, payers,
fee_schedule, disputes, cases, evidence_artifacts, deadlines, audit_log,
plus reference tables (claim_id_alias, code_crosswalk, carc_rarc_lookup).
"""

import sqlite3
import os

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "oltp.db")

SCHEMA_SQL = """
-- Patients (SCD Type 1 — overwrite)
CREATE TABLE IF NOT EXISTS patients (
    patient_id      TEXT PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    dob             TEXT,
    gender          TEXT,
    address_line    TEXT,
    city            TEXT,
    state           TEXT,
    zip             TEXT,
    insurance_id    TEXT,
    source_system   TEXT,
    last_updated    TEXT NOT NULL
);

-- Payers
CREATE TABLE IF NOT EXISTS payers (
    payer_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    type        TEXT,
    state       TEXT
);

-- Providers (SCD Type 2)
CREATE TABLE IF NOT EXISTS providers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    npi                 TEXT NOT NULL,
    name_first          TEXT,
    name_last           TEXT,
    specialty_taxonomy  TEXT,
    state               TEXT,
    city                TEXT,
    zip                 TEXT,
    status              TEXT NOT NULL DEFAULT 'active',
    deactivation_date   TEXT,
    valid_from          TEXT NOT NULL,
    valid_to            TEXT NOT NULL DEFAULT '9999-12-31',
    is_current          INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_providers_npi ON providers (npi, is_current);

-- Claims (append-only with status)
CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    patient_id      TEXT,
    provider_npi    TEXT,
    payer_id        TEXT,
    date_of_service TEXT,
    date_filed      TEXT NOT NULL,
    facility_type   TEXT,
    total_billed    REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'received',
    source          TEXT NOT NULL DEFAULT 'edi_837',
    -- Claims may land before patient reference data, so patient/provider IDs
    -- are treated as soft references in this SQLite OLTP mock.
    FOREIGN KEY (payer_id) REFERENCES payers(payer_id)
);

-- Claim Lines
CREATE TABLE IF NOT EXISTS claim_lines (
    line_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        TEXT NOT NULL,
    cpt_code        TEXT NOT NULL,
    modifier        TEXT,
    units           INTEGER NOT NULL DEFAULT 1,
    billed_amount   REAL NOT NULL,
    diagnosis_codes TEXT,  -- JSON array
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);

-- Remittances (append-only)
CREATE TABLE IF NOT EXISTS remittances (
    remit_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id            TEXT NOT NULL,
    trace_number        TEXT,
    payer_id            TEXT,
    era_date            TEXT NOT NULL,
    paid_amount         REAL NOT NULL,
    allowed_amount      REAL,
    adjustment_reason   TEXT,  -- JSON
    denial_code         TEXT,
    check_number        TEXT,
    source              TEXT NOT NULL DEFAULT 'edi_835',
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_remit_trace_payer
    ON remittances (claim_id, trace_number, payer_id)
    WHERE trace_number IS NOT NULL;

-- Fee Schedule (SCD Type 2)
CREATE TABLE IF NOT EXISTS fee_schedule (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    payer_id    TEXT NOT NULL,
    cpt_code    TEXT NOT NULL,
    modifier    TEXT NOT NULL DEFAULT '',
    geo_region  TEXT NOT NULL,
    rate        REAL NOT NULL,
    rate_type   TEXT NOT NULL,
    valid_from  TEXT NOT NULL,
    valid_to    TEXT NOT NULL DEFAULT '9999-12-31',
    is_current  INTEGER NOT NULL DEFAULT 1,
    source      TEXT,
    loaded_date TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_fee_schedule_natural
    ON fee_schedule (payer_id, cpt_code, modifier, geo_region, rate_type, valid_from);
CREATE INDEX IF NOT EXISTS ix_fee_schedule_lookup
    ON fee_schedule (payer_id, cpt_code, modifier, geo_region, is_current);

-- Disputes
CREATE TABLE IF NOT EXISTS disputes (
    dispute_id      TEXT PRIMARY KEY,
    claim_id        TEXT NOT NULL,
    claim_line_id   INTEGER,
    case_id         TEXT,
    dispute_type    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    billed_amount   REAL,
    paid_amount     REAL,
    requested_amount REAL,
    qpa_amount      REAL,
    underpayment_amount REAL,
    filed_date      TEXT,
    open_negotiation_deadline TEXT,
    idr_initiation_deadline   TEXT,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
);

-- Cases
CREATE TABLE IF NOT EXISTS cases (
    case_id             TEXT PRIMARY KEY,
    assigned_analyst    TEXT,
    status              TEXT NOT NULL DEFAULT 'open',
    priority            TEXT,
    created_date        TEXT NOT NULL,
    last_activity_date  TEXT,
    outcome             TEXT,
    award_amount        REAL
);

-- Evidence Artifacts
CREATE TABLE IF NOT EXISTS evidence_artifacts (
    artifact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id             TEXT,
    type                TEXT NOT NULL,
    blob_url            TEXT,
    extracted_data      TEXT,  -- JSON
    classification_conf REAL,
    ocr_status          TEXT,
    content_hash        TEXT,
    uploaded_date       TEXT NOT NULL,
    uploaded_by         TEXT,
    FOREIGN KEY (case_id) REFERENCES cases(case_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uix_evidence_hash
    ON evidence_artifacts (content_hash)
    WHERE content_hash IS NOT NULL;

-- Deadlines
CREATE TABLE IF NOT EXISTS deadlines (
    deadline_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id         TEXT,
    dispute_id      TEXT,
    type            TEXT NOT NULL,
    due_date        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    alerted_date    TEXT,
    completed_date  TEXT,
    FOREIGN KEY (case_id) REFERENCES cases(case_id),
    FOREIGN KEY (dispute_id) REFERENCES disputes(dispute_id)
);

-- Audit Log
CREATE TABLE IF NOT EXISTS audit_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,
    entity_id       TEXT NOT NULL,
    action          TEXT NOT NULL,
    user_id         TEXT,
    timestamp       TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    ai_agent        TEXT,
    ai_confidence   REAL,
    ai_model_version TEXT
);

-- Reference: Claim ID Alias (for payer claim ID → canonical mapping)
CREATE TABLE IF NOT EXISTS claim_id_alias (
    payer_claim_id      TEXT NOT NULL,
    canonical_claim_id  TEXT NOT NULL,
    payer_id            TEXT,
    PRIMARY KEY (payer_claim_id, payer_id)
);

-- Reference: Code Crosswalk (legacy → standard code mapping)
CREATE TABLE IF NOT EXISTS code_crosswalk (
    legacy_code     TEXT NOT NULL,
    standard_code   TEXT NOT NULL,
    source_system   TEXT,
    PRIMARY KEY (legacy_code, source_system)
);

-- Reference: CARC/RARC Lookup
CREATE TABLE IF NOT EXISTS carc_rarc_lookup (
    code        TEXT PRIMARY KEY,
    code_type   TEXT NOT NULL,  -- 'CARC' or 'RARC'
    description TEXT NOT NULL,
    category    TEXT  -- 'denial', 'contractual', 'patient_resp', 'informational'
);

-- Staging: Fee Schedule
CREATE TABLE IF NOT EXISTS stg_fee_schedule (
    cpt_code        TEXT NOT NULL,
    modifier        TEXT,
    geo_region      TEXT NOT NULL,
    rate            REAL NOT NULL,
    rate_type       TEXT NOT NULL,
    effective_date  TEXT NOT NULL
);

-- Staging: Providers
CREATE TABLE IF NOT EXISTS stg_providers (
    npi                 TEXT NOT NULL,
    name_first          TEXT,
    name_last           TEXT,
    specialty_taxonomy  TEXT,
    state               TEXT,
    city                TEXT,
    zip                 TEXT,
    status              TEXT NOT NULL DEFAULT 'active',
    deactivation_date   TEXT
);
"""

# Seed reference data
SEED_SQL = """
-- Seed payers
INSERT OR IGNORE INTO payers (payer_id, name, type, state) VALUES
    ('AETNA', 'Aetna', 'commercial', 'CT'),
    ('UHC', 'UnitedHealthcare', 'commercial', 'MN'),
    ('CIGNA', 'Cigna', 'commercial', 'CT'),
    ('BCBS_CA', 'Blue Cross Blue Shield CA', 'commercial', 'CA'),
    ('MEDICARE', 'Medicare', 'government', 'US'),
    ('MEDICAID_CA', 'Medi-Cal', 'government', 'CA');

-- Seed CARC codes
INSERT OR IGNORE INTO carc_rarc_lookup (code, code_type, description, category) VALUES
    ('1', 'CARC', 'Deductible Amount', 'patient_resp'),
    ('2', 'CARC', 'Coinsurance Amount', 'patient_resp'),
    ('3', 'CARC', 'Co-payment Amount', 'patient_resp'),
    ('4', 'CARC', 'The procedure code is inconsistent with the modifier used', 'denial'),
    ('16', 'CARC', 'Claim/service lacks information needed for adjudication', 'denial'),
    ('18', 'CARC', 'Exact duplicate claim/service', 'denial'),
    ('29', 'CARC', 'The time limit for filing has expired', 'denial'),
    ('45', 'CARC', 'Charge exceeds fee schedule/maximum allowable', 'contractual'),
    ('50', 'CARC', 'These are non-covered services because this is not deemed a medical necessity', 'denial'),
    ('96', 'CARC', 'Non-covered charge(s)', 'denial'),
    ('97', 'CARC', 'The benefit for this service is included in the payment for another service', 'contractual'),
    ('197', 'CARC', 'Precertification/authorization/notification absent', 'denial'),
    ('204', 'CARC', 'This service/equipment/drug is not covered under the patients current benefit plan', 'denial'),
    ('CO', 'GROUP', 'Contractual Obligation', 'contractual'),
    ('PR', 'GROUP', 'Patient Responsibility', 'patient_resp'),
    ('OA', 'GROUP', 'Other Adjustment', 'informational'),
    ('CR', 'GROUP', 'Corrections and Reversals', 'informational');
"""


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.executescript(SEED_SQL)
    conn.commit()
    # Initialize DLQ and event log tables
    from ingestion.dlq import init_dlq
    from ingestion.events import init_events
    init_dlq(conn)
    init_events(conn)
    return conn


def reset_db(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    if os.path.exists(db_path):
        os.remove(db_path)
    return init_db(db_path)
