-- ============================================================================
-- Medical Billing Arbitration — OLTP Schema (Azure SQL)
-- Based on architecture document Section 6.2
-- IDEMPOTENT: safe to re-run (IF NOT EXISTS on all CREATE statements)
-- ============================================================================

-- Run against: Azure SQL Database (medbill_oltp)
-- Connection: sqlcmd -S <server>.database.windows.net -d medbill_oltp -U medbilladmin -P <password>

-- Required for computed columns and filtered indexes on Azure SQL
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- ============================================================================
-- Reference / Lookup Tables
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'payers')
CREATE TABLE payers (
    payer_id        INT IDENTITY(1,1) PRIMARY KEY,
    name            NVARCHAR(200)   NOT NULL,
    type            NVARCHAR(50)    NOT NULL,  -- commercial, medicare, medicaid, tricare
    state           NVARCHAR(2),
    is_active       BIT             NOT NULL DEFAULT 1,
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'providers')
CREATE TABLE providers (
    npi             CHAR(10)        PRIMARY KEY,
    tin             CHAR(9)         NOT NULL,
    name            NVARCHAR(200)   NOT NULL,
    specialty       NVARCHAR(100),
    facility_id     NVARCHAR(50),
    facility_name   NVARCHAR(200),
    state           NVARCHAR(2),
    is_active       BIT             NOT NULL DEFAULT 1,
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'patients')
CREATE TABLE patients (
    patient_id      INT IDENTITY(1,1) PRIMARY KEY,
    first_name      NVARCHAR(100)   NOT NULL,
    last_name       NVARCHAR(100)   NOT NULL,
    date_of_birth   DATE            NOT NULL,
    gender          CHAR(1),
    address_line1   NVARCHAR(200),
    city            NVARCHAR(100),
    state           NVARCHAR(2),
    zip_code        NVARCHAR(10),
    insurance_id    NVARCHAR(50),
    payer_id        INT             REFERENCES payers(payer_id),
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- Fee Schedule (SCD Type 2 — critical for arbitration)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fee_schedule')
CREATE TABLE fee_schedule (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    payer_id        INT             NOT NULL REFERENCES payers(payer_id),
    cpt_code        VARCHAR(10)     NOT NULL,
    modifier        VARCHAR(10),
    geo_region      NVARCHAR(50),
    rate            DECIMAL(12,2)   NOT NULL,
    rate_type       VARCHAR(30)     NOT NULL,  -- contracted, medicare, fair_health
    valid_from      DATE            NOT NULL,
    valid_to        DATE            NOT NULL DEFAULT '9999-12-31',
    is_current      BIT             NOT NULL DEFAULT 1,
    source          NVARCHAR(100),
    loaded_date     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fee_schedule_lookup')
CREATE INDEX IX_fee_schedule_lookup
    ON fee_schedule (payer_id, cpt_code, valid_from, valid_to)
    INCLUDE (rate, rate_type);

-- ============================================================================
-- Claims & Remittances
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'claims')
CREATE TABLE claims (
    claim_id        INT IDENTITY(1,1) PRIMARY KEY,
    external_claim_id NVARCHAR(50)  UNIQUE,
    patient_id      INT             NOT NULL REFERENCES patients(patient_id),
    provider_npi    CHAR(10)        NOT NULL REFERENCES providers(npi),
    payer_id        INT             NOT NULL REFERENCES payers(payer_id),
    date_of_service DATE            NOT NULL,
    date_filed      DATE,
    facility_id     NVARCHAR(50),
    place_of_service VARCHAR(5),
    total_billed    DECIMAL(12,2)   NOT NULL,
    status          VARCHAR(30)     NOT NULL DEFAULT 'filed',
        -- filed, acknowledged, paid, denied, underpaid, in_dispute, resolved
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'claim_lines')
CREATE TABLE claim_lines (
    line_id         INT IDENTITY(1,1) PRIMARY KEY,
    claim_id        INT             NOT NULL REFERENCES claims(claim_id),
    line_number     INT             NOT NULL,
    cpt_code        VARCHAR(10)     NOT NULL,
    modifier        VARCHAR(10),
    units           INT             NOT NULL DEFAULT 1,
    billed_amount   DECIMAL(12,2)   NOT NULL,
    diagnosis_codes NVARCHAR(200),  -- comma-separated ICD-10
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT UQ_claim_line UNIQUE (claim_id, line_number)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'remittances')
CREATE TABLE remittances (
    remit_id        INT IDENTITY(1,1) PRIMARY KEY,
    claim_id        INT             NOT NULL REFERENCES claims(claim_id),
    era_date        DATE            NOT NULL,
    paid_amount     DECIMAL(12,2)   NOT NULL,
    allowed_amount  DECIMAL(12,2),
    adjustment_reason NVARCHAR(200),
    denial_code     VARCHAR(20),
    check_number    NVARCHAR(50),
    source_type     VARCHAR(20)     DEFAULT 'edi_835',  -- edi_835, eob_pdf, manual
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- Disputes & Cases
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cases')
CREATE TABLE cases (
    case_id         INT IDENTITY(1,1) PRIMARY KEY,
    assigned_analyst NVARCHAR(100),
    status          VARCHAR(30)     NOT NULL DEFAULT 'open',
        -- open, in_review, negotiation, idr_initiated, idr_submitted, decided, closed
    priority        VARCHAR(10)     NOT NULL DEFAULT 'medium',
        -- low, medium, high, critical
    created_date    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    last_activity   DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    outcome         VARCHAR(30),    -- won, lost, settled, withdrawn
    award_amount    DECIMAL(12,2),
    closed_date     DATETIME2
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'disputes')
CREATE TABLE disputes (
    dispute_id      INT IDENTITY(1,1) PRIMARY KEY,
    claim_id        INT             NOT NULL REFERENCES claims(claim_id),
    claim_line_id   INT             REFERENCES claim_lines(line_id),
    case_id         INT             REFERENCES cases(case_id),
    dispute_type    VARCHAR(30)     NOT NULL,
        -- appeal, idr, state_arb, external_review
    status          VARCHAR(30)     NOT NULL DEFAULT 'open',
        -- open, negotiation, filed, evidence_submitted, decided, closed
    billed_amount   DECIMAL(12,2)   NOT NULL,
    paid_amount     DECIMAL(12,2)   NOT NULL,
    requested_amount DECIMAL(12,2),
    qpa_amount      DECIMAL(12,2),
    underpayment_amount AS (billed_amount - paid_amount) PERSISTED,
    filed_date      DATE,
    open_negotiation_deadline DATE,
    idr_initiation_deadline   DATE,
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- Evidence & Documents
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'evidence_artifacts')
CREATE TABLE evidence_artifacts (
    artifact_id     INT IDENTITY(1,1) PRIMARY KEY,
    case_id         INT             REFERENCES cases(case_id),  -- nullable for unlinked docs
    type            VARCHAR(30)     NOT NULL,
        -- eob, clinical, contract, correspondence, idr_decision
    blob_url        NVARCHAR(500),
    original_filename NVARCHAR(200),
    extracted_data  NVARCHAR(MAX),  -- JSON from Document Intelligence
    classification_confidence DECIMAL(5,4),
    ocr_status      VARCHAR(20)     DEFAULT 'pending',
        -- pending, processing, completed, failed, manual_review
    content_hash    VARCHAR(64),
    uploaded_date   DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UX_evidence_hash')
CREATE UNIQUE INDEX UX_evidence_hash ON evidence_artifacts (content_hash)
    WHERE content_hash IS NOT NULL;

-- ============================================================================
-- Deadlines & SLA Tracking
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'deadlines')
CREATE TABLE deadlines (
    deadline_id     INT IDENTITY(1,1) PRIMARY KEY,
    case_id         INT             NOT NULL REFERENCES cases(case_id),
    dispute_id      INT             REFERENCES disputes(dispute_id),
    type            VARCHAR(40)     NOT NULL,
        -- open_negotiation, idr_initiation, entity_selection, evidence_submission, decision
    due_date        DATE            NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
        -- pending, alerted, met, missed
    alerted_date    DATETIME2,
    completed_date  DATETIME2,
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_deadlines_due')
CREATE INDEX IX_deadlines_due ON deadlines (due_date, status);

-- ============================================================================
-- Audit Log
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'audit_log')
CREATE TABLE audit_log (
    log_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    entity_type     VARCHAR(50)     NOT NULL,
    entity_id       NVARCHAR(100)   NOT NULL,
    action          VARCHAR(30)     NOT NULL,  -- insert, update, delete, ai_generated, reviewed
    user_id         NVARCHAR(100),
    timestamp       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    old_value       NVARCHAR(MAX),
    new_value       NVARCHAR(MAX),
    ai_agent        VARCHAR(50),    -- document_classifier, narrative_drafter, etc.
    ai_confidence   DECIMAL(5,4),
    ai_model_version NVARCHAR(50)
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_audit_log_entity')
CREATE INDEX IX_audit_log_entity ON audit_log (entity_type, entity_id);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_audit_log_time')
CREATE INDEX IX_audit_log_time   ON audit_log (timestamp);

-- ============================================================================
-- Dead-Letter Queue (failed/rejected ingestion records)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dead_letter_queue')
CREATE TABLE dead_letter_queue (
    dlq_id          INT IDENTITY(1,1) PRIMARY KEY,
    source          NVARCHAR(50)    NOT NULL,
    entity_type     NVARCHAR(50)    NOT NULL,
    entity_id       NVARCHAR(100),
    error_category  NVARCHAR(30)    NOT NULL,
        -- parse_failure, validation_failure, transient_failure, unmatched_reference
    error_detail    NVARCHAR(MAX)   NOT NULL,
    payload_ref     NVARCHAR(MAX),
    created_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    resolved_at     DATETIME2,
    resolved_by     NVARCHAR(100),
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending'
);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_dlq_pending')
CREATE INDEX IX_dlq_pending ON dead_letter_queue (status) WHERE status = 'pending';

-- ============================================================================
-- Claim ID Alias (payer claim ID → canonical mapping)
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'claim_id_alias')
CREATE TABLE claim_id_alias (
    payer_claim_id      NVARCHAR(50)    NOT NULL,
    canonical_claim_id  NVARCHAR(50)    NOT NULL,
    payer_id            INT,
    PRIMARY KEY (payer_claim_id, payer_id)
);

-- ============================================================================
-- Enable CDC on core tables (run after initial data load)
-- ============================================================================
-- Uncomment and run these after seed data is loaded:
--
-- EXEC sys.sp_cdc_enable_db;
--
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'cases',       @role_name=NULL;
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'disputes',    @role_name=NULL;
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'claims',      @role_name=NULL;
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'remittances', @role_name=NULL;
-- EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'deadlines',   @role_name=NULL;

PRINT 'Schema created successfully.';
GO
