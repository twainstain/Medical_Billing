-- ============================================================================
-- Medical Billing Arbitration — Sample Seed Data
-- All data is synthetic — no real PHI
-- ============================================================================

-- ============================================================================
-- Payers
-- ============================================================================
INSERT INTO payers (name, type, state) VALUES
('Anthem Blue Cross',       'commercial', 'CA'),
('Aetna',                   'commercial', 'NY'),
('UnitedHealthcare',        'commercial', 'TX'),
('Cigna',                   'commercial', 'FL'),
('Humana',                  'commercial', 'GA'),
('Medicare Part B',         'medicare',   NULL),
('Medicaid - New York',     'medicaid',   'NY'),
('TRICARE West',            'tricare',    NULL);

-- ============================================================================
-- Providers
-- ============================================================================
INSERT INTO providers (npi, tin, name, specialty, facility_id, facility_name, state) VALUES
('1234567890', '123456789', 'Dr. Sarah Chen',       'Emergency Medicine', 'FAC001', 'Metro General Hospital',   'NY'),
('2345678901', '234567890', 'Dr. Michael Roberts',   'Orthopedic Surgery', 'FAC001', 'Metro General Hospital',   'NY'),
('3456789012', '345678901', 'Dr. Lisa Patel',        'Anesthesiology',     'FAC002', 'Riverside Medical Center', 'CA'),
('4567890123', '456789012', 'Dr. James Wilson',      'Radiology',          'FAC002', 'Riverside Medical Center', 'CA'),
('5678901234', '567890123', 'Dr. Amanda Foster',     'Cardiology',         'FAC003', 'Heart & Vascular Institute','TX'),
('6789012345', '678901234', 'Dr. David Kim',         'Neurosurgery',       'FAC001', 'Metro General Hospital',   'NY');

-- ============================================================================
-- Patients (synthetic names, no real data)
-- ============================================================================
INSERT INTO patients (first_name, last_name, date_of_birth, gender, address_line1, city, state, zip_code, insurance_id, payer_id) VALUES
('John',    'Smith',     '1975-03-15', 'M', '123 Main St',      'New York',    'NY', '10001', 'ANT-100001', 1),
('Maria',   'Garcia',    '1982-07-22', 'F', '456 Oak Ave',      'Los Angeles', 'CA', '90001', 'AET-200002', 2),
('Robert',  'Johnson',   '1968-11-30', 'M', '789 Pine Rd',      'Houston',     'TX', '77001', 'UHC-300003', 3),
('Jennifer','Williams',  '1990-01-05', 'F', '321 Elm St',        'Miami',       'FL', '33101', 'CIG-400004', 4),
('William', 'Brown',     '1955-09-18', 'M', '654 Cedar Ln',     'Atlanta',     'GA', '30301', 'HUM-500005', 5),
('Emily',   'Davis',     '1988-04-12', 'F', '987 Birch Dr',     'New York',    'NY', '10002', 'MCR-600006', 6),
('James',   'Martinez',  '1972-06-25', 'M', '147 Maple Way',    'Brooklyn',    'NY', '11201', 'MCD-700007', 7),
('Susan',   'Anderson',  '1965-12-08', 'F', '258 Walnut Ct',    'Dallas',      'TX', '75201', 'TRI-800008', 8),
('David',   'Taylor',    '1980-08-20', 'M', '369 Spruce Ave',   'San Diego',   'CA', '92101', 'ANT-100009', 1),
('Lisa',    'Thomas',    '1993-02-14', 'F', '741 Ash Blvd',     'New York',    'NY', '10003', 'AET-200010', 2);

-- ============================================================================
-- Fee Schedules (SCD Type 2 — multiple historical rates)
-- ============================================================================

-- Anthem rates for common ER/surgery codes
INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region, rate, rate_type, valid_from, valid_to, is_current, source) VALUES
-- 99283: ER visit moderate
(1, '99283', NULL, 'NY', 185.00, 'contracted', '2024-01-01', '2024-12-31', 0, 'Contract 2024'),
(1, '99283', NULL, 'NY', 195.00, 'contracted', '2025-01-01', '2025-12-31', 0, 'Contract 2025'),
(1, '99283', NULL, 'NY', 205.00, 'contracted', '2026-01-01', '9999-12-31', 1, 'Contract 2026'),
-- 99285: ER visit high severity
(1, '99285', NULL, 'NY', 450.00, 'contracted', '2024-01-01', '2024-12-31', 0, 'Contract 2024'),
(1, '99285', NULL, 'NY', 475.00, 'contracted', '2025-01-01', '2025-12-31', 0, 'Contract 2025'),
(1, '99285', NULL, 'NY', 498.00, 'contracted', '2026-01-01', '9999-12-31', 1, 'Contract 2026'),
-- 27447: Total knee replacement
(1, '27447', NULL, 'NY', 3200.00, 'contracted', '2024-01-01', '2024-12-31', 0, 'Contract 2024'),
(1, '27447', NULL, 'NY', 3350.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025');

-- Aetna rates
INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region, rate, rate_type, valid_from, valid_to, is_current, source) VALUES
(2, '99283', NULL, 'NY', 175.00, 'contracted', '2024-01-01', '2024-12-31', 0, 'Contract 2024'),
(2, '99283', NULL, 'NY', 190.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025'),
(2, '99285', NULL, 'NY', 420.00, 'contracted', '2024-01-01', '2024-12-31', 0, 'Contract 2024'),
(2, '99285', NULL, 'NY', 460.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025'),
(2, '27447', NULL, 'NY', 3100.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025');

-- UnitedHealthcare rates
INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region, rate, rate_type, valid_from, valid_to, is_current, source) VALUES
(3, '99283', NULL, 'TX', 165.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025'),
(3, '99285', NULL, 'TX', 430.00, 'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025'),
(3, '93010', NULL, 'TX', 85.00,  'contracted', '2025-01-01', '9999-12-31', 1, 'Contract 2025');

-- Medicare benchmark rates (used as QPA reference)
INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region, rate, rate_type, valid_from, valid_to, is_current, source) VALUES
(6, '99283', NULL, 'NY', 155.00, 'medicare', '2025-01-01', '2025-12-31', 0, 'CMS PFS 2025'),
(6, '99283', NULL, 'NY', 162.00, 'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026'),
(6, '99285', NULL, 'NY', 380.00, 'medicare', '2025-01-01', '2025-12-31', 0, 'CMS PFS 2025'),
(6, '99285', NULL, 'NY', 395.00, 'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026'),
(6, '27447', NULL, 'NY', 2800.00,'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026'),
(6, '93010', NULL, 'TX', 75.00,  'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026'),
(6, '99283', NULL, 'TX', 148.00, 'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026'),
(6, '99285', NULL, 'TX', 365.00, 'medicare', '2026-01-01', '9999-12-31', 1, 'CMS PFS 2026');

-- ============================================================================
-- Claims (mix of statuses — some paid, some underpaid, some denied)
-- ============================================================================
INSERT INTO claims (external_claim_id, patient_id, provider_npi, payer_id, date_of_service, date_filed, facility_id, place_of_service, total_billed, status) VALUES
-- Underpaid claims (will become disputes)
('CLM-2025-0001', 1, '1234567890', 1, '2025-06-15', '2025-06-20', 'FAC001', '23', 850.00,  'underpaid'),
('CLM-2025-0002', 2, '3456789012', 2, '2025-07-10', '2025-07-15', 'FAC002', '23', 1200.00, 'underpaid'),
('CLM-2025-0003', 3, '5678901234', 3, '2025-08-22', '2025-08-25', 'FAC003', '11', 520.00,  'underpaid'),
('CLM-2025-0004', 9, '2345678901', 1, '2025-05-10', '2025-05-15', 'FAC001', '21', 6500.00, 'in_dispute'),
('CLM-2025-0005', 10,'6789012345', 2, '2025-09-01', '2025-09-05', 'FAC001', '23', 4200.00, 'in_dispute'),
-- Paid claims (no dispute)
('CLM-2025-0006', 4, '1234567890', 4, '2025-04-01', '2025-04-05', 'FAC001', '23', 450.00,  'paid'),
('CLM-2025-0007', 5, '5678901234', 5, '2025-03-18', '2025-03-22', 'FAC003', '11', 320.00,  'paid'),
-- Denied claim
('CLM-2025-0008', 6, '4567890123', 6, '2025-10-05', '2025-10-08', 'FAC002', '22', 1800.00, 'denied'),
-- Recent / in-process claims
('CLM-2026-0001', 7, '1234567890', 7, '2026-01-10', '2026-01-15', 'FAC001', '23', 650.00,  'filed'),
('CLM-2026-0002', 8, '3456789012', 8, '2026-02-20', '2026-02-22', 'FAC002', '23', 980.00,  'filed');

-- ============================================================================
-- Claim Lines
-- ============================================================================
INSERT INTO claim_lines (claim_id, line_number, cpt_code, modifier, units, billed_amount, diagnosis_codes) VALUES
-- CLM-2025-0001 (ER visit, underpaid)
(1, 1, '99285', NULL, 1, 650.00, 'R10.9,R11.0'),
(1, 2, '99283', NULL, 1, 200.00, 'R10.9'),
-- CLM-2025-0002 (anesthesia, underpaid)
(2, 1, '00142', NULL, 1, 1200.00, 'K35.80'),
-- CLM-2025-0003 (cardiology, underpaid)
(3, 1, '93010', NULL, 1, 120.00,  'I25.10'),
(3, 2, '99285', NULL, 1, 400.00,  'I25.10,R00.0'),
-- CLM-2025-0004 (knee replacement, in dispute)
(4, 1, '27447', NULL, 1, 6500.00, 'M17.11'),
-- CLM-2025-0005 (neurosurgery, in dispute)
(5, 1, '61510', NULL, 1, 4200.00, 'C71.1'),
-- CLM-2025-0006 (paid normally)
(6, 1, '99283', NULL, 1, 450.00,  'J06.9'),
-- CLM-2025-0007 (paid normally)
(7, 1, '93010', NULL, 1, 320.00,  'I10'),
-- CLM-2025-0008 (denied)
(8, 1, '74177', NULL, 1, 1800.00, 'R10.0'),
-- CLM-2026-0001 (in process)
(9, 1, '99283', NULL, 1, 650.00,  'S52.501A'),
-- CLM-2026-0002 (in process)
(10, 1, '99285', NULL, 1, 980.00, 'I21.01');

-- ============================================================================
-- Remittances (payments received — some are underpayments)
-- ============================================================================
INSERT INTO remittances (claim_id, era_date, paid_amount, allowed_amount, adjustment_reason, denial_code, check_number, source_type) VALUES
-- Underpayments
(1, '2025-07-20', 380.00,  425.00,  'CO-45: Charges exceed contracted rate', NULL,   'CHK-10001', 'edi_835'),
(2, '2025-08-15', 520.00,  600.00,  'CO-45: Charges exceed contracted rate', NULL,   'CHK-10002', 'edi_835'),
(3, '2025-09-25', 210.00,  250.00,  'CO-45: Charges exceed contracted rate', NULL,   'CHK-10003', 'edi_835'),
(4, '2025-06-15', 2100.00, 2800.00, 'CO-45: Charges exceed contracted rate', NULL,   'CHK-10004', 'edi_835'),
(5, '2025-10-10', 1800.00, 2200.00, 'CO-45: Charges exceed contracted rate', NULL,   'CHK-10005', 'eob_pdf'),
-- Full payments
(6, '2025-05-05', 420.00,  420.00,  NULL, NULL, 'CHK-10006', 'edi_835'),
(7, '2025-04-20', 310.00,  310.00,  NULL, NULL, 'CHK-10007', 'edi_835'),
-- Denial
(8, '2025-11-01', 0.00,    0.00,    'CO-197: Precertification/authorization/notification absent', 'CO-197', 'CHK-10008', 'edi_835');

-- ============================================================================
-- Cases
-- ============================================================================
INSERT INTO cases (assigned_analyst, status, priority, created_date, last_activity, outcome, award_amount, closed_date) VALUES
('Ana Rodriguez',  'negotiation',     'high',     '2025-08-01', '2026-03-20', NULL,     NULL,     NULL),
('Mark Thompson',  'idr_initiated',   'high',     '2025-08-20', '2026-03-18', NULL,     NULL,     NULL),
('Ana Rodriguez',  'in_review',       'medium',   '2025-10-01', '2026-03-15', NULL,     NULL,     NULL),
('Sarah Lee',      'idr_submitted',   'critical', '2025-06-20', '2026-03-10', NULL,     NULL,     NULL),
('Mark Thompson',  'decided',         'high',     '2025-10-15', '2026-02-28', 'won',    3500.00,  '2026-02-28'),
-- Historical closed case
('Ana Rodriguez',  'closed',          'medium',   '2025-03-01', '2025-06-15', 'lost',   NULL,     '2025-06-15');

-- ============================================================================
-- Disputes
-- ============================================================================
INSERT INTO disputes (claim_id, claim_line_id, case_id, dispute_type, status, billed_amount, paid_amount, requested_amount, qpa_amount, filed_date, open_negotiation_deadline, idr_initiation_deadline) VALUES
-- Active disputes
(1, 1,  1, 'idr',       'negotiation',       650.00,  380.00,  650.00,  475.00,  '2025-08-05', '2025-09-04', '2025-09-10'),
(2, 3,  2, 'idr',       'filed',             1200.00, 520.00,  1200.00, 580.00,  '2025-08-25', '2025-09-24', '2025-09-30'),
(3, 4,  3, 'appeal',    'open',              400.00,  210.00,  400.00,  365.00,  '2025-10-05', '2025-11-04', NULL),
(4, 6,  4, 'idr',       'evidence_submitted',6500.00, 2100.00, 6500.00, 3350.00, '2025-06-25', '2025-07-25', '2025-07-31'),
(5, 7,  5, 'idr',       'decided',           4200.00, 1800.00, 4200.00, 2200.00, '2025-10-20', '2025-11-19', '2025-11-25'),
-- Historical closed
(1, 2,  6, 'appeal',    'closed',            200.00,  120.00,  200.00,  155.00,  '2025-03-10', '2025-04-09', NULL);

-- ============================================================================
-- Evidence Artifacts
-- ============================================================================
INSERT INTO evidence_artifacts (case_id, type, blob_url, original_filename, classification_confidence, ocr_status) VALUES
(1, 'eob',            'https://medbillstore.blob.core.windows.net/documents/case1/eob_anthem_2025.pdf',      'Anthem_EOB_20250720.pdf',       0.9650, 'completed'),
(1, 'clinical',       'https://medbillstore.blob.core.windows.net/documents/case1/er_notes.pdf',             'ER_Notes_Patient1.pdf',          0.9820, 'completed'),
(2, 'eob',            'https://medbillstore.blob.core.windows.net/documents/case2/eob_aetna_2025.pdf',       'Aetna_EOB_20250815.pdf',        0.9510, 'completed'),
(2, 'contract',       'https://medbillstore.blob.core.windows.net/documents/case2/contract_aetna.pdf',       'Aetna_Contract_2025.pdf',       0.9900, 'completed'),
(3, 'eob',            'https://medbillstore.blob.core.windows.net/documents/case3/eob_uhc.pdf',              'UHC_EOB_20250925.pdf',          0.9380, 'completed'),
(4, 'eob',            'https://medbillstore.blob.core.windows.net/documents/case4/eob_anthem_knee.pdf',      'Anthem_EOB_Knee_20250615.pdf',  0.9720, 'completed'),
(4, 'clinical',       'https://medbillstore.blob.core.windows.net/documents/case4/op_report.pdf',            'Operative_Report_Knee.pdf',     0.9550, 'completed'),
(4, 'contract',       'https://medbillstore.blob.core.windows.net/documents/case4/contract_anthem_surg.pdf', 'Anthem_Surgery_Contract.pdf',   0.9870, 'completed'),
(4, 'correspondence', 'https://medbillstore.blob.core.windows.net/documents/case4/idr_filing.pdf',           'IDR_Filing_Case4.pdf',          0.9900, 'completed'),
(5, 'idr_decision',   'https://medbillstore.blob.core.windows.net/documents/case5/idr_decision.pdf',         'IDR_Decision_Case5.pdf',        0.9950, 'completed');

-- ============================================================================
-- Deadlines
-- ============================================================================
INSERT INTO deadlines (case_id, dispute_id, type, due_date, status, alerted_date, completed_date) VALUES
-- Case 1: in negotiation
(1, 1, 'open_negotiation',  '2025-09-04', 'met',     '2025-08-25', '2025-09-02'),
(1, 1, 'idr_initiation',    '2025-09-10', 'met',     '2025-09-06', '2025-09-08'),
(1, 1, 'evidence_submission','2026-04-10', 'pending', NULL,          NULL),
-- Case 2: IDR initiated
(2, 2, 'open_negotiation',  '2025-09-24', 'met',     '2025-09-14', '2025-09-20'),
(2, 2, 'idr_initiation',    '2025-09-30', 'met',     '2025-09-26', '2025-09-28'),
(2, 2, 'entity_selection',  '2026-04-01', 'pending', NULL,          NULL),
-- Case 3: in review (approaching deadline)
(3, 3, 'open_negotiation',  '2026-04-04', 'alerted', '2026-03-25', NULL),
-- Case 4: evidence submitted, awaiting decision
(4, 4, 'open_negotiation',  '2025-07-25', 'met',     '2025-07-15', '2025-07-22'),
(4, 4, 'idr_initiation',    '2025-07-31', 'met',     '2025-07-27', '2025-07-29'),
(4, 4, 'evidence_submission','2025-12-01', 'met',     '2025-11-20', '2025-11-28'),
(4, 4, 'decision',          '2026-04-15', 'pending', NULL,          NULL),
-- Case 5: decided
(5, 5, 'open_negotiation',  '2025-11-19', 'met',     '2025-11-09', '2025-11-15'),
(5, 5, 'idr_initiation',    '2025-11-25', 'met',     '2025-11-21', '2025-11-23'),
(5, 5, 'decision',          '2026-02-28', 'met',     '2026-02-20', '2026-02-28');

-- ============================================================================
-- Audit Log (sample entries)
-- ============================================================================
INSERT INTO audit_log (entity_type, entity_id, action, user_id, timestamp, old_value, new_value, ai_agent, ai_confidence, ai_model_version) VALUES
('case',     1, 'insert',       'ana.rodriguez',  '2025-08-01 09:00:00', NULL,            '{"status":"open"}',           NULL, NULL, NULL),
('case',     1, 'update',       'ana.rodriguez',  '2025-08-05 14:30:00', '{"status":"open"}', '{"status":"negotiation"}', NULL, NULL, NULL),
('dispute',  1, 'insert',       'system',         '2025-08-05 14:31:00', NULL,            '{"dispute_type":"idr"}',      NULL, NULL, NULL),
('evidence', 1, 'ai_generated', 'system',         '2025-08-06 10:00:00', NULL,            '{"classification":"eob"}',    'document_classifier', 0.9650, 'claude-opus-4-6'),
('evidence', 2, 'ai_generated', 'system',         '2025-08-06 10:05:00', NULL,            '{"classification":"clinical"}','document_classifier', 0.9820, 'claude-opus-4-6'),
('case',     4, 'update',       'sarah.lee',      '2025-11-28 16:00:00', '{"status":"idr_initiated"}', '{"status":"idr_submitted"}', NULL, NULL, NULL),
('case',     5, 'update',       'mark.thompson',  '2026-02-28 11:00:00', '{"status":"idr_submitted"}', '{"status":"decided","outcome":"won","award_amount":3500}', NULL, NULL, NULL);

PRINT 'Sample data loaded successfully.';
PRINT '';
PRINT 'Summary:';
PRINT '  - 8 payers';
PRINT '  - 6 providers';
PRINT '  - 10 patients';
PRINT '  - 30 fee schedule rates (with SCD Type 2 history)';
PRINT '  - 10 claims (3 underpaid, 2 in dispute, 2 paid, 1 denied, 2 new)';
PRINT '  - 10 claim lines';
PRINT '  - 8 remittances';
PRINT '  - 6 cases (various statuses)';
PRINT '  - 6 disputes';
PRINT '  - 10 evidence artifacts';
PRINT '  - 14 deadlines';
PRINT '  - 7 audit log entries';
GO
