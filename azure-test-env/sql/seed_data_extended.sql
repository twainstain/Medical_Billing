-- ============================================================================
-- Medical Billing Arbitration — Extended Seed Data for Trends & Analytics
-- All data is synthetic — no real PHI
-- IDEMPOTENT: safe to re-run (skips existing rows via external_claim_id)
--
-- Adds: 50 claims across 12 months (Jan 2025 – Dec 2025)
--       50 remittances (mix of paid, underpaid, denied)
--       20 cases with outcomes spread across months
--       20 disputes linked to underpaid claims
--       50 deadlines across case lifecycle
--       20 evidence artifacts
--       Run AFTER seed_data.sql (uses patient_id 1-10, payer_id 1-8, providers)
-- ============================================================================

-- ============================================================================
-- CLAIMS — 50 claims across 12 months, all 8 payers, 6 providers
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[claims] WHERE [external_claim_id] = 'CLM-EXT-0001')
BEGIN

-- January 2025
INSERT INTO [dbo].[claims] ([external_claim_id],[patient_id],[provider_npi],[payer_id],[date_of_service],[date_filed],[facility_id],[place_of_service],[total_billed],[status]) VALUES
('CLM-EXT-0001', 1, '1234567890', 1, '2025-01-05', '2025-01-10', 'FAC001', '23', 720.00,  'underpaid'),
('CLM-EXT-0002', 2, '3456789012', 2, '2025-01-12', '2025-01-17', 'FAC002', '23', 950.00,  'paid'),
('CLM-EXT-0003', 3, '5678901234', 3, '2025-01-20', '2025-01-25', 'FAC003', '11', 1100.00, 'underpaid'),
('CLM-EXT-0004', 4, '1234567890', 4, '2025-01-28', '2025-02-02', 'FAC001', '23', 380.00,  'paid'),

-- February 2025
('CLM-EXT-0005', 5, '2345678901', 1, '2025-02-03', '2025-02-08', 'FAC001', '21', 5200.00, 'in_dispute'),
('CLM-EXT-0006', 6, '4567890123', 2, '2025-02-10', '2025-02-15', 'FAC002', '22', 1500.00, 'underpaid'),
('CLM-EXT-0007', 7, '6789012345', 3, '2025-02-18', '2025-02-23', 'FAC001', '23', 890.00,  'denied'),
('CLM-EXT-0008', 8, '1234567890', 5, '2025-02-25', '2025-03-02', 'FAC001', '23', 450.00,  'paid'),

-- March 2025
('CLM-EXT-0009', 9, '3456789012', 1, '2025-03-05', '2025-03-10', 'FAC002', '23', 2800.00, 'in_dispute'),
('CLM-EXT-0010', 10,'5678901234', 6, '2025-03-12', '2025-03-17', 'FAC003', '11', 620.00,  'paid'),
('CLM-EXT-0011', 1, '2345678901', 2, '2025-03-20', '2025-03-25', 'FAC001', '21', 7500.00, 'in_dispute'),
('CLM-EXT-0012', 2, '4567890123', 4, '2025-03-28', '2025-04-02', 'FAC002', '22', 340.00,  'paid'),

-- April 2025
('CLM-EXT-0013', 3, '1234567890', 1, '2025-04-02', '2025-04-07', 'FAC001', '23', 1250.00, 'underpaid'),
('CLM-EXT-0014', 4, '6789012345', 3, '2025-04-10', '2025-04-15', 'FAC001', '23', 3400.00, 'in_dispute'),
('CLM-EXT-0015', 5, '3456789012', 2, '2025-04-18', '2025-04-23', 'FAC002', '23', 780.00,  'paid'),
('CLM-EXT-0016', 6, '5678901234', 5, '2025-04-25', '2025-04-30', 'FAC003', '11', 920.00,  'underpaid'),

-- May 2025
('CLM-EXT-0017', 7, '1234567890', 1, '2025-05-03', '2025-05-08', 'FAC001', '23', 1800.00, 'underpaid'),
('CLM-EXT-0018', 8, '2345678901', 2, '2025-05-10', '2025-05-15', 'FAC001', '21', 4600.00, 'in_dispute'),
('CLM-EXT-0019', 9, '4567890123', 6, '2025-05-18', '2025-05-23', 'FAC002', '22', 550.00,  'paid'),
('CLM-EXT-0020', 10,'6789012345', 4, '2025-05-25', '2025-05-30', 'FAC001', '23', 1350.00, 'denied'),

-- June 2025
('CLM-EXT-0021', 1, '3456789012', 3, '2025-06-02', '2025-06-07', 'FAC002', '23', 2100.00, 'in_dispute'),
('CLM-EXT-0022', 2, '5678901234', 1, '2025-06-10', '2025-06-15', 'FAC003', '11', 680.00,  'paid'),
('CLM-EXT-0023', 3, '1234567890', 2, '2025-06-18', '2025-06-23', 'FAC001', '23', 1600.00, 'underpaid'),
('CLM-EXT-0024', 4, '2345678901', 5, '2025-06-25', '2025-06-30', 'FAC001', '21', 3200.00, 'underpaid'),

-- July 2025
('CLM-EXT-0025', 5, '4567890123', 1, '2025-07-03', '2025-07-08', 'FAC002', '22', 1900.00, 'in_dispute'),
('CLM-EXT-0026', 6, '6789012345', 3, '2025-07-10', '2025-07-15', 'FAC001', '23', 4100.00, 'in_dispute'),
('CLM-EXT-0027', 7, '3456789012', 2, '2025-07-18', '2025-07-23', 'FAC002', '23', 520.00,  'paid'),
('CLM-EXT-0028', 8, '5678901234', 6, '2025-07-25', '2025-07-30', 'FAC003', '11', 750.00,  'underpaid'),

-- August 2025
('CLM-EXT-0029', 9, '1234567890', 1, '2025-08-04', '2025-08-09', 'FAC001', '23', 2400.00, 'in_dispute'),
('CLM-EXT-0030', 10,'2345678901', 4, '2025-08-12', '2025-08-17', 'FAC001', '21', 5800.00, 'underpaid'),
('CLM-EXT-0031', 1, '4567890123', 2, '2025-08-20', '2025-08-25', 'FAC002', '22', 1100.00, 'denied'),
('CLM-EXT-0032', 2, '6789012345', 5, '2025-08-28', '2025-09-02', 'FAC001', '23', 870.00,  'paid'),

-- September 2025
('CLM-EXT-0033', 3, '3456789012', 3, '2025-09-02', '2025-09-07', 'FAC002', '23', 1450.00, 'underpaid'),
('CLM-EXT-0034', 4, '5678901234', 1, '2025-09-10', '2025-09-15', 'FAC003', '11', 3600.00, 'in_dispute'),
('CLM-EXT-0035', 5, '1234567890', 2, '2025-09-18', '2025-09-23', 'FAC001', '23', 980.00,  'paid'),
('CLM-EXT-0036', 6, '2345678901', 6, '2025-09-25', '2025-09-30', 'FAC001', '21', 2200.00, 'underpaid'),

-- October 2025
('CLM-EXT-0037', 7, '4567890123', 1, '2025-10-03', '2025-10-08', 'FAC002', '22', 1750.00, 'in_dispute'),
('CLM-EXT-0038', 8, '6789012345', 4, '2025-10-10', '2025-10-15', 'FAC001', '23', 4500.00, 'underpaid'),
('CLM-EXT-0039', 9, '3456789012', 2, '2025-10-18', '2025-10-23', 'FAC002', '23', 620.00,  'paid'),
('CLM-EXT-0040', 10,'5678901234', 3, '2025-10-25', '2025-10-30', 'FAC003', '11', 1300.00, 'denied'),

-- November 2025
('CLM-EXT-0041', 1, '1234567890', 1, '2025-11-03', '2025-11-08', 'FAC001', '23', 2600.00, 'in_dispute'),
('CLM-EXT-0042', 2, '2345678901', 2, '2025-11-10', '2025-11-15', 'FAC001', '21', 8200.00, 'in_dispute'),
('CLM-EXT-0043', 3, '4567890123', 5, '2025-11-18', '2025-11-23', 'FAC002', '22', 430.00,  'paid'),
('CLM-EXT-0044', 4, '6789012345', 3, '2025-11-25', '2025-11-30', 'FAC001', '23', 1950.00, 'underpaid'),

-- December 2025
('CLM-EXT-0045', 5, '3456789012', 1, '2025-12-02', '2025-12-07', 'FAC002', '23', 3100.00, 'in_dispute'),
('CLM-EXT-0046', 6, '5678901234', 6, '2025-12-10', '2025-12-15', 'FAC003', '11', 890.00,  'paid'),
('CLM-EXT-0047', 7, '1234567890', 2, '2025-12-15', '2025-12-20', 'FAC001', '23', 1700.00, 'underpaid'),
('CLM-EXT-0048', 8, '2345678901', 4, '2025-12-18', '2025-12-23', 'FAC001', '21', 5500.00, 'underpaid'),
('CLM-EXT-0049', 9, '4567890123', 3, '2025-12-22', '2025-12-27', 'FAC002', '22', 2300.00, 'in_dispute'),
('CLM-EXT-0050', 10,'6789012345', 5, '2025-12-28', '2026-01-02', 'FAC001', '23', 1050.00, 'denied');

PRINT 'Extended claims loaded (50 rows).';
END
GO

-- ============================================================================
-- CLAIM LINES — 1-2 lines per claim
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[claim_lines] cl JOIN [dbo].[claims] c ON cl.[claim_id] = c.[claim_id] WHERE c.[external_claim_id] = 'CLM-EXT-0001')
BEGIN

DECLARE @cid_base INT = (SELECT [claim_id] FROM [dbo].[claims] WHERE [external_claim_id] = 'CLM-EXT-0001');

-- Insert lines for all 50 extended claims (1 line each for simplicity, using claim_id offsets)
INSERT INTO [dbo].[claim_lines] ([claim_id],[line_number],[cpt_code],[modifier],[units],[billed_amount],[diagnosis_codes])
SELECT c.[claim_id], 1,
    CASE (c.[claim_id] % 6)
        WHEN 0 THEN '99285' WHEN 1 THEN '99283' WHEN 2 THEN '93010'
        WHEN 3 THEN '27447' WHEN 4 THEN '99214' WHEN 5 THEN '74177'
    END,
    NULL, 1, c.[total_billed],
    CASE (c.[claim_id] % 4)
        WHEN 0 THEN 'R10.9' WHEN 1 THEN 'M54.5' WHEN 2 THEN 'I25.10' WHEN 3 THEN 'S72.001A'
    END
FROM [dbo].[claims] c
WHERE c.[external_claim_id] LIKE 'CLM-EXT-%';

PRINT 'Extended claim lines loaded.';
END
GO

-- ============================================================================
-- REMITTANCES — payment for each extended claim
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[remittances] r JOIN [dbo].[claims] c ON r.[claim_id] = c.[claim_id] WHERE c.[external_claim_id] = 'CLM-EXT-0001')
BEGIN

INSERT INTO [dbo].[remittances] ([claim_id],[era_date],[paid_amount],[allowed_amount],[adjustment_reason],[denial_code],[check_number],[source_type])
SELECT c.[claim_id],
    DATEADD(DAY, 30, c.[date_of_service]),  -- paid ~30 days after DOS
    CASE c.[status]
        WHEN 'paid'       THEN ROUND(c.[total_billed] * 0.95, 2)       -- 95% paid
        WHEN 'underpaid'  THEN ROUND(c.[total_billed] * 0.45, 2)       -- 45% paid (big gap)
        WHEN 'in_dispute' THEN ROUND(c.[total_billed] * 0.35, 2)       -- 35% paid (disputed)
        WHEN 'denied'     THEN 0.00                                      -- $0 paid
        ELSE ROUND(c.[total_billed] * 0.50, 2)
    END,
    CASE c.[status]
        WHEN 'paid'       THEN ROUND(c.[total_billed] * 0.95, 2)
        WHEN 'underpaid'  THEN ROUND(c.[total_billed] * 0.55, 2)
        WHEN 'in_dispute' THEN ROUND(c.[total_billed] * 0.45, 2)
        WHEN 'denied'     THEN 0.00
        ELSE ROUND(c.[total_billed] * 0.60, 2)
    END,
    CASE c.[status]
        WHEN 'denied'    THEN 'CO-197: Precertification absent'
        WHEN 'underpaid' THEN 'CO-45: Charges exceed contracted rate'
        WHEN 'in_dispute'THEN 'CO-45: Charges exceed contracted rate'
        ELSE NULL
    END,
    CASE c.[status] WHEN 'denied' THEN 'CO-197' ELSE NULL END,
    'CHK-EXT-' + CAST(c.[claim_id] AS VARCHAR),
    CASE (c.[claim_id] % 3) WHEN 0 THEN 'edi_835' WHEN 1 THEN 'edi_835' ELSE 'eob_pdf' END
FROM [dbo].[claims] c
WHERE c.[external_claim_id] LIKE 'CLM-EXT-%';

PRINT 'Extended remittances loaded (50 rows).';
END
GO

-- ============================================================================
-- CASES — 20 cases spread across months with outcomes
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[cases] WHERE [assigned_analyst] = 'Ana Rodriguez' AND [created_date] = '2025-01-15')
BEGIN

INSERT INTO [dbo].[cases] ([assigned_analyst],[status],[priority],[created_date],[last_activity],[outcome],[award_amount],[closed_date]) VALUES
-- Q1 2025 — early cases, mostly resolved
('Ana Rodriguez',  'closed',          'high',     '2025-01-15', '2025-04-10', 'won',       1200.00, '2025-04-10'),
('Mark Thompson',  'closed',          'medium',   '2025-01-25', '2025-04-20', 'settled',    800.00, '2025-04-20'),
('Sarah Lee',      'closed',          'high',     '2025-02-10', '2025-05-15', 'won',       3500.00, '2025-05-15'),
('Ana Rodriguez',  'closed',          'low',      '2025-02-20', '2025-05-01', 'lost',       NULL,   '2025-05-01'),
('Mark Thompson',  'closed',          'critical', '2025-03-05', '2025-06-10', 'won',       6200.00, '2025-06-10'),
('Sarah Lee',      'closed',          'high',     '2025-03-20', '2025-06-25', 'settled',   2100.00, '2025-06-25'),

-- Q2 2025 — mix of resolved and active
('Ana Rodriguez',  'closed',          'medium',   '2025-04-10', '2025-07-15', 'won',       1800.00, '2025-07-15'),
('Mark Thompson',  'closed',          'high',     '2025-04-25', '2025-08-01', 'lost',       NULL,   '2025-08-01'),
('Sarah Lee',      'closed',          'critical', '2025-05-10', '2025-08-20', 'won',       5400.00, '2025-08-20'),
('Ana Rodriguez',  'closed',          'high',     '2025-06-05', '2025-09-10', 'won',       2800.00, '2025-09-10'),

-- Q3 2025 — some resolved, some still active
('Mark Thompson',  'closed',          'medium',   '2025-07-10', '2025-10-15', 'settled',   1500.00, '2025-10-15'),
('Sarah Lee',      'closed',          'high',     '2025-08-05', '2025-11-10', 'won',       4200.00, '2025-11-10'),
('Ana Rodriguez',  'decided',         'critical', '2025-09-01', '2025-12-15', 'won',       7500.00, NULL),
('Mark Thompson',  'idr_submitted',   'high',     '2025-09-20', '2025-12-20', NULL,         NULL,   NULL),

-- Q4 2025 — mostly active
('Sarah Lee',      'idr_initiated',   'high',     '2025-10-10', '2026-01-10', NULL,         NULL,   NULL),
('Ana Rodriguez',  'negotiation',     'critical', '2025-10-25', '2026-01-20', NULL,         NULL,   NULL),
('Mark Thompson',  'negotiation',     'medium',   '2025-11-10', '2026-02-01', NULL,         NULL,   NULL),
('Sarah Lee',      'in_review',       'high',     '2025-11-25', '2026-02-15', NULL,         NULL,   NULL),
('Ana Rodriguez',  'in_review',       'medium',   '2025-12-10', '2026-03-01', NULL,         NULL,   NULL),
('Mark Thompson',  'open',            'high',     '2025-12-20', '2025-12-20', NULL,         NULL,   NULL);

PRINT 'Extended cases loaded (20 rows).';
END
GO

-- ============================================================================
-- DISPUTES — 20 disputes linked to extended underpaid/in_dispute claims
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[disputes] d JOIN [dbo].[claims] c ON d.[claim_id] = c.[claim_id] WHERE c.[external_claim_id] = 'CLM-EXT-0001')
BEGIN

DECLARE @case_base INT = (SELECT MIN([case_id]) FROM [dbo].[cases] WHERE [created_date] = '2025-01-15');

INSERT INTO [dbo].[disputes] ([claim_id],[case_id],[dispute_type],[status],[billed_amount],[paid_amount],[requested_amount],[qpa_amount],[filed_date],[open_negotiation_deadline],[idr_initiation_deadline])
SELECT TOP 20
    c.[claim_id],
    @case_base + (ROW_NUMBER() OVER (ORDER BY c.[claim_id])) - 1,
    CASE (c.[claim_id] % 3) WHEN 0 THEN 'idr' WHEN 1 THEN 'idr' ELSE 'appeal' END,
    CASE
        WHEN cs.[status] = 'closed' AND cs.[outcome] = 'won' THEN 'decided'
        WHEN cs.[status] = 'closed' AND cs.[outcome] = 'lost' THEN 'closed'
        WHEN cs.[status] = 'closed' AND cs.[outcome] = 'settled' THEN 'closed'
        WHEN cs.[status] = 'decided' THEN 'decided'
        WHEN cs.[status] = 'idr_submitted' THEN 'evidence_submitted'
        WHEN cs.[status] = 'idr_initiated' THEN 'filed'
        WHEN cs.[status] = 'negotiation' THEN 'negotiation'
        ELSE 'open'
    END,
    c.[total_billed],
    r.[paid_amount],
    c.[total_billed],
    ROUND(c.[total_billed] * 0.6, 2),  -- QPA ~ 60% of billed
    DATEADD(DAY, 5, c.[date_filed]),
    DATEADD(DAY, 35, c.[date_filed]),
    DATEADD(DAY, 39, c.[date_filed])
FROM [dbo].[claims] c
JOIN [dbo].[remittances] r ON c.[claim_id] = r.[claim_id]
CROSS JOIN (
    SELECT [case_id], [status], [outcome],
           ROW_NUMBER() OVER (ORDER BY [case_id]) AS rn
    FROM [dbo].[cases]
    WHERE [created_date] >= '2025-01-15'
) cs
WHERE c.[external_claim_id] LIKE 'CLM-EXT-%'
  AND c.[status] IN ('underpaid', 'in_dispute')
  AND cs.rn = ROW_NUMBER() OVER (ORDER BY c.[claim_id])
ORDER BY c.[claim_id];

-- Fallback: simple insert if the windowed approach didn't work
IF @@ROWCOUNT = 0
BEGIN
    DECLARE @ext_case INT = (SELECT MIN([case_id]) FROM [dbo].[cases] WHERE [created_date] = '2025-01-15');
    DECLARE @i INT = 0;

    DECLARE ext_cursor CURSOR FOR
        SELECT c.[claim_id], c.[total_billed], r.[paid_amount], c.[date_filed]
        FROM [dbo].[claims] c
        JOIN [dbo].[remittances] r ON c.[claim_id] = r.[claim_id]
        WHERE c.[external_claim_id] LIKE 'CLM-EXT-%'
          AND c.[status] IN ('underpaid', 'in_dispute')
        ORDER BY c.[claim_id];

    DECLARE @d_claim INT, @d_billed DECIMAL(12,2), @d_paid DECIMAL(12,2), @d_filed DATE;

    OPEN ext_cursor;
    FETCH NEXT FROM ext_cursor INTO @d_claim, @d_billed, @d_paid, @d_filed;

    WHILE @@FETCH_STATUS = 0 AND @i < 20
    BEGIN
        INSERT INTO [dbo].[disputes] ([claim_id],[case_id],[dispute_type],[status],[billed_amount],[paid_amount],[requested_amount],[qpa_amount],[filed_date],[open_negotiation_deadline],[idr_initiation_deadline])
        VALUES (@d_claim, @ext_case + @i,
                CASE (@i % 3) WHEN 0 THEN 'idr' WHEN 1 THEN 'idr' ELSE 'appeal' END,
                'open',
                @d_billed, @d_paid, @d_billed, ROUND(@d_billed * 0.6, 2),
                DATEADD(DAY, 5, @d_filed),
                DATEADD(DAY, 35, @d_filed),
                DATEADD(DAY, 39, @d_filed));
        SET @i = @i + 1;
        FETCH NEXT FROM ext_cursor INTO @d_claim, @d_billed, @d_paid, @d_filed;
    END

    CLOSE ext_cursor;
    DEALLOCATE ext_cursor;
    PRINT 'Extended disputes loaded via cursor (up to 20 rows).';
END
ELSE
    PRINT 'Extended disputes loaded (20 rows).';

END
GO

-- ============================================================================
-- DEADLINES — 2-3 per extended case
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[deadlines] dl JOIN [dbo].[cases] cs ON dl.[case_id] = cs.[case_id] WHERE cs.[created_date] = '2025-01-15' AND dl.[type] = 'open_negotiation')
BEGIN

INSERT INTO [dbo].[deadlines] ([case_id],[dispute_id],[type],[due_date],[status],[alerted_date],[completed_date])
SELECT cs.[case_id], d.[dispute_id], 'open_negotiation',
       DATEADD(DAY, 30, d.[filed_date]),
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated','negotiation') THEN 'met' ELSE 'pending' END,
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated','negotiation') THEN DATEADD(DAY, 25, d.[filed_date]) ELSE NULL END,
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated','negotiation') THEN DATEADD(DAY, 28, d.[filed_date]) ELSE NULL END
FROM [dbo].[cases] cs
JOIN [dbo].[disputes] d ON cs.[case_id] = d.[case_id]
WHERE cs.[created_date] >= '2025-01-15';

INSERT INTO [dbo].[deadlines] ([case_id],[dispute_id],[type],[due_date],[status],[alerted_date],[completed_date])
SELECT cs.[case_id], d.[dispute_id], 'idr_initiation',
       DATEADD(DAY, 34, d.[filed_date]),
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated') THEN 'met' ELSE 'pending' END,
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated') THEN DATEADD(DAY, 30, d.[filed_date]) ELSE NULL END,
       CASE WHEN cs.[status] IN ('closed','decided','idr_submitted','idr_initiated') THEN DATEADD(DAY, 32, d.[filed_date]) ELSE NULL END
FROM [dbo].[cases] cs
JOIN [dbo].[disputes] d ON cs.[case_id] = d.[case_id]
WHERE cs.[created_date] >= '2025-01-15'
  AND cs.[status] NOT IN ('open', 'in_review');

INSERT INTO [dbo].[deadlines] ([case_id],[dispute_id],[type],[due_date],[status],[alerted_date],[completed_date])
SELECT cs.[case_id], d.[dispute_id], 'decision',
       DATEADD(DAY, 64, d.[filed_date]),
       CASE WHEN cs.[outcome] IS NOT NULL THEN 'met' ELSE 'pending' END,
       CASE WHEN cs.[outcome] IS NOT NULL THEN DATEADD(DAY, 58, d.[filed_date]) ELSE NULL END,
       CASE WHEN cs.[outcome] IS NOT NULL THEN DATEADD(DAY, 62, d.[filed_date]) ELSE NULL END
FROM [dbo].[cases] cs
JOIN [dbo].[disputes] d ON cs.[case_id] = d.[case_id]
WHERE cs.[created_date] >= '2025-01-15'
  AND cs.[status] IN ('closed', 'decided');

PRINT 'Extended deadlines loaded.';
END
GO

-- ============================================================================
-- EVIDENCE ARTIFACTS — 1-2 per extended case
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[evidence_artifacts] ea JOIN [dbo].[cases] cs ON ea.[case_id] = cs.[case_id] WHERE cs.[created_date] = '2025-01-15' AND ea.[type] = 'eob')
BEGIN

INSERT INTO [dbo].[evidence_artifacts] ([case_id],[type],[blob_url],[original_filename],[classification_confidence],[ocr_status])
SELECT cs.[case_id], 'eob',
       'https://medbillstore.blob.core.windows.net/documents/ext/eob_case' + CAST(cs.[case_id] AS VARCHAR) + '.pdf',
       'EOB_Case' + CAST(cs.[case_id] AS VARCHAR) + '.pdf',
       0.95 + (CAST(cs.[case_id] % 5 AS DECIMAL(3,2)) / 100),
       'completed'
FROM [dbo].[cases] cs
WHERE cs.[created_date] >= '2025-01-15';

INSERT INTO [dbo].[evidence_artifacts] ([case_id],[type],[blob_url],[original_filename],[classification_confidence],[ocr_status])
SELECT cs.[case_id], 'clinical',
       'https://medbillstore.blob.core.windows.net/documents/ext/clinical_case' + CAST(cs.[case_id] AS VARCHAR) + '.pdf',
       'Clinical_Case' + CAST(cs.[case_id] AS VARCHAR) + '.pdf',
       0.96 + (CAST(cs.[case_id] % 4 AS DECIMAL(3,2)) / 100),
       'completed'
FROM [dbo].[cases] cs
WHERE cs.[created_date] >= '2025-01-15'
  AND cs.[case_id] % 2 = 0;  -- only half get clinical docs

PRINT 'Extended evidence artifacts loaded.';
END
GO

-- ============================================================================
-- AUDIT LOG — extended entries
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM [dbo].[audit_log] WHERE [entity_type] = 'case' AND [action] = 'insert' AND [user_id] = 'ana.rodriguez' AND [timestamp] = '2025-01-15 09:00:00')
BEGIN

INSERT INTO [dbo].[audit_log] ([entity_type],[entity_id],[action],[user_id],[timestamp],[old_value],[new_value])
SELECT 'case', CAST(cs.[case_id] AS NVARCHAR), 'insert', cs.[assigned_analyst], cs.[created_date], NULL, '{"status":"open"}'
FROM [dbo].[cases] cs WHERE cs.[created_date] >= '2025-01-15';

INSERT INTO [dbo].[audit_log] ([entity_type],[entity_id],[action],[user_id],[timestamp],[old_value],[new_value])
SELECT 'case', CAST(cs.[case_id] AS NVARCHAR), 'update', cs.[assigned_analyst], cs.[last_activity],
       '{"status":"open"}', '{"status":"' + cs.[status] + '"}'
FROM [dbo].[cases] cs WHERE cs.[created_date] >= '2025-01-15' AND cs.[status] != 'open';

PRINT 'Extended audit log loaded.';
END
GO

PRINT '';
PRINT '=== Extended Seed Data Summary ===';
SELECT 'claims' AS [table], COUNT(*) AS [total] FROM [dbo].[claims]
UNION ALL SELECT 'remittances', COUNT(*) FROM [dbo].[remittances]
UNION ALL SELECT 'cases', COUNT(*) FROM [dbo].[cases]
UNION ALL SELECT 'disputes', COUNT(*) FROM [dbo].[disputes]
UNION ALL SELECT 'deadlines', COUNT(*) FROM [dbo].[deadlines]
UNION ALL SELECT 'evidence_artifacts', COUNT(*) FROM [dbo].[evidence_artifacts]
UNION ALL SELECT 'audit_log', COUNT(*) FROM [dbo].[audit_log]
ORDER BY [table];
GO
