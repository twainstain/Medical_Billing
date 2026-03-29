-- ============================================================================
-- Medical Billing Arbitration — Full Database Verification Script
-- Run after: run_sql.sh (schema + staging + seed + extended + gold views)
-- Format: [dbo].[table] for Azure Data Studio / VS Code / SSMS
-- ============================================================================

SET NOCOUNT ON;

-- ============================================================================
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 1: TABLE EXISTENCE CHECK                          ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    t.[name] AS [table_name],
    CASE WHEN t.[name] IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS [status]
FROM (VALUES
    ('payers'), ('providers'), ('patients'), ('fee_schedule'),
    ('claims'), ('claim_lines'), ('remittances'),
    ('cases'), ('disputes'), ('deadlines'),
    ('evidence_artifacts'), ('audit_log'), ('dead_letter_queue'),
    ('claim_id_alias'),
    ('cdc_watermarks'), ('stg_fee_schedule'), ('stg_providers'), ('stg_backfill_claims')
) AS expected([name])
LEFT JOIN sys.tables t ON t.[name] = expected.[name]
ORDER BY
    CASE WHEN t.[name] IS NULL THEN 0 ELSE 1 END,  -- MISSING first
    expected.[name];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 2: STORED PROCEDURES                              ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    expected.[name] AS [procedure_name],
    CASE WHEN p.[name] IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS [status]
FROM (VALUES
    ('usp_merge_fee_schedule'), ('usp_merge_providers'), ('usp_update_cdc_watermark')
) AS expected([name])
LEFT JOIN sys.procedures p ON p.[name] = expected.[name]
ORDER BY expected.[name];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 3: GOLD VIEW EXISTENCE                            ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    expected.[name] AS [view_name],
    CASE WHEN v.[name] IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS [status]
FROM (VALUES
    ('gold_recovery_by_payer'), ('gold_cpt_analysis'), ('gold_payer_scorecard'),
    ('gold_financial_summary'), ('gold_claims_aging'), ('gold_case_pipeline'),
    ('gold_deadline_compliance'), ('gold_underpayment_detection'),
    ('gold_win_loss_analysis'), ('gold_analyst_productivity'),
    ('gold_time_to_resolution'), ('gold_provider_performance'), ('gold_monthly_trends')
) AS expected([name])
LEFT JOIN sys.views v ON v.[name] = expected.[name]
ORDER BY
    CASE WHEN v.[name] IS NULL THEN 0 ELSE 1 END,
    expected.[name];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 4: INDEX EXISTENCE                                ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    expected.[name] AS [index_name],
    CASE WHEN i.[name] IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END AS [status]
FROM (VALUES
    ('IX_fee_schedule_lookup'), ('IX_deadlines_due'),
    ('IX_audit_log_entity'), ('IX_audit_log_time'),
    ('UX_evidence_hash'), ('IX_dlq_pending'),
    ('IX_stg_fs_lookup'), ('IX_stg_prov_npi')
) AS expected([name])
LEFT JOIN sys.indexes i ON i.[name] = expected.[name]
ORDER BY
    CASE WHEN i.[name] IS NULL THEN 0 ELSE 1 END,
    expected.[name];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 5: ROW COUNTS (seed data verification)            ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT [table], [rows],
    CASE
        WHEN [table] = 'payers'              AND [rows] >= 8   THEN 'OK'
        WHEN [table] = 'providers'           AND [rows] >= 6   THEN 'OK'
        WHEN [table] = 'patients'            AND [rows] >= 10  THEN 'OK'
        WHEN [table] = 'fee_schedule'        AND [rows] >= 24  THEN 'OK'
        WHEN [table] = 'claims'              AND [rows] >= 60  THEN 'OK (base+extended)'
        WHEN [table] = 'claim_lines'         AND [rows] >= 60  THEN 'OK (base+extended)'
        WHEN [table] = 'remittances'         AND [rows] >= 58  THEN 'OK (base+extended)'
        WHEN [table] = 'cases'               AND [rows] >= 26  THEN 'OK (base+extended)'
        WHEN [table] = 'disputes'            AND [rows] >= 26  THEN 'OK (base+extended)'
        WHEN [table] = 'deadlines'           AND [rows] >= 50  THEN 'OK (base+extended)'
        WHEN [table] = 'evidence_artifacts'  AND [rows] >= 40  THEN 'OK (base+extended)'
        WHEN [table] = 'audit_log'           AND [rows] >= 40  THEN 'OK (base+extended)'
        WHEN [table] = 'dead_letter_queue'   AND [rows] >= 0   THEN 'OK (empty is fine)'
        WHEN [rows] = 0                                         THEN 'EMPTY — seed data missing'
        ELSE 'LOW — may need re-seed'
    END AS [status]
FROM (
    SELECT 'payers'              AS [table], COUNT(*) AS [rows] FROM [dbo].[payers]
    UNION ALL SELECT 'providers',           COUNT(*) FROM [dbo].[providers]
    UNION ALL SELECT 'patients',            COUNT(*) FROM [dbo].[patients]
    UNION ALL SELECT 'fee_schedule',        COUNT(*) FROM [dbo].[fee_schedule]
    UNION ALL SELECT 'claims',              COUNT(*) FROM [dbo].[claims]
    UNION ALL SELECT 'claim_lines',         COUNT(*) FROM [dbo].[claim_lines]
    UNION ALL SELECT 'remittances',         COUNT(*) FROM [dbo].[remittances]
    UNION ALL SELECT 'cases',               COUNT(*) FROM [dbo].[cases]
    UNION ALL SELECT 'disputes',            COUNT(*) FROM [dbo].[disputes]
    UNION ALL SELECT 'deadlines',           COUNT(*) FROM [dbo].[deadlines]
    UNION ALL SELECT 'evidence_artifacts',  COUNT(*) FROM [dbo].[evidence_artifacts]
    UNION ALL SELECT 'audit_log',           COUNT(*) FROM [dbo].[audit_log]
    UNION ALL SELECT 'dead_letter_queue',   COUNT(*) FROM [dbo].[dead_letter_queue]
) counts
ORDER BY [table];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 6: GOLD VIEW ROW COUNTS                           ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT [view], [rows],
    CASE
        WHEN [rows] > 0 THEN 'OK'
        ELSE 'EMPTY — check dependencies'
    END AS [status]
FROM (
    SELECT 'gold_recovery_by_payer'      AS [view], COUNT(*) AS [rows] FROM [dbo].[gold_recovery_by_payer]
    UNION ALL SELECT 'gold_cpt_analysis',           COUNT(*) FROM [dbo].[gold_cpt_analysis]
    UNION ALL SELECT 'gold_payer_scorecard',        COUNT(*) FROM [dbo].[gold_payer_scorecard]
    UNION ALL SELECT 'gold_financial_summary',      COUNT(*) FROM [dbo].[gold_financial_summary]
    UNION ALL SELECT 'gold_claims_aging',           COUNT(*) FROM [dbo].[gold_claims_aging]
    UNION ALL SELECT 'gold_case_pipeline',          COUNT(*) FROM [dbo].[gold_case_pipeline]
    UNION ALL SELECT 'gold_deadline_compliance',    COUNT(*) FROM [dbo].[gold_deadline_compliance]
    UNION ALL SELECT 'gold_underpayment_detection', COUNT(*) FROM [dbo].[gold_underpayment_detection]
    UNION ALL SELECT 'gold_win_loss_analysis',      COUNT(*) FROM [dbo].[gold_win_loss_analysis]
    UNION ALL SELECT 'gold_analyst_productivity',   COUNT(*) FROM [dbo].[gold_analyst_productivity]
    UNION ALL SELECT 'gold_time_to_resolution',     COUNT(*) FROM [dbo].[gold_time_to_resolution]
    UNION ALL SELECT 'gold_provider_performance',   COUNT(*) FROM [dbo].[gold_provider_performance]
    UNION ALL SELECT 'gold_monthly_trends',         COUNT(*) FROM [dbo].[gold_monthly_trends]
) counts
ORDER BY [view];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 7: REFERENTIAL INTEGRITY                          ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT [check], [orphan_count],
    CASE WHEN [orphan_count] = 0 THEN 'OK' ELSE 'BROKEN — orphan rows found' END AS [status]
FROM (
    SELECT 'claims → payers (payer_id)' AS [check],
        (SELECT COUNT(*) FROM [dbo].[claims] c LEFT JOIN [dbo].[payers] p ON c.[payer_id] = p.[payer_id] WHERE p.[payer_id] IS NULL) AS [orphan_count]
    UNION ALL SELECT 'claims → providers (provider_npi)',
        (SELECT COUNT(*) FROM [dbo].[claims] c LEFT JOIN [dbo].[providers] pr ON c.[provider_npi] = pr.[npi] WHERE pr.[npi] IS NULL)
    UNION ALL SELECT 'claims → patients (patient_id)',
        (SELECT COUNT(*) FROM [dbo].[claims] c LEFT JOIN [dbo].[patients] p ON c.[patient_id] = p.[patient_id] WHERE p.[patient_id] IS NULL)
    UNION ALL SELECT 'remittances → claims (claim_id)',
        (SELECT COUNT(*) FROM [dbo].[remittances] r LEFT JOIN [dbo].[claims] c ON r.[claim_id] = c.[claim_id] WHERE c.[claim_id] IS NULL)
    UNION ALL SELECT 'disputes → claims (claim_id)',
        (SELECT COUNT(*) FROM [dbo].[disputes] d LEFT JOIN [dbo].[claims] c ON d.[claim_id] = c.[claim_id] WHERE c.[claim_id] IS NULL)
    UNION ALL SELECT 'disputes → cases (case_id)',
        (SELECT COUNT(*) FROM [dbo].[disputes] d LEFT JOIN [dbo].[cases] cs ON d.[case_id] = cs.[case_id] WHERE cs.[case_id] IS NULL)
    UNION ALL SELECT 'deadlines → cases (case_id)',
        (SELECT COUNT(*) FROM [dbo].[deadlines] dl LEFT JOIN [dbo].[cases] cs ON dl.[case_id] = cs.[case_id] WHERE cs.[case_id] IS NULL)
    UNION ALL SELECT 'deadlines → disputes (dispute_id)',
        (SELECT COUNT(*) FROM [dbo].[deadlines] dl LEFT JOIN [dbo].[disputes] d ON dl.[dispute_id] = d.[dispute_id] WHERE d.[dispute_id] IS NULL)
    UNION ALL SELECT 'evidence → cases (case_id)',
        (SELECT COUNT(*) FROM [dbo].[evidence_artifacts] ea WHERE ea.[case_id] IS NOT NULL AND ea.[case_id] NOT IN (SELECT [case_id] FROM [dbo].[cases]))
) checks
ORDER BY
    CASE WHEN [orphan_count] > 0 THEN 0 ELSE 1 END,
    [check];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 8: DATA QUALITY CHECKS                            ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT [check], [count],
    CASE
        WHEN [check] LIKE '%should%0%' AND [count] = 0 THEN 'OK'
        WHEN [check] LIKE '%should%0%' AND [count] > 0 THEN 'ISSUE'
        WHEN [check] LIKE '%should >%' AND [count] > 0 THEN 'OK'
        WHEN [check] LIKE '%should >%' AND [count] = 0 THEN 'ISSUE'
        ELSE 'INFO'
    END AS [status]
FROM (
    -- Claims with negative billed (should be 0)
    SELECT 'claims with negative total_billed (should be 0)' AS [check],
        (SELECT COUNT(*) FROM [dbo].[claims] WHERE [total_billed] < 0) AS [count]
    -- Remittances with negative paid (should be 0)
    UNION ALL SELECT 'remittances with negative paid_amount (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[remittances] WHERE [paid_amount] < 0)
    -- Disputes where paid > billed (should be 0)
    UNION ALL SELECT 'disputes where paid > billed (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[disputes] WHERE [paid_amount] > [billed_amount])
    -- Cases with outcome but no closed_date (should be 0)
    UNION ALL SELECT 'decided cases without outcome (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[cases] WHERE [status] = 'decided' AND [outcome] IS NULL)
    -- Met deadlines without completed_date (should be 0)
    UNION ALL SELECT 'met deadlines without completed_date (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[deadlines] WHERE [status] = 'met' AND [completed_date] IS NULL)
    -- Claims with valid statuses
    UNION ALL SELECT 'claims with invalid status (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[claims] WHERE [status] NOT IN ('filed','acknowledged','paid','denied','underpaid','in_dispute','resolved'))
    -- Cases with valid statuses
    UNION ALL SELECT 'cases with invalid status (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[cases] WHERE [status] NOT IN ('open','in_review','negotiation','idr_initiated','idr_submitted','decided','closed'))
    -- Disputes with valid types
    UNION ALL SELECT 'disputes with invalid type (should be 0)',
        (SELECT COUNT(*) FROM [dbo].[disputes] WHERE [dispute_type] NOT IN ('appeal','idr','state_arb','external_review','underpayment'))
    -- Monthly coverage (should > 0)
    UNION ALL SELECT 'distinct months in claims (should > 6 for trends)',
        (SELECT COUNT(DISTINCT FORMAT([date_of_service], 'yyyy-MM')) FROM [dbo].[claims])
    -- Payer coverage
    UNION ALL SELECT 'payers with claims (should > 4)',
        (SELECT COUNT(DISTINCT [payer_id]) FROM [dbo].[claims])
    -- Provider coverage
    UNION ALL SELECT 'providers with claims (should > 3)',
        (SELECT COUNT(DISTINCT [provider_npi]) FROM [dbo].[claims])
    -- Cases with outcomes (for win/loss analysis)
    UNION ALL SELECT 'cases with outcomes (should > 5 for win/loss)',
        (SELECT COUNT(*) FROM [dbo].[cases] WHERE [outcome] IS NOT NULL)
    -- Analyst coverage
    UNION ALL SELECT 'distinct analysts (should > 2)',
        (SELECT COUNT(DISTINCT [assigned_analyst]) FROM [dbo].[cases])
) checks
ORDER BY
    CASE
        WHEN [check] LIKE '%should be 0%' AND [count] > 0 THEN 0
        WHEN [check] LIKE '%should >%' AND [count] = 0 THEN 1
        ELSE 2
    END,
    [check];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 9: FINANCIAL TOTALS                               ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    (SELECT COUNT(*)          FROM [dbo].[claims])         AS [total_claims],
    (SELECT SUM([total_billed]) FROM [dbo].[claims])       AS [total_billed],
    (SELECT SUM([paid_amount])  FROM [dbo].[remittances])  AS [total_paid],
    (SELECT SUM([total_billed]) FROM [dbo].[claims]) -
    (SELECT SUM([paid_amount])  FROM [dbo].[remittances])  AS [total_underpayment],
    ROUND(
        (SELECT SUM([paid_amount]) FROM [dbo].[remittances]) * 100.0 /
        NULLIF((SELECT SUM([total_billed]) FROM [dbo].[claims]), 0)
    , 2)                                                    AS [recovery_rate_pct],
    (SELECT COUNT(*) FROM [dbo].[remittances]
     WHERE [denial_code] IS NOT NULL AND [denial_code] != '') AS [denial_count],
    (SELECT SUM([award_amount]) FROM [dbo].[cases]
     WHERE [award_amount] IS NOT NULL)                      AS [total_awards];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  SECTION 10: MONTHLY TREND DATA (for Gold views)           ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
-- ============================================================================

SELECT
    FORMAT(c.[date_of_service], 'yyyy-MM') AS [month],
    COUNT(*)                               AS [claims],
    SUM(c.[total_billed])                  AS [billed],
    COALESCE(SUM(r.[paid_amount]), 0)      AS [paid],
    SUM(c.[total_billed]) - COALESCE(SUM(r.[paid_amount]), 0) AS [underpayment]
FROM [dbo].[claims] c
LEFT JOIN [dbo].[remittances] r ON c.[claim_id] = r.[claim_id]
GROUP BY FORMAT(c.[date_of_service], 'yyyy-MM')
ORDER BY [month];
GO

-- ============================================================================
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  VERIFICATION COMPLETE                                     ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';
PRINT 'Review results above. Look for:';
PRINT '  - MISSING in sections 1-4 → re-run run_sql.sh';
PRINT '  - EMPTY in section 5-6   → re-run seed_data.sql + seed_data_extended.sql';
PRINT '  - BROKEN in section 7    → referential integrity issue';
PRINT '  - ISSUE in section 8     → data quality problem';
PRINT '';
GO
