-- ============================================================================
-- Medical Billing Arbitration — OLTP Data Verification Script
-- Run against: medbill_oltp on Azure SQL
-- Format: [dbo].[table] for Azure Data Studio / VS Code / SSMS
-- ============================================================================

PRINT '=== 1. TABLE ROW COUNTS ===';
SELECT 'payers'             AS [table], COUNT(*) AS [rows] FROM [dbo].[payers]
UNION ALL SELECT 'providers',        COUNT(*) FROM [dbo].[providers]
UNION ALL SELECT 'patients',         COUNT(*) FROM [dbo].[patients]
UNION ALL SELECT 'fee_schedule',     COUNT(*) FROM [dbo].[fee_schedule]
UNION ALL SELECT 'claims',           COUNT(*) FROM [dbo].[claims]
UNION ALL SELECT 'claim_lines',      COUNT(*) FROM [dbo].[claim_lines]
UNION ALL SELECT 'remittances',      COUNT(*) FROM [dbo].[remittances]
UNION ALL SELECT 'cases',            COUNT(*) FROM [dbo].[cases]
UNION ALL SELECT 'disputes',         COUNT(*) FROM [dbo].[disputes]
UNION ALL SELECT 'deadlines',        COUNT(*) FROM [dbo].[deadlines]
UNION ALL SELECT 'evidence_artifacts', COUNT(*) FROM [dbo].[evidence_artifacts]
UNION ALL SELECT 'audit_log',        COUNT(*) FROM [dbo].[audit_log]
UNION ALL SELECT 'dead_letter_queue', COUNT(*) FROM [dbo].[dead_letter_queue]
ORDER BY [table];
GO

-- ============================================================================
PRINT '=== 2. CLAIMS — Recent + Status Breakdown ===';

SELECT [claim_id], [external_claim_id], [payer_id], [provider_npi],
       [total_billed], [status], [date_of_service], [created_at]
FROM [dbo].[claims]
ORDER BY [created_at] DESC;

SELECT [status], COUNT(*) AS [count], SUM([total_billed]) AS [total_billed]
FROM [dbo].[claims]
GROUP BY [status]
ORDER BY [count] DESC;
GO

-- ============================================================================
PRINT '=== 3. CLAIMS + REMITTANCES — Underpayment Check ===';

SELECT c.[claim_id], c.[external_claim_id],
       p.[name] AS [payer_name],
       c.[total_billed],
       COALESCE(r.[total_paid], 0) AS [total_paid],
       c.[total_billed] - COALESCE(r.[total_paid], 0) AS [underpayment],
       c.[status]
FROM [dbo].[claims] c
JOIN [dbo].[payers] p ON c.[payer_id] = p.[payer_id]
LEFT JOIN (
    SELECT [claim_id], SUM([paid_amount]) AS [total_paid]
    FROM [dbo].[remittances]
    GROUP BY [claim_id]
) r ON c.[claim_id] = r.[claim_id]
ORDER BY [underpayment] DESC;
GO

-- ============================================================================
PRINT '=== 4. REMITTANCES — Denials ===';

SELECT r.[remit_id], r.[claim_id], r.[paid_amount], r.[allowed_amount],
       r.[denial_code], r.[adjustment_reason], r.[source_type], r.[era_date]
FROM [dbo].[remittances] r
WHERE r.[denial_code] IS NOT NULL AND r.[denial_code] != ''
ORDER BY r.[era_date] DESC;
GO

-- ============================================================================
PRINT '=== 5. CASES — Pipeline Status ===';

SELECT cs.[case_id], cs.[assigned_analyst], cs.[status], cs.[priority],
       cs.[outcome], cs.[award_amount], cs.[created_date], cs.[closed_date]
FROM [dbo].[cases] cs
ORDER BY cs.[created_date] DESC;

SELECT [status], COUNT(*) AS [count]
FROM [dbo].[cases]
GROUP BY [status]
ORDER BY [count] DESC;
GO

-- ============================================================================
PRINT '=== 6. DISPUTES — Active ===';

SELECT d.[dispute_id], d.[claim_id], d.[case_id], d.[dispute_type],
       d.[status], d.[billed_amount], d.[paid_amount],
       d.[billed_amount] - d.[paid_amount] AS [underpayment_amount],
       d.[qpa_amount], d.[filed_date]
FROM [dbo].[disputes] d
ORDER BY [underpayment_amount] DESC;
GO

-- ============================================================================
PRINT '=== 7. DEADLINES — At Risk + Missed ===';

SELECT dl.[deadline_id], dl.[case_id], dl.[type], dl.[due_date], dl.[status],
       DATEDIFF(DAY, CAST(SYSUTCDATETIME() AS DATE), dl.[due_date]) AS [days_remaining]
FROM [dbo].[deadlines] dl
WHERE dl.[status] IN ('pending', 'alerted')
ORDER BY dl.[due_date] ASC;

SELECT [status], COUNT(*) AS [count]
FROM [dbo].[deadlines]
GROUP BY [status]
ORDER BY [count] DESC;
GO

-- ============================================================================
PRINT '=== 8. DEAD-LETTER QUEUE — Pending Failures ===';

SELECT [dlq_id], [source], [entity_type], [entity_id],
       [error_category], [error_detail], [created_at]
FROM [dbo].[dead_letter_queue]
WHERE [status] = 'pending'
ORDER BY [created_at] DESC;
GO

-- ============================================================================
PRINT '=== 9. AUDIT LOG — Recent Activity ===';

SELECT TOP 20
       [log_id], [entity_type], [entity_id], [action], [user_id],
       [timestamp], [ai_agent]
FROM [dbo].[audit_log]
ORDER BY [timestamp] DESC;
GO

-- ============================================================================
PRINT '=== 10. GOLD VIEWS — Quick Check ===';

SELECT 'gold_financial_summary'      AS [view], COUNT(*) AS [rows] FROM [dbo].[gold_financial_summary]
UNION ALL SELECT 'gold_recovery_by_payer',    COUNT(*) FROM [dbo].[gold_recovery_by_payer]
UNION ALL SELECT 'gold_payer_scorecard',      COUNT(*) FROM [dbo].[gold_payer_scorecard]
UNION ALL SELECT 'gold_claims_aging',         COUNT(*) FROM [dbo].[gold_claims_aging]
UNION ALL SELECT 'gold_case_pipeline',        COUNT(*) FROM [dbo].[gold_case_pipeline]
UNION ALL SELECT 'gold_deadline_compliance',  COUNT(*) FROM [dbo].[gold_deadline_compliance]
UNION ALL SELECT 'gold_underpayment_detection', COUNT(*) FROM [dbo].[gold_underpayment_detection]
UNION ALL SELECT 'gold_win_loss_analysis',    COUNT(*) FROM [dbo].[gold_win_loss_analysis]
UNION ALL SELECT 'gold_analyst_productivity', COUNT(*) FROM [dbo].[gold_analyst_productivity]
UNION ALL SELECT 'gold_provider_performance', COUNT(*) FROM [dbo].[gold_provider_performance]
UNION ALL SELECT 'gold_monthly_trends',       COUNT(*) FROM [dbo].[gold_monthly_trends]
ORDER BY [view];
GO

-- ============================================================================
PRINT '=== 11. FINANCIAL SUMMARY (from Gold view) ===';

SELECT [metric_name], [metric_value]
FROM [dbo].[gold_financial_summary]
ORDER BY [metric_name];
GO

-- ============================================================================
PRINT '=== 12. PAYER SCORECARD (from Gold view) ===';

SELECT [payer_name], [total_claims], [total_billed], [total_paid],
       [total_underpayment], [payment_rate_pct], [denial_rate_pct], [risk_tier]
FROM [dbo].[gold_payer_scorecard]
ORDER BY [total_underpayment] DESC;
GO

-- ============================================================================
PRINT '=== 13. REFERENTIAL INTEGRITY CHECK ===';

SELECT 'orphan remittances (no claim)' AS [check],
       COUNT(*) AS [count]
FROM [dbo].[remittances] r
LEFT JOIN [dbo].[claims] c ON r.[claim_id] = c.[claim_id]
WHERE c.[claim_id] IS NULL
UNION ALL
SELECT 'orphan disputes (no claim)',
       COUNT(*)
FROM [dbo].[disputes] d
LEFT JOIN [dbo].[claims] c ON d.[claim_id] = c.[claim_id]
WHERE c.[claim_id] IS NULL
UNION ALL
SELECT 'orphan disputes (no case)',
       COUNT(*)
FROM [dbo].[disputes] d
LEFT JOIN [dbo].[cases] cs ON d.[case_id] = cs.[case_id]
WHERE cs.[case_id] IS NULL
UNION ALL
SELECT 'orphan deadlines (no case)',
       COUNT(*)
FROM [dbo].[deadlines] dl
LEFT JOIN [dbo].[cases] cs ON dl.[case_id] = cs.[case_id]
WHERE cs.[case_id] IS NULL;
GO

PRINT '=== VERIFICATION COMPLETE ===';
GO
