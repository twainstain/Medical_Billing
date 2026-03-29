-- ============================================================================
-- Gold Layer Views — Business-ready aggregations for AI Agent + Power BI
-- Mirrors the 8 Gold tables from olap/gold.py, queryable via SQL
-- ============================================================================

-- 1. Recovery by Payer
CREATE OR ALTER VIEW gold_recovery_by_payer AS
SELECT
    p.payer_id,
    p.name                                          AS payer_name,
    COUNT(c.claim_id)                               AS total_claims,
    SUM(c.total_billed)                             AS total_billed,
    COALESCE(SUM(r.total_paid), 0)                  AS total_paid,
    SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0) AS total_underpayment,
    SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) AS denial_count,
    CASE WHEN SUM(c.total_billed) > 0
         THEN ROUND(COALESCE(SUM(r.total_paid), 0) * 100.0 / SUM(c.total_billed), 2)
         ELSE 0 END                                 AS recovery_rate_pct,
    CASE WHEN COUNT(c.claim_id) > 0
         THEN ROUND(SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(c.claim_id), 2)
         ELSE 0 END                                 AS denial_rate_pct
FROM claims c
JOIN payers p ON c.payer_id = p.payer_id
LEFT JOIN (
    SELECT claim_id,
           SUM(paid_amount)   AS total_paid,
           MAX(CASE WHEN denial_code IS NOT NULL AND denial_code != '' THEN 1 ELSE 0 END) AS has_denial
    FROM remittances
    GROUP BY claim_id
) r ON c.claim_id = r.claim_id
GROUP BY p.payer_id, p.name;
GO

-- 2. CPT Code Analysis
CREATE OR ALTER VIEW gold_cpt_analysis AS
SELECT
    cl.cpt_code,
    COUNT(DISTINCT c.claim_id)                      AS claim_count,
    SUM(cl.billed_amount)                           AS total_billed,
    COALESCE(SUM(r.paid_per_line), 0)               AS total_paid,
    ROUND(AVG(cl.billed_amount), 2)                 AS avg_billed,
    ROUND(AVG(COALESCE(r.paid_per_line, 0)), 2)     AS avg_paid,
    CASE WHEN SUM(cl.billed_amount) > 0
         THEN ROUND(COALESCE(SUM(r.paid_per_line), 0) * 100.0 / SUM(cl.billed_amount), 2)
         ELSE 0 END                                 AS payment_ratio_pct,
    mc.rate                                         AS medicare_rate,
    fh.rate                                         AS fair_health_rate
FROM claim_lines cl
JOIN claims c ON cl.claim_id = c.claim_id
LEFT JOIN (
    SELECT rm.claim_id,
           rm.paid_amount / NULLIF(lc.line_count, 0) AS paid_per_line
    FROM remittances rm
    CROSS APPLY (
        SELECT COUNT(*) AS line_count FROM claim_lines WHERE claim_id = rm.claim_id
    ) lc
) r ON c.claim_id = r.claim_id
LEFT JOIN fee_schedule mc
    ON cl.cpt_code = mc.cpt_code AND mc.rate_type = 'medicare' AND mc.is_current = 1
LEFT JOIN fee_schedule fh
    ON cl.cpt_code = fh.cpt_code AND fh.rate_type = 'fair_health' AND fh.is_current = 1
GROUP BY cl.cpt_code, mc.rate, fh.rate;
GO

-- 3. Payer Scorecard
CREATE OR ALTER VIEW gold_payer_scorecard AS
SELECT
    p.payer_id,
    p.name                                          AS payer_name,
    p.type                                          AS payer_type,
    COUNT(c.claim_id)                               AS total_claims,
    SUM(c.total_billed)                             AS total_billed,
    COALESCE(SUM(r.total_paid), 0)                  AS total_paid,
    SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0) AS total_underpayment,
    ROUND(AVG(c.total_billed - COALESCE(r.total_paid, 0)), 2) AS avg_underpayment,
    SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) AS denial_count,
    CASE WHEN SUM(c.total_billed) > 0
         THEN ROUND(COALESCE(SUM(r.total_paid), 0) * 100.0 / SUM(c.total_billed), 2)
         ELSE 0 END                                 AS payment_rate_pct,
    CASE WHEN COUNT(c.claim_id) > 0
         THEN ROUND(SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(c.claim_id), 2)
         ELSE 0 END                                 AS denial_rate_pct,
    CASE
        WHEN COALESCE(SUM(r.total_paid), 0) * 100.0 / NULLIF(SUM(c.total_billed), 1) >= 80
             AND SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(c.claim_id), 0) <= 10
        THEN 'low'
        WHEN COALESCE(SUM(r.total_paid), 0) * 100.0 / NULLIF(SUM(c.total_billed), 1) >= 50
             OR SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(c.claim_id), 0) <= 30
        THEN 'medium'
        ELSE 'high'
    END                                             AS risk_tier
FROM claims c
JOIN payers p ON c.payer_id = p.payer_id
LEFT JOIN (
    SELECT claim_id,
           SUM(paid_amount) AS total_paid,
           MAX(CASE WHEN denial_code IS NOT NULL AND denial_code != '' THEN 1 ELSE 0 END) AS has_denial
    FROM remittances
    GROUP BY claim_id
) r ON c.claim_id = r.claim_id
GROUP BY p.payer_id, p.name, p.type;
GO

-- 4. Financial Summary (pivoted as rows for flexibility)
CREATE OR ALTER VIEW gold_financial_summary AS
SELECT metric_name, metric_value FROM (
    SELECT
        CAST(COUNT(*)                                               AS DECIMAL(18,2)) AS total_claims,
        CAST(SUM(c.total_billed)                                    AS DECIMAL(18,2)) AS total_billed,
        CAST(COALESCE(SUM(r.total_paid), 0)                         AS DECIMAL(18,2)) AS total_paid,
        CAST(SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0)   AS DECIMAL(18,2)) AS total_underpayment,
        CAST(CASE WHEN SUM(c.total_billed) > 0
             THEN ROUND(COALESCE(SUM(r.total_paid), 0) * 100.0 / SUM(c.total_billed), 2)
             ELSE 0 END                                             AS DECIMAL(18,2)) AS recovery_rate_pct,
        CAST(SUM(CASE WHEN r.total_paid > 0 THEN 1 ELSE 0 END)     AS DECIMAL(18,2)) AS paid_claims,
        CAST(SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END)     AS DECIMAL(18,2)) AS denial_count,
        CAST(CASE WHEN COUNT(*) > 0
             THEN ROUND(SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
             ELSE 0 END                                             AS DECIMAL(18,2)) AS denial_rate_pct,
        CAST(CASE WHEN COUNT(*) > 0
             THEN ROUND(SUM(c.total_billed) / COUNT(*), 2)
             ELSE 0 END                                             AS DECIMAL(18,2)) AS avg_billed_per_claim,
        CAST(CASE WHEN COUNT(*) > 0
             THEN ROUND((SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0)) / COUNT(*), 2)
             ELSE 0 END                                             AS DECIMAL(18,2)) AS avg_underpayment_per_claim
    FROM claims c
    LEFT JOIN (
        SELECT claim_id,
               SUM(paid_amount)   AS total_paid,
               MAX(CASE WHEN denial_code IS NOT NULL AND denial_code != '' THEN 1 ELSE 0 END) AS has_denial
        FROM remittances
        GROUP BY claim_id
    ) r ON c.claim_id = r.claim_id
) src
UNPIVOT (
    metric_value FOR metric_name IN (
        total_claims, total_billed, total_paid, total_underpayment,
        recovery_rate_pct, paid_claims, denial_count, denial_rate_pct,
        avg_billed_per_claim, avg_underpayment_per_claim
    )
) unpvt;
GO

-- 5. Claims Aging
CREATE OR ALTER VIEW gold_claims_aging AS
SELECT
    aging_bucket,
    COUNT(*)                                        AS claim_count,
    SUM(total_billed)                               AS total_billed,
    SUM(total_billed) - COALESCE(SUM(total_paid), 0) AS total_unpaid,
    CASE WHEN (SELECT COUNT(*) FROM claims) > 0
         THEN ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM claims), 2)
         ELSE 0 END                                 AS pct_of_total
FROM (
    SELECT c.claim_id, c.total_billed,
           COALESCE(r.total_paid, 0) AS total_paid,
           CASE
               WHEN DATEDIFF(DAY, c.date_of_service, SYSUTCDATETIME()) <= 30  THEN '0-30 days'
               WHEN DATEDIFF(DAY, c.date_of_service, SYSUTCDATETIME()) <= 60  THEN '31-60 days'
               WHEN DATEDIFF(DAY, c.date_of_service, SYSUTCDATETIME()) <= 90  THEN '61-90 days'
               WHEN DATEDIFF(DAY, c.date_of_service, SYSUTCDATETIME()) <= 180 THEN '91-180 days'
               ELSE '180+ days'
           END AS aging_bucket
    FROM claims c
    LEFT JOIN (
        SELECT claim_id, SUM(paid_amount) AS total_paid
        FROM remittances GROUP BY claim_id
    ) r ON c.claim_id = r.claim_id
) bucketed
GROUP BY aging_bucket;
GO

-- 6. Case Pipeline
CREATE OR ALTER VIEW gold_case_pipeline AS
SELECT
    cs.status,
    COUNT(cs.case_id)                               AS case_count,
    COALESCE(SUM(d.billed_amount), 0)               AS total_billed,
    COALESCE(SUM(d.underpayment_amount), 0)         AS total_underpayment,
    ROUND(AVG(CAST(DATEDIFF(DAY, cs.created_date, SYSUTCDATETIME()) AS FLOAT)), 1) AS avg_age_days,
    CASE WHEN COUNT(dl.deadline_id) > 0
         THEN ROUND(SUM(CASE WHEN dl.status = 'met' THEN 1 ELSE 0 END) * 100.0 / COUNT(dl.deadline_id), 2)
         ELSE 0 END                                 AS sla_compliance_pct
FROM cases cs
LEFT JOIN disputes d ON cs.case_id = d.case_id
LEFT JOIN deadlines dl ON cs.case_id = dl.case_id
GROUP BY cs.status;
GO

-- 7. Deadline Compliance
CREATE OR ALTER VIEW gold_deadline_compliance AS
SELECT
    dl.type                                         AS deadline_type,
    COUNT(*)                                        AS total_deadlines,
    SUM(CASE WHEN dl.status = 'met' THEN 1 ELSE 0 END)     AS met_count,
    SUM(CASE WHEN dl.status = 'missed' THEN 1 ELSE 0 END)  AS missed_count,
    SUM(CASE WHEN dl.status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
    SUM(CASE WHEN dl.status = 'pending'
              AND dl.due_date BETWEEN CAST(SYSUTCDATETIME() AS DATE)
                                  AND DATEADD(DAY, 5, CAST(SYSUTCDATETIME() AS DATE))
         THEN 1 ELSE 0 END)                        AS at_risk_count,
    CASE WHEN COUNT(*) > 0
         THEN ROUND(SUM(CASE WHEN dl.status = 'met' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
         ELSE 0 END                                 AS compliance_pct
FROM deadlines dl
GROUP BY dl.type;
GO

-- 8. Underpayment Detection
CREATE OR ALTER VIEW gold_underpayment_detection AS
SELECT
    d.claim_id,
    c.payer_id,
    p.name                                          AS payer_name,
    c.provider_npi,
    c.date_of_service,
    d.billed_amount,
    d.paid_amount,
    d.qpa_amount,
    d.underpayment_amount,
    CASE WHEN d.billed_amount > 0
         THEN ROUND(d.underpayment_amount * 100.0 / d.billed_amount, 2)
         ELSE 0 END                                 AS underpayment_pct,
    CASE WHEN d.underpayment_amount > 25 AND d.billed_amount > COALESCE(d.qpa_amount, 0)
         THEN 1 ELSE 0 END                         AS arbitration_eligible,
    d.status                                        AS dispute_status
FROM disputes d
JOIN claims c ON d.claim_id = c.claim_id
JOIN payers p ON c.payer_id = p.payer_id
WHERE d.underpayment_amount > 0;
GO

-- ============================================================================
-- NEW: Business Success Metrics (views 9-13)
-- ============================================================================

-- 9. Win/Loss Analysis — outcomes by payer, dispute type, and CPT code
CREATE OR ALTER VIEW gold_win_loss_analysis AS
SELECT
    p.payer_id,
    p.name                                          AS payer_name,
    d.dispute_type,
    cs.outcome,
    COUNT(cs.case_id)                               AS case_count,
    COALESCE(SUM(d.billed_amount), 0)               AS total_billed,
    COALESCE(SUM(d.underpayment_amount), 0)         AS total_disputed,
    COALESCE(SUM(cs.award_amount), 0)               AS total_awarded,
    CASE WHEN COUNT(cs.case_id) > 0
         THEN ROUND(SUM(CASE WHEN cs.outcome = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(cs.case_id), 2)
         ELSE 0 END                                 AS win_rate_pct,
    CASE WHEN SUM(d.underpayment_amount) > 0
         THEN ROUND(COALESCE(SUM(cs.award_amount), 0) * 100.0 / SUM(d.underpayment_amount), 2)
         ELSE 0 END                                 AS recovery_roi_pct,
    ROUND(AVG(COALESCE(cs.award_amount, 0)), 2)     AS avg_award
FROM cases cs
JOIN disputes d ON cs.case_id = d.case_id
JOIN claims c ON d.claim_id = c.claim_id
JOIN payers p ON c.payer_id = p.payer_id
WHERE cs.status IN ('decided', 'closed')
GROUP BY p.payer_id, p.name, d.dispute_type, cs.outcome;
GO

-- 10. Analyst Productivity — cases per analyst with resolution metrics
CREATE OR ALTER VIEW gold_analyst_productivity AS
SELECT
    cs.assigned_analyst,
    COUNT(cs.case_id)                               AS total_cases,
    SUM(CASE WHEN cs.status NOT IN ('closed', 'decided') THEN 1 ELSE 0 END) AS active_cases,
    SUM(CASE WHEN cs.status IN ('closed', 'decided') THEN 1 ELSE 0 END)     AS resolved_cases,
    SUM(CASE WHEN cs.outcome = 'won' THEN 1 ELSE 0 END)                     AS won_cases,
    SUM(CASE WHEN cs.outcome = 'lost' THEN 1 ELSE 0 END)                    AS lost_cases,
    SUM(CASE WHEN cs.outcome = 'settled' THEN 1 ELSE 0 END)                 AS settled_cases,
    CASE WHEN SUM(CASE WHEN cs.status IN ('closed', 'decided') THEN 1 ELSE 0 END) > 0
         THEN ROUND(SUM(CASE WHEN cs.outcome = 'won' THEN 1 ELSE 0 END) * 100.0
              / SUM(CASE WHEN cs.status IN ('closed', 'decided') THEN 1 ELSE 0 END), 2)
         ELSE 0 END                                 AS win_rate_pct,
    COALESCE(SUM(cs.award_amount), 0)               AS total_recovered,
    COALESCE(SUM(d.underpayment_amount), 0)         AS total_disputed,
    ROUND(AVG(CASE WHEN cs.closed_date IS NOT NULL
              THEN CAST(DATEDIFF(DAY, cs.created_date, cs.closed_date) AS FLOAT)
              END), 1)                              AS avg_resolution_days,
    SUM(CASE WHEN cs.priority = 'critical' THEN 1 ELSE 0 END) AS critical_cases,
    SUM(CASE WHEN cs.priority = 'high' THEN 1 ELSE 0 END)     AS high_priority_cases
FROM cases cs
LEFT JOIN disputes d ON cs.case_id = d.case_id
GROUP BY cs.assigned_analyst;
GO

-- 11. Time-to-Resolution — cycle time analysis by status and priority
CREATE OR ALTER VIEW gold_time_to_resolution AS
SELECT
    cs.status,
    cs.priority,
    COUNT(cs.case_id)                               AS case_count,
    ROUND(AVG(CAST(DATEDIFF(DAY, cs.created_date, COALESCE(cs.closed_date, SYSUTCDATETIME())) AS FLOAT)), 1) AS avg_days,
    MIN(DATEDIFF(DAY, cs.created_date, COALESCE(cs.closed_date, SYSUTCDATETIME()))) AS min_days,
    MAX(DATEDIFF(DAY, cs.created_date, COALESCE(cs.closed_date, SYSUTCDATETIME()))) AS max_days,
    COALESCE(SUM(d.underpayment_amount), 0)         AS total_at_stake,
    COALESCE(SUM(cs.award_amount), 0)               AS total_recovered,
    CASE WHEN COUNT(cs.case_id) > 0
         THEN ROUND(SUM(CASE WHEN cs.outcome = 'won' THEN 1 ELSE 0 END) * 100.0 / COUNT(cs.case_id), 2)
         ELSE 0 END                                 AS win_rate_pct
FROM cases cs
LEFT JOIN disputes d ON cs.case_id = d.case_id
GROUP BY cs.status, cs.priority;
GO

-- 12. Provider Performance — recovery and dispute metrics by provider
CREATE OR ALTER VIEW gold_provider_performance AS
SELECT
    pr.npi                                          AS provider_npi,
    pr.name                                         AS provider_name,
    pr.specialty,
    pr.facility_name,
    COUNT(DISTINCT c.claim_id)                      AS total_claims,
    COUNT(DISTINCT d.dispute_id)                    AS total_disputes,
    SUM(c.total_billed)                             AS total_billed,
    COALESCE(SUM(r.total_paid), 0)                  AS total_paid,
    SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0) AS total_underpayment,
    CASE WHEN SUM(c.total_billed) > 0
         THEN ROUND(COALESCE(SUM(r.total_paid), 0) * 100.0 / SUM(c.total_billed), 2)
         ELSE 0 END                                 AS payment_rate_pct,
    CASE WHEN COUNT(DISTINCT c.claim_id) > 0
         THEN ROUND(COUNT(DISTINCT d.dispute_id) * 100.0 / COUNT(DISTINCT c.claim_id), 2)
         ELSE 0 END                                 AS dispute_rate_pct,
    COALESCE(SUM(cs.award_amount), 0)               AS total_recovered
FROM providers pr
JOIN claims c ON pr.npi = c.provider_npi
LEFT JOIN (
    SELECT claim_id, SUM(paid_amount) AS total_paid
    FROM remittances GROUP BY claim_id
) r ON c.claim_id = r.claim_id
LEFT JOIN disputes d ON c.claim_id = d.claim_id
LEFT JOIN cases cs ON d.case_id = cs.case_id
GROUP BY pr.npi, pr.name, pr.specialty, pr.facility_name;
GO

-- 13. Monthly Trends — volume and financial metrics by month
CREATE OR ALTER VIEW gold_monthly_trends AS
SELECT
    FORMAT(c.date_of_service, 'yyyy-MM')            AS month,
    COUNT(c.claim_id)                               AS claim_count,
    SUM(c.total_billed)                             AS total_billed,
    COALESCE(SUM(r.total_paid), 0)                  AS total_paid,
    SUM(c.total_billed) - COALESCE(SUM(r.total_paid), 0) AS total_underpayment,
    CASE WHEN SUM(c.total_billed) > 0
         THEN ROUND(COALESCE(SUM(r.total_paid), 0) * 100.0 / SUM(c.total_billed), 2)
         ELSE 0 END                                 AS recovery_rate_pct,
    SUM(CASE WHEN r.has_denial = 1 THEN 1 ELSE 0 END) AS denial_count,
    COUNT(DISTINCT d.dispute_id)                    AS new_disputes,
    COUNT(DISTINCT CASE WHEN cs.status IN ('closed', 'decided') THEN cs.case_id END) AS resolved_cases,
    COALESCE(SUM(cs.award_amount), 0)               AS total_awarded
FROM claims c
LEFT JOIN (
    SELECT claim_id,
           SUM(paid_amount) AS total_paid,
           MAX(CASE WHEN denial_code IS NOT NULL AND denial_code != '' THEN 1 ELSE 0 END) AS has_denial
    FROM remittances GROUP BY claim_id
) r ON c.claim_id = r.claim_id
LEFT JOIN disputes d ON c.claim_id = d.claim_id
LEFT JOIN cases cs ON d.case_id = cs.case_id
GROUP BY FORMAT(c.date_of_service, 'yyyy-MM');
GO

PRINT 'Gold views created successfully (13 views).';
GO
