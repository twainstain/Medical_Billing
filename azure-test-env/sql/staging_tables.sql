-- ============================================================================
-- Medical Billing Arbitration — Staging & CDC Watermark Tables (Azure SQL)
-- Run AFTER schema.sql — these support ADF batch loads and CDC tracking
-- ============================================================================

-- ============================================================================
-- CDC Watermark Tracking (used by ADF incremental copy pipelines)
-- ============================================================================

CREATE TABLE cdc_watermarks (
    source_table    NVARCHAR(128)   PRIMARY KEY,
    last_sync_ts    DATETIME2       NOT NULL DEFAULT '1900-01-01',
    rows_synced     INT             NOT NULL DEFAULT 0,
    total_inserts   BIGINT          NOT NULL DEFAULT 0,
    total_updates   BIGINT          NOT NULL DEFAULT 0,
    total_deletes   BIGINT          NOT NULL DEFAULT 0,
    sync_status     VARCHAR(20)     NOT NULL DEFAULT 'ok',
    updated_at      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- Staging: Fee Schedule (landing zone for batch CSV uploads)
-- ADF copies CSVs from Bronze blob → stg_fee_schedule → MERGE into fee_schedule
-- ============================================================================

CREATE TABLE stg_fee_schedule (
    payer_id        NVARCHAR(50)    NOT NULL,
    cpt_code        VARCHAR(10)     NOT NULL,
    modifier        VARCHAR(10),
    geo_region      NVARCHAR(50),
    rate            DECIMAL(12,2)   NOT NULL,
    rate_type       VARCHAR(30)     NOT NULL,
    valid_from      DATE            NOT NULL,
    valid_to        DATE            NOT NULL DEFAULT '9999-12-31',
    source          NVARCHAR(100),
    batch_id        NVARCHAR(100),
    loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE INDEX IX_stg_fs_lookup ON stg_fee_schedule (payer_id, cpt_code, valid_from);

-- ============================================================================
-- Staging: Providers (landing zone for NPPES CSV batch loads)
-- ADF copies NPPES CSV from Bronze → stg_providers → MERGE into providers
-- ============================================================================

CREATE TABLE stg_providers (
    npi             CHAR(10)        NOT NULL,
    tin             CHAR(9),
    name_first      NVARCHAR(100),
    name_last       NVARCHAR(100),
    name_full       NVARCHAR(200),
    specialty       NVARCHAR(100),
    state           NVARCHAR(2),
    city            NVARCHAR(100),
    zip             NVARCHAR(10),
    status          VARCHAR(20)     DEFAULT 'active',
    deactivation_date DATE,
    batch_id        NVARCHAR(100),
    loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE INDEX IX_stg_prov_npi ON stg_providers (npi);

-- ============================================================================
-- Staging: Backfill Claims (landing zone for historical claim CSVs)
-- ============================================================================

CREATE TABLE stg_backfill_claims (
    claim_id        NVARCHAR(50)    NOT NULL,
    patient_id      NVARCHAR(50)    NOT NULL,
    provider_npi    CHAR(10)        NOT NULL,
    payer_id        NVARCHAR(50)    NOT NULL,
    date_of_service DATE            NOT NULL,
    total_billed    DECIMAL(12,2)   NOT NULL,
    cpt_codes       NVARCHAR(500),
    diagnosis_codes NVARCHAR(500),
    batch_id        NVARCHAR(100),
    loaded_at       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);

-- ============================================================================
-- Stored Procedure: MERGE fee schedule with SCD Type 2
-- Called by ADF after staging load completes
-- ============================================================================
GO

CREATE OR ALTER PROCEDURE usp_merge_fee_schedule
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2 = SYSUTCDATETIME();
    DECLARE @today DATE = CAST(@now AS DATE);
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_expired INT = 0;

    -- Step 1: Expire existing current rates that have changed
    UPDATE fs
    SET fs.valid_to   = DATEADD(DAY, -1, stg.valid_from),
        fs.is_current = 0
    FROM fee_schedule fs
    INNER JOIN stg_fee_schedule stg
        ON fs.payer_id = (SELECT TOP 1 payer_id FROM payers WHERE name = stg.payer_id OR CAST(payer_id AS NVARCHAR) = stg.payer_id)
        AND fs.cpt_code = stg.cpt_code
        AND ISNULL(fs.modifier, '') = ISNULL(stg.modifier, '')
        AND ISNULL(fs.geo_region, '') = ISNULL(stg.geo_region, '')
    WHERE fs.is_current = 1
        AND fs.rate != stg.rate
        AND (@batch_id IS NULL OR stg.batch_id = @batch_id);

    SET @rows_expired = @@ROWCOUNT;

    -- Step 2: Insert new rates (only where rate changed or new CPT/payer combo)
    INSERT INTO fee_schedule (payer_id, cpt_code, modifier, geo_region, rate, rate_type,
                              valid_from, valid_to, is_current, source, loaded_date)
    SELECT
        p.payer_id, stg.cpt_code, stg.modifier, stg.geo_region, stg.rate, stg.rate_type,
        stg.valid_from, stg.valid_to, 1, stg.source, @now
    FROM stg_fee_schedule stg
    LEFT JOIN payers p ON p.name = stg.payer_id OR CAST(p.payer_id AS NVARCHAR) = stg.payer_id
    WHERE NOT EXISTS (
        SELECT 1 FROM fee_schedule fs
        WHERE fs.payer_id = p.payer_id
            AND fs.cpt_code = stg.cpt_code
            AND ISNULL(fs.modifier, '') = ISNULL(stg.modifier, '')
            AND ISNULL(fs.geo_region, '') = ISNULL(stg.geo_region, '')
            AND fs.is_current = 1
            AND fs.rate = stg.rate
    )
    AND p.payer_id IS NOT NULL
    AND (@batch_id IS NULL OR stg.batch_id = @batch_id);

    SET @rows_inserted = @@ROWCOUNT;

    -- Step 3: Update watermark
    MERGE cdc_watermarks AS tgt
    USING (SELECT 'fee_schedule' AS source_table) AS src
    ON tgt.source_table = src.source_table
    WHEN MATCHED THEN
        UPDATE SET
            last_sync_ts  = @now,
            rows_synced   = @rows_inserted + @rows_expired,
            total_inserts = tgt.total_inserts + @rows_inserted,
            total_updates = tgt.total_updates + @rows_expired,
            updated_at    = @now
    WHEN NOT MATCHED THEN
        INSERT (source_table, last_sync_ts, rows_synced, total_inserts, total_updates, updated_at)
        VALUES ('fee_schedule', @now, @rows_inserted + @rows_expired, @rows_inserted, @rows_expired, @now);

    -- Step 4: Clean staging
    IF @batch_id IS NOT NULL
        DELETE FROM stg_fee_schedule WHERE batch_id = @batch_id;
    ELSE
        TRUNCATE TABLE stg_fee_schedule;

    SELECT @rows_inserted AS rows_inserted, @rows_expired AS rows_expired;
END;
GO

-- ============================================================================
-- Stored Procedure: MERGE providers with SCD Type 2
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_merge_providers
    @batch_id NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2 = SYSUTCDATETIME();
    DECLARE @rows_updated INT = 0;
    DECLARE @rows_inserted INT = 0;

    -- Update existing providers where data changed
    UPDATE p
    SET p.name       = COALESCE(stg.name_full, CONCAT(stg.name_first, ' ', stg.name_last)),
        p.specialty  = stg.specialty,
        p.state      = stg.state,
        p.is_active  = CASE WHEN stg.status = 'active' THEN 1 ELSE 0 END
    FROM providers p
    INNER JOIN stg_providers stg ON p.npi = stg.npi
    WHERE (p.name != COALESCE(stg.name_full, CONCAT(stg.name_first, ' ', stg.name_last))
        OR p.specialty != stg.specialty
        OR p.state != stg.state)
        AND (@batch_id IS NULL OR stg.batch_id = @batch_id);

    SET @rows_updated = @@ROWCOUNT;

    -- Insert new providers
    INSERT INTO providers (npi, tin, name, specialty, state, is_active)
    SELECT stg.npi, COALESCE(stg.tin, '000000000'),
           COALESCE(stg.name_full, CONCAT(stg.name_first, ' ', stg.name_last)),
           stg.specialty, stg.state,
           CASE WHEN stg.status = 'active' THEN 1 ELSE 0 END
    FROM stg_providers stg
    WHERE NOT EXISTS (SELECT 1 FROM providers p WHERE p.npi = stg.npi)
        AND (@batch_id IS NULL OR stg.batch_id = @batch_id);

    SET @rows_inserted = @@ROWCOUNT;

    -- Update watermark
    MERGE cdc_watermarks AS tgt
    USING (SELECT 'providers' AS source_table) AS src
    ON tgt.source_table = src.source_table
    WHEN MATCHED THEN
        UPDATE SET
            last_sync_ts  = @now,
            rows_synced   = @rows_inserted + @rows_updated,
            total_inserts = tgt.total_inserts + @rows_inserted,
            total_updates = tgt.total_updates + @rows_updated,
            updated_at    = @now
    WHEN NOT MATCHED THEN
        INSERT (source_table, last_sync_ts, rows_synced, total_inserts, total_updates, updated_at)
        VALUES ('providers', @now, @rows_inserted + @rows_updated, @rows_inserted, @rows_updated, @now);

    -- Clean staging
    IF @batch_id IS NOT NULL
        DELETE FROM stg_providers WHERE batch_id = @batch_id;
    ELSE
        TRUNCATE TABLE stg_providers;

    SELECT @rows_inserted AS rows_inserted, @rows_updated AS rows_updated;
END;
GO

-- ============================================================================
-- Stored Procedure: Update CDC watermark (called by ADF after copy)
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_update_cdc_watermark
    @source_table NVARCHAR(128),
    @new_watermark NVARCHAR(50),
    @rows_synced INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2 = SYSUTCDATETIME();

    MERGE cdc_watermarks AS tgt
    USING (SELECT @source_table AS source_table) AS src
    ON tgt.source_table = src.source_table
    WHEN MATCHED THEN
        UPDATE SET
            last_sync_ts  = TRY_CAST(@new_watermark AS DATETIME2),
            rows_synced   = @rows_synced,
            total_inserts = tgt.total_inserts + @rows_synced,
            updated_at    = @now
    WHEN NOT MATCHED THEN
        INSERT (source_table, last_sync_ts, rows_synced, total_inserts, updated_at)
        VALUES (@source_table, TRY_CAST(@new_watermark AS DATETIME2), @rows_synced, @rows_synced, @now);
END;
GO

PRINT 'Staging tables and merge procedures created successfully.';
GO
