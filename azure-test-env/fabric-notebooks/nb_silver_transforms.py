# Fabric Notebook: nb_silver_transforms
# Silver Layer — Cleaned, conformed, joined data (Bronze Delta → Silver Delta)
#
# ── Medallion Architecture: Silver Layer ──────────────────────────────────
# This is the second layer of the Medallion pipeline (Bronze → Silver → Gold).
# Silver = cleaned, deduplicated, conformed data ready for analytical joins.
#
# ── CDC Resolution (Bronze → Silver) ─────────────────────────────────────
# Bronze stores raw CDC events as an append-only log: multiple rows per entity
# (one for each Insert, Update, or Delete event). Silver resolves this to a
# single "current state" row per entity using a window function:
#
#   1. Partition by primary key (e.g., claim_id)
#   2. Order by _cdc_timestamp DESC (latest event first)
#   3. Take row_number = 1 (the most recent CDC event)
#   4. Exclude rows where _cdc_operation = 'D' (deleted in source)
#
# This pattern converts an event log into a point-in-time snapshot — equivalent
# to a materialized view of the current OLTP state.
#
# ── SCD Type 2 Handling ──────────────────────────────────────────────────
# fee_schedule uses SCD Type 2 (Slowly Changing Dimensions): instead of
# overwriting old rates, the OLTP keeps both old and new rows with
# valid_from/valid_to/is_current columns. Silver preserves all SCD2 rows
# so Gold can query rates at any point in time (critical for arbitration
# where the rate-at-date-of-service determines the expected payment).
#
# ── Transform Patterns ───────────────────────────────────────────────────
# Each transform follows the same pattern:
#   1. resolve_cdc_to_current() — read Bronze, apply CDC windowing
#   2. Clean / join / enrich — add payer names, compute derived columns
#   3. write_silver() — MERGE INTO Delta (upsert on primary key)
#
# MERGE INTO ensures idempotency: re-running the notebook with the same
# Bronze data produces the same Silver output without duplicates.
#
# Silver tables (9):
#   - silver_claims:             claims + claim_lines denormalized
#   - silver_remittances:        remittances enriched with payer names
#   - silver_claim_remittance:   joined claim ↔ remittance for underpayment analysis
#   - silver_fee_schedule:       all rates (SCD2 — includes historical + current)
#   - silver_providers:          current provider records
#   - silver_patients:           cleaned demographics
#   - silver_disputes:           disputes enriched with claim/payer context
#   - silver_cases:              cases with dispute counts and financial summary
#   - silver_deadlines:          deadlines with SLA compliance tracking
#
# Schedule: triggered by pl_master_orchestrator after Bronze notebook completes
# ---------------------------------------------------------------------------

# %%
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, sum as spark_sum, count as spark_count,
    max as spark_max, avg, when, datediff, current_date, collect_list, size, concat_ws,
    row_number, to_json, struct, expr, round as spark_round
)
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Enable schema auto-merge so MERGE INTO can handle new columns
# (e.g., cpt_codes added to disputes) without failing on schema mismatch.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

LAKEHOUSE_BRONZE = "Tables/bronze"
LAKEHOUSE_SILVER = "Tables/silver"

# %% [markdown]
# ## Helper: Resolve CDC to current state

# %%
def resolve_cdc_to_current(table_name: str, pk_column: str):
    """Read Bronze Delta, apply CDC operations, return current-state DataFrame.

    Returns None if the Bronze table doesn't exist yet (distinguishes
    'table not created' from 'table exists but is empty after CDC resolution').

    This is the core CDC resolution logic. Bronze has multiple rows per entity
    (one per change event). We collapse them to a single row per primary key:

      1. Window by pk_column, ordered by _cdc_timestamp DESC
      2. row_number() = 1 → keep only the latest event per entity
      3. Filter out 'D' (delete) operations → deleted entities disappear
      4. Drop all CDC metadata columns → clean business columns only

    After this, the DataFrame looks like a normal table snapshot — as if
    we'd queried the OLTP directly with SELECT * FROM {table}.

    Note: Watermark-based CDC (our current implementation) cannot detect
    hard deletes in the source. If native CDC (sp_cdc_enable_table) is
    enabled later, 'D' operations will flow through and be handled here.
    """
    bronze_path = f"{LAKEHOUSE_BRONZE}/{table_name}"

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        return None

    df = spark.read.format("delta").load(bronze_path)

    # Window: latest CDC event per primary key, most recent first.
    # This handles the case where a row was inserted then updated multiple
    # times — we only want the final state.
    w = Window.partitionBy(pk_column).orderBy(col("_cdc_timestamp").desc())

    current = (df
               .withColumn("_rn", row_number().over(w))
               .filter(col("_rn") == 1)          # latest event per PK
               .filter(col("_cdc_operation") != "D")  # exclude deletes
               .drop("_rn", "_cdc_operation", "_cdc_timestamp",
                     "_source_table", "_bronze_loaded_ts", "_source_file"))

    return current


def write_silver(df: DataFrame, table_name: str, pk_column: str):
    """Write DataFrame to Silver Delta table using MERGE (upsert).

    MERGE INTO (Delta Lake's upsert) matches source rows to target by
    primary key:
      - If the PK exists in target → UPDATE all columns (latest values)
      - If the PK is new → INSERT the row

    This makes the pipeline idempotent: running the same data twice
    produces the same result. It also handles CDC updates correctly —
    when a claim's status changes from 'submitted' to 'paid', the Silver
    row is updated in place rather than creating a duplicate.

    First run uses overwrite mode to bootstrap the table with its schema.
    """
    silver_path = f"{LAKEHOUSE_SILVER}/{table_name}"
    # _silver_ts tracks when the Silver layer last processed this row
    df = df.withColumn("_silver_ts", current_timestamp())

    if DeltaTable.isDeltaTable(spark, silver_path):
        try:
            delta_table = DeltaTable.forPath(spark, silver_path)
            (delta_table.alias("tgt")
             .merge(df.alias("src"), f"tgt.{pk_column} = src.{pk_column}")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        except Exception as e:
            if "DELTA_SCHEMA_CHANGE" in str(e):
                # Schema evolved — overwrite to reset table with new schema
                (df.write.format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .save(silver_path))
            else:
                raise
    else:
        (df.write.format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(silver_path))

    return spark.read.format("delta").load(silver_path).count()


# %% [markdown]
# ## Transform: Patients
# Simple passthrough — resolve CDC to current state and select demographics.
# Patient data comes from FHIR R4 (fhir_patients.json) and HL7 v2 ADT
# messages, both normalized during ingestion into a common schema.

# %%
def transform_patients():
    df = resolve_cdc_to_current("patients", "patient_id")
    if df is None or df.rdd.isEmpty():
        return 0

    # Column mapping: Azure SQL schema uses date_of_birth/zip_code, not dob/zip
    silver = df.select(
        "patient_id", "first_name", "last_name",
        col("date_of_birth").alias("dob"), "gender",
        "state", col("zip_code").alias("zip"), "insurance_id"
    )
    return write_silver(silver, "patients", "patient_id")

# %% [markdown]
# ## Transform: Providers (SCD Type 1 — current only)
# Providers use SCD Type 1 (upsert): when a provider's address or status
# changes, the old row is overwritten. We only keep is_current=1 rows.
# Source data comes from NPPES (National Plan & Provider Enumeration System).

# %%
def transform_providers():
    df = resolve_cdc_to_current("providers", "npi")
    if df is None or df.rdd.isEmpty():
        return 0

    # Filter to current providers only (SCD1 — no historical tracking)
    # Azure SQL schema uses: name, specialty, state (not name_first/name_last/specialty_taxonomy)
    silver = (df
              .filter(col("is_active") == 1)
              .select("npi", "name", "specialty", "facility_name",
                      "state", "tin"))
    return write_silver(silver, "providers", "npi")

# %% [markdown]
# ## Transform: Claims + Lines (denormalized)
# The OLTP has two tables: claims (header) and claim_lines (service lines).
# Each claim has 1+ service lines, each with a CPT code and units.
# In Silver, we denormalize by aggregating lines into the claims row:
#   - line_count: number of service lines
#   - cpt_codes: JSON array of all CPT codes on the claim (e.g., ["99213", "97110"])
#   - total_units: sum of billable units across all lines
#
# Source: EDI 837 (Professional/Institutional) parsed by parsers/edi_837.py

# %%
def transform_claims():
    claims = resolve_cdc_to_current("claims", "claim_id")
    lines = resolve_cdc_to_current("claim_lines", "line_id")

    if claims is None or claims.rdd.isEmpty():
        return 0

    if lines is not None and not lines.rdd.isEmpty():
        # Aggregate service lines per claim — collapses 1:N into a single row.
        # CPT codes are stored as a JSON array for downstream Gold analysis
        # (Gold's agg_cpt_analysis explodes this back to per-CPT rows).
        lines_agg = (lines
                     .groupBy("claim_id")
                     .agg(
                         spark_count("*").alias("line_count"),
                         collect_list("cpt_code").alias("cpt_codes_arr"),
                         spark_sum(coalesce(col("units"), lit(1))).alias("total_units")
                     ))

        # LEFT join: claims without lines (rare, but possible for voided claims)
        # still appear in Silver with line_count = 0.
        silver = (claims
                  .join(lines_agg, "claim_id", "left")
                  .withColumn("line_count", coalesce(col("line_count"), lit(0)))
                  .withColumn("cpt_codes", to_json(col("cpt_codes_arr")))
                  .withColumn("total_units", coalesce(col("total_units"), lit(0))))
    else:
        # No claim_lines Bronze table — claims still go through with defaults
        silver = (claims
                  .withColumn("line_count", lit(0))
                  .withColumn("cpt_codes", lit("[]"))
                  .withColumn("total_units", lit(0)))

    # Azure SQL schema uses: facility_id (not facility_type), no "source" column
    silver = silver.select(
        "claim_id", "patient_id", "provider_npi", "payer_id",
        "date_of_service", "date_filed", "facility_id",
        "total_billed", "status",
        "line_count", "cpt_codes", "total_units"
    )
    return write_silver(silver, "claims", "claim_id")

# %% [markdown]
# ## Transform: Remittances (enriched with payer names)
# Remittances (a.k.a. ERA / Electronic Remittance Advice) come from EDI 835
# files sent by payers. Each remittance maps to a claim and reports the
# paid_amount, allowed_amount, and denial/adjustment codes (CARC/RARC).
# We enrich with payer_name to make downstream analysis human-readable.

# %%
def transform_remittances():
    remit = resolve_cdc_to_current("remittances", "remit_id")
    claims = resolve_cdc_to_current("claims", "claim_id")
    payers = resolve_cdc_to_current("payers", "payer_id")

    if remit is None or remit.rdd.isEmpty():
        return 0

    # Remittances don't have payer_id directly — get it from claims
    if claims is not None and not claims.rdd.isEmpty():
        claims_payer = claims.select(
            col("claim_id").alias("c_claim_id"),
            col("payer_id")
        )
        remit = remit.join(claims_payer, remit.claim_id == claims_payer.c_claim_id, "left").drop("c_claim_id")
    else:
        remit = remit.withColumn("payer_id", lit(None).cast("int"))

    if payers is not None and not payers.rdd.isEmpty():
        payer_names = payers.select(
            col("payer_id").alias("p_payer_id"),
            col("name").alias("payer_name")
        )
        silver = (remit
                  .join(payer_names, col("payer_id") == payer_names.p_payer_id, "left")
                  .select(
                      "remit_id", "claim_id", "payer_id", "payer_name",
                      "era_date", "paid_amount", "allowed_amount",
                      "adjustment_reason", "denial_code", "check_number",
                      col("source_type").alias("source")
                  ))
    else:
        silver = (remit
                  .withColumn("payer_name", lit(None).cast("string"))
                  .select(
                      "remit_id", "claim_id", "payer_id", "payer_name",
                      "era_date", "paid_amount", "allowed_amount",
                      "adjustment_reason", "denial_code", "check_number",
                      col("source_type").alias("source")
                  ))

    return write_silver(silver, "remittances", "remit_id")

# %% [markdown]
# ## Transform: Claim ↔ Remittance join (underpayment analysis)
#
# This is the most critical Silver table for arbitration. It joins every
# claim with its remittance(s) to compute:
#   - total_paid: sum of all payments received for this claim
#   - underpayment: total_billed - total_paid (positive = provider is owed money)
#   - has_denial: whether any remittance had a denial code (CARC 50, etc.)
#
# A single claim may have multiple remittances (e.g., primary + secondary
# insurance, or partial payment + adjustment). We aggregate all remittances
# per claim_id before joining to claims.
#
# This table is used as the primary input for Gold aggregations:
# recovery_by_payer, financial_summary, CPT analysis, claims aging, etc.
#
# Note: This uses full OVERWRITE instead of MERGE because the join logic
# means every row depends on the full state of both silver_claims and
# silver_remittances — incremental upsert wouldn't capture changes in
# the remittance side that affect existing claim_remittance rows.

# %%
def transform_claim_remittance():
    """Join silver_claims with silver_remittances for underpayment detection."""
    claims_path = f"{LAKEHOUSE_SILVER}/claims"
    remit_path = f"{LAKEHOUSE_SILVER}/remittances"
    payers = resolve_cdc_to_current("payers", "payer_id")

    if not DeltaTable.isDeltaTable(spark, claims_path):
        return 0

    claims = spark.read.format("delta").load(claims_path)
    remit = spark.read.format("delta").load(remit_path) if DeltaTable.isDeltaTable(spark, remit_path) else None

    if remit is not None:
        # Aggregate all remittances per claim: a claim paid in two installments
        # or by primary + secondary payer will have multiple remittance rows.
        remit_agg = (remit
                     .groupBy("claim_id")
                     .agg(
                         spark_sum("paid_amount").alias("total_paid"),
                         spark_sum("allowed_amount").alias("total_allowed"),
                         spark_count("*").alias("remittance_count"),
                         # has_denial = 1 if ANY remittance carries a denial code
                         # (e.g., CARC 50 = services not covered)
                         spark_max(when(
                             (col("denial_code").isNotNull()) & (col("denial_code") != ""),
                             lit(1)
                         ).otherwise(lit(0))).alias("has_denial")
                     ))

        # LEFT join: claims without any remittance yet appear with total_paid=0
        # (these are unpaid/pending claims — still important for aging analysis)
        joined = (claims
                  .join(remit_agg, "claim_id", "left")
                  .withColumn("total_paid", coalesce(col("total_paid"), lit(0)))
                  .withColumn("total_allowed", coalesce(col("total_allowed"), lit(0)))
                  .withColumn("remittance_count", coalesce(col("remittance_count"), lit(0)))
                  .withColumn("has_denial", coalesce(col("has_denial"), lit(0)))
                  # underpayment = what the provider billed minus what they received.
                  # Positive value = payer underpaid → potential arbitration case.
                  .withColumn("underpayment", col("total_billed") - col("total_paid")))
    else:
        # No remittances yet — treat all claims as fully unpaid
        joined = (claims
                  .withColumn("total_paid", lit(0))
                  .withColumn("total_allowed", lit(0))
                  .withColumn("remittance_count", lit(0))
                  .withColumn("has_denial", lit(0))
                  .withColumn("underpayment", col("total_billed")))

    # Enrich with payer display names for BI
    if payers is not None and not payers.rdd.isEmpty():
        payer_names = payers.select(
            col("payer_id").alias("p_id"), col("name").alias("payer_name")
        )
        silver = (joined
                  .join(payer_names, joined.payer_id == payer_names.p_id, "left")
                  .select(
                      "claim_id", "patient_id", "provider_npi", "payer_id",
                      "payer_name", "date_of_service", "total_billed",
                      "total_paid", "total_allowed", "underpayment",
                      "cpt_codes", "line_count", "remittance_count",
                      "has_denial", col("status").alias("claim_status")
                  ))
    else:
        silver = (joined
                  .withColumn("payer_name", lit(None).cast("string"))
                  .select(
                      "claim_id", "patient_id", "provider_npi", "payer_id",
                      "payer_name", "date_of_service", "total_billed",
                      "total_paid", "total_allowed", "underpayment",
                      "cpt_codes", "line_count", "remittance_count",
                      "has_denial", col("status").alias("claim_status")
                  ))

    # Full overwrite (not MERGE) — see note above on why this table
    # cannot be incrementally upserted.
    silver_path = f"{LAKEHOUSE_SILVER}/claim_remittance"
    silver = silver.withColumn("_silver_ts", current_timestamp())
    (silver.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(silver_path))

    return spark.read.format("delta").load(silver_path).count()

# %% [markdown]
# ## Transform: Fee Schedule (SCD Type 2 — all versions)
#
# Fee schedules use SCD Type 2: when Medicare or a payer publishes new rates,
# the old row's valid_to is set to (new_effective_date - 1 day) and a new
# row is inserted with valid_from = effective_date, valid_to = 9999-12-31.
#
# Example for CPT 99213 with a 2026 rate increase:
#   payer_id=MEDICARE, cpt=99213, rate=$125, valid_from=2025-01-01, valid_to=2025-12-31, is_current=0
#   payer_id=MEDICARE, cpt=99213, rate=$128, valid_from=2026-01-01, valid_to=9999-12-31, is_current=1
#
# Silver keeps ALL versions (not just current) so Gold can query the rate
# that was effective at a claim's date_of_service — critical for determining
# whether a payer underpaid relative to the contractual or benchmark rate.

# %%
def transform_fee_schedule():
    df = resolve_cdc_to_current("fee_schedule", "id")
    if df is None or df.rdd.isEmpty():
        return 0

    # Preserve all SCD2 rows (both current and historical) — the Gold layer
    # joins on date_of_service BETWEEN valid_from AND valid_to to get the
    # correct rate at the time of service.
    silver = df.select(
        "id", "payer_id", "cpt_code", "modifier", "geo_region",
        "rate", "rate_type", "valid_from", "valid_to", "is_current", "source"
    )
    return write_silver(silver, "fee_schedule", "id")

# %% [markdown]
# ## Transform: Disputes (enriched with claim/payer context)
#
# A dispute is created when underpayment is detected on a claim. The dispute
# record itself only has claim_id, amounts, and status — but for analysis
# we need the payer name, provider NPI, date of service, and CPT codes.
#
# This transform enriches disputes by joining:
#   - claims → payer_id, provider_npi, date_of_service
#   - payers → payer_name (display name)
#   - silver_claims → cpt_codes (denormalized CPT array from transform_claims)
#
# Key domain fields:
#   - qpa_amount: Qualifying Payment Amount — the median in-network rate,
#     used under the No Surprises Act (NSA) as the default payment standard
#   - underpayment_amount: billed - paid (what the provider is owed)
#   - dispute_type: "underpayment", "denial", or "balance_billing"

# %%
def transform_disputes():
    disputes = resolve_cdc_to_current("disputes", "dispute_id")
    if disputes is None or disputes.rdd.isEmpty():
        return 0

    claims = resolve_cdc_to_current("claims", "claim_id")
    payers = resolve_cdc_to_current("payers", "payer_id")

    # Pull CPT codes from the already-denormalized silver_claims table
    # (avoids re-aggregating claim_lines here)
    claims_path = f"{LAKEHOUSE_SILVER}/claims"
    if DeltaTable.isDeltaTable(spark, claims_path):
        silver_claims = spark.read.format("delta").load(claims_path)
        cpt_lookup = silver_claims.select(
            col("claim_id").alias("cl_id"), col("cpt_codes")
        )
    else:
        cpt_lookup = None

    # Build claim context join (payer_id, provider_npi, date_of_service)
    # Disputes don't have payer_id directly — get it from claims
    if claims is not None and not claims.rdd.isEmpty():
        claims_ctx = claims.select(
            col("claim_id").alias("c_claim_id"),
            col("payer_id").alias("payer_id"),
            col("provider_npi").alias("c_provider_npi"),
            col("date_of_service").alias("c_dos")
        )
        silver = disputes.join(claims_ctx, disputes.claim_id == claims_ctx.c_claim_id, "left")
    else:
        silver = (disputes
                  .withColumn("payer_id", lit(None).cast("int"))
                  .withColumn("c_provider_npi", lit(None).cast("string"))
                  .withColumn("c_dos", lit(None).cast("string")))

    # Enrich with payer name
    if payers is not None and not payers.rdd.isEmpty():
        payer_names = payers.select(
            col("payer_id").alias("p_id"), col("name").alias("payer_name")
        )
        silver = silver.join(payer_names, col("payer_id") == payer_names.p_id, "left")
    else:
        silver = silver.withColumn("payer_name", lit(None).cast("string"))

    # Enrich with CPT codes from silver_claims
    if cpt_lookup is not None:
        silver = silver.join(cpt_lookup, disputes.claim_id == cpt_lookup.cl_id, "left")
    else:
        silver = silver.withColumn("cpt_codes", lit(None))

    silver = silver.select(
        "dispute_id", disputes.claim_id, "case_id", "payer_id", "payer_name",
        "dispute_type", disputes.status, "billed_amount", "paid_amount",
        "qpa_amount", "underpayment_amount", "filed_date",
        col("c_provider_npi").alias("provider_npi"),
        col("c_dos").alias("date_of_service"),
        "cpt_codes"
    )
    return write_silver(silver, "disputes", "dispute_id")

# %% [markdown]
# ## Transform: Cases (with dispute summary)
#
# A case is an arbitration workflow container that groups one or more disputes
# against the same payer. Cases track the NSA state machine:
#   open → in_review → negotiation → idr_initiated → idr_submitted → decided → closed
#
# We enrich each case with aggregated dispute financials (total_billed,
# total_underpayment) and compute age_days for pipeline aging analysis.

# %%
def transform_cases():
    cases = resolve_cdc_to_current("cases", "case_id")
    if cases is None or cases.rdd.isEmpty():
        return 0

    # Pull dispute financials from silver_disputes to avoid re-resolving from Bronze
    disputes_path = f"{LAKEHOUSE_SILVER}/disputes"
    if DeltaTable.isDeltaTable(spark, disputes_path):
        disputes = spark.read.format("delta").load(disputes_path)
        dispute_agg = (disputes
                       .groupBy("case_id")
                       .agg(
                           spark_count("*").alias("dispute_count"),
                           coalesce(spark_sum("billed_amount"), lit(0)).alias("total_billed"),
                           coalesce(spark_sum("underpayment_amount"), lit(0)).alias("total_underpayment")
                       ))
    else:
        dispute_agg = None

    silver = cases.withColumn(
        "age_days", datediff(current_date(), col("created_date"))
    )

    if dispute_agg is not None:
        silver = (silver
                  .join(dispute_agg, "case_id", "left")
                  .withColumn("dispute_count", coalesce(col("dispute_count"), lit(0)))
                  .withColumn("total_billed", coalesce(col("total_billed"), lit(0)))
                  .withColumn("total_underpayment", coalesce(col("total_underpayment"), lit(0))))
    else:
        silver = (silver
                  .withColumn("dispute_count", lit(0))
                  .withColumn("total_billed", lit(0))
                  .withColumn("total_underpayment", lit(0)))

    # Azure SQL schema uses "last_activity" not "last_activity_date"
    silver = silver.select(
        "case_id", "assigned_analyst", "status", "priority",
        "created_date", col("last_activity").alias("last_activity_date"),
        "outcome", "award_amount",
        "dispute_count", "total_billed", "total_underpayment", "age_days"
    )
    return write_silver(silver, "cases", "case_id")

# %% [markdown]
# ## Transform: Deadlines (NSA SLA tracking)
#
# The No Surprises Act (NSA) imposes strict deadlines on the IDR
# (Independent Dispute Resolution) process:
#   - open_negotiation:    30 days from dispute filing
#   - idr_initiation:      4 business days after negotiation fails
#   - evidence_submission:  10 business days after IDR initiated
#   - idr_decision:         30 business days after IDR initiated
#
# Missing a deadline can forfeit the provider's right to arbitration
# or result in a default ruling. This transform computes:
#   - days_remaining: how many days until the deadline (NULL if already resolved)
#   - is_at_risk: 1 if due within 5 days and still pending (needs attention)
#
# The deadline_monitor_orchestrator (Durable Function) uses these fields
# to trigger alerts and escalations every 6 hours.

# %%
def transform_deadlines():
    dl = resolve_cdc_to_current("deadlines", "deadline_id")
    if dl is None or dl.rdd.isEmpty():
        return 0

    silver = (dl
              # days_remaining is NULL for already-resolved deadlines (met/missed)
              .withColumn("days_remaining",
                          when(col("status").isin("met", "missed"), lit(None))
                          .otherwise(datediff(col("due_date"), current_date())))
              # Flag deadlines due within 5 days as at-risk for analyst attention
              .withColumn("is_at_risk",
                          when((col("days_remaining").isNotNull()) &
                               (col("days_remaining") <= 5), lit(1))
                          .otherwise(lit(0)))
              .select(
                  "deadline_id", "case_id", "dispute_id", "type",
                  "due_date", "status", "completed_date",
                  "days_remaining", "is_at_risk"
              ))
    return write_silver(silver, "deadlines", "deadline_id")


# %% [markdown]
# ## Execute All Silver Transforms
#
# Execution order matters: some transforms depend on outputs of earlier ones.
# - patients, providers, claims, remittances, fee_schedule: independent, read from Bronze
# - claim_remittance: depends on silver_claims + silver_remittances
# - disputes: depends on silver_claims (for CPT codes)
# - cases: depends on silver_disputes (for financial aggregates)
# - deadlines: independent (reads from Bronze)

# %%
transforms = [
    ("silver_patients", transform_patients),
    ("silver_providers", transform_providers),
    ("silver_claims", transform_claims),         # must run before claim_remittance & disputes
    ("silver_remittances", transform_remittances),  # must run before claim_remittance
    ("silver_claim_remittance", transform_claim_remittance),  # depends on claims + remittances
    ("silver_fee_schedule", transform_fee_schedule),
    ("silver_disputes", transform_disputes),     # depends on silver_claims (for CPT codes)
    ("silver_cases", transform_cases),           # depends on silver_disputes
    ("silver_deadlines", transform_deadlines),
]

summary = {}
for name, fn in transforms:
    try:
        count = fn()
        summary[name] = count
        print(f"  {name:35s} → {count} rows")
    except Exception as e:
        summary[name] = f"ERROR: {e}"
        print(f"  {name:35s} → ERROR: {e}")

print(f"\nSilver transform complete: {summary}")
