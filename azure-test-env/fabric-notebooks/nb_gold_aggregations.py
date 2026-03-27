# Fabric Notebook: nb_gold_aggregations
# Gold Layer — Business-ready aggregations for Power BI (Silver Delta → Gold Delta)
#
# ── Medallion Architecture: Gold Layer ────────────────────────────────────
# This is the third and final layer of the Medallion pipeline.
# Gold = pre-computed, business-oriented aggregations optimized for BI.
#
# ── Purpose ──────────────────────────────────────────────────────────────
# Gold tables are the direct data source for Power BI via Direct Lake mode.
# Direct Lake reads Delta tables natively (no import/copy) — so Gold tables
# must be structured exactly as the BI report needs them: pre-aggregated,
# with human-readable labels, and ready for slicing without further joins.
#
# ── Write Strategy ───────────────────────────────────────────────────────
# Gold tables use full OVERWRITE on every run (not MERGE). This is intentional:
# aggregations depend on the complete Silver dataset, and recomputing from
# scratch is simpler and more reliable than trying to incrementally update
# summary rows when underlying detail changes. The tables are small (hundreds
# of rows, not millions) so full overwrite is fast.
#
# ── Gold Tables (8) ──────────────────────────────────────────────────────
#   1. gold_recovery_by_payer:       recovery metrics per payer
#   2. gold_cpt_analysis:            billed vs. paid per CPT code with benchmarks
#   3. gold_payer_scorecard:         payer behavior/risk tier classification
#   4. gold_financial_summary:       overall financial KPIs (key-value format)
#   5. gold_claims_aging:            claim aging buckets (0-30, 31-60, etc.)
#   6. gold_case_pipeline:           case status distribution + SLA compliance
#   7. gold_deadline_compliance:     deadline met/missed/at-risk by type
#   8. gold_underpayment_detection:  per-claim QPA analysis for arbitration eligibility
#
# Schedule: triggered by pl_master_orchestrator after Silver notebook completes
# ---------------------------------------------------------------------------

# %%
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, sum as spark_sum, count, avg,
    max as spark_max, when, datediff, current_date, round as spark_round,
    explode, from_json, expr, struct, collect_list
)
from pyspark.sql.types import ArrayType, StringType
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

LAKEHOUSE_SILVER = "Tables/silver"
LAKEHOUSE_GOLD = "Tables/gold"

# %% [markdown]
# ## Helpers

# %%
def read_silver(table_name: str) -> DataFrame:
    """Read a Silver Delta table, return None if it doesn't exist yet.

    Returns None (not empty DataFrame) so callers can distinguish between
    'table exists but has no rows' vs 'table hasn't been created yet'.
    """
    path = f"{LAKEHOUSE_SILVER}/{table_name}"
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    return None


def write_gold(df: DataFrame, table_name: str):
    """Write Gold table — full overwrite each run (materialized aggregation).

    Gold tables are small (tens to hundreds of rows) and represent
    pre-computed aggregations. Overwrite is the correct strategy because:
      1. Aggregations depend on the full Silver dataset — you can't
         incrementally update a SUM when underlying rows change.
      2. Power BI Direct Lake reads the latest Delta version automatically.
      3. overwriteSchema=true handles column additions without manual DDL.

    _gold_ts records when the aggregation was computed (for freshness monitoring).
    """
    path = f"{LAKEHOUSE_GOLD}/{table_name}"
    df = df.withColumn("_gold_ts", current_timestamp())
    (df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(path))
    return spark.read.format("delta").load(path).count()


# %% [markdown]
# ## Gold: Recovery by Payer
#
# Primary BI table for the "Recovery Analysis" dashboard page.
# Shows how much money each payer owes vs. has paid, driving prioritization
# of which payers to pursue for arbitration.
#
# Key metrics:
#   - recovery_rate_pct: total_paid / total_billed × 100 — higher is better
#   - denial_rate_pct: % of claims with a denial code — flags problematic payers
#   - total_underpayment: the aggregate $ amount recoverable per payer

# %%
def agg_recovery_by_payer():
    cr = read_silver("claim_remittance")
    if cr is None:
        return 0

    gold = (cr
            .filter(col("payer_id").isNotNull())
            .groupBy("payer_id")
            .agg(
                spark_max("payer_name").alias("payer_name"),
                count("*").alias("total_claims"),
                spark_sum("total_billed").alias("total_billed"),
                spark_sum("total_paid").alias("total_paid"),
                spark_sum("underpayment").alias("total_underpayment"),
                # Recovery rate: what % of billed amounts have been collected
                spark_round(
                    when(spark_sum("total_billed") > 0,
                         spark_sum("total_paid") / spark_sum("total_billed") * 100)
                    .otherwise(0), 2
                ).alias("recovery_rate_pct"),
                # Average per-claim payment rate (differs from aggregate recovery
                # rate because it weights each claim equally, not by dollar amount)
                spark_round(
                    avg(when(col("total_billed") > 0,
                             col("total_paid") / col("total_billed") * 100)
                        .otherwise(0)), 2
                ).alias("avg_payment_pct"),
                spark_sum("has_denial").alias("denial_count"),
                # Denial rate: % of claims with any denial code (CARC 50, etc.)
                spark_round(
                    when(count("*") > 0,
                         spark_sum("has_denial") * 100.0 / count("*"))
                    .otherwise(0), 2
                ).alias("denial_rate_pct")
            ))

    return write_gold(gold, "recovery_by_payer")


# %% [markdown]
# ## Gold: CPT Code Analysis
#
# Analyzes payment patterns per CPT (Current Procedural Terminology) code.
# Shows what providers billed vs. received for each procedure type, and
# compares against Medicare and FAIR Health benchmark rates.
#
# This is critical for arbitration: under the NSA, the IDR entity must
# consider the Qualifying Payment Amount (QPA) — typically derived from
# the median in-network rate. Comparing paid amounts to Medicare and
# FAIR Health rates helps identify systematically underpaid procedures.
#
# Proportional allocation: A claim may have multiple CPT codes (e.g.,
# 99213 + 97110). Since total_billed/total_paid are at the claim level,
# we allocate them proportionally across CPT codes on that claim.

# %%
def agg_cpt_analysis():
    cr = read_silver("claim_remittance")
    claims = read_silver("claims")
    fs = read_silver("fee_schedule")
    if cr is None or claims is None:
        return 0

    # Explode the JSON CPT array (e.g., '["99213","97110"]') back into
    # individual rows — one row per (claim_id, cpt_code) pair.
    claims_cpt = (claims
                  .withColumn("cpt_arr",
                              from_json(col("cpt_codes"), ArrayType(StringType())))
                  .select("claim_id", explode("cpt_arr").alias("cpt_code")))

    # Join with claim_remittance for financial data (billed + paid amounts)
    joined = (claims_cpt
              .join(cr.select("claim_id", "total_billed", "total_paid"),
                    "claim_id", "inner"))

    # Count CPT codes per claim for proportional allocation.
    # A claim with 3 CPT codes allocates 1/3 of total_billed to each code.
    cpt_per_claim = (claims_cpt
                     .groupBy("claim_id")
                     .agg(count("*").alias("n_codes")))

    allocated = (joined
                 .join(cpt_per_claim, "claim_id", "left")
                 .withColumn("per_code_billed", col("total_billed") / col("n_codes"))
                 .withColumn("per_code_paid", col("total_paid") / col("n_codes")))

    cpt_stats = (allocated
                 .groupBy("cpt_code")
                 .agg(
                     count("*").alias("claim_count"),
                     spark_round(spark_sum("per_code_billed"), 2).alias("total_billed"),
                     spark_round(spark_sum("per_code_paid"), 2).alias("total_paid"),
                     spark_round(avg("per_code_billed"), 2).alias("avg_billed"),
                     spark_round(avg("per_code_paid"), 2).alias("avg_paid"),
                     # payment_ratio: what % of billed was actually paid for this CPT
                     spark_round(
                         when(spark_sum("per_code_billed") > 0,
                              spark_sum("per_code_paid") / spark_sum("per_code_billed") * 100)
                         .otherwise(0), 2
                     ).alias("payment_ratio_pct")
                 ))

    # Join benchmark rates from fee_schedule (SCD2 — current rates only).
    # Medicare rate = CMS Physician Fee Schedule (PFS) rate for this CPT.
    # FAIR Health rate = 80th percentile of charges in the geographic area.
    # Both are used as reference points in arbitration / IDR proceedings.
    if fs is not None:
        medicare_rates = (fs
                          .filter((col("payer_id") == "MEDICARE") & (col("is_current") == 1))
                          .select(col("cpt_code").alias("m_cpt"), col("rate").alias("medicare_rate")))
        fair_health_rates = (fs
                             .filter((col("payer_id") == "FAIR_HEALTH") & (col("is_current") == 1))
                             .select(col("cpt_code").alias("f_cpt"), col("rate").alias("fair_health_rate")))

        cpt_stats = (cpt_stats
                     .join(medicare_rates, cpt_stats.cpt_code == medicare_rates.m_cpt, "left")
                     .join(fair_health_rates, cpt_stats.cpt_code == fair_health_rates.f_cpt, "left")
                     .drop("m_cpt", "f_cpt"))
    else:
        cpt_stats = (cpt_stats
                     .withColumn("medicare_rate", lit(None))
                     .withColumn("fair_health_rate", lit(None)))

    return write_gold(cpt_stats, "cpt_analysis")


# %% [markdown]
# ## Gold: Payer Scorecard
#
# Rates each payer's behavior across payment and denial metrics, then assigns
# a risk tier (low / medium / high). The scorecard drives two decisions:
#   1. Which payers to prioritize for dispute filing (high-risk first)
#   2. Which payer contracts to renegotiate (chronic underpayers)
#
# Risk tier logic:
#   - LOW:    payment_rate >= 80% AND denial_rate <= 10% (reliable payer)
#   - MEDIUM: payment_rate >= 50% OR denial_rate <= 30% (some issues)
#   - HIGH:   everything else (systematic underpayment or high denials)

# %%
def agg_payer_scorecard():
    cr = read_silver("claim_remittance")
    payers = read_silver("payers") if read_silver("payers") is not None else None
    if cr is None:
        return 0

    stats = (cr
             .filter(col("payer_id").isNotNull())
             .groupBy("payer_id")
             .agg(
                 spark_max("payer_name").alias("payer_name"),
                 count("*").alias("total_claims"),
                 spark_round(
                     when(spark_sum("total_billed") > 0,
                          spark_sum("total_paid") / spark_sum("total_billed") * 100)
                     .otherwise(0), 2
                 ).alias("payment_rate_pct"),
                 spark_round(
                     when(count("*") > 0,
                          spark_sum("has_denial") * 100.0 / count("*"))
                     .otherwise(0), 2
                 ).alias("denial_rate_pct"),
                 spark_round(avg("underpayment"), 2).alias("avg_underpayment"),
                 spark_sum("underpayment").alias("total_underpayment")
             ))

    # Risk tier classification based on payment behavior thresholds.
    # These thresholds can be adjusted based on business rules — currently
    # aligned with industry benchmarks for commercial payer performance.
    gold = (stats
            .withColumn("risk_tier",
                        when((col("payment_rate_pct") >= 80) & (col("denial_rate_pct") <= 10), "low")
                        .when((col("payment_rate_pct") >= 50) | (col("denial_rate_pct") <= 30), "medium")
                        .otherwise("high"))
            # avg_days_to_pay: placeholder — requires payment date tracking
            # not yet in the Silver layer. Future enhancement.
            .withColumn("avg_days_to_pay", lit(None).cast("double"))
            .withColumn("payer_type", lit(None).cast("string")))

    # Enrich payer_type from bronze payers if available
    if payers is not None:
        payer_info = payers.select(
            col("payer_id").alias("p_id"), col("type").alias("p_type")
        )
        gold = (gold
                .join(payer_info, gold.payer_id == payer_info.p_id, "left")
                .withColumn("payer_type", coalesce(col("p_type"), col("payer_type")))
                .drop("p_id", "p_type"))

    gold = gold.select(
        "payer_id", "payer_name", "payer_type", "total_claims",
        "avg_days_to_pay", "payment_rate_pct", "denial_rate_pct",
        "avg_underpayment", "total_underpayment", "risk_tier"
    )
    return write_gold(gold, "payer_scorecard")


# %% [markdown]
# ## Gold: Financial Summary
#
# Executive-level KPIs in a key-value (metric_name, metric_value) format.
# This powers the top-of-dashboard KPI cards (total billed, total underpayment,
# recovery rate, denial rate, etc.). Key-value format is used instead of a
# single wide row so Power BI can display each metric as an independent card.

# %%
def agg_financial_summary():
    cr = read_silver("claim_remittance")
    if cr is None:
        return 0

    # Compute all KPIs in a single pass over claim_remittance
    stats = cr.agg(
        count("*").alias("total_claims"),
        spark_sum("total_billed").alias("total_billed"),
        spark_sum("total_paid").alias("total_paid"),
        spark_sum("underpayment").alias("total_underpayment"),
        spark_sum(when(col("total_paid") > 0, 1).otherwise(0)).alias("paid_claims"),
        spark_sum("has_denial").alias("denial_count")
    ).collect()[0]

    total_claims = stats["total_claims"] or 0
    total_billed = stats["total_billed"] or 0
    total_paid = stats["total_paid"] or 0
    total_underpayment = stats["total_underpayment"] or 0
    paid_claims = stats["paid_claims"] or 0
    denial_count = stats["denial_count"] or 0

    recovery_rate = round(total_paid / total_billed * 100, 2) if total_billed > 0 else 0
    denial_rate = round(denial_count / total_claims * 100, 2) if total_claims > 0 else 0
    avg_billed = round(total_billed / total_claims, 2) if total_claims > 0 else 0
    avg_underpayment = round(total_underpayment / total_claims, 2) if total_claims > 0 else 0

    metrics = [
        ("total_claims", float(total_claims)),
        ("total_billed", float(total_billed)),
        ("total_paid", float(total_paid)),
        ("total_underpayment", float(total_underpayment)),
        ("recovery_rate_pct", recovery_rate),
        ("paid_claims", float(paid_claims)),
        ("denial_count", float(denial_count)),
        ("denial_rate_pct", denial_rate),
        ("avg_billed_per_claim", avg_billed),
        ("avg_underpayment_per_claim", avg_underpayment),
    ]

    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("metric_name", StringType()),
        StructField("metric_value", DoubleType()),
        StructField("metric_detail", StringType())
    ])
    gold = spark.createDataFrame(
        [(name, value, None) for name, value in metrics], schema
    )
    return write_gold(gold, "financial_summary")


# %% [markdown]
# ## Gold: Claims Aging
#
# Groups claims by how old they are (days since date_of_service), using
# standard revenue cycle aging buckets: 0-30, 31-60, 61-90, 91-180, 180+.
#
# Aging analysis helps prioritize collections:
#   - 0-30 days: normal processing window, most claims settle here
#   - 31-60 days: follow-up needed, may need ERA status check
#   - 61-90 days: escalation required, approaching timely filing limits
#   - 91-180 days: at risk of timely filing denial (most payers require
#     claims within 90-180 days of service)
#   - 180+ days: likely requires appeal or arbitration to recover

# %%
def agg_claims_aging():
    cr = read_silver("claim_remittance")
    if cr is None:
        return 0

    aged = (cr
            .filter(col("date_of_service").isNotNull())
            .withColumn("age_days", datediff(current_date(), col("date_of_service")))
            .withColumn("aging_bucket",
                        when(col("age_days") <= 30, "0-30 days")
                        .when(col("age_days") <= 60, "31-60 days")
                        .when(col("age_days") <= 90, "61-90 days")
                        .when(col("age_days") <= 180, "91-180 days")
                        .otherwise("180+ days")))

    total_claims = aged.count()

    gold = (aged
            .groupBy("aging_bucket")
            .agg(
                count("*").alias("claim_count"),
                spark_sum("total_billed").alias("total_billed"),
                spark_sum("underpayment").alias("total_unpaid")
            )
            .withColumn("pct_of_total",
                        spark_round(col("claim_count") / lit(max(total_claims, 1)) * 100, 2)))

    return write_gold(gold, "claims_aging")


# %% [markdown]
# ## Gold: Case Pipeline
#
# Shows the distribution of arbitration cases across the NSA state machine:
#   open → in_review → negotiation → idr_initiated → idr_submitted → decided → closed
#
# For each status bucket, computes:
#   - case_count: how many cases are in this stage
#   - total_underpayment: dollar amount at stake in this stage
#   - avg_age_days: how long cases have been in the system
#   - sla_compliance_pct: % of deadlines met for cases in this stage
#
# This drives the "Case Pipeline" funnel chart in Power BI, helping
# managers see where cases are bottlenecked and whether SLA compliance
# is slipping at any stage.

# %%
def agg_case_pipeline():
    cases = read_silver("cases")
    deadlines = read_silver("deadlines")
    if cases is None:
        return 0

    pipeline = (cases
                .groupBy("status")
                .agg(
                    count("*").alias("case_count"),
                    coalesce(spark_sum("total_billed"), lit(0)).alias("total_billed"),
                    coalesce(spark_sum("total_underpayment"), lit(0)).alias("total_underpayment"),
                    spark_round(avg("age_days"), 1).alias("avg_age_days")
                ))

    if deadlines is not None:
        # SLA compliance per case status — cross-join cases with their deadlines
        # to compute what % of deadlines are being met at each pipeline stage.
        sla = (deadlines
               .join(cases.select(col("case_id"), col("status").alias("c_status")), "case_id", "inner")
               .groupBy("c_status")
               .agg(
                   count("*").alias("total_dl"),
                   spark_sum(when(col("status") == "met", 1).otherwise(0)).alias("met_dl")
               )
               .withColumn("sla_compliance_pct",
                           spark_round(
                               when(col("total_dl") > 0, col("met_dl") / col("total_dl") * 100)
                               .otherwise(0), 2
                           ))
               .select(col("c_status").alias("sla_status"), "sla_compliance_pct"))

        gold = (pipeline
                .join(sla, pipeline.status == sla.sla_status, "left")
                .withColumn("sla_compliance_pct",
                            coalesce(col("sla_compliance_pct"), lit(0)))
                .drop("sla_status"))
    else:
        gold = pipeline.withColumn("sla_compliance_pct", lit(0))

    return write_gold(gold, "case_pipeline")


# %% [markdown]
# ## Gold: Deadline Compliance
#
# Breaks down NSA deadline performance by deadline type (open_negotiation,
# idr_initiation, evidence_submission, idr_decision). For each type:
#   - met_count / missed_count / at_risk_count / pending_count
#   - compliance_pct = met / total × 100
#
# This is a key compliance metric: the NSA requires timely action at each
# stage. Missed deadlines can result in forfeiture of arbitration rights
# or default rulings against the provider.

# %%
def agg_deadline_compliance():
    dl = read_silver("deadlines")
    if dl is None:
        return 0

    gold = (dl
            .groupBy("type")
            .agg(
                count("*").alias("total_deadlines"),
                spark_sum(when(col("status") == "met", 1).otherwise(0)).alias("met_count"),
                spark_sum(when(col("status") == "missed", 1).otherwise(0)).alias("missed_count"),
                spark_sum(when(col("status") == "pending", 1).otherwise(0)).alias("pending_count"),
                spark_sum(when(col("is_at_risk") == 1, 1).otherwise(0)).alias("at_risk_count")
            )
            .withColumn("compliance_pct",
                        spark_round(
                            when(col("total_deadlines") > 0,
                                 col("met_count") / col("total_deadlines") * 100)
                            .otherwise(0), 2
                        ))
            .withColumnRenamed("type", "deadline_type"))

    return write_gold(gold, "deadline_compliance")


# %% [markdown]
# ## Gold: Underpayment Detection (Arbitration Eligibility)
#
# Per-claim analysis of whether an underpaid claim qualifies for NSA
# Independent Dispute Resolution (IDR / arbitration).
#
# Arbitration eligibility criteria (simplified):
#   1. underpayment_amount > $25 — de minimis threshold (not worth filing
#      IDR for trivial amounts due to the $50 administrative fee)
#   2. billed_amount > QPA (Qualifying Payment Amount) — the provider's
#      charge exceeds the median in-network rate, indicating the payer
#      paid below the expected benchmark
#
# The QPA is the payer's median contracted rate for the same service in
# the same geographic area. Under the NSA, it's the starting point for
# determining fair payment in out-of-network and surprise billing disputes.
#
# underpayment_pct helps prioritize: a 50% underpayment on a $1000 claim
# is more actionable than a 5% underpayment on a $100 claim.

# %%
def agg_underpayment_detection():
    disputes = read_silver("disputes")
    if disputes is None:
        return 0

    gold = (disputes
            .filter(col("underpayment_amount") > 0)
            # What % of the billed amount was underpaid
            .withColumn("underpayment_pct",
                        spark_round(
                            when(col("billed_amount") > 0,
                                 col("underpayment_amount") / col("billed_amount") * 100)
                            .otherwise(0), 2
                        ))
            # NSA arbitration eligibility: underpayment > $25 AND billed > QPA
            .withColumn("arbitration_eligible",
                        when((col("underpayment_amount") > 25) &
                             (col("billed_amount") > coalesce(col("qpa_amount"), lit(0))),
                             lit(1))
                        .otherwise(lit(0)))
            .withColumnRenamed("status", "dispute_status")
            .select(
                "claim_id", "payer_id", "payer_name", "provider_npi",
                "date_of_service", "billed_amount", "paid_amount",
                "qpa_amount", "underpayment_amount", "underpayment_pct",
                "arbitration_eligible", "dispute_status"
            ))

    return write_gold(gold, "underpayment_detection")


# %% [markdown]
# ## Execute All Gold Aggregations
#
# Gold aggregations are independent — each reads from Silver and writes its
# own Gold table. Order doesn't matter. After completion, all 8 Gold Delta
# tables are available for Power BI Direct Lake consumption.

# %%
aggregations = [
    ("gold_recovery_by_payer", agg_recovery_by_payer),
    ("gold_cpt_analysis", agg_cpt_analysis),
    ("gold_payer_scorecard", agg_payer_scorecard),
    ("gold_financial_summary", agg_financial_summary),
    ("gold_claims_aging", agg_claims_aging),
    ("gold_case_pipeline", agg_case_pipeline),
    ("gold_deadline_compliance", agg_deadline_compliance),
    ("gold_underpayment_detection", agg_underpayment_detection),
]

summary = {}
for name, fn in aggregations:
    try:
        count = fn()
        summary[name] = count
        print(f"  {name:35s} → {count} rows")
    except Exception as e:
        summary[name] = f"ERROR: {e}"
        print(f"  {name:35s} → ERROR: {e}")

print(f"\nGold aggregation complete: {summary}")
print("\nGold tables are now available for Power BI Direct Lake consumption.")
# After this notebook completes, Power BI reports connected via Direct Lake
# mode will automatically reflect the latest data on next refresh — no ETL
# import step needed. Direct Lake reads Delta Lake files natively from OneLake.
