# Fabric Notebook: nb_bronze_cdc
# Bronze Layer — CDC Parquet ingestion into Delta Lake
#
# ── Medallion Architecture: Bronze Layer ──────────────────────────────────
# This is the first layer of the Medallion (Bronze → Silver → Gold) pipeline.
# Bronze = raw, append-only landing zone. Every CDC event is stored exactly
# as it arrived from the source OLTP database, with no transformations.
#
# ── CDC (Change Data Capture) Overview ────────────────────────────────────
# The OLTP database (Azure SQL) tracks row-level changes via watermark columns
# (updated_at, created_at, last_activity). ADF's pl_cdc_incremental_copy
# pipeline reads rows WHERE watermark_col > last_sync_ts and writes them as
# date-partitioned Parquet files into ADLS Gen2:
#
#   bronze/{table}/year=YYYY/month=MM/day=DD/{table}_{timestamp}.parquet
#
# Each Parquet row carries a _cdc_operation flag:
#   - "I" = Insert (new row in source)
#   - "U" = Update (existing row changed)
#   - "D" = Delete (row removed — used by native CDC if enabled)
#
# This notebook reads those Parquet files and appends them to Delta Lake
# tables in the Fabric Lakehouse. Delta gives us ACID transactions, time
# travel, and schema evolution on top of the raw CDC stream.
#
# ── Why Append-Only? ──────────────────────────────────────────────────────
# Bronze never updates or deletes rows — every CDC event is a new record.
# This preserves the full audit trail of every change. The Silver layer
# is responsible for resolving CDC events into the current-state view
# (latest per primary key, excluding deletes).
#
# Schedule: triggered by pl_master_orchestrator after CDC copy completes
# ---------------------------------------------------------------------------

# PARAMETERS (set by ADF pipeline or manual run)
# When triggered by pl_master_orchestrator, run_date is set to the pipeline
# trigger time. When empty, all unprocessed Parquet files are ingested.
run_date = ""  # e.g. "2026-03-26T10:00:00Z" — empty = process all unprocessed

# %% [markdown]
# ## Configuration

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
import json

spark = SparkSession.builder.getOrCreate()

# Lakehouse paths (Fabric mounts ADLS automatically via OneLake)
# BRONZE_ADLS_PATH = source Parquet files written by ADF CDC pipeline
# LAKEHOUSE_BRONZE = destination Delta tables in Fabric Lakehouse
BRONZE_ADLS_PATH = "abfss://bronze@{storage_account}.dfs.core.windows.net"
LAKEHOUSE_BRONZE = "Tables/bronze"

# Tables to process — must match the 9+ tables tracked by ADF's CDC pipeline
# (see adf/pipelines/pl_cdc_incremental_copy.json for the watermark columns).
# The OLTP schema has 14 tables total, but only these carry transactional data
# that changes over time and needs CDC replication into the Lakehouse.
CDC_TABLES = [
    "claims", "claim_lines", "remittances", "patients",
    "cases", "disputes", "deadlines", "evidence_artifacts",
    "audit_log", "payers", "providers", "fee_schedule"
]

# %% [markdown]
# ## Bronze Ingestion: Parquet → Delta Lake

# %%
def ingest_bronze_table(table_name: str, run_date: str = "") -> dict:
    """Read CDC Parquet from ADLS and append to Bronze Delta table.

    Bronze is append-only: every CDC event (I/U/D) is preserved as a new row.
    The Silver layer (nb_silver_transforms.py) resolves these into current-state
    views by windowing on the primary key and taking the latest non-delete event.

    Each Bronze row gets 5 metadata columns:
      _cdc_operation  — I (insert), U (update), D (delete)
      _cdc_timestamp  — when the change occurred in the source OLTP
      _source_table   — which OLTP table this row came from
      _bronze_loaded_ts — when this notebook ingested it (processing time)
      _source_file    — the ADLS Parquet file path (for lineage/debugging)
    """
    source_path = f"{BRONZE_ADLS_PATH}/{table_name}"
    delta_path = f"{LAKEHOUSE_BRONZE}/{table_name}"

    try:
        # Read new Parquet files written by ADF's CDC incremental copy.
        # ADF partitions output by date: bronze/{table}/year=YYYY/month=MM/day=DD/
        # Spark reads all partitions; in production you'd filter by run_date.
        df = spark.read.parquet(source_path)

        if df.rdd.isEmpty():
            return {"table": table_name, "status": "no_data", "rows": 0}

        # Stamp processing metadata — these columns track when and where
        # data entered the Lakehouse (distinct from the source CDC timestamp).
        df = (df
              .withColumn("_bronze_loaded_ts", current_timestamp())
              .withColumn("_source_file", input_file_name()))

        # Ensure CDC metadata columns exist. ADF normally adds these via the
        # Copy Activity's additional columns mapping. If they're missing
        # (e.g., initial full load), default to Insert operation + current time.
        if "_cdc_operation" not in df.columns:
            df = df.withColumn("_cdc_operation", lit("I"))
        if "_cdc_timestamp" not in df.columns:
            df = df.withColumn("_cdc_timestamp", current_timestamp())
        if "_source_table" not in df.columns:
            df = df.withColumn("_source_table", lit(table_name))

        row_count = df.count()

        # Append to existing Delta table, or create it on first run.
        # "append" mode = Bronze is insert-only; we never mutate past rows.
        # First-run uses "overwrite" to bootstrap the Delta table schema.
        if DeltaTable.isDeltaTable(spark, delta_path):
            df.write.format("delta").mode("append").save(delta_path)
        else:
            (df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .save(delta_path))

        # Compact small Parquet files inside the Delta table. Each CDC batch
        # creates many small files; compaction merges them for faster reads
        # in the Silver layer. This is safe on append-only tables.
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.optimize().executeCompaction()

        return {"table": table_name, "status": "success", "rows": row_count}

    except Exception as e:
        return {"table": table_name, "status": "error", "error": str(e), "rows": 0}


# %%
# Run Bronze ingestion for all CDC tables.
# Each table is processed sequentially — in production, you could parallelize
# with Spark's thread pool executor, but sequential is simpler to debug and
# sufficient for the ~12 tables in this pipeline.
results = []
for table in CDC_TABLES:
    result = ingest_bronze_table(table, run_date)
    results.append(result)
    status = result["status"]
    rows = result["rows"]
    print(f"  {table:30s} → {status} ({rows} rows)")

# Summary
total_rows = sum(r["rows"] for r in results)
success = sum(1 for r in results if r["status"] == "success")
errors = [r for r in results if r["status"] == "error"]

print(f"\nBronze CDC complete: {success}/{len(CDC_TABLES)} tables, {total_rows} total rows")
if errors:
    print(f"Errors: {json.dumps(errors, indent=2)}")

# %% [markdown]
# ## Vacuum old Delta versions (retain 7 days)
#
# Delta Lake keeps historical versions of data files for time-travel queries.
# Over time, these accumulate and consume storage. VACUUM removes files older
# than the retention threshold. 168 hours = 7 days, which gives enough runway
# to reprocess recent failures while keeping storage costs in check.
#
# Important: VACUUM is safe on append-only Bronze tables because we never
# update or delete rows — old versions are just prior append batches.

# %%
for table in CDC_TABLES:
    delta_path = f"{LAKEHOUSE_BRONZE}/{table}"
    try:
        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)
            delta_table.vacuum(168)  # 7 days in hours
    except Exception:
        pass  # Table may not exist yet on first run

print("Bronze vacuum complete")
