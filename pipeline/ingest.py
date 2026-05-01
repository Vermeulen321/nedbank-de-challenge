"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""


from datetime import datetime, timezone

from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.spark_utils import get_or_create_spark


# Explicit JSON schema for transactions.
# All scalar fields are read as STRING so that mixed-type DQ injections
# (Stage 2: numeric-coded currency `710`, epoch-int dates, string-quoted
# amounts) parse without nulling. Type casts happen in the Silver layer.
# The schema must be set explicitly to preserve every record — Spark's
# JSON inference would either drop or corrupt mixed-type rows.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("transaction_date", StringType()),
    StructField("transaction_time", StringType()),
    StructField("transaction_type", StringType()),
    StructField("merchant_category", StringType()),
    StructField("merchant_subcategory", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("channel", StringType()),
    StructField("location", StructType([
        StructField("province", StringType()),
    ])),
])


def run_ingestion(config: dict) -> dict:
    """Ingest raw source files into Bronze Delta tables.

    Returns a dict of raw source record counts for the dq_report (Stage 2+).
    """
    spark = get_or_create_spark(config)

    # Single consistent timestamp for the entire ingestion run.
    ingestion_ts = datetime.now(timezone.utc)
    ts_col = lit(ingestion_ts).cast("timestamp")

    inp = config["input"]
    bronze = config["output"]["bronze_path"]

    # ── accounts (CSV) ────────────────────────────────────────────────────────
    accounts = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(inp["accounts_path"])
        .withColumn("ingestion_timestamp", ts_col)
    )
    accounts.write.format("delta").mode("overwrite").save(bronze + "/accounts")

    # ── customers (CSV) ───────────────────────────────────────────────────────
    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(inp["customers_path"])
        .withColumn("ingestion_timestamp", ts_col)
    )
    customers.write.format("delta").mode("overwrite").save(bronze + "/customers")

    # ── transactions (JSONL — one JSON object per line) ───────────────────────
    # Read raw text and parse with the explicit schema above. The same pass
    # captures `_amount_was_string` from the raw line — a regex against the
    # JSON text is the only reliable way to distinguish a string-quoted
    # amount (TYPE_MISMATCH) from a numeric one once everything is cast to
    # STRING by the schema.
    transactions = (
        spark.read.text(inp["transactions_path"])
        .select(
            from_json(col("value"), TRANSACTION_SCHEMA).alias("d"),
            col("value").rlike(r'"amount"\s*:\s*"').alias("_amount_was_string"),
        )
        .select("d.*", "_amount_was_string")
        .withColumn("ingestion_timestamp", ts_col)
    )
    transactions.write.format("delta").mode("overwrite").save(bronze + "/transactions")

    # ── Source record counts for dq_report (Stage 2+) ─────────────────────────
    # Read counts from the written Delta tables to avoid re-scanning source files.
    accounts_raw = spark.read.format("delta").load(bronze + "/accounts").count()
    customers_raw = spark.read.format("delta").load(bronze + "/customers").count()
    transactions_raw = spark.read.format("delta").load(bronze + "/transactions").count()

    return {
        "accounts_raw": accounts_raw,
        "customers_raw": customers_raw,
        "transactions_raw": transactions_raw,
    }
