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

from pyspark.sql.functions import lit

from pipeline.spark_utils import get_or_create_spark


def run_ingestion(config: dict) -> None:
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
    # Spark's JSON reader handles nested location/metadata structs automatically.
    transactions = (
        spark.read
        .option("multiline", False)
        .json(inp["transactions_path"])
        .withColumn("ingestion_timestamp", ts_col)
    )
    transactions.write.format("delta").mode("overwrite").save(bronze + "/transactions")
