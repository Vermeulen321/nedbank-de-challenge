"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""


from pyspark.sql.functions import (
    col, concat, lit, to_date, to_timestamp, upper, trim,
    row_number, when,
)
from pyspark.sql.window import Window

from pipeline.spark_utils import get_or_create_spark


def run_transformation(config: dict, dq_rules: dict) -> None:
    spark = get_or_create_spark(config)
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    # ── Accounts ──────────────────────────────────────────────────────────────
    accts = spark.read.format("delta").load(bronze + "/accounts")
    accts = (
        accts
        .withColumn("open_date", to_date(col("open_date"), "yyyy-MM-dd"))
        .withColumn("last_activity_date", to_date(col("last_activity_date"), "yyyy-MM-dd"))
        .withColumn("credit_limit", col("credit_limit").cast("decimal(18,2)"))
        .withColumn("current_balance", col("current_balance").cast("decimal(18,2)"))
    )
    w_acct = Window.partitionBy("account_id").orderBy("ingestion_timestamp")
    accts = (
        accts
        .withColumn("_rn", row_number().over(w_acct))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    accts.write.format("delta").mode("overwrite").save(silver + "/accounts")

    # ── Customers ─────────────────────────────────────────────────────────────
    custs = spark.read.format("delta").load(bronze + "/customers")
    custs = custs.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
    w_cust = Window.partitionBy("customer_id").orderBy("ingestion_timestamp")
    custs = (
        custs
        .withColumn("_rn", row_number().over(w_cust))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    custs.write.format("delta").mode("overwrite").save(silver + "/customers")

    # ── Transactions ──────────────────────────────────────────────────────────
    txns = spark.read.format("delta").load(bronze + "/transactions")

    # Flatten the nested location struct to a top-level province column.
    txns = txns.withColumn("province", col("location.province"))

    # Pull DQ rule parameters from dq_rules.yaml.
    required_fields = dq_rules["null_checks"]["transactions"]
    valid_types = dq_rules["domain_checks"]["transaction_type"]["allowed"]

    # 1. NULL_REQUIRED — any required field is null.
    null_cond = None
    for field in required_fields:
        c = col(field).isNull()
        null_cond = c if null_cond is None else (null_cond | c)
    txns = txns.withColumn(
        "dq_flag",
        when(null_cond, lit("NULL_REQUIRED")).otherwise(lit(None).cast("string")),
    )

    # 2. DUPLICATE_DEDUPED — second or later occurrence of the same transaction_id.
    w_txn = Window.partitionBy("transaction_id").orderBy("ingestion_timestamp")
    txns = txns.withColumn("_rn", row_number().over(w_txn))
    txns = txns.withColumn(
        "dq_flag",
        when(col("dq_flag").isNull() & (col("_rn") > 1), lit("DUPLICATE_DEDUPED"))
        .otherwise(col("dq_flag")),
    ).drop("_rn")

    # 3. TYPE_MISMATCH — transaction_type not in the allowed domain.
    txns = txns.withColumn(
        "dq_flag",
        when(
            col("dq_flag").isNull() & ~col("transaction_type").isin(valid_types),
            lit("TYPE_MISMATCH"),
        ).otherwise(col("dq_flag")),
    )

    # 4. DATE_FORMAT — transaction_date cannot be parsed as YYYY-MM-DD.
    txns = txns.withColumn("_parsed_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    txns = txns.withColumn(
        "dq_flag",
        when(col("dq_flag").isNull() & col("_parsed_date").isNull(), lit("DATE_FORMAT"))
        .otherwise(col("dq_flag")),
    ).drop("_parsed_date")

    # 5. CURRENCY_VARIANT — source currency is not already "ZAR".
    txns = txns.withColumn(
        "dq_flag",
        when(
            col("dq_flag").isNull() & (upper(trim(col("currency"))) != lit("ZAR")),
            lit("CURRENCY_VARIANT"),
        ).otherwise(col("dq_flag")),
    )
    # Normalise all currency values to "ZAR" regardless of flag.
    txns = txns.withColumn("currency", lit("ZAR"))

    # 6. ORPHANED_ACCOUNT — account_id not present in the silver accounts table.
    silver_acct_ids = (
        spark.read.format("delta").load(silver + "/accounts")
        .select(col("account_id").alias("_known_account_id"))
        .withColumn("_acct_exists", lit(True))
    )
    txns = txns.join(silver_acct_ids, col("account_id") == col("_known_account_id"), how="left")
    txns = txns.withColumn(
        "dq_flag",
        when(col("dq_flag").isNull() & col("_acct_exists").isNull(), lit("ORPHANED_ACCOUNT"))
        .otherwise(col("dq_flag")),
    ).drop("_known_account_id", "_acct_exists")

    # ── Type casts (applied to all rows; flagged rows may produce nulls) ──────
    # Build transaction_timestamp from raw string fields before casting transaction_date.
    txns = txns.withColumn(
        "transaction_timestamp",
        to_timestamp(
            concat(col("transaction_date"), lit(" "), col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    txns = txns.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    txns = txns.withColumn("amount", col("amount").cast("decimal(18,2)"))

    # Select only the columns needed downstream; drop nested structs.
    txns = txns.select(
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_time",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )

    txns.write.format("delta").mode("overwrite").save(silver + "/transactions")
