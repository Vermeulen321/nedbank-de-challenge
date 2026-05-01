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
    coalesce, col, concat, date_format, from_unixtime, lit, to_date,
    to_timestamp, trim, row_number, when,
)
from pyspark.sql.window import Window

from pipeline.spark_utils import get_or_create_spark


def _parse_date_flex(date_col):
    """Parse a date column that may use YYYY-MM-DD, DD/MM/YYYY, or Unix epoch.

    Returns a DATE column; null if the value is truly unparseable.
    Tries formats in order: ISO → DD/MM/YYYY → Unix epoch integer.
    """
    return coalesce(
        to_date(date_col, "yyyy-MM-dd"),
        to_date(date_col, "dd/MM/yyyy"),
        to_date(from_unixtime(date_col.cast("long"))),
    )


def _is_nonstandard_date(date_col):
    """Returns a Column expression that is True when a non-null date value is
    not parseable as YYYY-MM-DD (i.e. it's in an alternative format or garbage).
    """
    return col(date_col).isNotNull() & to_date(col(date_col), "yyyy-MM-dd").isNull()


def run_transformation(config: dict, dq_rules: dict) -> dict:
    """Run Silver-layer transformation and return DQ stats for dq_report."""
    spark = get_or_create_spark(config)
    bronze = config["output"]["bronze_path"]
    silver = config["output"]["silver_path"]

    dq_stats: dict = {}

    # ── Accounts ──────────────────────────────────────────────────────────────
    accts = spark.read.format("delta").load(bronze + "/accounts")

    # Exclude records with null primary key (EXCLUDED_NULL_PK).
    null_pk_field = dq_rules.get("null_pk_checks", {}).get("accounts", "account_id")
    null_pk_count = accts.filter(col(null_pk_field).isNull()).count()
    dq_stats["null_account_id"] = null_pk_count
    accts = accts.filter(col(null_pk_field).isNotNull())

    # Count non-standard date values before normalisation.
    date_issues_accts = accts.filter(
        _is_nonstandard_date("open_date") | _is_nonstandard_date("last_activity_date")
    ).count()
    dq_stats["date_issues_accounts"] = date_issues_accts

    # Normalise dates — accept YYYY-MM-DD, DD/MM/YYYY, and Unix epoch.
    accts = (
        accts
        .withColumn("open_date", _parse_date_flex(col("open_date")))
        .withColumn("last_activity_date", _parse_date_flex(col("last_activity_date")))
        .withColumn("credit_limit", col("credit_limit").cast("decimal(18,2)"))
        .withColumn("current_balance", col("current_balance").cast("decimal(18,2)"))
    )

    # Deduplicate on natural key.
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

    # Count non-standard dob values before normalisation.
    date_issues_custs = custs.filter(_is_nonstandard_date("dob")).count()
    dq_stats["date_issues_customers"] = date_issues_custs

    custs = custs.withColumn("dob", _parse_date_flex(col("dob")))

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

    # 3. TYPE_MISMATCH — amount delivered as a JSON string rather than a number.
    #    `_amount_was_string` was captured at ingestion via regex on the raw line.
    #    Handling action: CAST_TO_DECIMAL (record retained, value cast below).
    txns = txns.withColumn(
        "dq_flag",
        when(
            col("dq_flag").isNull() & col("_amount_was_string"),
            lit("TYPE_MISMATCH"),
        ).otherwise(col("dq_flag")),
    )

    # 4. DATE_FORMAT — transaction_date not in YYYY-MM-DD format.
    #    Flag records whose date is not standard, then normalise using multi-format parser.
    txns = txns.withColumn(
        "dq_flag",
        when(
            col("dq_flag").isNull() & _is_nonstandard_date("transaction_date"),
            lit("DATE_FORMAT"),
        ).otherwise(col("dq_flag")),
    )

    # 5. CURRENCY_VARIANT — source currency value is anything other than the
    #    canonical "ZAR" string. Per Stage 2 spec, lowercase "zar" is a variant
    #    too — strict equality, not upper(trim()), is required.
    txns = txns.withColumn(
        "dq_flag",
        when(
            col("dq_flag").isNull() & (trim(col("currency")) != lit("ZAR")),
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

    # ── Type casts ────────────────────────────────────────────────────────────
    # Normalise transaction_date FIRST (handles DD/MM/YYYY and epoch variants),
    # then build transaction_timestamp from the canonical ISO date so all
    # rows produce a valid TIMESTAMP regardless of source format.
    txns = txns.withColumn("transaction_date", _parse_date_flex(col("transaction_date")))
    txns = txns.withColumn(
        "transaction_timestamp",
        to_timestamp(
            concat(
                date_format(col("transaction_date"), "yyyy-MM-dd"),
                lit(" "),
                col("transaction_time"),
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )
    txns = txns.withColumn("amount", col("amount").cast("decimal(18,2)"))

    # Handle merchant_subcategory — present in Stage 2 source, absent in Stage 1.
    merch_sub = (
        col("merchant_subcategory")
        if "merchant_subcategory" in txns.columns
        else lit(None).cast("string")
    )

    # Select only the columns needed downstream; drop nested structs.
    txns = txns.select(
        "transaction_id",
        "account_id",
        "transaction_date",
        "transaction_time",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        merch_sub.alias("merchant_subcategory"),
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )

    txns.write.format("delta").mode("overwrite").save(silver + "/transactions")

    # ── Collect DQ flag counts from silver for dq_report ─────────────────────
    flag_counts = (
        spark.read.format("delta").load(silver + "/transactions")
        .groupBy("dq_flag")
        .count()
        .collect()
    )
    flag_map = {row["dq_flag"]: row["count"] for row in flag_counts}

    dq_stats["duplicate_transactions"] = flag_map.get("DUPLICATE_DEDUPED", 0)
    dq_stats["orphaned_transactions"] = flag_map.get("ORPHANED_ACCOUNT", 0)
    dq_stats["date_format_transactions"] = flag_map.get("DATE_FORMAT", 0)
    dq_stats["currency_variants"] = flag_map.get("CURRENCY_VARIANT", 0)
    dq_stats["null_required_transactions"] = flag_map.get("NULL_REQUIRED", 0)
    dq_stats["amount_type_mismatch"] = flag_map.get("TYPE_MISMATCH", 0)

    return dq_stats
