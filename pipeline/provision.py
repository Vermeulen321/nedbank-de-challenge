"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""


from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, conv, current_date, datediff, floor, lit, substring, sha2, when,
)

from pipeline.spark_utils import get_or_create_spark


def _add_sk(df: DataFrame, natural_key_col: str, sk_col_name: str) -> DataFrame:
    """
    Attach a deterministic BIGINT surrogate key derived from the natural key.

    Strategy: take the first 15 hex characters of SHA-256(natural_key),
    convert from base-16 to base-10, cast to BIGINT.
    15 hex digits = 60 bits → fits in a signed BIGINT (max 63 bits positive).
    The key is stable across reruns on identical input data.
    """
    return df.withColumn(
        sk_col_name,
        conv(
            substring(sha2(col(natural_key_col).cast("string"), 256), 1, 15),
            16,
            10,
        ).cast("bigint"),
    )


def _age_band_expr(dob_col: str):
    """Return a Column expression that buckets age into standard bands."""
    age = floor(datediff(current_date(), col(dob_col)) / 365.25)
    return (
        when(age < 26, "18-25")
        .when(age < 36, "26-35")
        .when(age < 46, "36-45")
        .when(age < 56, "46-55")
        .when(age < 66, "56-65")
        .otherwise("65+")
    )


def run_provisioning(config: dict) -> None:
    spark = get_or_create_spark(config)
    silver = config["output"]["silver_path"]
    gold = config["output"]["gold_path"]

    # ── dim_customers ─────────────────────────────────────────────────────────
    custs = spark.read.format("delta").load(silver + "/customers")

    dim_customers = (
        custs
        .withColumn("age_band", _age_band_expr("dob"))
        .withColumn("risk_score", col("risk_score").cast("integer"))
    )
    dim_customers = _add_sk(dim_customers, "customer_id", "customer_sk")
    dim_customers = dim_customers.select(
        "customer_sk",
        "customer_id",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "age_band",
    )
    dim_customers.write.format("delta").mode("overwrite").save(gold + "/dim_customers")

    # ── dim_accounts ──────────────────────────────────────────────────────────
    accts = spark.read.format("delta").load(silver + "/accounts")

    dim_accounts = accts.withColumnRenamed("customer_ref", "customer_id")
    dim_accounts = _add_sk(dim_accounts, "account_id", "account_sk")
    dim_accounts = dim_accounts.select(
        "account_sk",
        "account_id",
        "customer_id",          # renamed from customer_ref — required for Validation Query 2
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
    )
    dim_accounts.write.format("delta").mode("overwrite").save(gold + "/dim_accounts")

    # ── fact_transactions ─────────────────────────────────────────────────────
    txns = spark.read.format("delta").load(silver + "/transactions")

    # Lookup tables: only the columns needed to resolve surrogate keys.
    acct_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup = dim_customers.select("customer_id", "customer_sk")

    # Join transactions → dim_accounts to get account_sk and customer_id.
    fact = txns.join(acct_lookup, on="account_id", how="left")
    # Join → dim_customers (via the customer_id brought in from dim_accounts).
    fact = fact.join(cust_lookup, on="customer_id", how="left")

    # Surrogate key for transactions.
    fact = _add_sk(fact, "transaction_id", "transaction_sk")

    # merchant_subcategory is absent from Stage 1 source; include as NULL
    # so the schema matches the spec and scoring queries don't fail.
    fact = fact.withColumn("merchant_subcategory", lit(None).cast("string"))

    fact = fact.select(
        "transaction_sk",
        "transaction_id",
        "account_sk",
        "customer_sk",
        "transaction_date",
        "transaction_timestamp",
        "transaction_type",
        "merchant_category",
        "merchant_subcategory",
        "amount",
        "currency",
        "channel",
        "province",
        "dq_flag",
        "ingestion_timestamp",
    )
    fact.write.format("delta").mode("overwrite").save(gold + "/fact_transactions")
