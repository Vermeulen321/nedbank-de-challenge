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


import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, conv, current_date, datediff, floor, lit, substring, sha2, when,
)

from pipeline.spark_utils import get_or_create_spark

# dq_flag values whose records are excluded from fact_transactions.
# - ORPHANED_ACCOUNT:  no valid account; quarantined.
# - DUPLICATE_DEDUPED: extra copy; only the first occurrence (dq_flag=NULL) is kept.
# - NULL_REQUIRED:     required field missing; cannot be reliably loaded.
_EXCLUDED_FLAGS = {"ORPHANED_ACCOUNT", "DUPLICATE_DEDUPED", "NULL_REQUIRED"}


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


def _write_dq_report(
    config: dict,
    dq_rules: dict,
    source_counts: dict,
    dq_stats: dict,
    gold_counts: dict,
    pipeline_start: float,
) -> None:
    """Write /data/output/dq_report.json conforming to dq_report_template.json."""
    import time

    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    duration = int(time.time() - pipeline_start)

    handling_actions = dq_rules.get("handling_actions", {})
    txn_raw = source_counts["transactions_raw"]
    acct_raw = source_counts["accounts_raw"]

    # Total non-standard dates across all source files.
    total_date_issues = (
        dq_stats.get("date_issues_accounts", 0)
        + dq_stats.get("date_issues_customers", 0)
        + dq_stats.get("date_format_transactions", 0)
    )

    # Build dq_issues list — omit zero-count entries per spec.
    issue_specs = [
        {
            "issue_type": "duplicate_transactions",
            "records_affected": dq_stats.get("duplicate_transactions", 0),
            "denominator": txn_raw,
            "handling_action": handling_actions.get("duplicate_transactions", "DEDUPLICATED_KEEP_FIRST"),
            "records_in_output": 0,
        },
        {
            "issue_type": "orphaned_transactions",
            "records_affected": dq_stats.get("orphaned_transactions", 0),
            "denominator": txn_raw,
            "handling_action": handling_actions.get("orphaned_transactions", "QUARANTINED"),
            "records_in_output": 0,
        },
        {
            "issue_type": "amount_type_mismatch",
            "records_affected": dq_stats.get("amount_type_mismatch", 0),
            "denominator": txn_raw,
            "handling_action": handling_actions.get("amount_type_mismatch", "CAST_TO_DECIMAL"),
            "records_in_output": dq_stats.get("amount_type_mismatch", 0),
        },
        {
            "issue_type": "date_format_inconsistency",
            "records_affected": total_date_issues,
            "denominator": txn_raw,
            "handling_action": handling_actions.get("date_format_inconsistency", "NORMALISED_DATE"),
            "records_in_output": total_date_issues,
        },
        {
            "issue_type": "currency_variants",
            "records_affected": dq_stats.get("currency_variants", 0),
            "denominator": txn_raw,
            "handling_action": handling_actions.get("currency_variants", "NORMALISED_CURRENCY"),
            "records_in_output": dq_stats.get("currency_variants", 0),
        },
        {
            "issue_type": "null_account_id",
            "records_affected": dq_stats.get("null_account_id", 0),
            "denominator": acct_raw,
            "handling_action": handling_actions.get("null_account_id", "EXCLUDED_NULL_PK"),
            "records_in_output": 0,
        },
    ]

    dq_issues = [
        {
            "issue_type": spec["issue_type"],
            "records_affected": spec["records_affected"],
            "percentage_of_total": round(
                spec["records_affected"] / spec["denominator"] * 100, 2
            ) if spec["denominator"] > 0 else 0.0,
            "handling_action": spec["handling_action"],
            "records_in_output": spec["records_in_output"],
        }
        for spec in issue_specs
        if spec["records_affected"] > 0
    ]

    report = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_timestamp,
        "stage": "2",
        "source_record_counts": source_counts,
        "dq_issues": dq_issues,
        "gold_layer_record_counts": gold_counts,
        "execution_duration_seconds": duration,
    }

    report_path = config["output"].get("dq_report_path", "/data/output/dq_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)


def run_provisioning(
    config: dict,
    dq_rules: dict = None,
    source_counts: dict = None,
    dq_stats: dict = None,
    pipeline_start: float = None,
) -> None:
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

    # Exclude records that are quarantined or deduped-away — they must not appear
    # in fact_transactions (confirmed by Validation Query 2 and dq_report counts).
    txns = txns.filter(
        col("dq_flag").isNull() | ~col("dq_flag").isin(list(_EXCLUDED_FLAGS))
    )

    # Lookup tables: only the columns needed to resolve surrogate keys.
    acct_lookup = dim_accounts.select("account_id", "account_sk", "customer_id")
    cust_lookup = dim_customers.select("customer_id", "customer_sk")

    # Join transactions → dim_accounts to get account_sk and customer_id.
    fact = txns.join(acct_lookup, on="account_id", how="left")
    # Join → dim_customers (via the customer_id brought in from dim_accounts).
    fact = fact.join(cust_lookup, on="customer_id", how="left")

    # Surrogate key for transactions.
    fact = _add_sk(fact, "transaction_id", "transaction_sk")

    # merchant_subcategory is included from silver (Stage 2 populates it;
    # Stage 1 silver has it as NULL due to the absent source field).
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

    # ── Write dq_report.json (Stage 2+) ───────────────────────────────────────
    if source_counts is not None and dq_stats is not None:
        gold_counts = {
            "fact_transactions": spark.read.format("delta").load(gold + "/fact_transactions").count(),
            "dim_accounts": spark.read.format("delta").load(gold + "/dim_accounts").count(),
            "dim_customers": spark.read.format("delta").load(gold + "/dim_customers").count(),
        }
        _write_dq_report(
            config, dq_rules or {}, source_counts, dq_stats, gold_counts, pipeline_start or 0.0
        )
