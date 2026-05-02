"""
Stage 3 — Streaming extension: process micro-batch JSONL files from /data/stream/.

Polling loop. On each cycle: list new files in the stream directory, parse
each into Spark, then upsert into two Gold-layer Delta tables:

  /data/output/stream_gold/current_balances/      (one row per account_id)
  /data/output/stream_gold/recent_transactions/   (last 50 per account_id)

`current_balances.current_balance` is computed by walking the batch
`dim_accounts.current_balance` baseline forward through stream events:
CREDIT and REVERSAL increase the balance; DEBIT and FEE decrease it.

The loop terminates when no new files have arrived for `quiesce_timeout`
seconds — required because the container has a 30-minute hard cap.

Schema reuse: we import `TRANSACTION_SCHEMA` from `pipeline.ingest` so
the streaming reader carries the same Stage 2 robustness against
mixed-type DQ injections (numeric currencies, string-quoted amounts,
non-ISO dates) without code duplication.
"""

import glob
import os
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce, col, concat, current_timestamp, date_format, from_json, lit,
    max as F_max, row_number, sum as F_sum, to_timestamp, when,
)
from pyspark.sql.window import Window

from pipeline.ingest import TRANSACTION_SCHEMA
from pipeline.spark_utils import get_or_create_spark
from pipeline.transform import _parse_date_flex


# Transaction types that increase the running balance. Anything else
# (DEBIT, FEE, unknown) decreases it. Kept as a module constant rather
# than a yaml entry: this is invariant accounting semantics, not a DQ rule.
_BALANCE_INCREASE_TYPES = ("CREDIT", "REVERSAL")


def _list_unprocessed(stream_dir: str, processed: set) -> list:
    """Return new stream files in chronological (filename) order."""
    pattern = os.path.join(stream_dir, "stream_*.jsonl")
    return sorted(f for f in glob.glob(pattern) if f not in processed)


def _parse_file(spark, path: str) -> DataFrame:
    """Parse a single stream file into a typed DataFrame.

    Reuses the Stage 2 explicit JSON schema so mixed-type DQ injections
    do not silently null any rows.
    """
    raw = spark.read.text(path)
    events = (
        raw.select(from_json(col("value"), TRANSACTION_SCHEMA).alias("d"))
        .select("d.*")
        .filter(col("transaction_id").isNotNull() & col("account_id").isNotNull())
        .withColumn("transaction_date", _parse_date_flex(col("transaction_date")))
        .withColumn("amount", col("amount").cast("decimal(18,2)"))
        .withColumn(
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
        .filter(col("amount").isNotNull() & col("transaction_timestamp").isNotNull())
    )
    return events


def _merge_current_balances(spark, events: DataFrame, dim_accounts: DataFrame, cb_path: str) -> None:
    """Upsert per-account running balance into `current_balances`.

    Algorithm:
      1. Aggregate the batch's events to a per-account net delta and the
         most recent transaction timestamp.
      2. Resolve each account's baseline:
           - if the account already has a row in current_balances, use that;
           - else fall back to dim_accounts.current_balance from the batch;
           - else 0 (orphaned stream events — discard penalty already
             accounted for in the batch dq_report).
      3. New balance = baseline + delta. Merge into the Delta table.
    """
    delta = F_sum(
        when(col("transaction_type").isin(*_BALANCE_INCREASE_TYPES), col("amount"))
        .otherwise(-col("amount"))
    ).alias("delta")
    last_ts = F_max("transaction_timestamp").alias("last_ts")

    per_account = events.groupBy("account_id").agg(delta, last_ts)

    baseline = dim_accounts.select(
        "account_id", col("current_balance").alias("_dim_bal")
    )

    if DeltaTable.isDeltaTable(spark, cb_path):
        existing = (
            spark.read.format("delta").load(cb_path)
            .select("account_id", col("current_balance").alias("_existing_bal"))
        )
        with_baseline = (
            per_account
            .join(existing, "account_id", "left")
            .join(baseline, "account_id", "left")
            .withColumn(
                "_baseline",
                coalesce(col("_existing_bal"), col("_dim_bal"), lit(0).cast("decimal(18,2)")),
            )
        )
    else:
        # First batch: no current_balances table yet, so the only baseline
        # is dim_accounts. Skip the empty-DataFrame fallback path because
        # spark.createDataFrame([], ...) imports pandas, which fails in the
        # base image due to a NumPy 2 / PyArrow ABI mismatch.
        with_baseline = (
            per_account
            .join(baseline, "account_id", "left")
            .withColumn(
                "_baseline",
                coalesce(col("_dim_bal"), lit(0).cast("decimal(18,2)")),
            )
        )

    new_state = with_baseline.select(
        col("account_id"),
        (col("_baseline") + col("delta")).cast("decimal(18,2)").alias("current_balance"),
        col("last_ts").alias("last_transaction_timestamp"),
        current_timestamp().alias("updated_at"),
    )

    if DeltaTable.isDeltaTable(spark, cb_path):
        (
            DeltaTable.forPath(spark, cb_path).alias("t")
            .merge(new_state.alias("u"), "t.account_id = u.account_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new_state.write.format("delta").mode("overwrite").save(cb_path)


def _merge_recent_transactions(spark, events: DataFrame, rt_path: str) -> None:
    """Merge events into `recent_transactions`, then truncate to last 50/account.

    Truncation strategy: read the merged table back and overwrite with
    rows where row_number() over (partition by account_id order by
    transaction_timestamp desc) ≤ 50. At Stage 3 sample volume
    (~2.4K events across 12 files) the rewrite is essentially free;
    at production scale this would be replaced with a Delta DELETE
    against the rows beyond position 50.
    """
    upsert = events.select(
        "account_id",
        "transaction_id",
        "transaction_timestamp",
        "amount",
        "transaction_type",
        "channel",
        current_timestamp().alias("updated_at"),
    )

    if DeltaTable.isDeltaTable(spark, rt_path):
        (
            DeltaTable.forPath(spark, rt_path).alias("t")
            .merge(
                upsert.alias("u"),
                "t.account_id = u.account_id AND t.transaction_id = u.transaction_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        upsert.write.format("delta").mode("overwrite").save(rt_path)

    # Retain only the 50 most recent rows per account.
    full = spark.read.format("delta").load(rt_path)
    w = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").desc())
    pruned = full.withColumn("_rn", row_number().over(w)).filter(col("_rn") <= 50).drop("_rn")
    pruned.write.format("delta").mode("overwrite").save(rt_path)


def run_stream_ingestion(config: dict) -> None:
    """Poll /data/stream/ and merge each new file into stream_gold tables.

    Returns when no new files have arrived for `quiesce_timeout_seconds`.
    """
    streaming = config.get("streaming") or {}
    stream_dir = streaming.get("stream_input_path", "/data/stream")
    out_root = streaming.get("stream_gold_path", "/data/output/stream_gold")
    poll_interval = streaming.get("poll_interval_seconds", 10)
    quiesce_timeout = streaming.get("quiesce_timeout_seconds", 60)

    if not os.path.isdir(stream_dir):
        # No stream directory mounted — nothing to do (e.g. Stage 1/2 reruns).
        return

    spark = get_or_create_spark(config)
    cb_path = out_root + "/current_balances"
    rt_path = out_root + "/recent_transactions"

    # Cache the batch dim_accounts snapshot once: it is the baseline for
    # every account that has not yet been updated by a stream event.
    gold = config["output"]["gold_path"]
    dim_accounts = (
        spark.read.format("delta").load(gold + "/dim_accounts")
        .select("account_id", "current_balance")
    ).cache()
    dim_accounts.count()  # materialise the cache

    processed: set = set()
    last_progress = time.time()

    while True:
        new_files = _list_unprocessed(stream_dir, processed)
        if new_files:
            for path in new_files:
                events = _parse_file(spark, path)
                if events.head(1):
                    _merge_current_balances(spark, events, dim_accounts, cb_path)
                    _merge_recent_transactions(spark, events, rt_path)
                processed.add(path)
            last_progress = time.time()
        elif time.time() - last_progress >= quiesce_timeout:
            break
        time.sleep(poll_interval)

    dim_accounts.unpersist()
