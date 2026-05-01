"""
Shared SparkSession factory.

Using getOrCreate() across all pipeline stages avoids the overhead of
tearing down and recreating a JVM between stages, and keeps peak memory
usage predictable within the 2 GB container limit.

Delta Lake JARs are pre-downloaded into $SPARK_HOME/jars/ during the Docker
build (see Dockerfile). At runtime they load automatically — no Ivy/Maven
download is attempted, which is required under --network=none.
"""

import os

from pyspark.sql import SparkSession


def get_or_create_spark(config: dict) -> SparkSession:
    # Redirect Spark's shuffle/spill scratch to a directory under /data/output.
    # The container provides only /data/output and a 512 MB tmpfs at /tmp;
    # at Stage 2 volume (3M transactions) shuffle spill exceeds the tmpfs
    # cap and the JVM aborts. /data/output sits on host disk with no fixed
    # quota, and Spark cleans the directory on session stop.
    spark_local_dir = config["spark"].get("local_dir", "/data/output/_spark_tmp")
    os.makedirs(spark_local_dir, exist_ok=True)

    return (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Bind to loopback — required with --network=none (no DNS resolution).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Stay within the 2 GB hard container memory limit.
        .config("spark.driver.memory", "1400m")
        # 32 shuffle partitions: small enough to avoid the 200-partition
        # default's overhead, large enough that 3M-row Stage 2 shuffles
        # spill in manageable chunks (8 partitions OOM'd at Stage 2 volume).
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.files.maxPartitionBytes", "67108864")  # 64 MB
        # Adaptive Query Execution: coalesces small shuffle partitions
        # post-shuffle and auto-promotes joins to broadcast where possible.
        # No-op overhead on Stage 1; meaningful speedup at Stage 2 volume.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Auto-broadcast joins with the build side under 32 MB. The Silver
        # accounts lookup (~300K rows × few cols) fits well below this.
        .config("spark.sql.autoBroadcastJoinThreshold", "33554432")
        .config("spark.local.dir", spark_local_dir)
        # Snappy extracts a native .so to /tmp, which fails under --tmpfs noexec.
        # gzip is pure-Java and produces valid Parquet readable by DuckDB/Spark.
        .config("spark.sql.parquet.compression.codec", "gzip")
        .getOrCreate()
    )
