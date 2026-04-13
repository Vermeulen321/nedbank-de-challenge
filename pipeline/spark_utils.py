"""
Shared SparkSession factory.

Using getOrCreate() across all pipeline stages avoids the overhead of
tearing down and recreating a JVM between stages, and keeps peak memory
usage predictable within the 2 GB container limit.

Delta Lake JARs are pre-downloaded into $SPARK_HOME/jars/ during the Docker
build (see Dockerfile). At runtime they load automatically — no Ivy/Maven
download is attempted, which is required under --network=none.
"""

from pyspark.sql import SparkSession


def get_or_create_spark(config: dict) -> SparkSession:
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
        # Reduce shuffle partitions from the default 200 to save RAM.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
        # Snappy extracts a native .so to /tmp, which fails under --tmpfs noexec.
        # gzip is pure-Java and produces valid Parquet readable by DuckDB/Spark.
        .config("spark.sql.parquet.compression.codec", "gzip")
        .getOrCreate()
    )
