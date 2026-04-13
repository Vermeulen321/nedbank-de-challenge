FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies you need beyond the base image.
# Leave requirements.txt empty if the base packages are sufficient.
WORKDIR /app
ENV PYTHONPATH=/app
# The base image sets SPARK_HOME to dist-packages but pip uses site-packages.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
# Tell Spark to bind to loopback before the JVM starts.
# Required when running with --network=none (no DNS resolution available).
ENV SPARK_LOCAL_IP=127.0.0.1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Delta Lake JARs into PySpark's own jars directory during build
# (network is available at build time). At runtime (--network=none), Spark loads
# them directly from $SPARK_HOME/jars without any Ivy/Maven call.
RUN python - <<'EOF'
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .master("local")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
).getOrCreate()
spark.stop()
EOF
RUN cp /root/.ivy2/jars/*.jar ${SPARK_HOME}/jars/

# Copy pipeline code and configuration into the image.
# Do NOT copy data files or output directories — these are injected at runtime
# via Docker volume mounts by the scoring system.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — must run the complete pipeline end-to-end without interactive input.
# The scoring system uses this CMD directly; do not require TTY or stdin.
CMD ["python", "pipeline/run_all.py"]
