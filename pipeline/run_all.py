"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.

Development note: this pipeline was developed with the assistance of Claude
(https://claude.ai), an AI coding assistant made by Anthropic.
"""

import time

import yaml

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.stream_ingest import run_stream_ingestion

CONFIG_PATH = "/data/config/pipeline_config.yaml"
# dq_rules.yaml: prefer the scorer-mounted version; fall back to the copy
# baked into the image in case the scorer does not mount it explicitly.
DQ_RULES_PATHS = ["/data/config/dq_rules.yaml", "/app/config/dq_rules.yaml"]

if __name__ == "__main__":
    pipeline_start = time.time()

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    dq_rules = None
    for path in DQ_RULES_PATHS:
        try:
            with open(path) as f:
                dq_rules = yaml.safe_load(f)
            break
        except FileNotFoundError:
            continue
    if dq_rules is None:
        raise FileNotFoundError(f"dq_rules.yaml not found at any of: {DQ_RULES_PATHS}")

    source_counts = run_ingestion(config)
    dq_stats = run_transformation(config, dq_rules)
    run_provisioning(config, dq_rules, source_counts, dq_stats, pipeline_start)

    # Stage 3: streaming pass. Runs only when a `streaming:` block is
    # present in pipeline_config.yaml AND /data/stream/ exists. The same
    # entry point therefore works unchanged at Stage 1 and Stage 2.
    if config.get("streaming"):
        run_stream_ingestion(config)
